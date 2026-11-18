#!/usr/bin/env python3
"""Prove offline pickup with three Pathweft processes over loopback."""
import argparse
import json
import socket
import socketserver
import struct
import subprocess
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path

from demo import MAX_FRAME, ROOT, Network, exact
from discovery_demo import free_ports


def read_frame(stream):
    header = exact(stream, 4)
    size = struct.unpack(">I", header)[0]
    assert 1 <= size <= MAX_FRAME
    return header + exact(stream, size)


class DropConfirmation(socketserver.StreamRequestHandler):
    """Forward the exchange up to the save acknowledgment, then close."""

    def handle(self):
        self.request.settimeout(4)
        try:
            with socket.create_connection(self.server.relay, timeout=4) as relay:
                relay.sendall(read_frame(self.request))  # hello
                self.request.sendall(read_frame(relay))  # challenge
                relay.sendall(read_frame(self.request))  # pickup
                self.request.sendall(read_frame(relay))  # copy
                read_frame(self.request)  # authenticated save acknowledgment
                self.server.dropped.set()
        except (OSError, RuntimeError):
            pass


class DropDepositReply(socketserver.StreamRequestHandler):
    """Let the relay finish storing before dropping the sender's reply."""

    def handle(self):
        self.request.settimeout(4)
        try:
            with socket.create_connection(self.server.relay, timeout=4) as relay:
                relay.sendall(read_frame(self.request))  # hello
                self.request.sendall(read_frame(relay))  # challenge
                relay.sendall(read_frame(self.request))  # deposit
                reply = read_frame(relay)
                if self.server.forward_replies.is_set():
                    self.request.sendall(reply)
                else:
                    self.server.dropped.set()
        except (OSError, RuntimeError):
            pass


@contextmanager
def intercept(address, handler):
    host, port = address.split(":")
    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), handler) as proxy:
        proxy.daemon_threads = True
        proxy.relay = (host, int(port))
        proxy.address = f"127.0.0.1:{proxy.server_address[1]}"
        proxy.dropped = threading.Event()
        proxy.forward_replies = threading.Event()
        worker = threading.Thread(target=proxy.serve_forever)
        worker.start()
        try:
            yield proxy
        finally:
            proxy.shutdown()
            worker.join()


class MailboxNetwork(Network):
    def __init__(self, binary):
        self.binary = binary
        self.temp = tempfile.TemporaryDirectory(prefix="pw-")
        self.root = Path(self.temp.name)
        self.processes, self.logs, self.identities = {}, {}, {}
        self.addresses = {node: f"127.0.0.1:{port}" for node, port in zip("ABC", free_ports(3))}
        try:
            for name in ("A", "B", "C", "pickup", "wrong"):
                self.identities[name] = self.cli("identity", "new", "--out", self.root / f"{name}.key")
            self.label = self.cli("mailbox", "new-label")["label"]
            self.start("A", contacts=[self.identities["C"]["public_key"]])
            self.start("B", mailbox={"trusted_wall_clock": True})
            self.relay = {
                "address": self.addresses["B"], "public_key": self.identities["B"]["public_key"],
            }
            self.cli("mailbox", "register", "--socket", self.root / "B.sock", "--label", self.label,
                     "--public-key", self.identities["pickup"]["public_key"])
        except BaseException:
            self.close()
            raise

    def start(self, name, **settings):
        if name in self.logs:
            self.logs[name].close()
        config = {
            "name": name, "identity": f"{name}.key", "control_socket": f"{name}.sock",
            "listen": self.addresses[name], **settings,
        }
        path = self.root / f"{name}.json"
        path.write_text(json.dumps(config))
        log = (self.root / f"{name}.log").open("a")
        self.logs[name] = log
        self.processes[name] = subprocess.Popen(
            [str(self.binary), "node", "--config", str(path)], stdout=log, stderr=subprocess.STDOUT,
        )
        self.wait_for(name, lambda value: "listen" in value)

    def start_recipient(self, identity="pickup", address=None, pin=True, receipt_route=False):
        relay = {**self.relay, "address": address or self.addresses["B"]}
        self.start(
            "C",
            pickup={"relay": relay, "identity": f"{identity}.key", "labels": [self.label]},
            contacts=[self.identities["A"]["public_key"]] if pin else [],
            routes=[{"destination": self.identities["A"]["id"], "via": self.addresses["A"]}]
            if receipt_route else [],
        )

    def start_sender(self):
        self.start("A", contacts=[self.identities["C"]["public_key"]])

    def crash(self, name):
        process = self.processes[name]
        process.kill()
        process.wait(timeout=3)

    def relay_status(self, stored, retired):
        return self.wait_for("B", lambda status: (
            status["mailbox"]["stored"] == stored and status["mailbox"]["retired"] == retired
        ))

    def outgoing(self, message_id):
        entries = self.request("A", {"command": "outbox"})["entries"]
        return next(entry for entry in entries if entry["message_id"] == message_id)


def demonstrate(binary):
    message = "sealed for an offline recipient — café"
    with MailboxNetwork(binary) as net:
        state_file = net.root / "B.sock.mailbox.json"
        with intercept(net.addresses["B"], DropDepositReply) as proxy:
            queued = net.cli(
                "mailbox", "queue", "--socket", net.root / "A.sock", "--label", net.label,
                "--relay-address", proxy.address, "--relay-key", net.relay["public_key"],
                "--to", net.identities["C"]["id"], "--message", message,
            )
            message_id = queued["message_id"]
            assert queued["expires_at_ms"] - queued["created_at_ms"] == 86_400_000
            assert proxy.dropped.wait(5), "relay never replied to the deposit"
            net.relay_status(stored=1, retired=0)
            assert "C" not in net.processes
            assert net.outgoing(message_id)["mailbox"]["relay_result"] is None
            print("PASS: A deposited a sealed copy while C was offline, and the relay's reply was dropped.")

            persisted = state_file.read_text()
            record = json.loads(persisted)["records"][0]
            assert record["copy"]["hops_after_deposit"] == 7
            net.crash("A")
            net.crash("B")
            net.start("B", mailbox={"trusted_wall_clock": True})
            net.relay_status(stored=1, retired=0)
            proxy.forward_replies.set()
            net.start_sender()
            net.wait_for("A", lambda _: net.outgoing(message_id)["mailbox"]["relay_result"] == "stored")
            assert net.outgoing(message_id)["state"] == "unconfirmed"
            assert net.outgoing(message_id)["attempts"] >= 2
            assert json.loads(state_file.read_text())["records"] == [record]
            print("PASS: sender and relay restarted. The retry reused the copy ID, sealed bytes and deadline.")

        rejected = net.request("B", {"command": "status"})["rejected"]
        net.start_recipient(identity="wrong")
        net.wait_for("B", lambda value: value["rejected"] > rejected)
        assert net.request("C", {"command": "inbox"})["entries"] == []
        net.stop("C")
        net.relay_status(stored=1, retired=0)
        print("PASS: knowing the deposit label did not grant pickup access.")

        rejected = net.request("B", {"command": "status"})["rejected"]
        net.start_recipient(pin=False)
        net.wait_for("B", lambda value: value["rejected"] > rejected)
        assert net.request("C", {"command": "inbox"})["entries"] == []
        net.stop("C")
        net.relay_status(stored=1, retired=0)
        print("PASS: C declined to save text from an unpinned sender. The relay kept its copy.")

        with intercept(net.addresses["B"], DropConfirmation) as proxy:
            net.start_recipient(address=proxy.address)
            assert proxy.dropped.wait(5), "recipient never sent a save acknowledgment"
            net.relay_status(stored=1, retired=0)
            saved = net.request("C", {"command": "inbox_read", "message_id": message_id})
            assert saved["text"] == message
            assert net.outgoing(message_id)["state"] == "unconfirmed"
            net.crash("C")
        print("PASS: automatic pickup saved the message even though the save acknowledgment was lost.")

        net.start_recipient(receipt_route=True)
        net.relay_status(stored=0, retired=1)
        net.wait_for("A", lambda _: net.outgoing(message_id)["state"] == "delivered")
        assert net.outgoing(message_id)["receipt_id"]
        assert len(net.request("C", {"command": "inbox"})["entries"]) == 1
        assert net.request("C", {"command": "status"})["duplicates"] == 1
        assert net.log_text("C").count('"event":"text_received"') == 1
        assert json.loads(state_file.read_text())["records"][0]["copy"] is None
        print("PASS: C re-picked up the same copy after restart, saved no duplicate, and confirmed deletion.")
        print("PASS: A marked delivery only after receiving C's end-to-end receipt on a return path.")

        relay_view = persisted + state_file.read_text() + net.log_text("B")
        for value in (message, message_id, net.identities["A"]["id"], net.identities["C"]["id"],
                      net.identities["A"]["public_key"], net.identities["C"]["public_key"]):
            assert value not in relay_view
        print("PASS: relay storage and logs contained neither message text nor personal endpoint IDs or keys.")


class DiscoveredMailboxNetwork(MailboxNetwork):
    def __init__(self, binary):
        self.binary = binary
        self.temp = tempfile.TemporaryDirectory(prefix="pw-")
        self.root = Path(self.temp.name)
        self.processes, self.logs, self.identities = {}, {}, {}
        topology = "AXBYC"
        self.addresses = {
            node: f"127.0.0.1:{port}" for node, port in zip(topology, free_ports(len(topology)))
        }
        self.links = {
            node: [
                {"address": self.addresses[peer], "kind": "lan"}
                for peer in topology[max(0, at - 1):at + 2] if peer != node
            ]
            for at, node in enumerate(topology)
        }
        try:
            for name in (*topology, "sender-route", "pickup"):
                self.identities[name] = self.cli("identity", "new", "--out", self.root / f"{name}.key")
            self.label = self.cli("mailbox", "new-label")["label"]
            self.relay = {"public_key": self.identities["B"]["public_key"]}
            self.start("X", links=self.links["X"])
            self.start("Y", links=self.links["Y"])
            self.start_relay()
            self.start("A", links=self.links["A"], contacts=[self.identities["C"]["public_key"]],
                       mailbox_route={"relay_key": self.relay["public_key"], "identity": "sender-route.key"})
            self.cli("mailbox", "register", "--socket", self.root / "B.sock", "--label", self.label,
                     "--public-key", self.identities["pickup"]["public_key"])
            self.wait_for("A", lambda status: status["discovery"]["neighbours"] == 1)
        except BaseException:
            self.close()
            raise

    def start_relay(self):
        self.start("B", links=self.links["B"], mailbox={"trusted_wall_clock": True},
                   mailbox_route_contacts=[self.identities[key]["public_key"] for key in ("sender-route", "pickup")])

    def start_discovered_recipient(self):
        self.start("C", links=self.links["C"], contacts=[self.identities["A"]["public_key"]],
                   mailbox_route={"relay_key": self.relay["public_key"], "identity": "pickup.key"},
                   pickup={"relay": self.relay, "identity": "pickup.key", "labels": [self.label]})


def demonstrate_discovery(binary):
    message = "offline delivery over four discovered hops"
    with DiscoveredMailboxNetwork(binary) as net:
        queued = net.cli("mailbox", "queue", "--socket", net.root / "A.sock", "--label", net.label,
                         "--relay-key", net.relay["public_key"], "--to", net.identities["C"]["id"],
                         "--message", message)
        identifier = queued["message_id"]
        net.wait_for("A", lambda _: net.outgoing(identifier)["mailbox"]["relay_result"] == "stored")
        assert net.outgoing(identifier)["state"] == "unconfirmed"
        assert "C" not in net.processes
        state_file = net.root / "B.sock.mailbox.json"
        persisted = state_file.read_text()
        record = json.loads(persisted)["records"][0]
        assert record["deposit_hops"] == 2
        assert net.request("X", {"command": "status"})["forwarded"] > 0
        print("PASS: A discovered B through X and deposited over two handoffs while C was offline.")

        net.crash("B")
        net.start_relay()
        net.relay_status(stored=1, retired=0)
        net.start_discovered_recipient()
        net.relay_status(stored=0, retired=1)
        assert net.request("C", {"command": "inbox_read", "message_id": identifier})["text"] == message
        inbox = json.loads((net.root / "C.sock.inbox.json").read_text())
        assert len(inbox["messages"]) == 1
        assert inbox["messages"][0]["packet"]["remaining_hops"] == 4
        assert net.request("Y", {"command": "status"})["forwarded"] > 0
        print("PASS: B restarted and C collected through Y. Four handoffs consumed four of eight hops.")

        net.wait_for("A", lambda _: net.outgoing(identifier)["state"] == "delivered")
        for name in "AXBYC":
            assert net.request(name, {"command": "status"})["routes"] == 0
        print("PASS: A received C's delivery receipt over a discovered path with zero manual routes.")

        relay_view = persisted + state_file.read_text() + "".join(net.log_text(n) for n in "XBY")
        for value in (message, identifier, *(net.identities[n][key] for n in "AC" for key in ("id", "public_key"))):
            assert value not in relay_view
        print("PASS: the mailbox host and both intermediate relays logged no endpoint IDs, keys or text.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="use an already built executable")
    parser.add_argument("--discovered", action="store_true", help="test five nodes with discovered paths")
    args = parser.parse_args()
    if args.binary:
        binary = args.binary.resolve()
    else:
        subprocess.run(["cargo", "build", "--locked", "-p", "pathweft"],
                       cwd=ROOT, check=True, timeout=600)
        binary = ROOT / "target/debug/pathweft"
    (demonstrate_discovery if args.discovered else demonstrate)(binary)
