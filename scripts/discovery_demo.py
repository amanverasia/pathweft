#!/usr/bin/env python3
"""Discovery proof: three real Pathweft node processes on loopback, zero routes, only links.

A -- B -- C. A and C pin each other; B is a pure relay. No `routes` are configured or added.
"""
import argparse
import json
import re
from pathlib import Path
import socket
import subprocess
import tempfile
import time

import demo

ROOT = demo.ROOT


def free_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


class DiscoveryNetwork(demo.Network):
    def __init__(self, binary):
        self.binary = binary
        self.temp = tempfile.TemporaryDirectory(prefix="pw-")
        self.root = Path(self.temp.name)
        self.processes, self.logs, self.identities, self.addresses = {}, {}, {}, {}
        try:
            ports = dict(zip("ABC", free_ports(3)))
            self.addresses = {n: f"127.0.0.1:{ports[n]}" for n in "ABC"}
            for name in "ABC":
                self.identities[name] = self.cli("identity", "new", "--out", self.root / f"{name}.key")
            contacts = {"A": ["C"], "B": [], "C": ["A"]}
            links = {"A": ["B"], "B": ["A", "C"], "C": ["B"]}
            # Started back to back: a rate-limited reply hello is sent as soon as the 1 s limit
            # allows, so the last neighbour is not left waiting for the 30 s hello.
            for name in "ABC":
                config = {"name": name, "identity": f"{name}.key", "control_socket": f"{name}.sock",
                          "listen": self.addresses[name],
                          "contacts": [self.identities[n]["public_key"] for n in contacts[name]],
                          "links": [{"address": self.addresses[n], "kind": "lan"} for n in links[name]]}
                path = self.root / f"{name}.json"
                path.write_text(json.dumps(config))
                log = open(self.root / f"{name}.log", "w")
                self.logs[name] = log
                self.processes[name] = subprocess.Popen([str(binary), "node", "--config", str(path)],
                                                       stdout=log, stderr=subprocess.STDOUT)
                self.wait_for(name, lambda value: "listen" in value)
            for name, count in (("A", 1), ("B", 2), ("C", 1)):
                self.wait_for(name, lambda v, c=count: v["discovery"]["neighbours"] >= c)
            time.sleep(.4)  # let the far side finish its half of the hello exchange
        except BaseException:
            self.close()
            raise

    def send(self, source, destination, message, succeeds=True, timeout_ms=8000):
        return self.cli("send", "--socket", self.root / f"{source}.sock", "--to",
                        self.identities[destination]["id"], "--message", message,
                        "--timeout-ms", str(timeout_ms), succeeds=succeeds)

    def texts(self, name):
        rows = [json.loads(line) for line in self.log_text(name).splitlines() if line.startswith("{")]
        return [r["details"]["text"] for r in rows if r["event"] == "text_received"]


def demonstrate(binary):
    to_c, to_a = "discovery text A to C: café", "discovery text C to A: reply"
    with DiscoveryNetwork(binary) as net:
        status = net.request("A", {"command": "status"})
        assert status["routes"] == 0 and status["discovery"]["enabled"] is True
        reply = net.send("A", "C", to_c)
        assert reply["status"] == "delivered" and reply["recipient"] == net.identities["C"]["id"]
        assert reply["receipt_id"]
        assert net.texts("C") == [to_c]
        print("PASS: A -> C delivered over a discovered path with an authenticated receipt (zero routes).")
        reply = net.send("C", "A", to_a)
        assert reply["status"] == "delivered" and reply["receipt_id"]
        assert net.texts("A") == [to_a] and net.texts("C") == [to_c]
        print("PASS: C -> A delivered and receipted in the other direction.")

        b = net.request("B", {"command": "status"})
        assert b["forwarded"] >= 4 and b["delivered_texts"] == 0 and b["routes"] == 0, b
        log = net.log_text("B")
        assert "discovery text" not in log and "café" not in log and "text_received" not in log
        print(f"PASS: B relayed {b['forwarded']} hop frames and its log holds no plaintext.")

        uuid = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
        secrets = set()
        for n in "AC":
            secrets |= {net.identities[n]["id"], net.identities[n]["public_key"]}
            secrets |= set(uuid.findall(net.log_text(n)))
        assert len(secrets) >= 8, secrets  # two ids, two keys, texts and receipts
        relay_view = net.log_text("B") + json.dumps(b)
        leaked = [x for x in secrets if x in relay_view]
        assert not leaked, f"relay B saw {leaked}"
        assert '"forwarded"' in relay_view
        print(f"PASS: B's log and status hold none of A's or C's IDs or keys, or {len(secrets) - 4} message/receipt IDs.")

        net.stop("B")
        net.send("A", "C", "this must not be delivered", succeeds=False, timeout_ms=6000)
        assert net.texts("C") == [to_c]
        print("PASS: with B stopped, A -> C fails and nothing is delivered.")
    print("Discovery loopback proof complete. Real Bluetooth/LAN links and phones are separate tests.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="use an already built executable; skip Cargo build")
    args = parser.parse_args()
    if args.binary:
        binary = args.binary.resolve()
    else:
        subprocess.run(["cargo", "build", "--locked", "-p", "pathweft"], cwd=ROOT, check=True, timeout=600)
        binary = ROOT / "target/debug/pathweft"
    demonstrate(binary)
