#!/usr/bin/env python3
"""Local process proof of persisted outgoing text and conservative crash recovery."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import platform
from pathlib import Path
import socket
import struct
import subprocess
import time

from demo import Network, exact


def entry(network, message_id):
    rows = network.cli("outbox", "--socket", network.root / "A.sock", "list")["entries"]
    return next(row for row in rows if row["message_id"] == message_id)


def wait_state(network, message_id, state):
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        current = entry(network, message_id)
        if current["state"] == state:
            return current
        time.sleep(.03)
    raise AssertionError(f"message did not reach {state}: {current}")


def restart(network, name):
    assert network.processes[name].poll() is not None
    network.processes[name] = subprocess.Popen(
        [str(network.binary), "node", "--config", str(network.root / f"{name}.json")],
        stdout=network.logs[name], stderr=subprocess.STDOUT)
    network.addresses[name] = network.wait_for(name, lambda row: "listen" in row)["listen"]


def wait_state_long(network, message_id, state, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        current = entry(network, message_id)
        if current["state"] == state:
            return current
        time.sleep(.2)
    raise AssertionError(f"message did not reach {state}: {current}")


def route(network, source, destination, address):
    network.cli("route", "--socket", network.root / f"{source}.sock", "--to",
                network.identities[destination]["id"], "--via", address)


def queue(network, message, ttl=60):
    return network.cli("queue", "--socket", network.root / "A.sock", "--to",
                       network.identities["C"]["id"], "--message", message,
                       "--ttl-seconds", str(ttl))["message_id"]


def demonstrate(binary, checks):
    def passed(check):
        checks.append(check)
        print(f"PASS: {check}")

    with Network(binary) as network:
        network.stop("B")
        marker = "🌍" * 128  # Exact maximum 512 UTF-8 bytes.
        message_id = queue(network, marker)
        assert entry(network, message_id)["state"] == "queued"
        persisted = network.root / "A.sock.outbox.json"
        assert persisted.stat().st_mode & 0o777 == 0o600
        assert marker not in persisted.read_text()
        packet = json.loads(persisted.read_text())["entries"][0]["packet"]
        passed("encrypted queued text is persisted privately while the relay is down")

        network.processes["A"].kill()
        network.processes["A"].wait(timeout=3)
        assert (network.root / "A.sock").exists()
        restart(network, "A")
        assert entry(network, message_id)["state"] == "queued"
        assert json.loads(persisted.read_text())["entries"][0]["packet"] == packet
        passed("SIGKILL restart reclaims the stale socket and retains the same sealed envelope")

        restart(network, "B")
        # Restore the return/transit paths before allowing A's queued text out.
        for source, destination, via in [("B", "A", "A"), ("B", "C", "C"),
                                         ("C", "A", "B"), ("C", "B", "B"),
                                         ("A", "B", "B"), ("A", "C", "B")]:
            route(network, source, destination, network.addresses[via])
        delivered = wait_state(network, message_id, "delivered")
        assert delivered["receipt_id"]
        assert network.request("C", {"command": "status"})["delivered_texts"] == 1
        assert network.request("B", {"command": "status"})["forwarded"] >= 2
        rows = [json.loads(row) for row in network.log_text("C").splitlines()]
        assert any(row["event"] == "text_received" and row["details"]["text"] == marker for row in rows)
        assert marker not in network.log_text("B")
        passed("recovered queued text crosses A-B-C once and persists an authenticated receipt")

        network.processes["A"].kill()
        network.processes["A"].wait(timeout=3)
        restart(network, "A")
        assert entry(network, message_id)["state"] == "delivered"
        time.sleep(.4)
        assert network.request("C", {"command": "status"})["delivered_texts"] == 1
        passed("delivered history survives another crash without resending")

        cancelled = queue(network, "cancel before a route is restored")
        network.cli("outbox", "--socket", network.root / "A.sock", "cancel", "--message-id", cancelled)
        expired = queue(network, "expire before a route is restored", ttl=1)
        wait_state(network, expired, "expired")
        route(network, "A", "C", network.addresses["B"])
        time.sleep(.4)
        assert entry(network, cancelled)["state"] == "cancelled"
        assert network.request("C", {"command": "status"})["delivered_texts"] == 1
        network.cli("outbox", "--socket", network.root / "A.sock", "forget", "--message-id", cancelled)
        passed("cancelled and expired records never transmit when a route later returns")

        with socket.socket() as sink:
            sink.bind(("127.0.0.1", 0))
            sink.listen(2)
            sink.settimeout(4)
            via = f"127.0.0.1:{sink.getsockname()[1]}"
            route(network, "A", "C", via)
            uncertain = queue(network, "receipt deliberately withheld")
            radio, _ = sink.accept()
            with radio:
                radio.settimeout(2)
                length = struct.unpack(">I", exact(radio, 4))[0]
                assert 1 <= length <= 32768
                received = json.loads(exact(radio, length))
                assert received["id"] == uncertain
            wait_state(network, uncertain, "in_flight")
            # Preserve the test sink in on-disk configuration so the restarted
            # worker's first recovery attempt has a route immediately.
            config_path = network.root / "A.json"
            config = json.loads(config_path.read_text())
            config["routes"] = [{"destination": network.identities["C"]["id"], "via": via}]
            config_path.write_text(json.dumps(config))
            network.processes["A"].kill()
            network.processes["A"].wait(timeout=3)
            restart(network, "A")
            assert entry(network, uncertain)["state"] == "unconfirmed"
            route(network, "B", "A", network.addresses["A"])
            route(network, "A", "C", via)
            sink.settimeout(4)
            query, _ = sink.accept()
            with query:
                query.settimeout(2)
                length = struct.unpack(">I", exact(query, 4))[0]
                assert 1 <= length <= 32768
                request = json.loads(exact(query, length))
            assert request["id"] != uncertain
            # Inject the first recovery packet, then restore the real relay path.
            # Normally this is the receipt query; a slow test host can already have
            # advanced to redelivery, which is also a valid recovery result.
            network.inject("C", request)
            route(network, "A", "C", network.addresses["B"])
            confirmed = wait_state_long(network, uncertain, "delivered", 20)
            assert confirmed["receipt_queries"] >= 1
            assert confirmed["redeliveries"] >= 1
            rows = [json.loads(row) for row in network.log_text("C").splitlines()]
            assert any(row["event"] == "redelivery_received"
                       and row["details"]["message_id"] == uncertain for row in rows)
            assert sum(1 for row in rows if row["event"] == "text_received"
                       and row["details"]["text"] == "receipt deliberately withheld") == 1
            assert "receipt deliberately withheld" not in network.log_text("B")
            passed("crash after write queries for a receipt, then redelivers lost text exactly once")
        # Lost receipt, answered later from the recipient's durable inbox.
        route(network, "A", "C", network.addresses["B"])
        with socket.socket() as dead:
            dead.bind(("127.0.0.1", 0))
            lost_return = f"127.0.0.1:{dead.getsockname()[1]}"  # closed after this block: connections refused
        route(network, "C", "A", lost_return)
        recovered_text = "receipt lost, then recovered from the saved inbox"
        recovered = queue(network, recovered_text)
        def c_rows():
            return [json.loads(row) for row in network.log_text("C").splitlines()]
        deadline = time.monotonic() + 6
        while not any(row["event"] == "text_received" and row["details"]["text"] == recovered_text for row in c_rows()):
            assert time.monotonic() < deadline, "C did not receive the text"
            time.sleep(.03)
        time.sleep(.5)
        assert entry(network, recovered)["state"] in ("in_flight", "unconfirmed")
        inbox_ids = [row["message_id"] for row in network.cli("inbox", "--socket", network.root / "C.sock", "list")["entries"]]
        assert recovered in inbox_ids
        # Restart the recipient: the answer must come from durable state, not memory.
        network.processes["C"].kill()
        network.processes["C"].wait(timeout=3)
        restart(network, "C")
        # A and C have both restarted, so refresh every route that names their ports.
        for source, destination, via in [("C", "A", "B"), ("C", "B", "B"), ("B", "C", "C"), ("B", "A", "A")]:
            route(network, source, destination, network.addresses[via])
        try:
            confirmed = wait_state_long(network, recovered, "delivered", 40)
        except AssertionError:
            for name in "CBA":
                interesting = [row for row in network.log_text(name).splitlines()
                               if any(k in row for k in ("receipt", "dropped", "rejected", "error", "forwarded", "ready"))]
                print(f"--- {name} ---", *interesting[-12:], sep="\n")
            raise
        assert confirmed["receipt_id"]
        rows = c_rows()
        assert sum(1 for row in rows if row["event"] == "text_received" and row["details"]["text"] == recovered_text) == 1
        assert any(row["event"] == "receipt_requested" and row["details"]["message_id"] == recovered
                   and row["details"]["known"] is True for row in rows)
        assert recovered_text not in network.log_text("B")
        passed("lost receipt is recovered after recipient restart, from its saved inbox, with the text shown once")

        # The original text arrived, but every receipt path is broken. Keep the
        # recipient down through the first query so recovery must redeliver the
        # saved original; its durable inbox must suppress a second text event.
        with socket.socket() as dead:
            dead.bind(("127.0.0.1", 0))
            lost_return = f"127.0.0.1:{dead.getsockname()[1]}"
        route(network, "C", "A", lost_return)
        duplicate_text = "saved text receives a duplicate redelivery, shown once"
        duplicate = queue(network, duplicate_text)
        deadline = time.monotonic() + 6
        while not any(row["event"] == "text_received" and row["details"]["text"] == duplicate_text
                      for row in c_rows()):
            assert time.monotonic() < deadline, "C did not save the original text"
            time.sleep(.03)
        network.processes["C"].kill()
        network.processes["C"].wait(timeout=3)
        deadline = time.monotonic() + 15
        while entry(network, duplicate)["receipt_queries"] < 1:
            assert time.monotonic() < deadline, "A did not query for the missing receipt"
            time.sleep(.1)
        restart(network, "C")
        for source, destination, via in [("C", "A", "B"), ("C", "B", "B"),
                                         ("B", "C", "C"), ("B", "A", "A")]:
            route(network, source, destination, network.addresses[via])
        confirmed = wait_state_long(network, duplicate, "delivered", 40)
        assert confirmed["redeliveries"] >= 1
        rows = c_rows()
        assert sum(1 for row in rows if row["event"] == "text_received"
                   and row["details"]["text"] == duplicate_text) == 1
        assert any(row["event"] == "redelivery_received"
                   and row["details"]["message_id"] == duplicate for row in rows)
        assert network.request("C", {"command": "status"})["duplicates"] >= 1
        assert duplicate_text not in network.log_text("B")
        passed("redelivery after a lost receipt crosses the relay but does not show saved text twice")

        network.stop("A")
        assert not (network.root / "A.sock").exists()
        assert persisted.exists()
        passed("graceful stop removes the control socket while retaining durable history")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    checks = []
    report = {"test": "local-process-durable-outbox", "passed": False,
              "utc": datetime.now(timezone.utc).isoformat(), "platform": platform.system(),
              "machine": platform.machine(), "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              "checks": checks, "hardware_tested": False,
              "limits": "Local TCP/process SIGKILL proof; no radio, Android OS or power-loss guarantee"}
    try:
        demonstrate(args.binary.resolve(), checks)
        report["passed"] = True
    except BaseException as error:
        report["error"] = str(error)
        raise
    finally:
        if args.report:
            args.report.write_text(json.dumps(report, indent=2) + "\n")
