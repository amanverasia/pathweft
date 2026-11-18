#!/usr/bin/env python3
"""Reproducible desktop proof, using three real Pathweft node processes."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent
MAX_FRAME = 32768


def exact(stream, length):
    data = bytearray()
    while len(data) < length:
        block = stream.recv(length - len(data))
        if not block:
            raise RuntimeError("connection closed before the frame was complete")
        data.extend(block)
    return bytes(data)


def write_frame(stream, value):
    data = json.dumps(value).encode()
    assert len(data) <= MAX_FRAME
    stream.sendall(struct.pack(">I", len(data)) + data)


class Network:
    def __init__(self, binary, relay=True):
        self.binary = binary
        self.temp = tempfile.TemporaryDirectory(prefix="pw-")
        self.root = Path(self.temp.name)
        self.processes, self.logs, self.identities, self.addresses = {}, {}, {}, {}
        try:
            for name in "ABC":
                self.identities[name] = self.cli("identity", "new", "--out", self.root / f"{name}.key")
            for name in "ABC":
                config = {"name": name, "identity": f"{name}.key", "control_socket": f"{name}.sock",
                          "listen": "127.0.0.1:0", "contacts": [self.identities[n]["public_key"] for n in "ABC" if n != name]}
                if name == "B":
                    config["relay"] = relay
                path = self.root / f"{name}.json"
                path.write_text(json.dumps(config))
                log = open(self.root / f"{name}.log", "w")
                self.logs[name] = log
                self.processes[name] = subprocess.Popen([str(binary), "node", "--config", str(path)],
                                                       stdout=log, stderr=subprocess.STDOUT)
                status = self.wait_for(name, lambda value: "listen" in value)
                self.addresses[name] = status["listen"]
            # No direct A–C Pathweft route: each endpoint's only next hop is B.
            for source, destination, via in [("A", "B", "B"), ("A", "C", "B"),
                                             ("B", "A", "A"), ("B", "C", "C"),
                                             ("C", "A", "B"), ("C", "B", "B")]:
                self.cli("route", "--socket", self.root / f"{source}.sock", "--to",
                         self.identities[destination]["id"], "--via", self.addresses[via])
        except BaseException:
            self.close()
            raise

    def cli(self, *args, succeeds=True):
        result = subprocess.run([str(self.binary), *map(str, args)], capture_output=True, text=True, timeout=15)
        if succeeds:
            if result.returncode:
                raise AssertionError(f"CLI failed: {result.stderr}")
            return json.loads(result.stdout)
        assert result.returncode != 0, "command incorrectly reported successful delivery"
        return result

    def request(self, name, request):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
            stream.settimeout(3)
            stream.connect(str(self.root / f"{name}.sock"))
            write_frame(stream, request)
            length = struct.unpack(">I", exact(stream, 4))[0]
            assert 1 <= length <= MAX_FRAME
            reply = json.loads(exact(stream, length))
            assert reply["ok"], reply
            return reply["result"]

    def wait_for(self, name, predicate):
        deadline = time.monotonic() + 5
        last = None
        while time.monotonic() < deadline:
            if self.processes[name].poll() is not None:
                raise AssertionError(f"node {name} exited: {self.log_text(name)}")
            try:
                last = self.request(name, {"command": "status"})
                if predicate(last):
                    return last
            except (OSError, RuntimeError):
                pass
            time.sleep(.02)
        raise AssertionError(f"node {name} did not reach expected state: {last}; {self.log_text(name)}")

    def send(self, source, destination, message, succeeds=True):
        return self.cli("send", "--socket", self.root / f"{source}.sock", "--to",
                        self.identities[destination]["id"], "--message", message,
                        "--timeout-ms", "500", succeeds=succeeds)

    def inject(self, name, packet=None, oversized=False):
        host, port = self.addresses[name].rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=2) as stream:
            if oversized:
                stream.sendall(struct.pack(">I", MAX_FRAME + 1))
            else:
                write_frame(stream, packet)

    def log_text(self, name):
        self.logs[name].flush()
        return (self.root / f"{name}.log").read_text()

    def stop(self, name):
        process = self.processes[name]
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)

    def close(self):
        for name in self.processes:
            self.stop(name)
        for log in self.logs.values():
            log.close()
        self.temp.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        if kind:
            for name in self.logs:
                print(f"--- node {name} diagnostics ---\n{self.log_text(name)}")
        self.close()


def demonstrate(binary):
    marker = 'private demo text: "café"\nsecond line'
    with Network(binary) as network:
        reply = network.send("A", "C", marker)
        assert reply["status"] == "delivered"
        assert reply["recipient"] == network.identities["C"]["id"]
        network.wait_for("B", lambda s: s["forwarded"] >= 2)
        assert network.request("C", {"command": "status"})["delivered_texts"] == 1
        received = [json.loads(line) for line in network.log_text("C").splitlines()]
        assert any(row["event"] == "text_received" and row["details"]["text"] == marker for row in received)
        assert "private demo text" not in network.log_text("B")
        assert all(os.stat(network.root / f"{n}.key").st_mode & 0o077 == 0 for n in "ABC")
        network.cli("identity", "new", "--out", network.root / "A.key", succeeds=False)
        print("PASS: A -> B -> C delivered text; C's encrypted receipt returned via B.")
        print("PASS: B's logs contain forwarding metadata and no private message text.")

        # A valid-sized but unauthenticated packet must not become an application message.
        invalid = {"version": 1, "id": str(uuid.uuid4()), "source": network.identities["A"]["id"],
                   "destination": network.identities["C"]["id"], "remaining_hops": 8,
                   "nonce": [0] * 24, "ciphertext": [0] * 16}
        network.inject("B", invalid)
        network.wait_for("C", lambda s: s["rejected"] >= 1)
        network.inject("B", invalid)
        network.wait_for("B", lambda s: s["duplicates"] >= 1)
        network.inject("B", oversized=True)
        network.wait_for("B", lambda s: s["rejected"] >= 1)
        assert network.request("C", {"command": "status"})["delivered_texts"] == 1
        print("PASS: invalid ciphertext, duplicate packets, and oversized frames were rejected.")

        network.stop("B")
        assert not (network.root / "B.sock").exists(), "B left its control socket behind"
        network.send("A", "C", "this must not be delivered", succeeds=False)
        assert network.request("C", {"command": "status"})["delivered_texts"] == 1
        print("PASS: stopping B prevents A -> C delivery; the sender reports failure.")

    with Network(binary, relay=False) as network:
        assert network.request("A", {"command": "status"})["relay"] is True
        network.send("A", "C", "transit is disabled", succeeds=False)
        assert network.request("C", {"command": "status"})["delivered_texts"] == 0
        assert network.request("B", {"command": "status"})["relay"] is False
        reply = network.send("B", "C", "the relay owner's own message")
        assert reply["status"] == "delivered"
        print("PASS: relay opt-out blocks transit while preserving B's own messaging.")

    print("Desktop development proof complete. Android, internet-isolation and router-free radio validation require separate tests.")


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
