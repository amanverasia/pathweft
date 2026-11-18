#!/usr/bin/env python3
"""Run the configured A-B-C proof on three hosts with prebuilt Pathweft binaries."""
import argparse
from datetime import datetime, timezone
import ipaddress
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time

# Runs locally or over SSH. Private keys never leave their host. Only synthetic
# test text and public identity/status information return to the coordinator.
HELPER = r'''
import json, os, platform, shutil, signal, subprocess, sys, tempfile, time
from pathlib import Path

request = json.load(sys.stdin)
op = request["op"]

def owned_process(root):
    pid_file = root / "pid"
    if not pid_file.exists():
        return None
    pid = int(pid_file.read_text())
    result = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True)
    # Refuse to signal a reused PID or an unrelated process.
    if result.returncode or str(root / "node.json") not in result.stdout:
        return None
    return pid

def stop(root):
    pid = owned_process(root)
    forced = False
    if pid:
        os.kill(pid, signal.SIGINT)
        deadline = time.monotonic() + 5
        while owned_process(root) and time.monotonic() < deadline:
            time.sleep(.05)
        if owned_process(root):
            forced = True
            os.kill(pid, signal.SIGKILL)
    return {"forced": forced, "socket_removed": not (root / "control.sock").exists()}

if op == "create":
    binary = str(Path(request["binary"]).resolve(strict=True))
    version = subprocess.run([binary, "--version"], capture_output=True, text=True, check=True, timeout=5)
    root = Path(tempfile.mkdtemp(prefix="pw-net-", dir="/tmp"))
    result = {"root": str(root), "hostname": platform.node(), "os": platform.system(),
              "architecture": platform.machine(), "version": version.stdout.strip()}
elif op == "run":
    completed = subprocess.run([request["binary"], *request["args"]], capture_output=True, text=True, timeout=20)
    result = {"code": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr}
else:
    root = Path(request["root"])
    if root.parent != Path("/tmp") or not root.name.startswith("pw-net-"):
        raise RuntimeError("refusing a runtime directory outside this test's /tmp prefix")
    if op == "start":
        if owned_process(root):
            raise RuntimeError("this test node is already running")
        config_path = root / "node.json"
        config_path.write_text(json.dumps(request["config"]))
        with (root / "node.log").open("a") as log:
            process = subprocess.Popen([request["binary"], "node", "--config", str(config_path)],
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=False)
        (root / "pid").write_text(str(process.pid))
        # Keep the SSH/Python parent attached for the node's full lifetime. On
        # macOS this preserves the documented SSH command-line launch context.
        print(json.dumps({"pid": process.pid}), flush=True)
        raise SystemExit(process.wait())
    elif op == "stop":
        result = stop(root)
    elif op == "log":
        result = {"log": (root / "node.log").read_text() if (root / "node.log").exists() else ""}
    elif op == "destroy":
        result = stop(root)
        shutil.rmtree(root)
    else:
        raise RuntimeError("unknown operation")
print(json.dumps(result))
'''


def require(condition, message):
    if not condition:
        raise AssertionError(message)


class Node:
    def __init__(self, spec):
        self.spec = spec
        self.name = spec["name"]
        self.binary = spec["binary"]
        require(Path(self.binary).is_absolute(), "binary paths must be absolute on their host")
        self.address = str(ipaddress.IPv4Address(spec["address"]))
        self.ssh = spec.get("ssh")
        if self.ssh:
            require(not self.ssh.startswith("-"), "invalid SSH target")
        self.root = None
        self.info = None
        self.identity = None
        self.status = None
        self.manager = None

    def command(self, helper=HELPER):
        if self.ssh:
            command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                       "-o", "ConnectionAttempts=1", "-o", "StrictHostKeyChecking=yes"]
            if self.spec.get("identity_file"):
                command += ["-i", self.spec["identity_file"], "-o", "IdentitiesOnly=yes"]
            if self.spec.get("known_hosts_file"):
                command += ["-o", "UserKnownHostsFile=" + self.spec["known_hosts_file"]]
            command += [self.ssh, shlex.join(["python3", "-c", helper])]
        else:
            command = [sys.executable, "-c", helper]
        return command

    def payload(self, op, **kwargs):
        result = {"op": op, "binary": self.binary, **kwargs}
        if self.root:
            result["root"] = self.root
        return json.dumps(result)

    def call(self, op, **kwargs):
        result = subprocess.run(self.command(), input=self.payload(op, **kwargs), capture_output=True,
                                text=True, timeout=35)
        require(result.returncode == 0, f"{self.name} {op} failed: {result.stderr}")
        return json.loads(result.stdout)

    def cli(self, *args, succeeds=True):
        result = self.call("run", args=list(args))
        if succeeds:
            require(result["code"] == 0, f"{self.name} CLI failed: {result['stderr']}")
            return json.loads(result["stdout"])
        require(result["code"] != 0, f"{self.name} unexpectedly reported successful delivery")
        return result

    def prepare(self):
        self.info = self.call("create")
        self.root = self.info["root"]
        self.identity = self.cli("identity", "new", "--out", self.root + "/identity.key")
        print(f"{self.name}: {self.info['hostname']} / {self.info['os']} {self.info['architecture']}", flush=True)

    def start(self, nodes, relay=None):
        config = {"name": self.name, "identity": "identity.key", "control_socket": "control.sock",
                  "listen": self.address + ":0",
                  "contacts": [node.identity["public_key"] for node in nodes if node != self]}
        if relay is not None:
            config["relay"] = relay
        self.manager = subprocess.Popen(self.command(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True)
        self.manager.stdin.write(self.payload("start", config=config))
        self.manager.stdin.close()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.manager.poll() is not None:
                raise RuntimeError(f"{self.name} launcher exited: {self.manager.stderr.read()}")
            response = self.call("run", args=["status", "--socket", self.root + "/control.sock"])
            if response["code"] == 0:
                self.status = json.loads(response["stdout"])
                require(self.status["listen"].rsplit(":", 1)[0] == self.address,
                        "node did not bind the requested interface")
                return
            time.sleep(.05)
        raise RuntimeError(f"{self.name} failed to start: {self.call('log')['log']}")

    def inspect(self):
        return self.cli("status", "--socket", self.root + "/control.sock")

    def route(self, destination, via):
        return self.cli("route", "--socket", self.root + "/control.sock", "--to",
                        destination.identity["id"], "--via", via.status["listen"])

    def send(self, destination, message, succeeds=True):
        result = self.cli("send", "--socket", self.root + "/control.sock", "--to",
                          destination.identity["id"], "--message", message,
                          "--timeout-ms", "3000" if succeeds else "700", succeeds=succeeds)
        if succeeds:
            require(result["status"] == "delivered" and result["recipient"] == destination.identity["id"],
                    "delivery receipt does not match the intended recipient")
        return result

    def stop(self):
        result = self.call("stop")
        self.finish_manager()
        require(not result["forced"] and result["socket_removed"],
                f"{self.name} did not shut down cleanly: {result}")

    def finish_manager(self):
        if self.manager:
            self.manager.wait(timeout=10)
            self.manager.stdout.close()
            self.manager.stderr.close()
            self.manager = None

    def cleanup(self):
        if self.root:
            self.call("destroy")
            self.finish_manager()
            self.root = None


def configure(a, b, c):
    for source, destination, via in [(a, b, b), (a, c, b), (b, a, a),
                                     (b, c, c), (c, a, b), (c, b, b)]:
        source.route(destination, via)


def demonstrate(specs, node_factory=Node):
    require(len(specs) == 3 and [s["name"] for s in specs] == list("ABC"),
            "config needs exactly three nodes, ordered A, B, C")
    nodes = [node_factory(spec) for spec in specs]
    a, b, c = nodes
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "checks": [], "nodes": []}

    def passed(message):
        report["checks"].append(message)
        print("PASS: " + message, flush=True)

    try:
        for node in nodes:
            node.prepare()
        require(len({node.identity["id"] for node in nodes}) == 3, "identities must be distinct")
        for node in nodes:
            node.start(nodes)
            report["nodes"].append({**node.info, "name": node.name, "listen": node.status["listen"],
                                    "management": node.ssh or "local"})
            require(node.status["relay"] is True, "relay must default to on")
        configure(a, b, c)
        outbound = 'pathweft physical A-to-C: "café"\nacross three hosts'
        inbound = "pathweft physical C-to-A: return journey"
        a.send(c, outbound)
        c.send(a, inbound)
        require(a.inspect()["delivered_texts"] == 1 and c.inspect()["delivered_texts"] == 1,
                "endpoint delivery counts differ")
        relay = b.inspect()
        require(relay["forwarded"] >= 4 and relay["delivered_texts"] == 0,
                "B did not forward both messages and both receipts")
        relay_log = b.call("log")["log"]
        require("pathweft physical" not in relay_log, "relay log exposed plaintext")
        for node, expected in [(a, inbound), (c, outbound)]:
            rows = [json.loads(line) for line in node.call("log")["log"].splitlines()]
            require(any(row["event"] == "text_received" and row["details"]["text"] == expected
                        for row in rows), "recipient did not recover the original text")
        report["relay_counters"] = relay
        passed("A -> B -> C and C -> B -> A text and authenticated receipts")
        passed("B forwards metadata without plaintext; endpoints recover exact text")

        b.stop()
        a.send(c, "relay is stopped; delivery must fail", succeeds=False)
        require(c.inspect()["delivered_texts"] == 1, "text arrived after B stopped")
        passed("Stopping B prevents A -> C delivery and cleans up B's socket")

        b.start(nodes, relay=False)
        configure(a, b, c)
        a.send(c, "relay opted out; transit must fail", succeeds=False)
        require(c.inspect()["delivered_texts"] == 1, "opted-out relay forwarded text")
        require(b.inspect()["relay"] is False and b.inspect()["rejected"] >= 1,
                "relay opt-out did not reject transit")
        b.send(c, "pathweft physical: B's own message")
        require(c.inspect()["delivered_texts"] == 2, "relay owner's message was not delivered")
        passed("Relay opt-out blocks transit and preserves B's own messaging")
        report["result"] = "passed"
        report["transport"] = "TCP via the explicit listener addresses; SSH used only for management"
        report["internet_isolation"] = "not measured"
        report["direct_endpoint_link"] = "not blocked by the OS; excluded only by configured Pathweft routes"
        return report
    except Exception:
        for node in nodes:
            if node.root:
                try:
                    print(f"--- {node.name} diagnostics ---\n{node.call('log')['log']}", file=sys.stderr)
                except Exception as error:
                    print(f"Unable to read {node.name} diagnostics: {error}", file=sys.stderr)
        raise
    finally:
        errors = []
        for node in nodes:
            try:
                node.cleanup()
            except Exception as error:
                errors.append(f"{node.name} ({node.root} on {node.ssh or 'local'}): {error}")
        if errors:
            raise RuntimeError("Runtime cleanup failed: " + "; ".join(errors))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="JSON with an ordered nodes array")
    parser.add_argument("--report", type=Path, help="write the successful validation report")
    args = parser.parse_args()
    report = demonstrate(json.loads(args.config.read_text())["nodes"])
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print("Configured A-B-C proof complete. Internet isolation and router-free radios require separate tests.")
