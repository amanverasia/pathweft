#!/usr/bin/env python3
"""Repeat the A-B-C proof with a debug Android app and desktop nodes over LAN."""
import argparse
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import socket
import subprocess
import time

from lan_demo import Node, demonstrate, require

ADB_HELPER = r'''
import json, subprocess, sys
request = json.load(sys.stdin)
selection = ["-s", request["serial"]] if request.get("serial") else ["-d"]
result = subprocess.run([request["adb"], *selection, *request["args"]], capture_output=True, text=True, timeout=25)
print(json.dumps({"code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}))
'''


class AndroidNode(Node):
    PACKAGE = "org.estudely.pathweft"

    def __init__(self, spec):
        super().__init__({**spec, "binary": "/unused-on-android"})
        self.original_settings = None
        self.original_running = False
        self.last_adb_error = ""

    def adb(self, *args):
        payload = {"adb": self.spec.get("adb", "adb"), "serial": self.spec.get("adb_serial"), "args": list(args)}
        process = subprocess.run(self.command(ADB_HELPER), input=json.dumps(payload),
                                 capture_output=True, text=True, timeout=35)
        require(process.returncode == 0, f"ADB management failed: {process.stderr}")
        result = json.loads(process.stdout)
        self.last_adb_error = result["stderr"]
        require(result["code"] == 0, f"ADB command failed: {result['stderr']} {result['stdout']}")
        return result["stdout"]

    def control(self, operation, payload=None, succeeds=True):
        encoded = base64.b64encode(json.dumps(payload or {}).encode()).decode()
        output = self.adb("shell", "content", "call", "--uri", "content://" + self.PACKAGE + ".debug",
                          "--method", operation, "--arg", encoded)
        match = re.search(r"result=([A-Za-z0-9+/=]+)", output)
        require(match is not None, f"Android control returned no encoded result: {output}; {self.last_adb_error}")
        reply = json.loads(base64.b64decode(match.group(1)))
        if succeeds:
            require(reply["ok"], f"Android {operation} failed: {reply.get('error')}")
            return reply["result"]
        require(not reply["ok"], "Android incorrectly reported successful delivery")
        return reply

    def call(self, operation, **kwargs):
        if operation == "log":
            events = self.control("events")["events"]
            return {"log": "\n".join(json.dumps(event, ensure_ascii=False) for event in events)}
        raise ValueError("unsupported Android management operation")

    def prepare(self):
        self.adb("shell", "am", "start", "-n", self.PACKAGE + "/.MainActivity")
        self.root = "app-private Pathweft state (persistent identity retained)"
        self.original_settings = self.control("settings")
        self.original_running = self.control("status").get("running", False)
        if self.original_running:
            self.stop()
        self.identity = self.control("identity")
        self.info = {"hostname": self.adb("shell", "getprop", "ro.product.model").strip(),
                     "os": "Android " + self.adb("shell", "getprop", "ro.build.version.release").strip(),
                     "architecture": self.adb("shell", "getprop", "ro.product.cpu.abi").strip(),
                     "version": "Pathweft Android 0.1.0 debug", "root": self.root}
        print(f"{self.name}: {self.info['hostname']} / {self.info['os']} {self.info['architecture']}", flush=True)

    def start(self, nodes, relay=None):
        self.adb("shell", "am", "start", "-W", "-n", self.PACKAGE + "/.MainActivity")
        config = {"name": self.name, "listen": "0.0.0.0:0",
                  "contacts": [node.identity["public_key"] for node in nodes if node != self], "routes": []}
        if relay is not None:
            config["relay"] = relay
        self.control("configure", config)
        self.control("start")
        self.status = self.inspect()
        require(self.status.get("running"), "Android node did not start")
        self.status["listen"] = self.address + ":" + self.status["listen"].rsplit(":", 1)[1]
        if self.spec.get("background"):
            self.adb("shell", "input", "keyevent", "KEYCODE_HOME")
            self.info["activity_backgrounded"] = True

    def inspect(self):
        return self.control("status")

    def route(self, destination, via):
        return self.control("route", {"to": destination.identity["id"], "via": via.status["listen"]})

    def send(self, destination, message, succeeds=True):
        result = self.control("send", {"to": destination.identity["id"], "message": message,
                                       "timeout_ms": 3000 if succeeds else 700}, succeeds=succeeds)
        if succeeds:
            require(result["status"] == "delivered" and result["recipient"] == destination.identity["id"],
                    "Android accepted the wrong delivery receipt")
        return result

    def stop(self):
        self.control("stop")
        require(not self.inspect().get("running", False), "Android node failed to stop")
        if self.status:
            port = int(self.status["listen"].rsplit(":", 1)[1])
            try:
                connection = socket.create_connection((self.address, port), timeout=2)
            except OSError:
                pass
            else:
                connection.close()
                raise AssertionError("Android TCP listener remained open after stop")

    def cleanup(self):
        if self.root:
            self.control("stop")
            if self.original_settings is not None:
                self.control("configure", self.original_settings)
            if self.original_running:
                self.adb("shell", "am", "start", "-W", "-n", self.PACKAGE + "/.MainActivity")
                self.control("start")
            self.root = None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    specs = json.loads(args.config.read_text())["nodes"]
    require(any(spec.get("kind") == "android" for spec in specs), "config needs an Android node")
    report = demonstrate(specs, lambda spec: AndroidNode(spec) if spec.get("kind") == "android" else Node(spec))
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    report["android_management"] = "Debug-only ContentProvider restricted to ADB shell/app UID; no network control listener"
    report["android_cleanup"] = "Stopped test service; restored prior settings/running state; kept app-private identity and installed app"
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print("Mixed Android/desktop LAN proof complete. WAN isolation and router-free radio tests remain.")
