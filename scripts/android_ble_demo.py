#!/usr/bin/env python3
"""Run on the Linux USB host: opt-in Pixel/Linux BLE proof via the project's instrumentation test.

Install the matching debug and androidTest APKs first. The phone must be unlocked
and available exclusively for this test. USB manages tests; every data listener
and route is loopback-only, and Bluetooth is the only cross-device data link.
"""
import argparse
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time

PACKAGE = "org.estudely.pathweft"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--ble-binary", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--serial", help="default: the single USB device (-d)")
    args = parser.parse_args()
    adb = ["adb", "-s", args.serial] if args.serial else ["adb", "-d"]
    report = {"test": "android-linux-ble", "passed": False, "utc": datetime.now(timezone.utc).isoformat(),
              "data_transport": "LE L2CAP; nodes and next hops loopback-only", "checks": [],
              "limits": "two physical devices; WAN remains connected; not the three-device milestone"}

    def run(command, timeout=25):
        return subprocess.run([str(x) for x in command], check=True, capture_output=True, text=True, timeout=timeout).stdout

    def control(operation, payload=None):
        encoded = base64.b64encode(json.dumps(payload or {}).encode()).decode()
        output = run([*adb, "shell", "content", "call", "--uri", f"content://{PACKAGE}.debug", "--method", operation, "--arg", encoded])
        match = re.search(r"result=([A-Za-z0-9+/=]+)", output)
        if not match:
            raise RuntimeError(f"Missing control response: {output}")
        reply = json.loads(base64.b64decode(match[1]))
        if not reply["ok"]:
            raise RuntimeError(f"{operation}: {reply}")
        return reply["result"]

    def wait(description, action, seconds=40):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if instrumentation is not None and instrumentation.poll() is not None:
                raise RuntimeError("Instrumentation ended early: " + instrument_log.read_text()[-5000:])
            result = action()
            if result:
                return result
            time.sleep(0.3)
        raise TimeoutError(description)

    def check(name, success):
        report["checks"].append({"name": name, "passed": bool(success)})
        print(("PASS " if success else "FAIL ") + name, flush=True)
        if not success:
            raise AssertionError(name)

    children = []
    instrumentation = None
    original = None
    with tempfile.TemporaryDirectory(prefix="pw-ble-", dir="/tmp") as directory:
        root = Path(directory)
        instrument_log = root / "instrumentation.log"
        try:
            check("phone_initially_stopped", not control("status").get("running", False))
            original = control("settings")
            phone = control("identity")
            report["phone"] = {"model": run([*adb, "shell", "getprop", "ro.product.model"]).strip(),
                               "api": run([*adb, "shell", "getprop", "ro.build.version.sdk"]).strip()}
            key = root / "identity.json"
            linux = json.loads(run([args.binary, "identity", "new", "--out", key]))
            with socket.socket() as available:
                available.bind(("127.0.0.1", 0))
                link_port = available.getsockname()[1]
            config = root / "node.json"
            config.write_text(json.dumps({"name": "BLEOffice", "identity": str(key), "control_socket": str(root / "node.sock"),
                "listen": "127.0.0.1:0", "relay": True, "contacts": [phone["public_key"]],
                "routes": [{"destination": phone["id"], "via": f"127.0.0.1:{link_port}"}]}))
            node_log = root / "node.log"
            with node_log.open("w") as output:
                node = subprocess.Popen([str(args.binary), "node", "--config", str(config)], stdout=output, stderr=subprocess.STDOUT)
            children.append(node)
            wait("desktop node ready", lambda: '"event":"ready"' in node_log.read_text(), 10)
            status = json.loads(run([args.binary, "status", "--socket", root / "node.sock"]))
            check("desktop_listener_loopback_only", status["listen"].startswith("127.0.0.1:"))
            with instrument_log.open("w") as output:
                instrumentation = subprocess.Popen([*adb, "shell", "am", "instrument", "-w", "--user", "0",
                    "-e", "class", f"{PACKAGE}.BleHardwareTest", "-e", "bleHardware", "true",
                    "-e", "peerKey", linux["public_key"], "-e", "peerId", linux["id"],
                    f"{PACKAGE}.test/androidx.test.runner.AndroidJUnitRunner"], stdout=output, stderr=subprocess.STDOUT)
            wait("phone advertising", lambda: control("status").get("bluetooth", {}).get("advertising"), 40)
            check("phone_listener_loopback_only", control("status")["listen"].startswith("127.0.0.1:"))
            adapter_log = root / "adapter.log"
            with adapter_log.open("w") as output:
                adapter = subprocess.Popen([str(args.ble_binary), "connect", "--local", f"127.0.0.1:{link_port}",
                    "--node", status["listen"]], stdout=output, stderr=subprocess.STDOUT)
            children.append(adapter)
            wait("Bluetooth link and phone text", lambda: 'office Android BLE 🌍' in node_log.read_text(), 70)
            check("phone_to_linux_exact_utf8", True)
            reply = json.loads(run([args.binary, "send", "--socket", root / "node.sock", "--to", phone["id"],
                "--message", "office Linux BLE 🌍", "--timeout-ms", "10000"], 15))
            check("linux_to_phone_authenticated_receipt", reply.get("status") == "delivered")
            wait("phone final receipt handshake", lambda: 'office Android BLE final' in node_log.read_text())
            report["phone_before_stop"] = control("status")
            control("ble_stop")
            instrumentation.wait(timeout=25)
            transcript = instrument_log.read_text()
            report["instrumentation"] = transcript
            check("hardware_test_including_phone_receipts_and_link_down", "OK (1 test)" in transcript and "FAILURES" not in transcript)
            report["linux_status"] = json.loads(run([args.binary, "status", "--socket", root / "node.sock"]))
            report["adapter_events"] = [json.loads(line) for line in adapter_log.read_text().splitlines() if line.startswith("{")]
            check("linux_received_both_phone_texts", report["linux_status"]["delivered_texts"] == 2)
            check("phone_identity_preserved", control("identity") == phone)
            check("phone_settings_restored", control("settings") == original)
            check("phone_service_stopped", not control("status").get("running", True))
            report["passed"] = True
        except Exception as failure:
            report["error"] = str(failure)
            if instrument_log.exists(): report["instrumentation"] = instrument_log.read_text()[-6000:]
            for name in ["adapter", "node"]:
                path = root / f"{name}.log"
                if path.exists(): report[f"{name}_log_tail"] = path.read_text()[-4000:]
            raise
        finally:
            if instrumentation is not None and instrumentation.poll() is None:
                # Cancelling the host adb process alone leaves instrumentation
                # running on the phone. Stop only our target app first.
                subprocess.run([*adb, "shell", "am", "force-stop", PACKAGE], capture_output=True, timeout=10)
                instrumentation.terminate()
                instrumentation.wait(timeout=5)
            try:
                # A failed preflight must not stop an already-running owner node.
                if original is not None:
                    control("stop")
                    control("configure", original)
            except Exception as error:
                report["cleanup_error"] = str(error)
                report["passed"] = False
            for child in reversed(children):
                if child.poll() is None:
                    child.send_signal(signal.SIGINT)
                    try: child.wait(timeout=5)
                    except subprocess.TimeoutExpired: child.kill(); child.wait()
            args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
            if report.get("cleanup_error"):
                raise RuntimeError("Phone cleanup failed: " + report["cleanup_error"])


if __name__ == "__main__":
    main()
