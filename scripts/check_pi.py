#!/usr/bin/env python3
"""Run cross-built ARMv6 library tests and the real three-process demo on a Pi.

The directory contains pathweft-armv6, pathweft-ble-armv6, demo.py and unit-tests/
from scripts/build_pi.py. No Rust compiler or Bluetooth setting change is needed.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    root = args.directory.resolve(strict=True)
    report = {"test": "raspberry-pi-armv6-native", "passed": False,
              "utc": datetime.now(timezone.utc).isoformat(), "hostname": platform.node(),
              "architecture": platform.machine(), "kernel": platform.release(),
              "units": [], "rust_library_tests": 0,
              "scope": "native library tests and loopback three-process proof; no radio or WAN isolation claim"}
    try:
        model = Path("/proc/device-tree/model")
        if model.exists(): report["model"] = model.read_text().rstrip("\0")
        report["os_release"] = Path("/etc/os-release").read_text()
        report["libc"] = os.confstr("CS_GNU_LIBC_VERSION")
        for name in ["pathweft-armv6", "pathweft-ble-armv6"]:
            reply = subprocess.run([str(root / name), "--version"], capture_output=True, text=True, check=True, timeout=10)
            report[name + "_version"] = reply.stdout.strip()
        manifest = json.loads((root / "unit-tests/manifest.json").read_text())
        expected = {"pathweft_core", "pathweft_node", "pathweft_android", "pathweft_ble"}
        if {unit["crate"] for unit in manifest} != expected:
            raise RuntimeError("Expected four library test executables")
        for unit in manifest:
            name = unit["binary"]
            if Path(name).name != name: raise RuntimeError("Invalid test filename")
            started = time.monotonic()
            result = subprocess.run([str(root / "unit-tests" / name), "--test-threads=1"], capture_output=True, text=True, timeout=60)
            row = {"crate": unit["crate"], "code": result.returncode, "seconds": round(time.monotonic()-started, 2),
                   "stdout": result.stdout, "stderr": result.stderr}
            report["units"].append(row)
            summary = re.search(r"test result: ok\. (\d+) passed; 0 failed", result.stdout)
            if result.returncode or not summary: raise RuntimeError("Native unit tests failed: " + unit["crate"])
            report["rust_library_tests"] += int(summary.group(1))
            print(f"PASS {unit['crate']}: {summary.group(1)} tests on {platform.machine()}", flush=True)
        result = subprocess.run([sys.executable, str(root / "demo.py"), "--binary", str(root / "pathweft-armv6")],
                                capture_output=True, text=True, timeout=90)
        report["process_demo"] = {"code":result.returncode, "stdout":result.stdout, "stderr":result.stderr}
        if result.returncode: raise RuntimeError("Native three-process demo failed")
        print("PASS native three-process relay/receipt/opt-out proof", flush=True)
        report["passed"] = True
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        args.report.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
