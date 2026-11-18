#!/usr/bin/env python3
"""Cross-build the CLI/BLE adapter for the ARMv6 Raspberry Pi Zero W.

Requires Rust 1.100.0's arm-unknown-linux-gnueabihf target and a tool virtualenv
containing cargo-zigbuild==0.23.4 and ziglang==0.15.2. Heavy compilation stays on
the build host; nothing is installed or configured on the Pi by this script.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent.parent
TARGET = "arm-unknown-linux-gnueabihf"


def build(tool_bin, work_dir, tests=False):
    tool_bin = tool_bin.resolve(strict=True)
    work_dir = work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    python = tool_bin / "python"
    zigbuild = tool_bin / "cargo-zigbuild"
    if not python.is_file() or not zigbuild.is_file():
        raise SystemExit("Provide --tool-bin with the dedicated cross-build virtualenv's bin directory")
    env = os.environ.copy()
    env["PATH"] = str(tool_bin) + os.pathsep + env["PATH"]
    env["CARGO_ZIGBUILD_PYTHON_PATH"] = str(python)
    env["CARGO_ZIGBUILD_CACHE_DIR"] = str(work_dir / "wrappers")
    env["ZIG_GLOBAL_CACHE_DIR"] = str(work_dir / "zig-cache")
    env["CARGO_TARGET_DIR"] = str(work_dir / "target")
    env["RUSTFLAGS"] = (env.get("RUSTFLAGS", "") + " -C target-cpu=arm1176jzf-s -C strip=symbols").strip()
    env["SODIUM_DISABLE_PIE"] = "1"
    # Autoconf reads the unqualified variables. Do not let it select the build
    # host's archiver (the earlier Android build exposed that failure mode).
    for variable, command in [("AR", "ar"), ("RANLIB", "ranlib")]:
        wrapper = work_dir / f"zig-{command}"
        wrapper.write_text("#!/bin/sh\nexec " + shlex.join([str(python), "-m", "ziglang", command]) + ' "$@"\n')
        wrapper.chmod(0o700)
        env[variable] = str(wrapper)
        env[f"{variable}_{TARGET.replace('-', '_')}"] = str(wrapper)
    command = ["cargo", "zigbuild", "--locked", "--release", "--jobs", "2", "--target", TARGET + ".2.28"]
    subprocess.run([*command,
                    "-p", "pathweft", "-p", "pathweft-ble"], cwd=ROOT, env=env, check=True)
    artifacts = work_dir / "target" / TARGET / "release"
    print("Pi binaries:", artifacts, flush=True)
    if tests:
        output = work_dir / "test-artifacts.jsonl"
        packages = ["pathweft-core", "pathweft-node", "pathweft-android", "pathweft-ble"]
        with output.open("w") as log:
            subprocess.run([*command, "--tests", "--message-format=json", *[arg for package in packages for arg in ("-p", package)]],
                           cwd=ROOT, env=env, stdout=log, check=True)
        destination = work_dir / "unit-tests"
        destination.mkdir(exist_ok=True)
        manifest = []
        expected = {package.replace("-", "_") for package in packages}
        for line in output.read_text().splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            name = row.get("target", {}).get("name")
            if row.get("reason") == "compiler-artifact" and row.get("profile", {}).get("test") and row.get("executable") and name in expected:
                path = destination / (name + "-tests")
                shutil.copy2(row["executable"], path)
                manifest.append({"crate": name, "binary": path.name})
        if {row["crate"] for row in manifest} != expected:
            raise SystemExit("Did not obtain all four ARMv6 library test executables")
        (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print("Pi unit tests:", destination, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tool-bin", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--tests", action="store_true", help="also cross-build library test executables for native Pi validation")
    args = parser.parse_args()
    build(args.tool_bin, args.work_dir, args.tests)
