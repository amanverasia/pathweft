#!/usr/bin/env python3
"""Build Rust libraries and an Android debug APK (arm64 phone by default)."""
import argparse
import os
from pathlib import Path
import platform
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent.parent
NDK_VERSION = "28.2.13676358"
ABIS = {"arm64-v8a": "aarch64-linux-android", "x86_64": "x86_64-linux-android"}


def build(sdk, gradle, abis):
    sdk = sdk.resolve()
    ndk = Path(os.environ.get("ANDROID_NDK_HOME", sdk / "ndk" / NDK_VERSION))
    host = "darwin-x86_64" if platform.system() == "Darwin" else "linux-x86_64"
    tools = ndk / "toolchains/llvm/prebuilt" / host / "bin"
    if not (tools / "llvm-ar").is_file():
        raise SystemExit(f"Install NDK {NDK_VERSION} in {sdk}, or set ANDROID_NDK_HOME")
    env = os.environ.copy()
    env["ANDROID_HOME"] = str(sdk)
    env["ANDROID_SDK_ROOT"] = str(sdk)
    # libsodium's Autoconf build reads unqualified AR/RANLIB. Target-specific
    # cc-rs variables alone let it fall back to Apple's archiver on macOS.
    env["AR"] = str(tools / "llvm-ar")
    env["RANLIB"] = str(tools / "llvm-ranlib")
    env["STRIP"] = str(tools / "llvm-strip")
    env["SODIUM_DISABLE_PIE"] = "1"
    env["PATH"] = str(tools) + os.pathsep + env["PATH"]
    env["RUSTFLAGS"] = (env.get("RUSTFLAGS", "") +
                        " -C link-arg=-Wl,-z,max-page-size=16384 -C link-arg=-Wl,--no-undefined").strip()
    target = Path(env.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target.is_absolute():
        target = ROOT / target
    for abi in abis:
        triple = ABIS[abi]
        clang = tools / f"{triple}26-clang"
        key = triple.replace("-", "_")
        target_env = env.copy()
        target_env[f"CARGO_TARGET_{key.upper()}_LINKER"] = str(clang)
        target_env[f"CC_{key}"] = str(clang)
        target_env[f"AR_{key}"] = str(tools / "llvm-ar")
        target_env[f"RANLIB_{key}"] = str(tools / "llvm-ranlib")
        subprocess.run(["cargo", "build", "--locked", "--release", "-p", "pathweft-android",
                        "--target", triple], cwd=ROOT, env=target_env, check=True)
        library = target / triple / "release/libpathweft_android.so"
        symbols = subprocess.check_output([str(tools / "llvm-nm"), "-D", "--undefined-only", str(library)], text=True)
        if any(prefix in symbols for prefix in ["crypto_", "sodium_", "randombytes"]):
            raise SystemExit(f"{abi} library contains unresolved libsodium symbols")
        destination = ROOT / "android/app/src/main/jniLibs" / abi
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copy2(library, destination)
    command = [str(gradle.resolve())] if gradle else [str(ROOT / "android/gradlew")]
    subprocess.run([*command, "--no-daemon", f"-PpathweftAbis={','.join(abis)}", "assembleDebug", "assembleDebugAndroidTest", "lintDebug"],
                   cwd=ROOT / "android", env=env, check=True)
    print("APK:", ROOT / "android/app/build/outputs/apk/debug/app-debug.apk")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk", type=Path, default=os.environ.get("ANDROID_HOME"))
    parser.add_argument("--gradle", type=Path, help="use an existing Gradle 9.4.1 executable instead of the wrapper")
    parser.add_argument("--abi", action="append", choices=ABIS, help="repeat to package both supported ABIs; default: arm64-v8a")
    args = parser.parse_args()
    if not args.sdk:
        parser.error("provide --sdk or ANDROID_HOME")
    build(args.sdk, args.gradle, list(dict.fromkeys(args.abi or ["arm64-v8a"])))
