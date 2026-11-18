<img src="assets/icon.svg" width="64" height="64" alt="">

# Pathweft

Pathweft is an experimental networking platform for apps to exchange encrypted messages through participating devices. It can use local connections when internet access is unavailable. An explicitly configured relay can store sealed messages while a recipient is offline.

The project includes a Rust core, a desktop command-line node, Bluetooth LE adapters for macOS and Linux, and an Android development app using the same Rust node. The mailbox configuration is available through the desktop prototype.

## Run the local demos

Install Rustup, Python 3, a C compiler and Make. The repository selects Rust 1.100.0. The first build fetches dependencies and builds libsodium. The demos use local processes and temporary identities.

```sh
python3 scripts/demo.py
python3 scripts/discovery_demo.py
python3 scripts/mailbox_demo.py
python3 scripts/mailbox_demo.py --discovered
```

The discovered mailbox demo sends an encrypted message across two links to storage while the recipient is offline. The recipient later fetches it over two more links and sends a delivery receipt. The guide explains how to configure your own local test. [Mailbox setup and protocol](docs/relay-mailbox.md)

## Build and verify

```sh
cargo build --locked -p pathweft
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

For the Android development APK, install JDK 17, Android SDK platform 37 and NDK 28.2.13676358. Install the Rust Android target for your intended ABI using Rustup. Run the following with your SDK location.

```sh
rustup target add aarch64-linux-android x86_64-linux-android
python3 scripts/build_android.py --sdk /path/to/android-sdk --abi arm64-v8a --abi x86_64
```

The app builds without Google Play Services. Use only test identities and test traffic. The experimental protocol has not undergone an independent security review.

## License

Pathweft's original code uses the [Apache License 2.0](LICENSE).
