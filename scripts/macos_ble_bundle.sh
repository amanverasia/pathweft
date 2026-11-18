#!/bin/sh
# Wrap the pathweft-ble binary in a minimal app bundle so macOS can grant it
# Bluetooth access (command-line tools inherit their parent app's permission).
# Ad-hoc signed: macOS asks for Bluetooth permission again after each rebuild.
# Usage: scripts/macos_ble_bundle.sh   ->  target/macos/PathweftBLE.app
# Run:   open -n target/macos/PathweftBLE.app --stdout <log> --stderr <log> \
#          --args connect --local 127.0.0.1:<port> --node 127.0.0.1:<node-port>
# Or receive up to two channels with:
#          --args serve --local 127.0.0.1:<first-port> --local 127.0.0.1:<second-port> --node 127.0.0.1:<node-port>
set -eu
cd "$(dirname "$0")/.."
cargo build --locked -p pathweft-ble
app=target/macos/PathweftBLE.app
rm -rf "$app"
mkdir -p "$app/Contents/MacOS"
cp target/debug/pathweft-ble "$app/Contents/MacOS/pathweft-ble"
cat > "$app/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>CFBundleIdentifier</key><string>org.estudely.pathweft.ble</string>
  <key>CFBundleName</key><string>Pathweft BLE</string>
  <key>CFBundleDisplayName</key><string>Pathweft BLE</string>
  <key>CFBundleExecutable</key><string>pathweft-ble</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleVersion</key><string>1</string>
  <key>CFBundleShortVersionString</key><string>0.1.0</string>
  <key>LSMinimumSystemVersion</key><string>14.0</string>
  <key>LSUIElement</key><true/>
  <key>NSBluetoothAlwaysUsageDescription</key><string>Pathweft uses Bluetooth to send and relay messages directly between nearby devices, without a router or internet.</string>
</dict></plist>
PLIST
codesign --force --sign - "$app"
echo "$app"
