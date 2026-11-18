//! `pathweft-ble`: experimental Bluetooth LE link adapter.
//! Linux and macOS: `serve` (advertise/accept) or `connect` (scan/dial).
//! macOS must run from an app bundle with Bluetooth permission.

use anyhow::Result;
use clap::{Parser, Subcommand};
use pathweft_ble::bridge::{self, Events};
use std::{net::SocketAddr, sync::Arc};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Advertise and accept LE L2CAP links, bridging them to the local node.
    Serve {
        /// Loopback port a node's route uses to reach the BLE neighbour. Exists only while the link is up.
        #[arg(long, required = true)]
        local: Vec<SocketAddr>,
        /// The local node's TCP listener (loopback), where inbound frames are delivered.
        #[arg(long)]
        node: SocketAddr,
    },
    /// Find a Pathweft advertiser, keep one LE L2CAP link to it, and bridge it to the local node.
    Connect {
        #[arg(long)]
        local: SocketAddr,
        #[arg(long)]
        node: SocketAddr,
        /// Linux: optionally restrict discovery to this Bluetooth address.
        #[arg(long)]
        peer: Option<String>,
    },
}

fn main() -> Result<()> {
    let events: Events = Arc::new(|event, details| {
        println!(
            "{}",
            serde_json::json!({ "event": event, "details": details })
        );
    });
    let command = Cli::parse().command;
    events(
        "process_started",
        serde_json::json!({"pid":std::process::id()}),
    );
    match command {
        Command::Serve { local, node } => serve(
            local
                .into_iter()
                .map(bridge::loopback)
                .collect::<Result<_>>()?,
            bridge::loopback(node)?,
            events,
        ),
        Command::Connect { local, node, peer } => connect(
            bridge::loopback(local)?,
            bridge::loopback(node)?,
            peer,
            events,
        ),
    }
}

#[cfg(target_os = "linux")]
fn serve(local: Vec<SocketAddr>, node: SocketAddr, events: Events) -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(async {
        tokio::select! {
            result = pathweft_ble::bluez::serve(local, node, events) => result,
            _ = tokio::signal::ctrl_c() => Ok(()),
        }
    })
}

#[cfg(target_os = "macos")]
fn serve(local: Vec<SocketAddr>, node: SocketAddr, events: Events) -> Result<()> {
    pathweft_ble::macos_peripheral::serve(local, node, events)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn serve(_: Vec<SocketAddr>, _: SocketAddr, _: Events) -> Result<()> {
    anyhow::bail!("`serve` is implemented for macOS and Linux only")
}

#[cfg(target_os = "macos")]
fn connect(
    local: SocketAddr,
    node: SocketAddr,
    peer: Option<String>,
    events: Events,
) -> Result<()> {
    anyhow::ensure!(
        peer.is_none(),
        "--peer is currently supported on Linux only"
    );
    pathweft_ble::macos::connect(local, node, events)
}

#[cfg(target_os = "linux")]
fn connect(
    local: SocketAddr,
    node: SocketAddr,
    peer: Option<String>,
    events: Events,
) -> Result<()> {
    let peer = peer.map(|value| value.parse()).transpose()?;
    tokio::runtime::Runtime::new()?.block_on(async {
        tokio::select! {
            result = pathweft_ble::bluez::connect(local, node, peer, events) => result,
            _ = tokio::signal::ctrl_c() => Ok(()),
        }
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn connect(_: SocketAddr, _: SocketAddr, _: Option<String>, _: Events) -> Result<()> {
    anyhow::bail!("`connect` is implemented for macOS and Linux only")
}
