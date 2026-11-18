use pathweft_node::{
    self as node, DEFAULT_WAIT_MS, DiscoveryLinkKind, MAX_WAIT_MS, MailboxRelay, Request, Response,
    storage, wire,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use pathweft_core::{ContactCard, Identity, NodeId, Payload, PublicKey, verification_code};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::net::UnixStream;
use uuid::Uuid;

mod qr;

#[derive(Parser)]
#[command(
    name = "pathweft",
    version,
    about = "Experimental desktop relay prototype"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create or inspect an identity. Private keys are never printed.
    Identity {
        #[command(subcommand)]
        command: IdentityCommand,
    },
    /// Share and check contact keys: QR contact cards and verification codes.
    Contact {
        #[command(subcommand)]
        command: ContactCommand,
    },
    /// Run a node with explicitly configured contacts and routes.
    Node {
        #[arg(long)]
        config: PathBuf,
    },
    /// Send text and wait for an authenticated receipt from the recipient.
    Send {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        to: NodeId,
        #[arg(long)]
        message: String,
        #[arg(long, default_value_t = 3000)]
        timeout_ms: u64,
    },
    /// Save encrypted text for delivery when its next-hop connection is available.
    Queue {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        to: NodeId,
        #[arg(long)]
        message: String,
        #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u64).range(1..=86400))]
        ttl_seconds: u64,
    },
    /// Set up or send to a prototype offline mailbox.
    Mailbox {
        #[command(subcommand)]
        command: MailboxCommand,
    },
    /// Inspect delivery history, cancel unsent text, or forget terminal records.
    Outbox {
        #[arg(long)]
        socket: PathBuf,
        #[command(subcommand)]
        command: OutboxCommand,
    },
    /// List, read or forget encrypted incoming messages retained on this node.
    Inbox {
        #[arg(long)]
        socket: PathBuf,
        #[command(subcommand)]
        command: InboxCommand,
    },
    /// Inspect a running node's public status/counters.
    Status {
        #[arg(long)]
        socket: PathBuf,
    },
    /// Report a live discovery link up or down (the node needs discovery enabled).
    Link {
        #[command(subcommand)]
        command: LinkCommand,
    },
    /// Add or replace a manually configured route.
    Route {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        to: NodeId,
        #[arg(long)]
        via: SocketAddr,
    },
}

#[derive(Subcommand)]
enum MailboxCommand {
    /// Generate a private deposit label to share with one contact.
    NewLabel,
    /// Allow a label on a node configured to store sealed copies.
    Register {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        label: Uuid,
        #[arg(long)]
        public_key: PublicKey,
    },
    /// Queue text for a contact's pinned pickup relay.
    Queue {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        to: NodeId,
        #[arg(long)]
        message: String,
        #[arg(long)]
        label: Uuid,
        #[arg(long)]
        relay_address: Option<SocketAddr>,
        #[arg(long)]
        relay_key: PublicKey,
        #[arg(long, default_value_t = DEFAULT_WAIT_MS / 1000,
            value_parser = clap::value_parser!(u64).range(1..=MAX_WAIT_MS / 1000))]
        ttl_seconds: u64,
    },
}

#[derive(Subcommand)]
enum LinkCommand {
    /// Tell the node a neighbour address is reachable.
    Up {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        address: SocketAddr,
        #[arg(long, value_parser = parse_link_kind)]
        kind: DiscoveryLinkKind,
    },
    /// Tell the node a neighbour address is gone.
    Down {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        address: SocketAddr,
    },
}

fn parse_link_kind(text: &str) -> Result<DiscoveryLinkKind, String> {
    match text {
        "lan" => Ok(DiscoveryLinkKind::Lan),
        "ble" => Ok(DiscoveryLinkKind::Ble),
        "other" => Ok(DiscoveryLinkKind::Other),
        _ => Err("kind must be lan, ble or other".into()),
    }
}

#[derive(Subcommand)]
enum OutboxCommand {
    List,
    Cancel {
        #[arg(long)]
        message_id: String,
    },
    Forget {
        #[arg(long)]
        message_id: String,
    },
}

#[derive(Subcommand)]
enum InboxCommand {
    List,
    Read {
        #[arg(long)]
        message_id: String,
    },
    Forget {
        #[arg(long)]
        message_id: String,
    },
}

#[derive(Subcommand)]
enum ContactCommand {
    /// Print your contact card as text and as a terminal QR code for someone to scan.
    Card {
        #[arg(long)]
        identity: PathBuf,
        /// Optional label shown to the other person (1-32 ASCII letters, digits, '_' or '-').
        #[arg(long)]
        name: Option<String>,
        /// Print only the card text.
        #[arg(long)]
        no_qr: bool,
    },
    /// Check someone's card and print the public key to pin as a contact.
    Parse { card: String },
    /// Pin someone's key on a running node (in memory, like `route`; also add it to the config file to keep it).
    Add {
        #[arg(long)]
        socket: PathBuf,
        /// Their contact card or 64-hex public key.
        #[arg(long)]
        peer: String,
    },
    /// Print the code to compare with the other person, in person or by voice (never over Pathweft).
    Code {
        #[arg(long)]
        identity: PathBuf,
        /// Their contact card or 64-hex public key.
        #[arg(long)]
        peer: String,
    },
}

fn peer_key(peer: &str) -> Result<PublicKey> {
    if peer.starts_with("pathweft:") {
        Ok(ContactCard::parse(peer)?.key)
    } else {
        Ok(peer.parse()?)
    }
}

#[derive(Subcommand)]
enum IdentityCommand {
    New {
        #[arg(long)]
        out: PathBuf,
    },
    Show {
        #[arg(long)]
        identity: PathBuf,
    },
}

fn public_identity(identity: &Identity) -> Value {
    json!({"id":identity.id(), "public_key":identity.public_key()})
}

async fn exchange(socket: &Path, request: &Request, wait: Duration) -> Result<Value> {
    let reply: Response = tokio::time::timeout(wait, async {
        let mut stream = UnixStream::connect(socket)
            .await
            .context("connect to local node")?;
        wire::write(&mut stream, request).await?;
        wire::read(&mut stream).await
    })
    .await
    .context("local node request timed out")??;
    if !reply.ok {
        bail!(
            "{}",
            reply
                .error
                .unwrap_or_else(|| "node rejected request".into())
        );
    }
    reply.result.context("missing result from node")
}

#[tokio::main]
async fn main() -> Result<()> {
    match Args::parse().command {
        Command::Identity { command } => {
            let identity = match command {
                IdentityCommand::New { out } => {
                    let identity = Identity::generate()?;
                    storage::save_identity(&out, &identity)?;
                    identity
                }
                IdentityCommand::Show { identity } => storage::load_identity(&identity)?,
            };
            println!("{}", public_identity(&identity));
        }
        Command::Contact { command } => match command {
            ContactCommand::Card {
                identity,
                name,
                no_qr,
            } => {
                let identity = storage::load_identity(&identity)?;
                let card = ContactCard::new(identity.public_key(), name)?.to_string();
                if !no_qr {
                    print!("{}", qr::render(&card)?);
                }
                println!("{card}");
            }
            ContactCommand::Parse { card } => {
                let card = ContactCard::parse(&card)?;
                println!(
                    "{}",
                    json!({"id":card.key.node_id(), "public_key":card.key, "name":card.name,
                        "note":"Add public_key to your node's contacts. They must also add yours, or replies are dropped."})
                );
            }
            ContactCommand::Add { socket, peer } => {
                let request = Request::AddContact {
                    public_key: peer_key(&peer)?,
                };
                println!(
                    "{}",
                    exchange(&socket, &request, Duration::from_secs(5)).await?
                );
            }
            ContactCommand::Code { identity, peer } => {
                let identity = storage::load_identity(&identity)?;
                println!(
                    "{}",
                    verification_code(&identity.public_key(), &peer_key(&peer)?)?
                );
            }
        },
        Command::Node { config } => node::run(&config).await?,
        Command::Queue {
            socket,
            to,
            message,
            ttl_seconds,
        } => {
            let reply = exchange(
                &socket,
                &Request::Queue {
                    to,
                    message,
                    ttl_ms: ttl_seconds * 1000,
                },
                Duration::from_secs(3),
            )
            .await?;
            println!("{reply}");
        }
        Command::Mailbox { command } => {
            let (socket, request) = match command {
                MailboxCommand::NewLabel => {
                    println!("{}", json!({"label": Uuid::new_v4()}));
                    return Ok(());
                }
                MailboxCommand::Register {
                    socket,
                    label,
                    public_key,
                } => (socket, Request::RegisterMailbox { label, public_key }),
                MailboxCommand::Queue {
                    socket,
                    to,
                    message,
                    label,
                    relay_address,
                    relay_key,
                    ttl_seconds,
                } => (
                    socket,
                    Request::QueueMailbox {
                        to,
                        message,
                        label,
                        relay: MailboxRelay {
                            address: relay_address,
                            public_key: relay_key,
                        },
                        ttl_ms: ttl_seconds * 1000,
                    },
                ),
            };
            println!(
                "{}",
                exchange(&socket, &request, Duration::from_secs(3)).await?
            );
        }
        Command::Outbox { socket, command } => {
            let request = match command {
                OutboxCommand::List => Request::Outbox,
                OutboxCommand::Cancel { message_id } => Request::Cancel {
                    message_id: message_id.parse().context("invalid message UUID")?,
                },
                OutboxCommand::Forget { message_id } => Request::Forget {
                    message_id: message_id.parse().context("invalid message UUID")?,
                },
            };
            println!(
                "{}",
                exchange(&socket, &request, Duration::from_secs(3)).await?
            );
        }
        Command::Inbox { socket, command } => {
            let request = match command {
                InboxCommand::List => Request::Inbox,
                InboxCommand::Read { message_id } => Request::InboxRead {
                    message_id: message_id.parse().context("invalid message UUID")?,
                },
                InboxCommand::Forget { message_id } => Request::InboxForget {
                    message_id: message_id.parse().context("invalid message UUID")?,
                },
            };
            println!(
                "{}",
                exchange(&socket, &request, Duration::from_secs(3)).await?
            );
        }
        Command::Send {
            socket,
            to,
            message,
            timeout_ms,
        } => {
            Payload::Text {
                text: message.clone(),
            }
            .validate()?;
            anyhow::ensure!(
                (100..=10_000).contains(&timeout_ms),
                "timeout-ms must be 100..10000"
            );
            let reply = exchange(
                &socket,
                &Request::Send {
                    to,
                    message,
                    timeout_ms,
                },
                Duration::from_millis(timeout_ms + 3000),
            )
            .await?;
            println!("{reply}");
        }
        Command::Status { socket } => {
            println!(
                "{}",
                exchange(&socket, &Request::Status, Duration::from_secs(3)).await?
            );
        }
        Command::Link { command } => {
            let (socket, request) = match command {
                LinkCommand::Up {
                    socket,
                    address,
                    kind,
                } => (socket, Request::LinkUp { address, kind }),
                LinkCommand::Down { socket, address } => (socket, Request::LinkDown { address }),
            };
            println!(
                "{}",
                exchange(&socket, &request, Duration::from_secs(3)).await?
            );
        }
        Command::Route { socket, to, via } => {
            println!(
                "{}",
                exchange(&socket, &Request::Route { to, via }, Duration::from_secs(3)).await?
            );
        }
    }
    Ok(())
}
