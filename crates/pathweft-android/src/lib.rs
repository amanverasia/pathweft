//! Small JNI boundary; desktop and Android host the same node implementation.
use anyhow::{Context, Result, bail, ensure};
use jni::{
    JNIEnv,
    objects::{JClass, JString},
    sys::jstring,
};
use pathweft_core::{ContactCard, Identity, PublicKey, verification_code};
use pathweft_node::{Config, DiscoveryLink, Node, Request, Response, Route, storage};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::runtime::{Builder, Runtime};

const EVENT_CAPACITY: usize = 64;

fn default_name() -> String {
    "Android".into()
}
fn default_listen() -> std::net::SocketAddr {
    "0.0.0.0:0".parse().unwrap()
}
fn default_relay() -> bool {
    true
}
fn default_powered() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    #[serde(default = "default_name")]
    name: String,
    #[serde(default = "default_listen")]
    listen: std::net::SocketAddr,
    #[serde(default = "default_relay")]
    relay: bool,
    #[serde(default)]
    contacts: Vec<PublicKey>,
    #[serde(default)]
    routes: Vec<Route>,
    #[serde(default)]
    links: Vec<DiscoveryLink>,
    /// "Find routes automatically": runs discovery even with no static links.
    #[serde(default)]
    discovery: bool,
    #[serde(default = "default_powered")]
    powered: bool,
}

impl Settings {
    fn config(self, directory: &Path) -> Config {
        Config {
            name: self.name,
            identity: directory.join("identity.key"),
            control_socket: directory.join("control.sock"),
            listen: self.listen,
            relay: self.relay,
            contacts: self.contacts,
            routes: self.routes,
            links: self.links,
            discovery: self.discovery,
            powered: self.powered,
            mailbox: None,
            pickup: None,
            mailbox_route: None,
            mailbox_route_contacts: Vec::new(),
        }
    }
}

struct Host {
    runtime: Runtime,
    node: Node,
}

#[derive(Default)]
struct Bridge {
    host: Option<Host>,
    events: Arc<Mutex<VecDeque<Value>>>,
}

fn identity(directory: &Path) -> Result<Identity> {
    ensure!(
        directory.is_absolute(),
        "app state directory must be absolute"
    );
    storage::private_directory(directory)?;
    let path = directory.join("identity.key");
    if !path.exists() {
        storage::save_identity(&path, &Identity::generate()?)?;
    }
    storage::load_identity(&path)
}

impl Bridge {
    fn dispatch(&mut self, directory: &Path, operation: &str, payload: &str) -> Result<Response> {
        ensure!(
            payload.len() <= 128 * 1024,
            "native request exceeds size limit"
        );
        match operation {
            "identity" => {
                let identity = identity(directory)?;
                Ok(Response::success(
                    json!({"id":identity.id(),"public_key":identity.public_key()}),
                ))
            }
            "validate" | "start" => {
                let settings: Settings =
                    serde_json::from_str(payload).context("invalid node settings")?;
                let identity = identity(directory)?;
                let config = settings.config(directory);
                config.validate(identity.id())?;
                if operation == "validate" {
                    return Ok(Response::success(json!({"valid":true})));
                }
                ensure!(self.host.is_none(), "node already running");
                // Shared Node startup holds a private file lock before reclaiming
                // a stale control socket or recovering queued encrypted messages.
                let runtime = Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()?;
                self.events.lock().unwrap().clear();
                let queue = self.events.clone();
                let node = runtime.block_on(Node::start(
                    config,
                    Arc::new(move |value| {
                        let mut queue = queue.lock().unwrap();
                        if queue.len() == EVENT_CAPACITY {
                            queue.pop_front();
                        }
                        queue.push_back(value);
                    }),
                ))?;
                let status = node.status();
                self.host = Some(Host { runtime, node });
                Ok(Response::success(status))
            }
            "request" => {
                let host = self.host.as_ref().context("node is stopped")?;
                let request: Request =
                    serde_json::from_str(payload).context("invalid control request")?;
                Ok(host.runtime.block_on(host.node.request(request)))
            }
            // Contact exchange: these use only the saved identity and work while stopped.
            "contact_card" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct CardRequest {
                    name: Option<String>,
                }
                let request: CardRequest =
                    serde_json::from_str(payload).context("invalid card request")?;
                let identity = identity(directory)?;
                let card = ContactCard::new(identity.public_key(), request.name)?.to_string();
                Ok(Response::success(
                    json!({"card":card, "qr":qr_rows(&card)?}),
                ))
            }
            "contact_parse" | "verification_code" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct PeerRequest {
                    peer: String,
                }
                let request: PeerRequest =
                    serde_json::from_str(payload).context("invalid contact request")?;
                let (key, name) = if request.peer.starts_with("pathweft:") {
                    let card = ContactCard::parse(&request.peer)?;
                    (card.key, card.name)
                } else {
                    (request.peer.parse::<PublicKey>()?, None)
                };
                let own = identity(directory)?.public_key();
                ensure!(key != own, "this is your own contact card");
                Ok(Response::success(json!({
                    "id": key.node_id(), "public_key": key, "name": name,
                    "code": verification_code(&own, &key)?,
                })))
            }
            "events" => {
                let drained: Vec<_> = self.events.lock().unwrap().drain(..).collect();
                Ok(Response::success(json!({"events":drained})))
            }
            "stop" => {
                if let Some(host) = self.host.take() {
                    host.runtime.block_on(host.node.shutdown())?;
                }
                Ok(Response::success(json!({"running":false})))
            }
            _ => bail!("unknown native operation"),
        }
    }
}

/// QR module rows ('1' = dark), including the 4-module quiet zone scanners need.
fn qr_rows(text: &str) -> Result<Vec<String>> {
    use qrcode::{Color, EcLevel, QrCode};
    let code = QrCode::with_error_correction_level(text.as_bytes(), EcLevel::M)?;
    let (width, colors, quiet) = (code.width(), code.to_colors(), 4);
    Ok((0..width + 2 * quiet)
        .map(|y| {
            (0..width + 2 * quiet)
                .map(|x| {
                    let inside =
                        (quiet..quiet + width).contains(&x) && (quiet..quiet + width).contains(&y);
                    if inside && colors[(y - quiet) * width + (x - quiet)] == Color::Dark {
                        '1'
                    } else {
                        '0'
                    }
                })
                .collect()
        })
        .collect())
}

fn handle(directory: &str, operation: &str, payload: &str) -> String {
    static BRIDGE: OnceLock<Mutex<Bridge>> = OnceLock::new();
    let result = std::panic::catch_unwind(|| {
        BRIDGE
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| anyhow::anyhow!("native host lock failed"))?
            .dispatch(Path::new(directory), operation, payload)
    });
    let response = match result {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => Response::failure(format!("{error:#}")),
        Err(_) => Response::failure("native operation panicked"),
    };
    serde_json::to_string(&response)
        .unwrap_or_else(|_| "{\"ok\":false,\"error\":\"response encoding failed\"}".into())
}

/// Java passes validated local String references; the jni crate handles their
/// lifetimes and conversion. No raw native handle is exposed to Kotlin.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_estudely_pathweft_NativeBridge_call(
    mut env: JNIEnv,
    _class: JClass,
    directory: JString,
    operation: JString,
    payload: JString,
) -> jstring {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> jni::errors::Result<jstring> {
            let directory: String = env.get_string(&directory)?.into();
            let operation: String = env.get_string(&operation)?.into();
            let payload: String = env.get_string(&payload)?.into();
            Ok(env
                .new_string(handle(&directory, &operation, &payload))?
                .into_raw())
        },
    ));
    match result {
        Ok(Ok(value)) => value,
        _ => {
            if !env.exception_check().unwrap_or(true) {
                let _ = env.throw_new(
                    "java/lang/IllegalStateException",
                    "Pathweft native bridge failed",
                );
            }
            std::ptr::null_mut()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contact_card_qr_parse_and_code_through_the_bridge() {
        let one = std::env::temp_dir().join(format!("pw-contact-a-{}", std::process::id()));
        let two = std::env::temp_dir().join(format!("pw-contact-b-{}", std::process::id()));
        let mut bridge = Bridge::default();
        let card = |dir: &Path, bridge: &mut Bridge| {
            bridge
                .dispatch(dir, "contact_card", r#"{"name":"phone"}"#)
                .unwrap()
                .result
                .unwrap()
        };
        let a = card(&one, &mut bridge);
        let b = card(&two, &mut bridge);
        let rows = a["qr"].as_array().unwrap();
        assert!(
            rows.len() >= 21 + 8 && rows.iter().all(|r| r.as_str().unwrap().len() == rows.len())
        );
        let peer = |dir: &Path, card: &Value, bridge: &mut Bridge, op: &str| {
            bridge.dispatch(dir, op, &json!({"peer":card["card"]}).to_string())
        };
        let a_reads_b = peer(&one, &b, &mut bridge, "contact_parse")
            .unwrap()
            .result
            .unwrap();
        let b_reads_a = peer(&two, &a, &mut bridge, "verification_code")
            .unwrap()
            .result
            .unwrap();
        assert_eq!(a_reads_b["name"], "phone");
        assert_eq!(
            a_reads_b["code"], b_reads_a["code"],
            "both devices show the same code"
        );
        assert!(
            peer(&one, &a, &mut bridge, "contact_parse").is_err(),
            "own card rejected"
        );
        assert!(
            bridge
                .dispatch(
                    &one,
                    "contact_parse",
                    r#"{"peer":"pathweft:contact/1/zz/0000"}"#
                )
                .is_err()
        );
        assert!(
            bridge
                .dispatch(&one, "contact_card", r#"{"name":"bad name"}"#)
                .is_err()
        );
        assert!(
            bridge.host.is_none(),
            "contact operations never start a node"
        );
        std::fs::remove_dir_all(one).unwrap();
        std::fs::remove_dir_all(two).unwrap();
    }
    #[test]
    fn settings_default_to_relaying_and_reject_private_path_injection() {
        let settings: Settings = serde_json::from_str("{}").unwrap();
        assert!(settings.relay);
        assert!(serde_json::from_str::<Settings>(r#"{"identity":"/other/key"}"#).is_err());
    }
    #[test]
    fn saved_settings_without_discovery_fields_still_load() {
        let old =
            r#"{"name":"Phone","listen":"0.0.0.0:0","relay":false,"contacts":[],"routes":[]}"#;
        let settings: Settings = serde_json::from_str(old).unwrap();
        assert!(settings.links.is_empty() && settings.powered && !settings.discovery);
        let config = settings.config(Path::new("/tmp/x"));
        assert!(config.links.is_empty() && config.powered && !config.discovery);
        let on = serde_json::from_str::<Settings>(r#"{"discovery":true}"#)
            .unwrap()
            .config(Path::new("/tmp/x"));
        assert!(on.discovery && on.links.is_empty());
        let new = r#"{"powered":false,"links":[{"address":"127.0.0.1:47101","kind":"ble"}]}"#;
        let config = serde_json::from_str::<Settings>(new)
            .unwrap()
            .config(Path::new("/tmp/x"));
        assert!(!config.powered && config.links.len() == 1);
        assert!(
            serde_json::from_str::<Settings>(r#"{"links":[{"address":"127.0.0.1:1","kind":"x"}]}"#)
                .is_err()
        );
    }
    #[test]
    fn invalid_settings_are_rejected_without_starting_or_stopping_a_host() {
        let directory = std::env::temp_dir().join(format!("pw-validate-{}", std::process::id()));
        let mut bridge = Bridge::default();
        assert!(
            bridge
                .dispatch(&directory, "validate", r#"{"name":"bad name"}"#)
                .is_err()
        );
        assert!(bridge.host.is_none());
        bridge.dispatch(&directory, "start", "{}").unwrap();
        let address = bridge.host.as_ref().unwrap().node.status()["listen"].clone();
        assert!(
            bridge
                .dispatch(
                    &directory,
                    "validate",
                    r#"{"routes":[{"destination":"invalid","via":"127.0.0.1:1"}]}"#
                )
                .is_err()
        );
        assert_eq!(
            bridge.host.as_ref().unwrap().node.status()["listen"],
            address
        );
        bridge.dispatch(&directory, "stop", "{}").unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn native_host_start_toggle_stop_and_identity_persistence() {
        let directory =
            std::env::temp_dir().join(format!("pathweft-bridge-{}", std::process::id()));
        assert!(!directory.exists());
        let mut bridge = Bridge::default();
        let first = bridge
            .dispatch(&directory, "identity", "{}")
            .unwrap()
            .result
            .unwrap();
        let started = bridge
            .dispatch(&directory, "start", r#"{"listen":"127.0.0.1:0"}"#)
            .unwrap();
        assert!(started.ok);
        let response = bridge
            .dispatch(
                &directory,
                "request",
                r#"{"command":"relay","enabled":false}"#,
            )
            .unwrap();
        assert_eq!(response.result.unwrap()["relay"], false);
        assert!(bridge.dispatch(&directory, "stop", "{}").unwrap().ok);
        assert!(!directory.join("control.sock").exists());
        let restored = bridge
            .dispatch(&directory, "identity", "{}")
            .unwrap()
            .result
            .unwrap();
        assert_eq!(first, restored);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
