//! Linux (BlueZ) peripheral role: advertise the Pathweft service and accept
//! LE L2CAP links. Never changes adapter power, pairing, discoverability
//! settings or the system Bluetooth service, and never calls the generic
//! `Device.Connect` (it can start a classic connection to dual-mode peers).

use crate::{PSM_CHARACTERISTIC_UUID, SERVICE_UUID, advertised_name, bridge};
use anyhow::{Context, Result, ensure};
use bluer::{
    adv::Advertisement,
    gatt::local::{Application, Characteristic, CharacteristicRead, Service},
    l2cap::{SocketAddr as L2capAddr, StreamListener},
};
use futures::FutureExt;
use std::{collections::BTreeSet, net::SocketAddr};

/// Scan using LE only and obtain the PSM from advertising data. The link uses
/// LE L2CAP directly, never the generic Device.Connect on dual-mode peers.
pub async fn connect(
    local: SocketAddr,
    node: SocketAddr,
    peer: Option<bluer::Address>,
    events: bridge::Events,
) -> Result<()> {
    use bluer::{DiscoveryFilter, DiscoveryTransport, l2cap::Stream};
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};
    let service: uuid::Uuid = SERVICE_UUID.parse()?;
    let session = bluer::Session::new().await?;
    let adapter = session.default_adapter().await?;
    ensure!(
        adapter.is_powered().await?,
        "Bluetooth adapter is off; Pathweft does not turn it on"
    );
    adapter
        .set_discovery_filter(DiscoveryFilter {
            transport: DiscoveryTransport::Le,
            uuids: std::collections::HashSet::from([service]),
            ..Default::default()
        })
        .await?;
    loop {
        events("scanning", serde_json::json!({}));
        let running = {
            let discovered = adapter.discover_devices_with_changes().await?;
            let updates = discovered
                .then(|change| advertised_channel(&adapter, change, peer, service))
                .filter_map(|value| futures::future::ready(value.transpose()));
            // Cached DeviceAdded entries may contain the old channel. Keep
            // scanning across failed dials so a fresh advertisement can update
            // it, rather than repeatedly stopping at the same cached entry.
            establish_link(
                updates,
                |address: L2capAddr| {
                    let events = events.clone();
                    async move {
                        events("discovered", serde_json::json!({"psm":address.psm}));
                        match timeout(Duration::from_secs(20), async {
                            let stream = Stream::connect(address).await?;
                            // A writable Linux socket may precede LE link setup.
                            while stream.peer_addr().is_err() {
                                sleep(Duration::from_millis(50)).await;
                            }
                            Ok::<_, anyhow::Error>(stream)
                        })
                        .await
                        {
                            Ok(Ok(stream)) => Ok(stream),
                            result => {
                                events(
                                    "connect_failed",
                                    serde_json::json!({"error":format!("{result:?}")}),
                                );
                                anyhow::bail!("BLE channel dial failed")
                            }
                        }
                    }
                },
                Duration::from_secs(1),
                local,
                node,
                events.clone(),
            )
            .await?
        }; // Release discovery once the link survives asynchronous setup errors.
        running.await?;
        sleep(Duration::from_secs(1)).await;
    }
}

async fn advertised_channel(
    adapter: &bluer::Adapter,
    change: bluer::AdapterEvent,
    peer: Option<bluer::Address>,
    service: uuid::Uuid,
) -> Result<Option<L2capAddr>> {
    let bluer::AdapterEvent::DeviceAdded(address) = change else {
        return Ok(None);
    };
    if peer.is_some_and(|wanted| wanted != address) {
        return Ok(None);
    }
    let device = adapter.device(address)?;
    let data = device.service_data().await?.unwrap_or_default();
    let psm = if let Some(bytes) = data.get(&service) {
        crate::psm_from_service_data(bytes)
    } else {
        device
            .name()
            .await?
            .as_deref()
            .and_then(crate::psm_from_name)
    };
    match psm {
        Some(psm) => Ok(Some(L2capAddr::new(
            address,
            device.address_type().await?,
            psm,
        ))),
        None => Ok(None),
    }
}

/// Own discovery until a dial succeeds. A changed target is tried immediately;
/// an unchanged target can retry after a transient failure without another event.
async fn dial_while_discovering<T, L, S, D, F>(
    updates: S,
    mut target: Option<T>,
    mut dial: D,
    retry: std::time::Duration,
) -> Result<(L, T)>
where
    T: Clone + PartialEq,
    S: futures::Stream<Item = Result<T>>,
    D: FnMut(T) -> F,
    F: std::future::Future<Output = Result<L>>,
{
    use futures::{StreamExt, pin_mut};
    use tokio::time::{Instant, sleep_until};
    pin_mut!(updates);
    let mut retry_at = Instant::now()
        + if target.is_some() {
            retry
        } else {
            std::time::Duration::ZERO
        };
    loop {
        tokio::select! {
            update = updates.next() => {
                let value = update.context("Bluetooth discovery ended")??;
                if target.as_ref() != Some(&value) {
                    target = Some(value);
                    retry_at = Instant::now();
                }
            }
            _ = sleep_until(retry_at), if target.is_some() => {
                if let Ok(link) = dial(target.as_ref().unwrap().clone()).await {
                    return Ok((link, target.take().unwrap()));
                }
                retry_at = Instant::now() + retry;
            }
        }
    }
}

type RunningLink = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;

/// Keep the same scan across channels that open and immediately fail. Linux
/// can report writable/getpeername before an asynchronous refusal arrives.
/// A stable channel releases discovery after the normal two-second I/O bound.
async fn establish_link<T, L, S, D, F>(
    updates: S,
    mut dial: D,
    retry: std::time::Duration,
    local: SocketAddr,
    node: SocketAddr,
    events: bridge::Events,
) -> Result<RunningLink>
where
    T: Clone + PartialEq,
    L: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
    S: futures::Stream<Item = Result<T>>,
    D: FnMut(T) -> F,
    F: std::future::Future<Output = Result<L>>,
{
    use futures::pin_mut;
    pin_mut!(updates);
    let mut target = None;
    loop {
        let (link, selected) =
            dial_while_discovering(&mut updates, target.take(), &mut dial, retry).await?;
        target = Some(selected);
        let mut running: RunningLink = Box::pin(bridge::run(link, local, node, events.clone()));
        tokio::select! {
            result = &mut running => {
                result?;
            }
            _ = tokio::time::sleep(bridge::IO_TIMEOUT) => return Ok(running),
        }
    }
}

pub async fn serve(
    locals: Vec<SocketAddr>,
    node: SocketAddr,
    events: bridge::Events,
) -> Result<()> {
    ensure!(
        (1..=2).contains(&locals.len()),
        "provide one or two --local ports"
    );
    ensure!(
        locals.iter().all(|p| p.ip().is_loopback() && p.port() != 0),
        "local ports must be fixed loopback ports"
    );
    ensure!(
        locals.len() == 1 || locals[0] != locals[1],
        "local ports must be distinct"
    );
    let service: uuid::Uuid = SERVICE_UUID.parse()?;
    let psm_char: uuid::Uuid = PSM_CHARACTERISTIC_UUID.parse()?;
    let session = bluer::Session::new().await.context("connect to BlueZ")?;
    let adapter = session
        .default_adapter()
        .await
        .context("no Bluetooth adapter")?;
    ensure!(
        adapter.is_powered().await?,
        "Bluetooth adapter is off; Pathweft does not turn it on"
    );
    let listener = StreamListener::bind(L2capAddr::new(
        adapter.address().await?,
        adapter.address_type().await?,
        0,
    ))
    .await
    .context("open LE L2CAP listener")?;
    let psm = listener.as_ref().local_addr()?.psm;

    let app = Application {
        services: vec![Service {
            uuid: service,
            primary: true,
            characteristics: vec![Characteristic {
                uuid: psm_char,
                read: Some(CharacteristicRead {
                    read: true,
                    fun: Box::new(move |_| async move { Ok(psm.to_le_bytes().to_vec()) }.boxed()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let _gatt = adapter
        .serve_gatt_application(app)
        .await
        .context("register GATT service")?;
    let name = advertised_name(psm);
    let _advertisement = adapter
        .advertise(Advertisement {
            service_uuids: BTreeSet::from([service]),
            local_name: Some(name.clone()),
            discoverable: Some(true),
            ..Default::default()
        })
        .await
        .context("start advertising")?;
    events(
        "advertising",
        serde_json::json!({ "service": SERVICE_UUID, "name": name, "psm": psm }),
    );

    let mut links: tokio::task::JoinSet<(u64, SocketAddr, Result<()>)> =
        tokio::task::JoinSet::new();
    let mut occupied = BTreeSet::new();
    let mut next_id = 0_u64;
    loop {
        // Reap finished tasks before assigning a reconnecting neighbour a port.
        while let Some(result) = links.try_join_next() {
            let (id, local, outcome) = result.context("BLE bridge task panicked")?;
            occupied.remove(&local);
            events(
                "channel_closed",
                serde_json::json!({"id":id,"local":local,"error":outcome.err().map(|e:anyhow::Error|format!("{e:#}"))}),
            );
        }
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("accept L2CAP link")?;
                let Some(local) = locals.iter().find(|p| !occupied.contains(*p)).copied() else {
                    drop(stream);
                    events("channel_rejected", serde_json::json!({"reason":"link limit"}));
                    continue;
                };
                occupied.insert(local);
                next_id = next_id.checked_add(1).context("channel id exhausted")?;
                let id = next_id;
                events("link_accepted", serde_json::json!({"id":id,"local":local,"peer_type":format!("{:?}",peer.addr_type)}));
                let sink = events.clone();
                let link_events: bridge::Events = std::sync::Arc::new(move |event, mut details| {
                    details["channel_id"] = serde_json::json!(id);
                    details["local"] = serde_json::json!(local);
                    sink(event, details);
                });
                links.spawn(async move { (id, local, bridge::run(stream, local, node, link_events).await) });
            }
            completed = links.join_next(), if !links.is_empty() => {
                let (id, local, outcome) = completed.unwrap().context("BLE bridge task panicked")?;
                occupied.remove(&local);
                events("channel_closed", serde_json::json!({"id":id,"local":local,"error":outcome.err().map(|e:anyhow::Error|format!("{e:#}"))}));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };

    struct Discovery(Arc<AtomicBool>);
    impl Drop for Discovery {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn updates(
        rx: mpsc::Receiver<u16>,
        discovery: Discovery,
    ) -> impl futures::Stream<Item = Result<u16>> {
        futures::stream::unfold((rx, discovery), |(mut rx, discovery)| async move {
            rx.recv().await.map(|value| (Ok(value), (rx, discovery)))
        })
    }

    #[tokio::test(start_paused = true)]
    async fn changed_channel_is_learned_while_the_same_discovery_stays_active() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(128).await.unwrap();
        let closed = Arc::new(AtomicBool::new(false));
        let mut attempts = Vec::new();
        let result = timeout(
            Duration::from_secs(2),
            dial_while_discovering(
                updates(rx, Discovery(closed.clone())),
                None,
                |psm| {
                    attempts.push(psm);
                    assert!(
                        !closed.load(Ordering::SeqCst),
                        "discovery ended before the dial"
                    );
                    let tx = tx.clone();
                    async move {
                        if psm == 128 {
                            tx.send(129).await.unwrap();
                            anyhow::bail!("cached channel closed");
                        }
                        Ok(psm)
                    }
                },
                Duration::from_secs(60),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, (129, 129));
        assert_eq!(attempts, [128, 129]);
        assert!(
            closed.load(Ordering::SeqCst),
            "successful dial must release discovery"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unchanged_channel_retries_after_a_transient_failure_without_another_event() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(128).await.unwrap();
        let closed = Arc::new(AtomicBool::new(false));
        let mut attempts = 0;
        let result = timeout(
            Duration::from_secs(2),
            dial_while_discovering(
                updates(rx, Discovery(closed.clone())),
                None,
                |psm| {
                    attempts += 1;
                    let first = attempts == 1;
                    async move {
                        if first {
                            anyhow::bail!("transient failure");
                        }
                        Ok(psm)
                    }
                },
                Duration::from_secs(1),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, (128, 128));
        assert_eq!(attempts, 2);
        assert!(closed.load(Ordering::SeqCst));
        drop(tx);
    }

    #[tokio::test(start_paused = true)]
    async fn an_immediate_link_refusal_keeps_scanning_until_a_stable_replacement() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(128).await.unwrap();
        let closed = Arc::new(AtomicBool::new(false));
        let (broken, gone) = tokio::io::duplex(1024);
        drop(gone);
        let (replacement, peer) = tokio::io::duplex(1024);
        let mut links = std::collections::VecDeque::from([broken, replacement]);
        let mut attempts = Vec::new();
        let node = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = local.local_addr().unwrap();
        drop(local);
        let events: bridge::Events = std::sync::Arc::new(move |event, _| {
            if event == "link_down" {
                tx.try_send(129).unwrap();
            }
        });
        let running = tokio::time::timeout(
            Duration::from_secs(5),
            establish_link(
                updates(rx, Discovery(closed.clone())),
                |psm| {
                    assert!(!closed.load(Ordering::SeqCst));
                    attempts.push(psm);
                    std::future::ready(Ok(links
                        .pop_front()
                        .expect("unexpected retry of cached channel")))
                },
                Duration::from_secs(1),
                address,
                node.local_addr().unwrap(),
                events,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(attempts, [128, 129]);
        assert!(
            closed.load(Ordering::SeqCst),
            "stable link should stop discovery"
        );
        drop(running);
        drop(peer);
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }
}
