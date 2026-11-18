//! Platform-independent frame bridge between one link (e.g. an LE L2CAP channel)
//! and the local Pathweft node.
//!
//! * Outbound: the node opens a TCP connection to `local` and writes one
//!   length-prefixed frame; the bridge copies that frame onto the link.
//! * Inbound: each frame read from the link is delivered to the node's own
//!   TCP listener at `node` on a fresh connection.
//!
//! `local` is bound only while the link is up. When the link fails the
//! listener is dropped, so the node's next send is refused instead of being
//! reported as forwarded. Frames are passed through unparsed; the node and the
//! recipient validate and authenticate them.

use anyhow::{Context, Result, bail, ensure};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinSet,
    time::timeout,
};

/// Must match `pathweft_node::wire::MAX_FRAME_BYTES`.
pub const MAX_FRAME_BYTES: usize = 32 * 1024;
/// Same per-operation bound the node uses for its own network I/O.
pub const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_LOCAL_HANDLERS: usize = 32;
const INBOUND_QUEUE: usize = 32;

/// Read one length-prefixed frame body. `Ok(None)` means a clean end of stream
/// before a new frame started.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Vec<u8>>> {
    let mut prefix = [0_u8; 4];
    match reader.read_exact(&mut prefix[..1]).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error).context("read frame length"),
    }
    reader
        .read_exact(&mut prefix[1..])
        .await
        .context("read frame length")?;
    let length = u32::from_be_bytes(prefix) as usize;
    ensure!(
        (1..=MAX_FRAME_BYTES).contains(&length),
        "frame length {length} outside 1..{MAX_FRAME_BYTES} bytes"
    );
    let mut body = vec![0_u8; length];
    reader
        .read_exact(&mut body)
        .await
        .context("read frame body")?;
    Ok(Some(body))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, body: &[u8]) -> Result<()> {
    ensure!(
        (1..=MAX_FRAME_BYTES).contains(&body.len()),
        "outgoing frame size outside limits"
    );
    writer.write_all(&(body.len() as u32).to_be_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await?;
    Ok(())
}

/// Bridge events, reported as `(event, details)`.
pub type Events = Arc<dyn Fn(&str, serde_json::Value) + Send + Sync>;

/// Run the bridge until the link closes or fails. Returns an error only if the
/// local listener cannot be bound; link loss is a normal return.
pub async fn run<L>(link: L, local: SocketAddr, node: SocketAddr, events: Events) -> Result<()>
where
    L: AsyncRead + AsyncWrite + Send + 'static,
{
    let listener = TcpListener::bind(local)
        .await
        .with_context(|| format!("bind local link port {local}"))?;
    events(
        "link_up",
        serde_json::json!({ "local": listener.local_addr()? }),
    );
    let (mut link_rx, link_tx) = tokio::io::split(link);
    let link_tx = Arc::new(Mutex::new(link_tx));
    let (down_tx, mut down) = watch::channel(false);
    let down_tx = Arc::new(down_tx);

    // A dedicated reader task: read_frame is not cancellation-safe, so it must
    // never sit inside a select! with other branches.
    let (inbound_tx, mut inbound) = mpsc::channel::<Vec<u8>>(INBOUND_QUEUE);
    let reader_down = down_tx.clone();
    let reader_events = events.clone();
    let mut reader = JoinSet::new();
    reader.spawn(async move {
        loop {
            match read_frame(&mut link_rx).await {
                Ok(Some(frame)) => {
                    if inbound_tx.send(frame).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    reader_events(
                        "link_error",
                        serde_json::json!({ "error": format!("{error:#}") }),
                    );
                    break;
                }
            }
        }
        let _ = reader_down.send(true);
    });

    let mut inbound_jobs = JoinSet::new();
    let mut outbound_jobs = JoinSet::new();
    loop {
        while inbound_jobs.try_join_next().is_some() {}
        while outbound_jobs.try_join_next().is_some() {}
        tokio::select! {
            _ = down.changed() => break,
            frame = inbound.recv(), if inbound_jobs.len() < MAX_LOCAL_HANDLERS => {
                let Some(frame) = frame else { break };
                let events = events.clone();
                inbound_jobs.spawn(async move {
                    let bytes = frame.len();
                    let result = timeout(IO_TIMEOUT, async {
                        let mut stream = TcpStream::connect(node).await.context("connect to local node")?;
                        write_frame(&mut stream, &frame).await
                    }).await;
                    match result {
                        Ok(Ok(())) => events("inbound", serde_json::json!({ "bytes": bytes })),
                        Ok(Err(error)) => events("inbound_failed", serde_json::json!({ "error": format!("{error:#}") })),
                        Err(_) => events("inbound_failed", serde_json::json!({ "error": "local node timed out" })),
                    }
                });
            }
            accepted = listener.accept() => {
                let Ok((mut stream, _)) = accepted else { continue };
                if outbound_jobs.len() >= MAX_LOCAL_HANDLERS { continue; }
                let (link_tx, down_tx, events) = (link_tx.clone(), down_tx.clone(), events.clone());
                outbound_jobs.spawn(async move {
                    let frame = match timeout(IO_TIMEOUT, read_frame(&mut stream)).await {
                        Ok(Ok(Some(frame))) => frame,
                        Ok(Ok(None)) => return,
                        Ok(Err(error)) => {
                            events("outbound_rejected", serde_json::json!({ "error": format!("{error:#}") }));
                            return;
                        }
                        Err(_) => {
                            events("outbound_rejected", serde_json::json!({ "error": "local frame timed out" }));
                            return;
                        }
                    };
                    // BLE links are slower than local TCP; allow more time than a LAN hop.
                    // The bound includes waiting for the previous writer.
                    match timeout(IO_TIMEOUT * 5, async {
                        let mut tx = link_tx.lock().await;
                        write_frame(&mut *tx, &frame).await
                    }).await {
                        Ok(Ok(())) => events("outbound", serde_json::json!({ "bytes": frame.len() })),
                        failure => {
                            let error = match failure { Ok(Err(error)) => format!("{error:#}"), _ => "link write timed out".into() };
                            events("link_error", serde_json::json!({ "error": error }));
                            let _ = down_tx.send(true);
                        }
                    }
                });
            }
            _ = inbound_jobs.join_next(), if !inbound_jobs.is_empty() => {},
            _ = outbound_jobs.join_next(), if !outbound_jobs.is_empty() => {},
        }
    }
    drop(listener);
    reader.abort_all();
    inbound_jobs.abort_all();
    outbound_jobs.abort_all();
    while reader.join_next().await.is_some() {}
    while inbound_jobs.join_next().await.is_some() {}
    while outbound_jobs.join_next().await.is_some() {}
    events("link_down", serde_json::json!({}));
    Ok(())
}

/// A link whose bytes are moved by callbacks rather than an async stream
/// (for example CoreBluetooth's run-loop streams). Bytes received from the
/// radio go into the returned sender; bytes for the radio are passed to
/// `outbound`, which acknowledges each chunk only after the radio drains it.
/// Both directions are bounded: 32 incoming chunks of at most 8192 bytes and
/// one outbound chunk awaiting acknowledgement. Dropping the sender ends the link: the pipe is shut down, the
/// bridge returns, the loopback port closes, and `closed` is called.
pub fn spawn_callback_link(
    runtime: &tokio::runtime::Handle,
    local: SocketAddr,
    node: SocketAddr,
    events: Events,
    outbound: impl Fn(Vec<u8>, oneshot::Sender<()>) + Send + 'static,
    closed: impl FnOnce() + Send + 'static,
) -> mpsc::Sender<Vec<u8>> {
    let (bridge_end, pump_end) = tokio::io::duplex(64 * 1024);
    let (inbound_tx, mut inbound_rx) = mpsc::channel::<Vec<u8>>(32);
    let (outbound_ended, ended) = oneshot::channel();
    let (mut pump_read, mut pump_write) = tokio::io::split(pump_end);
    runtime.spawn(async move {
        // Radio -> bridge. When the radio side drops the sender, shut the
        // write half so the bridge reads end-of-stream and returns.
        let inbound = tokio::spawn(async move {
            while let Some(bytes) = inbound_rx.recv().await {
                if bytes.len() > 8192 { break; }
                if pump_write.write_all(&bytes).await.is_err() {
                    break;
                }
            }
            let _ = pump_write.shutdown().await;
        });
        // Bridge -> radio.
        let outbound = tokio::spawn(async move {
            let mut buffer = vec![0_u8; 8192];
            while let Ok(n) = pump_read.read(&mut buffer).await {
                if n == 0 {
                    break;
                }
                let (done, wait) = oneshot::channel();
                outbound(buffer[..n].to_vec(), done);
                if !matches!(timeout(IO_TIMEOUT * 5, wait).await, Ok(Ok(()))) {
                    break;
                }
            }
            let _ = outbound_ended.send(());
        });
        tokio::select! {
            result = run(bridge_end, local, node, events.clone()) => {
                if let Err(error) = result {
                    events("bridge_failed", serde_json::json!({ "error": format!("{error:#}") }));
                }
            }
            _ = ended => events("link_error", serde_json::json!({"error":"radio callback ended or timed out"})),
        }
        inbound.abort();
        outbound.abort();
        let _ = inbound.await;
        let _ = outbound.await;
        closed();
    });
    inbound_tx
}

/// Parse and sanity-check a loopback address for the adapter's local side.
pub fn loopback(address: SocketAddr) -> Result<SocketAddr> {
    if !address.ip().is_loopback() {
        bail!("{address} must be a loopback address so frames cannot leave by another network");
    }
    Ok(address)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[tokio::test]
    async fn link_loss_closes_an_incomplete_local_frame_without_waiting_for_timeout() {
        let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = free_port().await;
        let (link, peer) = tokio::io::duplex(1024);
        let (events, log) = quiet();
        let bridge = tokio::spawn(run(link, local, node.local_addr().unwrap(), events));
        wait_until(|| log.lock().unwrap().iter().any(|e| e == "link_up")).await;
        let mut stalled = TcpStream::connect(local).await.unwrap();
        stalled.write_all(&[0]).await.unwrap();
        // Ensure the connection was accepted before causing the radio loss.
        tokio::task::yield_now().await;
        drop(peer);
        bridge.await.unwrap().unwrap();
        let mut byte = [0u8; 1];
        let result = timeout(Duration::from_millis(500), stalled.read(&mut byte)).await;
        assert!(
            matches!(result, Ok(Ok(0)) | Ok(Err(_))),
            "stalled local socket survived link shutdown"
        );
    }

    #[tokio::test]
    async fn cancelling_the_bridge_drops_its_radio_reader() {
        let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = free_port().await;
        let (link, mut peer) = tokio::io::duplex(1024);
        let (events, log) = quiet();
        let bridge = tokio::spawn(run(link, local, node.local_addr().unwrap(), events));
        wait_until(|| log.lock().unwrap().iter().any(|e| e == "link_up")).await;
        bridge.abort();
        assert!(bridge.await.unwrap_err().is_cancelled());
        let mut byte = [0u8; 1];
        assert_eq!(
            timeout(Duration::from_millis(500), peer.read(&mut byte))
                .await
                .expect("radio reader leaked")
                .unwrap(),
            0
        );
        assert!(TcpStream::connect(local).await.is_err());
    }

    #[tokio::test]
    async fn a_stalled_radio_callback_times_out_and_closes_the_route() {
        let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = free_port().await;
        let (events, log) = quiet();
        let (write_tx, mut write_rx) = mpsc::unbounded_channel();
        let (closed_tx, closed_rx) = oneshot::channel();
        let radio = spawn_callback_link(
            &tokio::runtime::Handle::current(),
            local,
            node.local_addr().unwrap(),
            events,
            move |bytes, done| {
                let _ = write_tx.send((bytes, done));
            },
            move || {
                let _ = closed_tx.send(());
            },
        );
        wait_until(|| log.lock().unwrap().iter().any(|e| e == "link_up")).await;
        let mut sender = TcpStream::connect(local).await.unwrap();
        write_frame(&mut sender, b"blocked on radio").await.unwrap();
        let (_bytes, held_acknowledgement) = write_rx.recv().await.unwrap();
        // Advance only after the callback is waiting, avoiding real socket I/O
        // under the fake clock. No ten-second wall-clock sleep is needed.
        tokio::time::pause();
        tokio::time::advance(IO_TIMEOUT * 5).await;
        closed_rx.await.unwrap();
        tokio::time::resume();
        assert!(TcpStream::connect(local).await.is_err());
        assert!(radio.is_closed());
        drop(held_acknowledgement);
    }

    #[tokio::test]
    async fn losing_one_link_keeps_the_other_next_hop_working() {
        let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first = free_port().await;
        let second = free_port().await;
        assert_ne!(first, second);
        let (one, peer_one) = tokio::io::duplex(1024);
        let (two, mut peer_two) = tokio::io::duplex(1024);
        let (events_one, log_one) = quiet();
        let (events_two, log_two) = quiet();
        let job_one = tokio::spawn(run(one, first, node.local_addr().unwrap(), events_one));
        let job_two = tokio::spawn(run(two, second, node.local_addr().unwrap(), events_two));
        wait_until(|| {
            log_one.lock().unwrap().iter().any(|e| e == "link_up")
                && log_two.lock().unwrap().iter().any(|e| e == "link_up")
        })
        .await;
        drop(peer_one);
        job_one.await.unwrap().unwrap();
        let mut sender = TcpStream::connect(second).await.unwrap();
        write_frame(&mut sender, b"still connected").await.unwrap();
        assert_eq!(
            read_frame(&mut peer_two).await.unwrap().unwrap(),
            b"still connected"
        );
        write_frame(&mut peer_two, b"return path").await.unwrap();
        let (mut delivered, _) = node.accept().await.unwrap();
        assert_eq!(
            read_frame(&mut delivered).await.unwrap().unwrap(),
            b"return path"
        );
        drop(peer_two);
        job_two.await.unwrap().unwrap();
    }

    fn quiet() -> (Events, Arc<StdMutex<Vec<String>>>) {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let sink = log.clone();
        (
            Arc::new(move |event: &str, _| sink.lock().unwrap().push(event.to_string())),
            log,
        )
    }

    async fn free_port() -> SocketAddr {
        TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
    }

    async fn wait_until(mut check: impl FnMut() -> bool) {
        for _ in 0..200 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached");
    }

    #[tokio::test]
    async fn frames_cross_in_both_directions_and_port_closes_with_the_link() {
        let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = free_port().await;
        let (link, mut peer) = tokio::io::duplex(64 * 1024);
        let (events, log) = quiet();
        let bridge = tokio::spawn(run(link, local, node.local_addr().unwrap(), events));
        wait_until(|| log.lock().unwrap().iter().any(|e| e == "link_up")).await;

        // Outbound: the node sends a full-size frame to the local port; it appears on the link.
        let big = vec![b'x'; MAX_FRAME_BYTES];
        let mut sender = TcpStream::connect(local).await.unwrap();
        write_frame(&mut sender, &big).await.unwrap();
        assert_eq!(read_frame(&mut peer).await.unwrap().unwrap(), big);

        // Inbound: a frame from the link is delivered to the node's listener.
        write_frame(&mut peer, b"{\"hello\":1}").await.unwrap();
        let (mut delivered, _) = node.accept().await.unwrap();
        assert_eq!(
            read_frame(&mut delivered).await.unwrap().unwrap(),
            b"{\"hello\":1}"
        );

        // Link loss: the bridge ends and the local port refuses new sends.
        drop(peer);
        bridge.await.unwrap().unwrap();
        assert!(TcpStream::connect(local).await.is_err());
        assert!(log.lock().unwrap().iter().any(|e| e == "link_down"));
    }

    #[tokio::test]
    async fn oversized_link_frame_drops_the_link() {
        let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = free_port().await;
        let (link, mut peer) = tokio::io::duplex(1024);
        let (events, log) = quiet();
        let bridge = tokio::spawn(run(link, local, node.local_addr().unwrap(), events));
        peer.write_all(&(MAX_FRAME_BYTES as u32 + 1).to_be_bytes())
            .await
            .unwrap();
        bridge.await.unwrap().unwrap();
        assert!(log.lock().unwrap().iter().any(|e| e == "link_error"));
    }

    #[tokio::test]
    async fn oversized_local_frame_is_rejected_without_touching_the_link() {
        let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = free_port().await;
        let (link, mut peer) = tokio::io::duplex(1024);
        let (events, log) = quiet();
        let _bridge = tokio::spawn(run(link, local, node.local_addr().unwrap(), events));
        wait_until(|| log.lock().unwrap().iter().any(|e| e == "link_up")).await;
        let mut sender = TcpStream::connect(local).await.unwrap();
        sender.write_all(&0_u32.to_be_bytes()).await.unwrap();
        wait_until(|| log.lock().unwrap().iter().any(|e| e == "outbound_rejected")).await;
        let mut byte = [0_u8; 1];
        assert!(
            timeout(Duration::from_millis(200), peer.read(&mut byte))
                .await
                .is_err(),
            "nothing reached the link"
        );
    }

    #[tokio::test]
    async fn dropping_a_callback_link_closes_the_local_port() {
        let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = free_port().await;
        let (events, log) = quiet();
        let (sent_tx, mut sent_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel::<()>();
        let radio = spawn_callback_link(
            &tokio::runtime::Handle::current(),
            local,
            node.local_addr().unwrap(),
            events,
            move |bytes, done| {
                let _ = sent_tx.send(bytes);
                let _ = done.send(());
            },
            move || {
                let _ = closed_tx.send(());
            },
        );
        wait_until(|| log.lock().unwrap().iter().any(|e| e == "link_up")).await;

        // Node -> radio.
        let mut sender = TcpStream::connect(local).await.unwrap();
        write_frame(&mut sender, b"out").await.unwrap();
        let mut seen = Vec::new();
        while seen.len() < 7 {
            seen.extend(sent_rx.recv().await.unwrap());
        }
        assert_eq!(&seen[4..], b"out");

        // Radio -> node, delivered in arbitrary chunks.
        radio.try_send(vec![0, 0, 0]).unwrap();
        radio.try_send(vec![2, b'i', b'n']).unwrap();
        let (mut delivered, _) = node.accept().await.unwrap();
        assert_eq!(read_frame(&mut delivered).await.unwrap().unwrap(), b"in");

        // The radio link drops: the port must close promptly.
        drop(radio);
        timeout(Duration::from_secs(2), closed_rx)
            .await
            .expect("link closed")
            .unwrap();
        assert!(TcpStream::connect(local).await.is_err());
    }

    #[test]
    fn local_side_must_be_loopback() {
        assert!(loopback("127.0.0.1:9000".parse().unwrap()).is_ok());
        assert!(loopback("192.0.2.10:9000".parse().unwrap()).is_err());
    }
}
