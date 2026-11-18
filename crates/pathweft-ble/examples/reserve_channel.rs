//! Hardware-test helper: reserve an LE PSM without advertising or carrying data.
//! Forces a restarted advertiser to receive a different dynamic channel.

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    use bluer::l2cap::{Socket, SocketAddr};

    let psm: u16 = std::env::args()
        .nth(1)
        .context("provide the old LE channel number")?
        .parse()?;
    ensure!((128..=255).contains(&psm), "channel must be 128..255");
    let session = bluer::Session::new().await?;
    let adapter = session.default_adapter().await?;
    // Reserve on the same adapter/address as the real advertiser.
    let socket = Socket::new_stream()?;
    socket.bind(SocketAddr::new(
        adapter.address().await?,
        adapter.address_type().await?,
        psm,
    ))?;
    println!(
        "{}",
        serde_json::json!({"event":"channel_reserved","details":{"psm":psm}})
    );
    tokio::signal::ctrl_c().await?;
    drop(socket);
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("this hardware-test helper requires Linux")
}
