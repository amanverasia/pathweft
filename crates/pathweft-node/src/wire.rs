use anyhow::{Context, Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME_BYTES: usize = 32 * 1024;

pub async fn read_bytes<R: AsyncRead + Unpin>(stream: &mut R) -> Result<Vec<u8>> {
    let length = stream.read_u32().await.context("read frame length")? as usize;
    ensure!(
        (1..=MAX_FRAME_BYTES).contains(&length),
        "frame length outside 1..32768 bytes"
    );
    let mut bytes = vec![0_u8; length];
    stream
        .read_exact(&mut bytes)
        .await
        .context("read frame body")?;
    Ok(bytes)
}

pub async fn read<T: DeserializeOwned, R: AsyncRead + Unpin>(stream: &mut R) -> Result<T> {
    let bytes = read_bytes(stream).await?;
    serde_json::from_slice(&bytes).context("invalid JSON frame")
}

pub async fn write_bytes<W: AsyncWrite + Unpin>(stream: &mut W, bytes: &[u8]) -> Result<()> {
    ensure!(bytes.len() <= MAX_FRAME_BYTES, "outgoing frame too large");
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(bytes).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn write<T: Serialize, W: AsyncWrite + Unpin>(stream: &mut W, value: &T) -> Result<()> {
    write_bytes(stream, &serde_json::to_vec(value)?).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn rejects_oversized_prefix_without_waiting_for_body() {
        let (mut a, mut b) = tokio::io::duplex(8);
        a.write_u32(MAX_FRAME_BYTES as u32 + 1).await.unwrap();
        assert!(read::<serde_json::Value, _>(&mut b).await.is_err());
    }
    #[tokio::test]
    async fn rejects_truncated_frame() {
        let (mut a, mut b) = tokio::io::duplex(16);
        a.write_u32(8).await.unwrap();
        a.write_all(b"{}").await.unwrap();
        drop(a);
        assert!(read::<serde_json::Value, _>(&mut b).await.is_err());
    }
}
