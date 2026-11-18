use anyhow::{Context, Result, ensure};
use pathweft_core::{Identity, NodeId};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs::{self, DirBuilder, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};
use zeroize::{Zeroize, Zeroizing};

pub(crate) fn state_path(control: &Path, suffix: &str) -> Result<PathBuf> {
    let mut name = control
        .file_name()
        .context("state path needs a file name")?
        .to_os_string();
    name.push(suffix);
    Ok(control.with_file_name(name))
}

fn private_state_file(path: &Path, limit: usize) -> Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "state must be a regular file"
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "state must be private (mode 0600)"
    );
    ensure!(metadata.len() <= limit as u64, "state exceeds size limit");
    Ok(metadata)
}

pub(crate) fn read_private_json<T: DeserializeOwned>(
    path: &Path,
    limit: usize,
) -> Result<Option<T>> {
    private_directory(path.parent().context("state needs a parent directory")?)?;
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let expected = private_state_file(path, limit)?;
    let mut file = fs::File::open(path)?;
    let actual = file.metadata()?;
    ensure!(
        expected.ino() == actual.ino() && expected.dev() == actual.dev(),
        "state changed while opening"
    );
    let mut bytes = Vec::new();
    (&mut file).take(limit as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= limit, "state exceeds size limit");
    Ok(Some(serde_json::from_slice(&bytes).context(
        "invalid private state; preserve it for recovery",
    )?))
}

pub(crate) fn temporary_path(path: &Path, owner: NodeId) -> PathBuf {
    state_path(path, &format!(".tmp-{owner}")).expect("validated private state path")
}

pub(crate) fn discard_private_temporary(path: &Path, owner: NodeId, limit: usize) -> Result<()> {
    let temporary = temporary_path(path, owner);
    match fs::symlink_metadata(&temporary) {
        Ok(_) => {
            private_state_file(&temporary, limit)?;
            fs::remove_file(&temporary)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

pub(crate) fn write_private_state(
    path: &Path,
    owner: NodeId,
    bytes: &[u8],
    limit: usize,
) -> Result<()> {
    ensure!(bytes.len() <= limit, "state exceeds size limit");
    private_directory(path.parent().context("state needs a parent directory")?)?;
    match fs::symlink_metadata(path) {
        Ok(_) => {
            private_state_file(path, limit)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let temporary = temporary_path(path, owner);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        fs::File::open(path.parent().unwrap())?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityFile {
    version: u8,
    secret_key: String,
}

impl Drop for IdentityFile {
    fn drop(&mut self) {
        self.secret_key.zeroize();
    }
}

pub fn private_directory(path: &Path) -> Result<()> {
    if !path.exists() {
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .context("create private state directory")?;
    }
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "state directory must be a real directory"
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "state directory must be private (mode 0700)"
    );
    Ok(())
}

pub fn save_identity(path: &Path, identity: &Identity) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty())
        && !parent.exists()
    {
        private_directory(parent)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .context("create a new private identity file (existing files are never overwritten)")?;
    let data = IdentityFile {
        version: 1,
        secret_key: hex::encode(identity.secret_bytes()),
    };
    let bytes = Zeroizing::new(serde_json::to_vec(&data)?);
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

pub fn load_identity(path: &Path) -> Result<Identity> {
    let metadata = fs::symlink_metadata(path).context("inspect identity file")?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "identity must be a regular file"
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "identity file must be private (mode 0600)"
    );
    let mut bytes = Zeroizing::new(Vec::new());
    fs::File::open(path)?.take(4097).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 4096, "identity file exceeds size limit");
    let data: IdentityFile = serde_json::from_slice(&bytes).context("invalid identity format")?;
    ensure!(data.version == 1, "unsupported identity file version");
    let decoded =
        Zeroizing::new(hex::decode(&data.secret_key).context("invalid private key encoding")?);
    ensure!(decoded.len() == 32, "private key must be 32 bytes");
    let mut secret = Zeroizing::new([0_u8; 32]);
    secret.copy_from_slice(&decoded);
    Identity::from_secret(*secret).map_err(Into::into)
}
