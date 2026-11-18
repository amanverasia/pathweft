//! The only unsafe boundary: sized buffers passed to libsodium's documented C API.
use crate::{
    DEFAULT_HOPS, Envelope, Error, MAX_PLAINTEXT_BYTES, NodeId, Payload, ProtectedContent,
    PublicKey, VERSION,
};
use std::sync::OnceLock;
use uuid::Uuid;
use zeroize::Zeroizing;

fn init() -> Result<(), Error> {
    static INITIALIZED: OnceLock<bool> = OnceLock::new();
    // SAFETY: sodium_init has no pointer arguments and is thread-safe.
    if *INITIALIZED.get_or_init(|| unsafe { sodium::sodium_init() >= 0 }) {
        Ok(())
    } else {
        Err(Error::Crypto)
    }
}

pub struct Identity {
    secret: Zeroizing<[u8; 32]>,
    public: PublicKey,
}

impl Identity {
    pub fn generate() -> Result<Self, Error> {
        init()?;
        let mut secret = Zeroizing::new([0_u8; 32]);
        let mut public = [0_u8; 32];
        // SAFETY: both output buffers have crypto_box's documented 32-byte sizes.
        let result =
            unsafe { sodium::crypto_box_keypair(public.as_mut_ptr(), secret.as_mut_ptr()) };
        if result != 0 {
            return Err(Error::Crypto);
        }
        Ok(Self {
            secret,
            public: PublicKey(public),
        })
    }

    /// Import a private key for local persistence, never from an untrusted peer.
    pub fn from_secret(secret: [u8; 32]) -> Result<Self, Error> {
        init()?;
        if secret == [0; 32] {
            return Err(Error::InvalidKey);
        }
        let secret = Zeroizing::new(secret);
        let mut public = [0_u8; 32];
        // SAFETY: crypto_scalarmult_base reads/writes fixed 32-byte buffers.
        let result =
            unsafe { sodium::crypto_scalarmult_base(public.as_mut_ptr(), secret.as_ptr()) };
        if result != 0 {
            return Err(Error::Crypto);
        }
        Ok(Self {
            secret,
            public: PublicKey(public),
        })
    }

    /// Private key export for the CLI's permission-restricted local identity file.
    pub fn secret_bytes(&self) -> &[u8; 32] {
        &self.secret
    }
    pub fn public_key(&self) -> PublicKey {
        self.public
    }
    pub fn id(&self) -> NodeId {
        self.public.node_id()
    }

    pub fn seal(&self, recipient: PublicKey, payload: Payload) -> Result<Envelope, Error> {
        init()?;
        payload.validate()?;
        if recipient.node_id() == self.id() {
            return Err(Error::InvalidEnvelope("identical endpoints"));
        }
        let id = Uuid::new_v4();
        let content = ProtectedContent {
            domain: "pathweft/box/v1".into(),
            version: VERSION,
            id,
            source: self.id(),
            destination: recipient.node_id(),
            payload,
        };
        let plaintext = Zeroizing::new(serde_json::to_vec(&content)?);
        if plaintext.len() > MAX_PLAINTEXT_BYTES {
            return Err(Error::InvalidText);
        }
        let mut nonce = [0_u8; 24];
        // SAFETY: a valid writable 24-byte buffer is supplied; libsodium is initialized.
        unsafe { sodium::randombytes_buf(nonce.as_mut_ptr().cast(), nonce.len()) };
        let mut ciphertext = vec![0_u8; plaintext.len() + 16];
        // SAFETY: output is plaintext length + MACBYTES; nonce and keys have the
        // fixed documented sizes. All buffers remain valid for the synchronous call.
        let result = unsafe {
            sodium::crypto_box_easy(
                ciphertext.as_mut_ptr(),
                plaintext.as_ptr(),
                plaintext.len() as u64,
                nonce.as_ptr(),
                recipient.0.as_ptr(),
                self.secret.as_ptr(),
            )
        };
        if result != 0 {
            return Err(Error::Crypto);
        }
        Ok(Envelope {
            version: VERSION,
            id,
            source: self.id(),
            destination: recipient.node_id(),
            remaining_hops: DEFAULT_HOPS,
            nonce,
            ciphertext,
        })
    }

    pub fn open(&self, sender: PublicKey, envelope: &Envelope) -> Result<Payload, Error> {
        init()?;
        envelope.validate()?;
        if envelope.destination != self.id() {
            return Err(Error::WrongRecipient);
        }
        if envelope.source != sender.node_id() {
            return Err(Error::WrongSender);
        }
        let mut plaintext = Zeroizing::new(vec![0_u8; envelope.ciphertext.len() - 16]);
        // SAFETY: ciphertext was checked to contain a MAC; output length is exactly
        // ciphertext length - MACBYTES. The nonce and keys are fixed-size buffers.
        let result = unsafe {
            sodium::crypto_box_open_easy(
                plaintext.as_mut_ptr(),
                envelope.ciphertext.as_ptr(),
                envelope.ciphertext.len() as u64,
                envelope.nonce.as_ptr(),
                sender.0.as_ptr(),
                self.secret.as_ptr(),
            )
        };
        if result != 0 {
            return Err(Error::Crypto);
        }
        let content: ProtectedContent = serde_json::from_slice(&plaintext)?;
        if content.domain != "pathweft/box/v1"
            || content.version != envelope.version
            || content.id != envelope.id
            || content.source != envelope.source
            || content.destination != envelope.destination
        {
            return Err(Error::HeaderMismatch);
        }
        content.payload.validate()?;
        Ok(content.payload)
    }
}

/// Fill a buffer from libsodium's CSPRNG. Panics only if libsodium cannot initialise.
pub(crate) fn random_fill(buf: &mut [u8]) {
    init().expect("libsodium failed to initialise");
    // SAFETY: the pointer and length describe a valid writable buffer.
    unsafe { sodium::randombytes_buf(buf.as_mut_ptr().cast(), buf.len()) };
}

const SEAL_BYTES: usize = 48;
const BOX_NONCE_BYTES: usize = 24;
const BOX_MAC_BYTES: usize = 16;

/// Anonymous sealed box: only the holder of `recipient`'s secret can open it, and
/// the sender is not revealed by the box itself.
pub fn seal_anonymous(recipient: PublicKey, message: &[u8]) -> Result<Vec<u8>, Error> {
    init()?;
    let mut out = vec![0_u8; message.len() + SEAL_BYTES];
    // SAFETY: output is message length + SEALBYTES; the key is a 32-byte buffer.
    let result = unsafe {
        sodium::crypto_box_seal(
            out.as_mut_ptr(),
            message.as_ptr(),
            message.len() as u64,
            recipient.0.as_ptr(),
        )
    };
    if result != 0 {
        return Err(Error::Crypto);
    }
    Ok(out)
}

impl Identity {
    /// Open an anonymous sealed box addressed to this identity.
    pub fn open_anonymous(&self, sealed: &[u8]) -> Result<Zeroizing<Vec<u8>>, Error> {
        init()?;
        if sealed.len() < SEAL_BYTES {
            return Err(Error::Crypto);
        }
        let mut plaintext = Zeroizing::new(vec![0_u8; sealed.len() - SEAL_BYTES]);
        // SAFETY: output is ciphertext length - SEALBYTES; keys are 32-byte buffers.
        let result = unsafe {
            sodium::crypto_box_seal_open(
                plaintext.as_mut_ptr(),
                sealed.as_ptr(),
                sealed.len() as u64,
                self.public.0.as_ptr(),
                self.secret.as_ptr(),
            )
        };
        if result != 0 {
            return Err(Error::Crypto);
        }
        Ok(plaintext)
    }

    /// Authenticated box to `recipient`; returns `nonce || ciphertext`. Callers put
    /// a purpose string in the message so these boxes stay distinct from envelopes.
    pub fn box_to(&self, recipient: PublicKey, message: &[u8]) -> Result<Vec<u8>, Error> {
        init()?;
        let mut out = vec![0_u8; BOX_NONCE_BYTES + message.len() + BOX_MAC_BYTES];
        let (nonce, ciphertext) = out.split_at_mut(BOX_NONCE_BYTES);
        // SAFETY: nonce is 24 bytes; output is message length + MACBYTES.
        unsafe { sodium::randombytes_buf(nonce.as_mut_ptr().cast(), nonce.len()) };
        // SAFETY: buffers have the documented crypto_box sizes and outlive the call.
        let result = unsafe {
            sodium::crypto_box_easy(
                ciphertext.as_mut_ptr(),
                message.as_ptr(),
                message.len() as u64,
                nonce.as_ptr(),
                recipient.0.as_ptr(),
                self.secret.as_ptr(),
            )
        };
        if result != 0 {
            return Err(Error::Crypto);
        }
        Ok(out)
    }

    /// Open `nonce || ciphertext` produced by [`Identity::box_to`] from `sender`.
    pub fn box_from(&self, sender: PublicKey, data: &[u8]) -> Result<Zeroizing<Vec<u8>>, Error> {
        init()?;
        if data.len() < BOX_NONCE_BYTES + BOX_MAC_BYTES {
            return Err(Error::Crypto);
        }
        let (nonce, ciphertext) = data.split_at(BOX_NONCE_BYTES);
        let mut plaintext = Zeroizing::new(vec![0_u8; ciphertext.len() - BOX_MAC_BYTES]);
        // SAFETY: output is ciphertext length - MACBYTES; nonce and keys are fixed size.
        let result = unsafe {
            sodium::crypto_box_open_easy(
                plaintext.as_mut_ptr(),
                ciphertext.as_ptr(),
                ciphertext.len() as u64,
                nonce.as_ptr(),
                sender.0.as_ptr(),
                self.secret.as_ptr(),
            )
        };
        if result != 0 {
            return Err(Error::Crypto);
        }
        Ok(plaintext)
    }
}
