//! Backup directory signature. The server never reads this file.
//! A snapshot checksum can be rewritten with the bytes, so the backup tool
//! records the signer and, when a key is set, a keyed digest of every file.

use std::{collections::BTreeMap, fs, path::Path};

use super::PersistenceError;

const HEADER: &str = "ruvio-backup-signature 1";
const FILE_NAME: &str = "SIGNATURE";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupSeal {
    pub signer: String,
    pub key: Vec<u8>,
}

impl Default for BackupSeal {
    fn default() -> Self {
        Self {
            signer: "ruvio-ctl".into(),
            key: Vec::new(),
        }
    }
}

pub fn write_backup_signature(directory: &Path, seal: &BackupSeal) -> Result<(), PersistenceError> {
    if seal.signer.is_empty() || seal.signer.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(PersistenceError::Failed(
            "backup signer must be one word".into(),
        ));
    }

    let files = file_digests(directory)?;
    let body = manifest_body(&seal.signer, &files);
    let mut text = body.clone();

    if !seal.key.is_empty() {
        text.push_str("mac ");
        text.push_str(&hex(&mac(&seal.key, body.as_bytes())));
        text.push('\n');
    }

    fs::write(directory.join(FILE_NAME), text)?;

    Ok(())
}

pub fn verify_backup_signature(directory: &Path, key: &[u8]) -> Result<(), PersistenceError> {
    let path = directory.join(FILE_NAME);

    if !path.exists() {
        return Ok(());
    }

    let text = fs::read_to_string(&path)?;
    let parsed = parse_manifest(&text)?;
    let files = file_digests(directory)?;

    if files != parsed.files {
        return Err(PersistenceError::Corruption(
            "backup signature does not match the directory".into(),
        ));
    }

    let Some(expected) = parsed.mac else {
        return Ok(());
    };

    if key.is_empty() {
        return Err(PersistenceError::Failed(
            "backup is signed; pass --sign-key or RUVIO_BACKUP_KEY".into(),
        ));
    }

    let actual = mac(key, manifest_body(&parsed.signer, &parsed.files).as_bytes());

    if actual != expected {
        return Err(PersistenceError::Corruption(
            "backup signature key does not match".into(),
        ));
    }

    Ok(())
}

struct ParsedManifest {
    signer: String,
    files: BTreeMap<String, [u8; 32]>,
    mac: Option<[u8; 32]>,
}

fn manifest_body(signer: &str, files: &BTreeMap<String, [u8; 32]>) -> String {
    let mut body = String::new();

    body.push_str(HEADER);
    body.push('\n');
    body.push_str("signer ");
    body.push_str(signer);
    body.push('\n');

    for (path, digest) in files {
        body.push_str(path);
        body.push(' ');
        body.push_str(&hex(digest));
        body.push('\n');
    }

    body
}

fn parse_manifest(text: &str) -> Result<ParsedManifest, PersistenceError> {
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();

    if header != HEADER {
        return Err(PersistenceError::Corruption(
            "backup signature header is not recognized".into(),
        ));
    }

    let signer_line = lines.next().unwrap_or_default();
    let Some(signer) = signer_line.strip_prefix("signer ") else {
        return Err(PersistenceError::Corruption(
            "backup signature is missing the signer".into(),
        ));
    };

    if signer.is_empty() || signer.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(PersistenceError::Corruption(
            "backup signer must be one word".into(),
        ));
    }

    let mut files = BTreeMap::new();
    let mut mac = None;

    for line in lines {
        if line.is_empty() {
            continue;
        }

        if let Some(encoded) = line.strip_prefix("mac ") {
            if mac.is_some() {
                return Err(PersistenceError::Corruption(
                    "backup signature has two mac lines".into(),
                ));
            }

            mac = Some(decode_hex(encoded)?);
            continue;
        }

        if mac.is_some() {
            return Err(PersistenceError::Corruption(
                "backup signature has a file after the mac".into(),
            ));
        }

        let Some((path, encoded)) = line.rsplit_once(' ') else {
            return Err(PersistenceError::Corruption(
                "backup signature file line is invalid".into(),
            ));
        };

        if path.is_empty()
            || files
                .insert(path.to_string(), decode_hex(encoded)?)
                .is_some()
        {
            return Err(PersistenceError::Corruption(
                "backup signature repeats a file".into(),
            ));
        }
    }

    Ok(ParsedManifest {
        signer: signer.to_string(),
        files,
        mac,
    })
}

fn file_digests(directory: &Path) -> Result<BTreeMap<String, [u8; 32]>, PersistenceError> {
    let mut files = BTreeMap::new();

    walk(directory, directory, &mut files)?;

    Ok(files)
}

fn walk(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<String, [u8; 32]>,
) -> Result<(), PersistenceError> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if name == FILE_NAME || name == "LOCK" {
            continue;
        }

        if entry.file_type()?.is_dir() {
            walk(root, &path, files)?;
            continue;
        }

        let relative = relative_path(root, &path)?;
        let bytes = fs::read(&path)?;

        files.insert(relative, *blake3::hash(&bytes).as_bytes());
    }

    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> Result<String, PersistenceError> {
    let mut parts = Vec::new();
    let stripped = path.strip_prefix(root).map_err(|_| {
        PersistenceError::Failed(format!("{} is outside the backup", path.display()))
    })?;

    for component in stripped.components() {
        let std::path::Component::Normal(part) = component else {
            return Err(PersistenceError::Failed(format!(
                "{} is not a plain relative path",
                path.display()
            )));
        };

        parts.push(part.to_string_lossy().into_owned());
    }

    Ok(parts.join("/"))
}

fn mac(key: &[u8], body: &[u8]) -> [u8; 32] {
    let material = blake3::hash(key);

    *blake3::keyed_hash(material.as_bytes(), body).as_bytes()
}

fn hex(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        text.push(ALPHABET[(byte >> 4) as usize] as char);
        text.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }

    text
}

fn decode_hex(text: &str) -> Result<[u8; 32], PersistenceError> {
    if text.len() != 64 {
        return Err(PersistenceError::Corruption(
            "backup signature digest is not 32 bytes".into(),
        ));
    }

    let mut bytes = [0u8; 32];

    for (index, chunk) in text.as_bytes().chunks(2).enumerate() {
        let high = hex_value(chunk[0])?;
        let low = hex_value(chunk[1])?;

        bytes[index] = (high << 4) | low;
    }

    Ok(bytes)
}

fn hex_value(byte: u8) -> Result<u8, PersistenceError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(PersistenceError::Corruption(
            "backup signature digest is not hex".into(),
        )),
    }
}
