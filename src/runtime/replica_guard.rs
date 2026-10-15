use std::{
    fs, io,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CatchupMarker {
    Preparing,
    Installed {
        generation: u64,
        snapshot: u64,
        through: u64,
    },
}

pub(super) fn path(data_dir: &Path, shard: u32) -> PathBuf {
    data_dir.join(format!("REPLICA_CATCHUP_{shard:04}"))
}

pub(super) fn read(data_dir: &Path, shard: u32) -> io::Result<Option<CatchupMarker>> {
    let bytes = match fs::read(path(data_dir, shard)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let marker = if bytes == b"preparing\n" {
        CatchupMarker::Preparing
    } else {
        let fields: Vec<_> = bytes.split(|byte| *byte == b' ').collect();
        let numbers: Option<Vec<u64>> = fields
            .iter()
            .map(|field| std::str::from_utf8(field).ok()?.trim().parse().ok())
            .collect();
        match numbers.as_deref() {
            Some([generation, snapshot, through]) if snapshot <= through => {
                CatchupMarker::Installed {
                    generation: *generation,
                    snapshot: *snapshot,
                    through: *through,
                }
            }
            _ => CatchupMarker::Preparing,
        }
    };
    Ok(Some(marker))
}

pub(super) fn write(data_dir: &Path, shard: u32, marker: CatchupMarker) -> io::Result<()> {
    let destination = path(data_dir, shard);
    let temporary = destination.with_extension("tmp");
    let bytes = match marker {
        CatchupMarker::Preparing => b"preparing\n".to_vec(),
        CatchupMarker::Installed {
            generation,
            snapshot,
            through,
        } => format!("{generation} {snapshot} {through}\n").into_bytes(),
    };
    let mut file = fs::File::create(&temporary)?;
    use io::Write;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(temporary, destination)?;
    sync_directory(data_dir)
}

pub(super) fn clear(data_dir: &Path, shard: u32) -> io::Result<()> {
    fs::remove_file(path(data_dir, shard))?;
    sync_directory(data_dir)
}

/// Directories cannot be opened for syncing on Windows, so only Unix flushes them.
fn sync_directory(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_survives_reopen_and_malformed_marker_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let dir = directory.path();
        write(dir, 2, CatchupMarker::Preparing).unwrap();
        assert_eq!(read(dir, 2).unwrap(), Some(CatchupMarker::Preparing));
        let installed = CatchupMarker::Installed {
            generation: 4,
            snapshot: 10,
            through: 19,
        };
        write(dir, 2, installed).unwrap();
        assert_eq!(read(dir, 2).unwrap(), Some(installed));
        fs::write(path(dir, 2), b"invalid").unwrap();
        assert_eq!(read(dir, 2).unwrap(), Some(CatchupMarker::Preparing));
        clear(dir, 2).unwrap();
        assert_eq!(read(dir, 2).unwrap(), None);
    }
}
