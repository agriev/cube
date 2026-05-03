//! Snapshot payload: turn a directory of files into a single byte
//! buffer and back. M5.2.
//!
//! ## Why a custom format
//!
//! Tar pulls in a heavy crate; rocksdb checkpoint is small, flat-ish,
//! and we control both ends of the wire. Custom binary keeps the
//! dependency surface minimal and the format machine-readable enough
//! to debug from a hex dump.
//!
//! ## Wire format
//!
//! ```text
//! +----------+
//! | count u32| number of file entries (BE)
//! +----------+
//! | per-entry repeat:
//! |   relpath_len u32  (BE)
//! |   relpath bytes    (UTF-8, no separator normalization)
//! |   data_len   u64   (BE)
//! |   data bytes
//! +----------+
//! ```
//!
//! Caps:
//!
//! - `relpath_len` capped at 1 KiB (paranoid; 256 B is plenty for
//!   RocksDB checkpoint names).
//! - `data_len` capped at 1 GiB per file (RocksDB SST files are
//!   typically 64–128 MiB; a 1 GiB cap is generous and matches the
//!   transport's framing budget set in M4.5.2).
//! - Total byte count capped at 8 GiB to prevent decompress-bomb
//!   style attacks where a 4 KiB header claims to expand to 1 PiB.
//!
//! Path safety
//! -----------
//!
//! `unpack_dir` rejects entries whose `relpath`:
//!
//! - is absolute (starts with `/`)
//! - contains a parent-dir component (`..`)
//! - is empty or contains a NUL byte
//!
//! Without this check, a malicious or corrupted snapshot could
//! escape the target dir via `../../etc/passwd`. Catastrophic
//! during a follower install.
//!
//! Subdirectories are created on demand from the relpath.

use std::fs;
use std::io::{Error, ErrorKind, Read, Result, Write};
use std::path::{Component, Path, PathBuf};

const MAX_RELPATH_LEN: u32 = 1024;
const MAX_FILE_DATA_LEN: u64 = 1024 * 1024 * 1024; // 1 GiB
const MAX_TOTAL_LEN: u64 = 8 * 1024 * 1024 * 1024; // 8 GiB

/// Pack every regular file under `dir` (recursively) into a single
/// byte buffer following the wire format above. Symlinks and
/// special files are skipped — RocksDB checkpoints don't produce
/// them and we don't want to be smuggling any either.
pub fn pack_dir(dir: &Path) -> Result<Vec<u8>> {
    if !dir.is_dir() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("pack_dir: not a directory: {:?}", dir),
        ));
    }

    let mut entries: Vec<(PathBuf, PathBuf)> = Vec::new();
    collect_files(dir, dir, &mut entries)?;

    // Stable order: pack in lexicographic relpath order. Without
    // this, two pack runs over the same directory could produce
    // different byte sequences (due to filesystem readdir ordering),
    // which breaks the determinism audit some operators run on
    // snapshot bytes.
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let count: u32 = entries
        .len()
        .try_into()
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "too many files"))?;

    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(&count.to_be_bytes());
    for (rel, abs) in entries {
        let rel_str = rel
            .to_str()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "non-UTF8 relpath"))?;
        let rel_bytes = rel_str.as_bytes();
        let rel_len: u32 = rel_bytes
            .len()
            .try_into()
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "relpath too long"))?;
        if rel_len > MAX_RELPATH_LEN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("relpath length {} exceeds cap", rel_len),
            ));
        }

        let data = fs::read(&abs)?;
        if (data.len() as u64) > MAX_FILE_DATA_LEN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("file too large: {} bytes ({:?})", data.len(), abs),
            ));
        }
        out.extend_from_slice(&rel_len.to_be_bytes());
        out.extend_from_slice(rel_bytes);
        out.extend_from_slice(&(data.len() as u64).to_be_bytes());
        out.extend_from_slice(&data);

        if (out.len() as u64) > MAX_TOTAL_LEN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("packed size exceeds cap of {} bytes", MAX_TOTAL_LEN),
            ));
        }
    }
    Ok(out)
}

fn collect_files(root: &Path, cur: &Path, out: &mut Vec<(PathBuf, PathBuf)>) -> Result<()> {
    for entry in fs::read_dir(cur)? {
        let entry = entry?;
        let abs = entry.path();
        let metadata = entry.file_type()?;
        if metadata.is_dir() {
            collect_files(root, &abs, out)?;
        } else if metadata.is_file() {
            // Strip the root prefix so the archive is relocatable.
            let rel = abs
                .strip_prefix(root)
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                .to_path_buf();
            out.push((rel, abs));
        }
        // Symlinks / sockets / FIFOs are silently skipped — RocksDB
        // checkpoints don't produce them, and we'd rather drop than
        // mis-handle them.
    }
    Ok(())
}

/// Unpack a buffer produced by [`pack_dir`] into `target`. The
/// target must already exist (we don't auto-create the root); any
/// missing intermediate subdirs in entries are created on demand.
///
/// Aborts on the first malformed entry — there's no partial-success
/// mode. The caller should unpack into a staging dir and only swap
/// it into place after `unpack_dir` returns Ok, so a failed install
/// leaves the previous good state untouched.
pub fn unpack_dir(buf: &[u8], target: &Path) -> Result<()> {
    if !target.is_dir() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unpack_dir: target not a directory: {:?}", target),
        ));
    }

    let mut cur = std::io::Cursor::new(buf);
    let mut hdr = [0u8; 4];
    cur.read_exact(&mut hdr)?;
    let count = u32::from_be_bytes(hdr);

    for _ in 0..count {
        let mut rel_len_buf = [0u8; 4];
        cur.read_exact(&mut rel_len_buf)?;
        let rel_len = u32::from_be_bytes(rel_len_buf);
        if rel_len == 0 || rel_len > MAX_RELPATH_LEN {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("malformed relpath length: {}", rel_len),
            ));
        }

        let mut rel_bytes = vec![0u8; rel_len as usize];
        cur.read_exact(&mut rel_bytes)?;
        let rel_str = std::str::from_utf8(&rel_bytes)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "relpath not UTF-8"))?;
        let rel_path = Path::new(rel_str);
        if !is_safe_relpath(rel_path) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("unsafe relpath rejected: {:?}", rel_path),
            ));
        }

        let mut data_len_buf = [0u8; 8];
        cur.read_exact(&mut data_len_buf)?;
        let data_len = u64::from_be_bytes(data_len_buf);
        if data_len > MAX_FILE_DATA_LEN {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("file data too large: {}", data_len),
            ));
        }

        let mut data = vec![0u8; data_len as usize];
        cur.read_exact(&mut data)?;

        let dst = target.join(rel_path);
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = fs::File::create(&dst)?;
        f.write_all(&data)?;
    }

    // Trailing bytes after the declared count are a corrupted
    // archive — surface that loudly rather than silently truncating.
    let mut tail = [0u8; 1];
    if cur.read(&mut tail)? != 0 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "trailing bytes after declared file count",
        ));
    }
    Ok(())
}

/// Reject relpaths that could escape the target dir or otherwise
/// look fishy. Rules: must not be absolute; must contain no
/// parent-dir or root components; must not contain a NUL byte.
fn is_safe_relpath(p: &Path) -> bool {
    if p.is_absolute() {
        return false;
    }
    for c in p.components() {
        match c {
            Component::Normal(s) => {
                if s.to_str().map(|x| x.contains('\0')).unwrap_or(true) {
                    return false;
                }
            }
            // Anything else is suspicious: ParentDir = "..",
            // RootDir = "/", Prefix = Windows drive letter,
            // CurDir = "." (we've already accounted for the cwd
            // via the `target` arg, so a literal `.` is harmless
            // but uncommon — we reject it for normalization).
            _ => return false,
        }
    }
    true
}

// =============================================================================
// Tests
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write(dir: &Path, rel: &str, data: &[u8]) {
        let dst = dir.join(rel);
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(dst, data).unwrap();
    }

    #[test]
    fn pack_unpack_round_trips_flat_dir() {
        let src = TempDir::new().unwrap();
        write(src.path(), "MANIFEST-000001", b"manifest-bytes");
        write(src.path(), "000003.sst", b"sst-bytes");
        write(src.path(), "OPTIONS", b"options");

        let bytes = pack_dir(src.path()).unwrap();

        let dst = TempDir::new().unwrap();
        unpack_dir(&bytes, dst.path()).unwrap();

        assert_eq!(fs::read(dst.path().join("MANIFEST-000001")).unwrap(), b"manifest-bytes");
        assert_eq!(fs::read(dst.path().join("000003.sst")).unwrap(), b"sst-bytes");
        assert_eq!(fs::read(dst.path().join("OPTIONS")).unwrap(), b"options");
    }

    #[test]
    fn pack_recurses_into_subdirs() {
        let src = TempDir::new().unwrap();
        write(src.path(), "files/sub/000001.sst", b"deep");
        write(src.path(), "top.txt", b"shallow");

        let bytes = pack_dir(src.path()).unwrap();
        let dst = TempDir::new().unwrap();
        unpack_dir(&bytes, dst.path()).unwrap();

        assert_eq!(fs::read(dst.path().join("files/sub/000001.sst")).unwrap(), b"deep");
        assert_eq!(fs::read(dst.path().join("top.txt")).unwrap(), b"shallow");
    }

    #[test]
    fn pack_is_deterministic_across_filesystem_orderings() {
        // Files are sorted by relpath before packing; running pack
        // twice over the same content must yield identical bytes
        // even if readdir returns entries in different orders.
        let src = TempDir::new().unwrap();
        write(src.path(), "z-last", b"last");
        write(src.path(), "a-first", b"first");
        write(src.path(), "m-middle", b"middle");
        let bytes1 = pack_dir(src.path()).unwrap();
        let bytes2 = pack_dir(src.path()).unwrap();
        assert_eq!(bytes1, bytes2, "pack must be deterministic");
    }

    #[test]
    fn unpack_rejects_parent_dir_traversal() {
        // Hand-craft a malicious archive: relpath = "../escape".
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&1u32.to_be_bytes()); // count
        let path = b"../escape";
        buf.extend_from_slice(&(path.len() as u32).to_be_bytes());
        buf.extend_from_slice(path);
        buf.extend_from_slice(&0u64.to_be_bytes());

        let dst = TempDir::new().unwrap();
        let err = unpack_dir(&buf, dst.path()).unwrap_err();
        assert!(
            format!("{}", err).contains("unsafe relpath"),
            "expected unsafe-relpath rejection, got {}",
            err
        );
    }

    #[test]
    fn unpack_rejects_absolute_path() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&1u32.to_be_bytes());
        let path = b"/etc/passwd";
        buf.extend_from_slice(&(path.len() as u32).to_be_bytes());
        buf.extend_from_slice(path);
        buf.extend_from_slice(&0u64.to_be_bytes());

        let dst = TempDir::new().unwrap();
        let err = unpack_dir(&buf, dst.path()).unwrap_err();
        assert!(format!("{}", err).contains("unsafe"));
    }

    #[test]
    fn unpack_rejects_trailing_bytes() {
        let src = TempDir::new().unwrap();
        write(src.path(), "x", b"y");
        let mut bytes = pack_dir(src.path()).unwrap();
        bytes.push(0x42);

        let dst = TempDir::new().unwrap();
        let err = unpack_dir(&bytes, dst.path()).unwrap_err();
        assert!(format!("{}", err).contains("trailing"));
    }

    #[test]
    fn unpack_rejects_oversize_relpath() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&1u32.to_be_bytes());
        // Claim a relpath longer than the cap.
        buf.extend_from_slice(&(MAX_RELPATH_LEN + 1).to_be_bytes());

        let dst = TempDir::new().unwrap();
        let err = unpack_dir(&buf, dst.path()).unwrap_err();
        assert!(format!("{}", err).contains("relpath length"));
    }

    #[test]
    fn pack_empty_dir_produces_zero_count_header() {
        let src = TempDir::new().unwrap();
        let bytes = pack_dir(src.path()).unwrap();
        assert_eq!(bytes, vec![0, 0, 0, 0]);
        // And unpack of that into a fresh dir must succeed (no-op).
        let dst = TempDir::new().unwrap();
        unpack_dir(&bytes, dst.path()).unwrap();
        assert!(fs::read_dir(dst.path()).unwrap().next().is_none());
    }
}
