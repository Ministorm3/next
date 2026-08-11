//! Exclusive ownership of an output folder, held for the life of the
//! process that acquired it.
//!
//! Two channel workers writing one output folder is a corruption engine:
//! each empties the folder on startup (deleting the other's heartbeat,
//! making it immortal to the idle timeout), each publishes playlists and
//! sidecars from its own segment numbering, and every consumer downstream
//! sees the two regimes alternate on disk. The 2026-08-10 overnight
//! incident ran exactly that way for eleven hours after a spawn race
//! started two workers for one channel.
//!
//! The lock is `flock(2)` on the folder's own directory descriptor, not on
//! a file inside the folder: startup preparation empties the folder, and a
//! lock file it deletes would let a second process lock a fresh inode at
//! the same path while the first still holds the old one. The directory
//! inode survives emptying.
//!
//! Advisory semantics are enough here because every writer of these
//! folders is this codebase, and every writer acquires the lock before
//! touching the folder. The kernel releases the lock when the holding
//! process exits, however it exits. The descriptor is not inherited by
//! child processes (Rust opens with `O_CLOEXEC`), so a transcoder that
//! outlives a killed worker does not hold the folder; it holds only until
//! its bounded `-t` duration expires.

use std::path::Path;

/// Holds the exclusive lock on an output folder. Dropping it (or process
/// exit) releases the folder to the next worker.
#[derive(Debug)]
pub struct FolderLock {
    _file: Option<std::fs::File>,
}

/// Takes the exclusive lock on `folder`, creating it first if needed.
///
/// Fails with [`std::io::ErrorKind::WouldBlock`] when another live process
/// already owns the folder. Callers should treat that as "refuse to start",
/// not as retryable within the same invocation: the owner releases only by
/// exiting.
pub fn lock_folder_exclusive(folder: &Path) -> Result<FolderLock, std::io::Error> {
    std::fs::create_dir_all(folder)?;

    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;

        let file = std::fs::File::open(folder)?;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(FolderLock { _file: Some(file) })
    }

    #[cfg(not(unix))]
    {
        // No supported deployment target is non-unix; a no-op guard keeps
        // the crate compiling there rather than silently pretending to
        // exclude anyone.
        Ok(FolderLock { _file: None })
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::lock_folder_exclusive;

    #[test]
    fn a_second_lock_on_a_held_folder_is_refused() {
        let dir = tempfile::tempdir().unwrap();

        let first = lock_folder_exclusive(dir.path()).unwrap();
        let second = lock_folder_exclusive(dir.path());
        assert_eq!(
            second.unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "a held folder must refuse a second exclusive lock"
        );

        drop(first);
        lock_folder_exclusive(dir.path())
            .expect("releasing the first lock must free the folder for the next worker");
    }

    #[test]
    fn the_lock_survives_the_folder_being_emptied() {
        let dir = tempfile::tempdir().unwrap();
        let _held = lock_folder_exclusive(dir.path()).unwrap();

        // Startup preparation empties the folder; the lock must still
        // exclude a second worker afterwards because it lives on the
        // directory inode, not on a file inside it.
        std::fs::write(dir.path().join("live000000.ts"), b"x").unwrap();
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            std::fs::remove_file(entry.unwrap().path()).unwrap();
        }

        let second = lock_folder_exclusive(dir.path());
        assert_eq!(second.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn a_missing_folder_is_created_and_locked() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("11");

        let _held = lock_folder_exclusive(&nested).unwrap();
        assert!(nested.is_dir());
        assert_eq!(
            lock_folder_exclusive(&nested).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
