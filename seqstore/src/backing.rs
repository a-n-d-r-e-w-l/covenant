use std::{
    fs::File,
    ops::{Deref, DerefMut},
};

use crate::error::Error;

/// The underlying storage used by [`RawStore`][crate::raw_store::RawStore].
///
/// Can either be an anonymous map (just in memory), or a file-backed map.
pub struct Backing(pub(crate) BackingInner);

pub(crate) enum BackingInner {
    File { file: File, map: memmap2::MmapMut },
    Anon(memmap2::MmapMut),
}

impl std::fmt::Debug for Backing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <BackingInner as std::fmt::Debug>::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for BackingInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackingInner::File { .. } => f.debug_struct("BackingFile").finish_non_exhaustive(),
            BackingInner::Anon(_) => f.debug_struct("BackingAnon").finish_non_exhaustive(),
        }
    }
}

impl Deref for BackingInner {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            BackingInner::File { map, .. } => map,
            BackingInner::Anon(map) => map,
        }
    }
}

impl DerefMut for BackingInner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            BackingInner::File { map, .. } => map,
            BackingInner::Anon(map) => map,
        }
    }
}

impl Backing {
    /// Initializes a file-backed mapping.
    ///
    /// # Safety
    ///
    /// It is important that the file (on the filesystem, not the [`File`]) does not get modified
    /// either **in- or out-of-process** until the returned [`Backing`] is dropped.
    ///
    /// Failing to ensure this could result in `panic!`s, reception of `SIGBUS`, data corruption,
    /// returning bogus data, etc. Failing to ensure this will _not_ result in UB.[^1]
    ///
    /// Typically, prevention of in-process modification requires only opening one instance pointing
    /// to the file, not creating any other memory maps from the file and so on.
    ///
    /// Prevention of out-of-process modification is _much_ harder, and in fact mostly impossible
    /// (you can't prevent the underlying storage device failing, after all).
    /// It is normally enough, however, to:
    /// * only use files in dedicated directories
    /// * prevent other instances of the process from trying to open the file via some kind of lock
    /// * hope that no unknown process tries to change it
    ///
    /// [^1]: Data is only borrowed for as short as possible in limited scopes. For example, we do
    /// not use zero-copy deserialization in the store, and refer to all data by offset. For the brief
    /// period that we are reading from a borrowed buffer, truncation of the file or modification of
    /// the currently-read bytes will result in `panic!`/`SIGBUS`/errors/bogus data. As such, we store
    /// no pointers _into_ the memory mapped region and all internal datastructures (and exposed
    /// interfaces) do not provide ways to hold onto the backing bytes.
    pub unsafe fn new_file(file: File) -> Result<Self, Error> {
        let map = unsafe { memmap2::MmapMut::map_mut(&file).map_err(Error::Map)? };
        Ok(Self(BackingInner::File { map, file }))
    }

    /// Initializes an in-memory mapping.
    ///
    /// Note that this uses an [anonymous memory map][memmap2::MmapMut::map_anon] and not a [`Vec<u8>`][std::vec::Vec]
    /// or similar.
    pub fn new_anon() -> Result<Self, Error> {
        Ok(Self(BackingInner::Anon(memmap2::MmapMut::map_anon(256).map_err(Error::Map)?)))
    }

    /// Initializes an in-memory mapping containing exactly the contents of `b`.
    ///
    /// Note that this _copies_ from `b`.
    pub fn new_from_buffer(b: &[u8]) -> Result<Self, Error> {
        let mut m = memmap2::MmapMut::map_anon(b.len()).map_err(Error::Map)?;
        m[..b.len()].copy_from_slice(b);
        Ok(Self(BackingInner::Anon(m)))
    }
}

impl BackingInner {
    pub(crate) fn write(&mut self, b: &[u8], position: &mut usize) -> Result<(), Error> {
        let req = *position + b.len();
        self.resize_for(req)?;
        let target = &mut self[*position..*position + b.len()];
        target.copy_from_slice(b);
        *position += b.len();
        Ok(())
    }

    /// Ensures there is enough space. Will not truncate.
    pub(crate) fn resize_for(&mut self, len: usize) -> Result<(), Error> {
        if self.len() <= len {
            self.resize_to(((len / 256) + 1) * 256)?;
        }
        Ok(())
    }

    /// Sets the size. This will truncate.
    fn resize_to(&mut self, size: usize) -> Result<(), Error> {
        match self {
            BackingInner::File { file, map } => {
                file.set_len(size as u64).map_err(Error::Resize)?;
                unsafe { map.remap(size, memmap2::RemapOptions::new().may_move(true)).map_err(Error::Resize)? };
            }
            BackingInner::Anon(map) => {
                unsafe { map.remap(size, memmap2::RemapOptions::new().may_move(true)).map_err(Error::Resize)? };
            }
        }
        Ok(())
    }

    fn map(&self) -> &memmap2::MmapMut {
        match self {
            BackingInner::File { map, .. } => map,
            BackingInner::Anon(map) => map,
        }
    }

    pub(crate) fn flush(&mut self) -> Result<(), Error> {
        self.map().flush().map_err(Error::Flush)?;
        Ok(())
    }

    pub(crate) fn flush_start_end(&mut self, start: usize, end: usize) -> Result<(), Error> {
        assert!(start <= end);
        if start == end {
            return Ok(());
        }
        self.map().flush_range(start, end - start).map_err(Error::Flush)?;
        Ok(())
    }

    pub(crate) fn flush_range(&mut self, start: usize, length: usize) -> Result<(), Error> {
        self.map().flush_range(start, length).map_err(Error::Flush)
    }
}
