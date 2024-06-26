use std::{
    fs::File,
    ops::{Deref, DerefMut},
};

use crate::error::Error;

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
    pub unsafe fn new_file(file: File) -> Result<Self, Error> {
        let map = unsafe { memmap2::MmapMut::map_mut(&file).map_err(Error::Map)? };
        Ok(Self(BackingInner::File { map, file }))
    }

    pub fn new_anon() -> Result<Self, Error> {
        Ok(Self(BackingInner::Anon(memmap2::MmapMut::map_anon(256).map_err(Error::Map)?)))
    }

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

    pub(crate) fn resize_for(&mut self, len: usize) -> Result<(), Error> {
        if self.len() <= len {
            self.resize_to(((len / 256) + 1) * 256)?;
        }
        Ok(())
    }

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

    pub(crate) fn map(&self) -> &memmap2::MmapMut {
        match self {
            BackingInner::File { map, .. } => map,
            BackingInner::Anon(map) => map,
        }
    }

    pub(crate) fn flush(&self) -> Result<(), Error> {
        self.map().flush().map_err(Error::Flush)?;
        Ok(())
    }

    pub(crate) fn flush_range(&self, start: usize, end: usize) -> Result<(), Error> {
        assert!(start <= end);
        if start == end {
            return Ok(());
        }
        self.map().flush_range(start, end - start).map_err(Error::Flush)?;
        Ok(())
    }
}
