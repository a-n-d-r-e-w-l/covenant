use std::{
    fs::File,
    ops::{Deref, DerefMut},
};

use crate::error::Error;

#[derive(Debug)]
pub struct Backing(BackingInner);

#[derive(Debug)]
enum BackingInner {
    File { file: File, map: memmap2::MmapMut },
    Anon(memmap2::MmapMut),
}

impl Deref for Backing {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            BackingInner::File { map, .. } => map,
            BackingInner::Anon(map) => map,
        }
    }
}

impl DerefMut for Backing {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.0 {
            BackingInner::File { map, .. } => map,
            BackingInner::Anon(map) => map,
        }
    }
}

impl Backing {
    pub fn new_file(file: File) -> Result<Self, Error> {
        let map = unsafe { memmap2::MmapMut::map_mut(&file).map_err(Error::Map)? };
        Ok(Backing(BackingInner::File { map, file }))
    }

    pub fn new_anon() -> Result<Self, Error> {
        Ok(Backing(BackingInner::Anon(
            memmap2::MmapMut::map_anon(256).map_err(Error::Map)?,
        )))
    }

    pub fn write(&mut self, b: &[u8], position: &mut usize) -> Result<(), Error> {
        let req = *position + b.len();
        self.resize_for(req)?;
        let target = &mut self[*position..*position + b.len()];
        target.copy_from_slice(b);
        *position += b.len();
        Ok(())
    }

    fn resize_for(&mut self, len: usize) -> Result<(), Error> {
        if self.len() <= len {
            self.resize_to(((len / 256) + 1) * 256)?;
        }
        Ok(())
    }

    fn resize_to(&mut self, size: usize) -> Result<(), Error> {
        match &mut self.0 {
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

    pub fn map(&self) -> &memmap2::MmapMut {
        match &self.0 {
            BackingInner::File { map, .. } => map,
            BackingInner::Anon(map) => map,
        }
    }

    pub fn flush(&self) -> Result<(), Error> {
        self.map().flush().map_err(Error::Flush)?;
        Ok(())
    }

    pub fn flush_range(&self, start: usize, end: usize) -> Result<(), Error> {
        assert!(start <= end);
        if start == end {
            return Ok(());
        }
        self.map().flush_range(start, end - start).map_err(Error::Flush)?;
        Ok(())
    }
}
