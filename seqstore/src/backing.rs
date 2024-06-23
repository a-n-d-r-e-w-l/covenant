use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct Backing(BackingInner);

#[derive(Debug)]
enum BackingInner {
    File { file: fs_err::File, map: memmap2::MmapMut },
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
    pub fn new_file(file: fs_err::File) -> anyhow::Result<Self> {
        let map = unsafe { memmap2::MmapMut::map_mut(&file)? };
        Ok(Backing(BackingInner::File { map, file }))
    }

    pub fn new_anon() -> anyhow::Result<Self> {
        Ok(Backing(BackingInner::Anon(memmap2::MmapMut::map_anon(256)?)))
    }

    fn resize_to(&mut self, size: usize) -> anyhow::Result<()> {
        match &mut self.0 {
            BackingInner::File { file, map } => {
                file.set_len(size as u64)?;
                unsafe { map.remap(size, memmap2::RemapOptions::new().may_move(true))? };
            }
            BackingInner::Anon(map) => {
                unsafe { map.remap(size, memmap2::RemapOptions::new().may_move(true))? };
            }
        }
        Ok(())
    }

    pub fn write(&mut self, b: &[u8], position: &mut usize) -> anyhow::Result<()> {
        let req = *position + b.len();
        self.resize_for(req)?;
        let target = &mut self[*position..*position + b.len()];
        target.copy_from_slice(b);
        *position += b.len();
        Ok(())
    }

    fn resize_for(&mut self, len: usize) -> anyhow::Result<()> {
        if self.len() <= len {
            self.resize_to(((len / 256) + 1) * 256)?;
        }
        Ok(())
    }

    pub fn map(&self) -> &memmap2::MmapMut {
        match &self.0 {
            BackingInner::File { map, .. } => map,
            BackingInner::Anon(map) => map,
        }
    }

    pub fn flush(&self) -> anyhow::Result<()> {
        self.map().flush()?;
        Ok(())
    }

    pub fn flush_range(&self, start: usize, end: usize) -> anyhow::Result<()> {
        assert!(start <= end);
        if start == end {
            return Ok(());
        }
        self.map().flush_range(start, end - start)?;
        Ok(())
    }
}
