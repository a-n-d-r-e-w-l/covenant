use std::{num::NonZeroU64, path::PathBuf};

use bytes::Bytes;
use seqstore::Backing;
pub mod ints_store;

#[derive(Debug)]
pub struct Lookup {
    fsts: phobos::Database,
    lookup: ints_store::IntsStore,
}

impl Lookup {
    pub unsafe fn new(dir: PathBuf, name: &str) -> anyhow::Result<Self> {
        let lookup_file = fs_err::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join(format!("{name}.lkp")))?;
        let backing = unsafe { Backing::new_file(lookup_file.into_parts().0) }?;
        let lookup = ints_store::IntsStore::new(backing)?;
        let opts = phobos::Database::builder(dir, name.to_owned()).create(true);
        let fsts = unsafe { opts.open() }?;
        Ok(Self { fsts, lookup })
    }

    pub unsafe fn open(dir: PathBuf, name: &str) -> anyhow::Result<Self> {
        let lookup_file = fs_err::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(dir.join(format!("{name}.lkp")))?;
        let backing = unsafe { Backing::new_file(lookup_file.into_parts().0) }?;
        let lookup = ints_store::IntsStore::open(backing)?;
        let opts = phobos::Database::builder(dir, name.to_owned()).create(false);
        let fsts = unsafe { opts.open() }?;
        Ok(Self { fsts, lookup })
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.fsts.flush()?;
        Ok(())
    }

    pub fn close(mut self) -> anyhow::Result<()> {
        self.flush()?;
        Ok(())
    }

    pub fn get_idx(&self, hash: &[u8]) -> Option<ints_store::Idx> {
        self.fsts.get(hash).and_then(ints_store::Idx::new)
    }

    pub fn get(&self, idx: ints_store::Idx) -> anyhow::Result<impl Iterator<Item = NonZeroU64>> {
        self.lookup.get(idx)
    }

    pub fn insert(&mut self, idx: ints_store::Idx, hash: &[u8], id: NonZeroU64) -> anyhow::Result<ints_store::Idx> {
        let old = idx.clone();
        let new = self.lookup.insert(idx, id)?;
        self.fsts.set(Bytes::copy_from_slice(hash), new.get())?;
        // From here, the hash can be used to find `id`.
        // Delete the old data - note that if this fails, then cleanup will not copy the old
        // data over as it is no longer referenced.
        self.lookup.remove(old)?;
        Ok(ints_store::Idx::from_packed(new))
    }

    pub fn set(&mut self, hash: &[u8], id: NonZeroU64) -> anyhow::Result<ints_store::Idx> {
        let id = self.lookup.set(id)?;
        self.fsts.set(Bytes::copy_from_slice(hash), id.get())?;
        Ok(ints_store::Idx::from_packed(id))
    }
}
