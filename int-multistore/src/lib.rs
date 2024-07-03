use std::{num::NonZeroU64, path::PathBuf};

use bytes::Bytes;
use seqstore::Backing;
pub mod ints_store;

#[derive(Debug)]
pub struct Lookup {
    fsts: phobos::Database,
    lookup: ints_store::IntsStore,
    dir: PathBuf,
    name: String,
}

impl Lookup {
    pub unsafe fn new(dir: PathBuf, name: &str) -> anyhow::Result<Self> {
        let lookup_file = fs_err::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join(file_name(name)))?;
        let backing = unsafe { Backing::new_file(lookup_file.into_parts().0) }?;
        let lookup = ints_store::IntsStore::new(backing)?;
        let opts = phobos::Database::builder(dir.clone(), name.to_owned()).create(true);
        let fsts = unsafe { opts.open() }?;
        Ok(Self {
            fsts,
            lookup,
            dir,
            name: name.to_owned(),
        })
    }

    pub unsafe fn open(dir: PathBuf, name: &str) -> anyhow::Result<Self> {
        let lookup_file = fs_err::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(dir.join(file_name(name)))?;
        let backing = unsafe { Backing::new_file(lookup_file.into_parts().0) }?;
        let lookup = ints_store::IntsStore::open(backing)?;
        let opts = phobos::Database::builder(dir.clone(), name.to_owned()).create(false);
        let fsts = unsafe { opts.open() }?;
        Ok(Self {
            fsts,
            lookup,
            dir,
            name: name.to_owned(),
        })
    }

    pub fn cleanup(&mut self) -> anyhow::Result<()> {
        let write_path = self.dir.join(format!(".{}.lkp~", self.name));
        let new_file = fs_err::OpenOptions::new().read(true).write(true).create(true).open(&write_path)?;
        let working = unsafe { Backing::new_file(new_file.into_parts().0) }?;
        let mut filter = self.lookup.filter(working)?;
        self.fsts.merge(|_, id| {
            if let Some(id) = ints_store::Idx::new(id) {
                filter.add(id.as_id())?;
            }
            Ok(())
        })?;
        filter.finish()?;
        let old = std::mem::replace(&mut self.lookup, ints_store::IntsStore::new(Backing::new_anon()?)?);
        // We want to _explicitly_ drop this here before moving files around on disk
        // to make it clear that the safety requirements are met. (simply discarding it would
        // still uphold the requirements, but it's better to be explicit)
        drop(old.close()?);

        let active_path = self.dir.join(file_name(&self.name));
        fs_err::rename(&write_path, &active_path)?;

        let file = fs_err::OpenOptions::new().read(true).write(true).create(false).open(active_path)?;
        let new = unsafe { Backing::new_file(file.into_parts().0) }?;
        self.lookup = ints_store::IntsStore::open(new)?;
        Ok(())
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

fn file_name(name: &str) -> String {
    format!("{name}.lkp")
}
