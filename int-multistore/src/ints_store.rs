use std::{io::Cursor, num::NonZeroU64};

use seqstore::{
    error::Error,
    raw_store::{OpenStoreOptions, RawStore, RecoveryStrategy},
    Backing,
};
use varuint::{ReadVarint, VarintSizeHint, WriteVarint};

#[derive(Debug)]
pub struct IntsStore(RawStore);

impl IntsStore {
    fn create<'a, E: Into<anyhow::Error>>(
        backing: Backing, f: impl FnOnce(OpenStoreOptions<'a>, Backing) -> Result<RawStore, E>,
    ) -> anyhow::Result<Self> {
        let op = RawStore::options()
            .exact_spec_magic(b"[varNZu64]")
            .recovery_strategy(RecoveryStrategy::Rollback);
        f(op, backing).map(Self).map_err(Into::into)
    }

    pub fn new(backing: Backing) -> anyhow::Result<Self> {
        Self::create(backing, OpenStoreOptions::new)
    }

    pub fn open(backing: Backing) -> anyhow::Result<Self> {
        Self::create(backing, OpenStoreOptions::open)
    }

    pub fn get(&self, idx: Idx) -> anyhow::Result<impl Iterator<Item = NonZeroU64>> {
        self.0.get(idx.0, Stored::load).map(Stored::items).map_err(Into::into)
    }

    pub fn remove(&mut self, idx: Idx) -> anyhow::Result<()> {
        self.0.remove(idx.0, |_| {})?;
        Ok(())
    }

    pub fn set(&mut self, n: NonZeroU64) -> anyhow::Result<seqstore::PackedId> {
        let bytes = Stored::single(n).to_bytes();
        let id = self.0.add(&bytes)?;
        Ok(id.pack())
    }

    pub fn insert(&mut self, idx: Idx, n: NonZeroU64) -> anyhow::Result<seqstore::PackedId> {
        self.insert_many(idx, std::iter::once(n))
    }

    pub fn insert_many(&mut self, idx: Idx, ns: impl IntoIterator<Item = NonZeroU64>) -> anyhow::Result<seqstore::PackedId> {
        let mut stored = self.0.get(idx.0, Stored::load)?;
        stored.extend(ns);
        let bytes = stored.to_bytes();
        let id = self.0.add(&bytes)?;
        Ok(id.pack())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Idx(seqstore::Id);

impl Idx {
    pub(crate) fn new(n: u64) -> Option<Self> {
        seqstore::PackedId::new(n).map(seqstore::Id::from_packed).map(Self)
    }

    pub(crate) fn from_packed(n: seqstore::PackedId) -> Self {
        Self(seqstore::Id::from_packed(n))
    }
}

#[derive(Debug)]
pub(crate) struct Stored {
    items: Vec<NonZeroU64>,
    byte_length: usize,
}

impl Stored {
    fn single(n: NonZeroU64) -> Self {
        Self {
            items: vec![n],
            byte_length: VarintSizeHint::varint_size(n.get()),
        }
    }

    fn load(b: &[u8]) -> Self {
        let mut items = Vec::with_capacity(b.len() / 2);
        let mut pos = 0;
        let mut byte_length = 0;
        while pos < b.len() {
            let s = pos;
            let n = read_varint::<u64>(b, &mut pos).unwrap(); // TODO: Error
            let l = pos - s;
            if let Some(n) = NonZeroU64::new(n) {
                items.push(n);
                byte_length += l;
            }
        }
        Self { items, byte_length }
    }

    fn items(self) -> impl Iterator<Item = NonZeroU64> {
        self.items.into_iter()
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut v = vec![0; self.byte_length];
        let mut pos = 0;
        for &it in &self.items {
            write_varint(it.get(), &mut v, &mut pos);
        }
        v
    }
}

impl Extend<NonZeroU64> for Stored {
    fn extend<T: IntoIterator<Item = NonZeroU64>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        self.items.reserve(iter.size_hint().0);
        for item in iter {
            if let Err(idx) = self.items.binary_search(&item) {
                self.items.insert(idx, item);
                self.byte_length += VarintSizeHint::varint_size(item.get());
            }
        }
    }
}

pub(crate) fn write_varint<N: VarintSizeHint>(n: N, buffer: &mut [u8], position: &mut usize)
where
    for<'a> Cursor<&'a mut [u8]>: WriteVarint<N>,
{
    let mut cur = Cursor::new(buffer);
    cur.set_position(*position as u64);
    match cur.write_varint(n) {
        Ok(_) => *position = cur.position() as usize,
        Err(_) => unreachable!(),
    }
}

pub(crate) fn read_varint<T>(buffer: &[u8], position: &mut usize) -> Result<T, Error>
where
    for<'a> Cursor<&'a [u8]>: ReadVarint<T>,
{
    let mut cur = Cursor::new(buffer);
    cur.set_position(*position as u64);
    match cur.read_varint() {
        Ok(n) => {
            *position = cur.position() as usize;
            Ok(n)
        }
        Err(_) => Err(Error::InvalidVarint { position: *position }),
    }
}
