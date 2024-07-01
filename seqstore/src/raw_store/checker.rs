use std::{
    fmt::{Debug, Formatter},
    hash::Hash,
};

use bstr::{BStr, BString, ByteSlice};
use indexmap::IndexMap;
use log::trace;
use thiserror::Error;

use crate::{backing::Backing, id::PackedId, raw_store::RawStore, Id};

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum CheckItem<'a, N> {
    Add(N, &'a [u8]),
    Remove(N),
    Check(N),
    CheckAll,
    Debug,
    Print,
}

impl<N: Debug> Debug for CheckItem<'_, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add(name, bytes) => f.debug_tuple("Add").field(name).field(&BStr::new(bytes)).finish(),
            Self::Remove(name) => f.debug_tuple("Remove").field(name).finish(),
            Self::Check(name) => f.debug_tuple("Check").field(name).finish(),
            Self::CheckAll => f.debug_tuple("CheckAll").finish(),
            Self::Debug => f.debug_tuple("Debug").finish(),
            Self::Print => f.debug_tuple("Print").finish(),
        }
    }
}

#[derive(Debug)]
pub struct Checker<N> {
    check: IndexMap<PackedId, Vec<u8>>,
    names: IndexMap<N, Id>,
    map: RawStore,
}

impl<N: Hash + Eq + Debug + Copy> Checker<N> {
    pub fn new(file: Backing) -> Result<Self, CheckerError> {
        Ok(Self {
            check: IndexMap::new(),
            names: IndexMap::new(),
            map: RawStore::options().exact_spec_magic(b"checker").new(file)?,
        })
    }

    pub fn execute(&mut self, item: CheckItem<N>) -> Result<Option<Id>, CheckerError> {
        match item {
            CheckItem::Add(name, bytes) => {
                let at = self.map.add(bytes)?;
                assert!(self.names.insert(name, at).is_none());
                assert!(self.check.insert(at.pack(), bytes.to_vec()).is_none());
                // debug!("{name:?} stored at {at}");
                Ok(Some(at))
            }
            CheckItem::Remove(name) => {
                // debug!("removing {name:?}");
                let at = self.names.swap_remove(&name).expect("removing name that was never inserted");
                let check = self.check.swap_remove(&at.pack()).expect("removing location that was never added");
                let stored = self.map.remove(at, ToOwned::to_owned)?;
                if check != stored {
                    Err(CheckerError::Mismatch {
                        expected: BString::new(check),
                        found: BString::new(stored),
                    })
                } else {
                    Ok(Some(at))
                }
            }
            CheckItem::Check(name) => {
                let at = *self.names.get(&name).expect("checking name that was never inserted");
                let check = self.check.get(&at.pack()).expect("checking location that was never added");
                let stored = self.map.get(at, ToOwned::to_owned)?;
                if *check != stored {
                    Err(CheckerError::Mismatch {
                        expected: BString::new(check.to_owned()),
                        found: BString::new(stored),
                    })
                } else {
                    Ok(None)
                }
            }
            CheckItem::CheckAll => {
                self.check_all()?;
                Ok(None)
            }
            CheckItem::Debug => {
                crate::raw_store::debug_map(&self.map.backing)?;
                Ok(None)
            }
            CheckItem::Print => {
                self.map.with_bytes(|b| trace!("{:?}", BStr::new(b.trim_end_with(|b| b == '\0'))));
                Ok(None)
            }
        }
    }

    pub fn reopen(&mut self) -> Result<(), CheckerError> {
        let map = std::mem::replace(&mut self.map, RawStore::options().new(Backing::new_anon()?)?);
        let backing = map.close()?;
        let map = RawStore::options().exact_spec_magic(b"checker").open(backing)?;
        self.map = map;
        Ok(())
    }

    pub fn check_all(&mut self) -> Result<(), CheckerError> {
        for name in self.names.keys().copied().collect::<Vec<_>>() {
            self.execute(CheckItem::Check(name))?;
        }
        Ok(())
    }

    pub fn map(&self) -> &RawStore {
        &self.map
    }

    pub fn names(&self) -> impl ExactSizeIterator<Item = N> + '_ {
        self.names.keys().copied()
    }

    pub fn keys(&self) -> impl ExactSizeIterator<Item = Id> + '_ {
        self.check.keys().copied().map(Id::from_packed)
    }
}

#[derive(Debug, Error)]
pub enum CheckerError {
    #[error(transparent)]
    Map(#[from] crate::error::Error),
    #[error(transparent)]
    Open(#[from] crate::error::OpenError),
    #[error("mismatch: expected {:?}, found {:?}", .expected, .found)]
    Mismatch { expected: BString, found: BString },
    #[error(transparent)]
    Other(#[from] std::io::Error),
}
