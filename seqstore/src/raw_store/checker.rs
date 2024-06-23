use std::{
    fmt::{Debug, Formatter},
    hash::Hash,
};

use bstr::{BStr, BString, ByteSlice};
use indexmap::IndexMap;
use log::trace;
use thiserror::Error;

use crate::{backing::Backing, raw_store::RawStore};

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
    check: IndexMap<u64, Vec<u8>>,
    names: IndexMap<N, u64>,
    map: RawStore,
}

impl<N: Hash + Eq + Debug + Copy> Checker<N> {
    pub fn new(file: Backing) -> Result<Self, CheckerError> {
        Ok(Self {
            check: IndexMap::new(),
            names: IndexMap::new(),
            map: RawStore::new(file)?,
        })
    }

    pub fn execute(&mut self, item: CheckItem<N>) -> Result<(), CheckerError> {
        match item {
            CheckItem::Add(name, bytes) => {
                let at = self.map.add(bytes)?;
                assert!(self.names.insert(name, at).is_none());
                assert!(self.check.insert(at, bytes.to_vec()).is_none());
                // debug!("{name:?} stored at {at}");
                Ok(())
            }
            CheckItem::Remove(name) => {
                // debug!("removing {name:?}");
                let at = self.names.swap_remove(&name).expect("removing name that was never inserted");
                let check = self.check.swap_remove(&at).expect("removing location that was never added");
                let stored = self.map.remove(at)?;
                if check != stored {
                    Err(CheckerError::Mismatch {
                        expected: BString::new(check),
                        found: BString::new(stored),
                    })
                } else {
                    Ok(())
                }
            }
            CheckItem::Check(name) => {
                let at = *self.names.get(&name).expect("checking name that was never inserted");
                let check = self.check.get(&at).expect("checking location that was never added");
                let stored = self.map.get(at)?;
                if *check != stored {
                    Err(CheckerError::Mismatch {
                        expected: BString::new(check.to_owned()),
                        found: BString::new(stored),
                    })
                } else {
                    Ok(())
                }
            }
            CheckItem::CheckAll => self.check_all(),
            CheckItem::Debug => crate::debug_map(&self.map).map_err(Into::into),
            CheckItem::Print => {
                self.map.with_bytes(|b| trace!("{:?}", BStr::new(b.trim_end_with(|b| b == '\0'))));
                Ok(())
            }
        }
    }

    pub fn reopen(&mut self) -> Result<(), CheckerError> {
        let map = std::mem::replace(&mut self.map, RawStore::new(Backing::new_anon()?)?);
        let backing = map.close()?;
        let map = RawStore::open(backing)?;
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

    pub fn keys(&self) -> impl ExactSizeIterator<Item = u64> + '_ {
        self.check.keys().copied()
    }
}

#[derive(Debug, Error)]
pub enum CheckerError {
    #[error(transparent)]
    Map(#[from] crate::error::Error),
    #[error("mismatch: expected {:?}, found {:?}", .expected, .found)]
    Mismatch { expected: BString, found: BString },
    #[error(transparent)]
    Other(#[from] std::io::Error),
}