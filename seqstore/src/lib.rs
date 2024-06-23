use std::num::NonZeroU64;

pub use backing::Backing;
pub use error::{Error, OpenError};

pub(crate) mod backing;
pub(crate) mod tag;

mod error;
pub mod raw_store;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Id(NonZeroU64);

#[cfg(feature = "serde")]
const _: () = {
    impl serde::Serialize for Id {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            <NonZeroU64 as serde::Serialize>::serialize(&self.0, serializer)
        }
    }

    impl<'de> serde::Deserialize<'de> for Id {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            <NonZeroU64 as serde::Deserialize<'de>>::deserialize(deserializer).map(Self)
        }
    }
};

// We want this to be accessible to all maps (when we have a private trait on all maps)
// but we don't want to make the fields of `raw_map::FileMap` pub(crate)
#[cfg(feature = "debug_map")]
pub fn debug_map(map: &raw_store::RawStore) -> Result<(), Error> {
    raw_store::debug_map(map)
}
