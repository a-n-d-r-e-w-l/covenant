use std::num::NonZeroU64;

pub use backing::Backing;

pub(crate) mod backing;
pub(crate) mod tag;
pub(crate) mod util;

pub mod error;
pub mod raw_store;

/// An opaque ID that serves as an index into a lookup.
///
/// While it implements [`Ord`], the comparison between two instances of `Id` is not guaranteed to
/// have semantic meaning - this `impl` is here simply to allow usage in a [BTreeMap][`std::collections::BTreeMap`]
/// or similar.
///
#[cfg_attr(
    feature = "serde",
    doc = "Similarly, while `Id` implements [`Serialize`][serde::Serialize] and\
    [`Deserialize`][serde::Deserialize], the serialized representation is intended to be opaque - \
    attempting to construct an `Id` from scratch via [`Deserialize`][serde::Deserialize] should not be done."
)]
///
/// It is **not** guaranteed that a direct conversion to [`u64`] will yield the location of the
/// corresponding item in the file.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
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

// We want this to be accessible to all maps (if/when we have a private trait on all maps)
// but we don't want to make the fields of `raw_store::RawStore` pub(crate)
/// A function that can describe the contents of a [`RawStore`][raw_store::RawStore], intended for debugging
/// when working on this crate itself.
///
/// Requires a [`log`]-compatible logger to be setup.
#[cfg(feature = "debug_map")]
pub fn debug_map(map: &raw_store::RawStore) -> Result<(), error::Error> {
    raw_store::debug_map(map)
}
