use std::{
    cmp::Ordering,
    fmt::{Debug, Formatter},
    num::NonZeroU64,
};

use crate::error::Error;

/// An opaque ID that serves as an index into a lookup.
///
#[cfg_attr(
    feature = "serde",
    doc = "It is worth noting that, while `Id` implements [`Serialize`][serde::Serialize] and\
    [`Deserialize`][serde::Deserialize], the serialized representation is intended to be opaque - \
    attempting to construct an `Id` from scratch via [`Deserialize`][serde::Deserialize] should not be done."
)]
#[derive(Copy, Clone, PartialEq, Hash)]
pub struct Id {
    at: usize,
    marker: u8,
}

impl Id {
    pub(crate) fn new(at: usize, length: usize) -> Self {
        Self {
            at,
            marker: Self::marker(length),
        }
    }

    pub(crate) fn at(&self) -> usize {
        self.at
    }

    pub(crate) fn verify(&self, length: u64) -> Result<(), Error> {
        if Self::marker(length as _) == self.marker {
            Ok(())
        } else {
            Err(Error::IdCheck(*self))
        }
    }

    fn marker(length: usize) -> u8 {
        // We want this to be sensitive to any change in length, but only have 1 byte to work with
        let lz = ((length.leading_zeros() >> 1).ilog2() & 0b11) as u8; // Wrapped magnitude
        let first = (length >> 61_u32.saturating_sub(length.leading_zeros())) as u8; // 2nd and 3rd bits
                                                                                     // (first would always be 1)
        let last = (length & 0b11) as u8;
        let r = length.to_ne_bytes().iter().copied().reduce(std::ops::BitXor::bitxor).unwrap();
        let reduce = (r & 0b11) ^ ((r & 0b1100) >> 2) ^ ((r & 0b110000) >> 4) ^ ((r & 0b11000000) >> 6);
        lz | (first << 2) | (last << 4) | (reduce) << 6
    }

    // TEMP: Only required due to restrictions on `retain`
    pub fn file_sort(a: &Self, b: &Self) -> Ordering {
        let o = a.at.cmp(&b.at);
        if matches!(o, Ordering::Equal) {
            assert_eq!(a.marker, b.marker)
        }
        o
    }

    pub fn pack(self) -> PackedId {
        PackedId::from_parts(self.at, self.marker)
    }

    pub fn from_packed(packed: PackedId) -> Self {
        let (at, length) = PackedId::unpack(packed);
        Self { at, marker: length }
    }
}

impl Debug for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Id").finish_non_exhaustive()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PackedId(NonZeroU64);

impl PackedId {
    pub fn new(n: u64) -> Option<Self> {
        NonZeroU64::new(n).map(Self)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }

    fn from_parts(at: usize, marker: u8) -> Self {
        if at.leading_zeros() < 8 {
            // No data can be packed into this Id, and there is no way to distinguish it from
            // 1 byte less + packed.
            // This would only happen with an attempt to index into the ~Petabyte range, so it's
            // not a huge loss
            panic!("too big");
        }
        Self(NonZeroU64::new(((at as u64) << 8) | (marker as u64)).expect("at least one bit is set"))
    }

    fn unpack(self) -> (usize, u8) {
        let s = self.0.get() as usize;
        let marker = (s & 0xFF) as u8;
        (s >> 8, marker)
    }
}

#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
const _: () = {
    impl serde::Serialize for Id {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            <PackedId as serde::Serialize>::serialize(&self.pack(), serializer)
        }
    }

    impl<'de> serde::Deserialize<'de> for Id {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            <PackedId as serde::Deserialize<'de>>::deserialize(deserializer).map(Self::from_packed)
        }
    }

    impl serde::Serialize for PackedId {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            <NonZeroU64 as serde::Serialize>::serialize(&self.0, serializer)
        }
    }

    impl<'de> serde::Deserialize<'de> for PackedId {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            <NonZeroU64 as serde::Deserialize<'de>>::deserialize(deserializer).map(Self)
        }
    }
};

#[cfg(test)]
mod tests {
    use super::*;

    const POSITIONS: &[u64] = &[1, 127, 128, 255, 256, 1 << 16, 1 << 24, 1 << 32, 1 << 40, 1 << 48, 1 << 55];

    #[test]
    fn roundtrip_pack() {
        for &position in POSITIONS {
            for length in 0..u8::MAX {
                let id = Id::new(position as _, length as _);
                let t = Id::from_packed(id.pack());
                assert_eq!(id, t);
            }
        }
    }

    #[test]
    fn verify_correct() {
        for &position in POSITIONS {
            for length in 0..u8::MAX {
                let id = Id::new(position as _, length as _);
                let r = id.verify(length as _);
                assert!(r.is_ok(), "{:?}", r.unwrap_err());
            }
        }
    }

    #[test]
    fn leading_zeros() {
        for &position in POSITIONS {
            for length in 0..u8::MAX {
                let id = Id::new(position as _, length as _);
                let o = position.leading_zeros();
                let p = id.pack().0.leading_zeros();
                assert!(p >= o - 8); // Ensure that we have lost a _maximum_ of 1 byte to marker
            }
        }
    }

    #[test]
    #[should_panic(expected = "too big")]
    fn too_big_to_pack() {
        PackedId::from_parts(1 << 56, 0);
    }
}
