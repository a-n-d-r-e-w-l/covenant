use std::fmt::Debug;

use super::{Gap, RawStore};
use crate::{
    error::{Error, OpenError},
    tag::MagicTag,
    Backing,
};

/// Options to open a [`RawStore`] with.
///
/// Typically created with [`options(..)`][RawStore::options], but it also has a [`Default`] `impl`.
///
/// See [`new`][Self::new] and [`open`][Self::open] for creating a store and opening an existing one
/// respectively.
#[derive(Debug)]
pub struct OpenStoreOptions<'a> {
    spec_magic: &'a [u8],
    recovery_strategy: RecoveryStrategy,
}

/// Methods that consume [`self`][Self] to open or create a [store][RawStore].
impl OpenStoreOptions<'_> {
    /// Create a new store.
    #[allow(clippy::new_ret_no_self, clippy::wrong_self_convention)]
    pub fn new(self, backing: Backing) -> Result<RawStore, Error> {
        RawStore::new(backing, self.spec_magic)
    }

    /// Attempts to open an existing store.
    ///
    /// This will fail if the header could not be parsed, or if the map contains invalid data
    /// that could not be recovered from.
    pub fn open(self, backing: Backing) -> Result<RawStore, OpenError> {
        RawStore::open(backing, self)
    }
}

/// Methods that allow configuring behaviour when opening a store.
///
/// # Header specialization
///
/// As mentioned in [`RawStore`'s type-level docs][RawStore], the map is "raw". To allow for other,
/// more specialized maps built on top of this to detect if the wrong _kind_ of specialized
/// map has written the file, you can specify "specialization magic bytes" that get included in
/// the file header, and must match to be able to load it.
///
/// These are called "specialization
/// [magic bytes](https://en.wikipedia.org/wiki/File_format#Magic_number)" (or simply "spec magic").
/// Spec magic can be written as any arbitrary byte sequence - though should be kept reasonably
/// short - and is _currently_ checked for exact equality upon opening.[^1]
///
/// Note that it is not possible to change a given store's spec magic after initial creation, even
/// by closing anr reopening it.
///
/// [^1]: Later, an option may be added to allow for a `FnOnce(&[u8]) -> bool` or similar to be used
/// as a spec magic checker.
// TODO: Notes here
impl<'a> OpenStoreOptions<'a> {
    /// Do not use "spec magic" (see above).
    ///
    /// Equivalent to [`self.exact_spec_magic(b"")`][Self::exact_spec_magic].
    pub fn no_spec_magic(self) -> Self {
        Self { spec_magic: b"", ..self }
    }

    /// Set the "spec magic" (see above) bytes.
    ///
    /// Using `b""` is equivalent to [`self.no_spec_magic()`][Self::no_spec_magic].
    pub fn exact_spec_magic(self, expected: &'a [u8]) -> Self {
        Self {
            spec_magic: expected,
            ..self
        }
    }

    /// Sets the recovery strategy used when encountering invalid/unexpected data during opening.
    ///
    /// Defaults to [`RecoveryStrategy::Error`] _i.e._ return an error if something is wrong.
    pub fn recovery_strategy(self, strategy: RecoveryStrategy) -> Self {
        Self {
            recovery_strategy: strategy,
            ..self
        }
    }
}

impl<'a> Default for OpenStoreOptions<'a> {
    fn default() -> Self {
        RawStore::options()
    }
}

/// How to recover from unexpected data (or lack thereof) when opening a store.
#[derive(Debug, Copy, Clone, Default)]
pub enum RecoveryStrategy {
    /// Return an error from [`open(..)`][OpenStoreOptions::open], allowing you to detect data
    /// corruption.
    #[default]
    Error,
    /// Rollback the store to a known good state.
    ///
    /// This will delete partially-written data (_i.e._ data for which the [`add(..)`][RawStore::add]
    /// call did not return).
    ///
    /// This will add an end tag to the end of the backing if not present.
    Rollback,
}

impl RawStore {
    /// Returns a builder for specifying options to open/create a [`RawStore`];
    pub fn options() -> OpenStoreOptions<'static> {
        OpenStoreOptions {
            spec_magic: b"",
            recovery_strategy: RecoveryStrategy::Error,
        }
    }

    fn new(backing: Backing, spec_magic: &[u8]) -> Result<Self, Error> {
        let mut backing = backing.0;
        // TODO: Error if nonempty
        let mut position = 0;
        backing.write(Self::HEADER_MAGIC, &mut position)?; // magic bytes
        backing.write(&Self::HEADER_VERSION, &mut position)?; // header version
        debug_assert_eq!(position, Self::HEADER_LENGTH);
        crate::util::write_varint_backing(spec_magic.len() as u64, &mut backing, &mut position)?;
        backing.write(spec_magic, &mut position)?;
        let header_length = position;
        MagicTag::End.write(&mut backing, &mut position)?;
        backing.flush()?;
        Ok(Self {
            backing,
            end: header_length,
            gaps: vec![],
            header_length,
        })
    }

    fn open(backing: Backing, options: OpenStoreOptions<'_>) -> Result<Self, OpenError> {
        let mut backing = backing.0;
        let spec_var_len = <u64 as varuint::VarintSizeHint>::varint_size(options.spec_magic.len() as _);
        let h_len = Self::HEADER_LENGTH + spec_var_len + options.spec_magic.len();
        if backing.len() < h_len {
            return Err(OpenError::TooSmall {
                found: backing.len(),
                expected: h_len,
            });
        }
        let header = &backing[..h_len];
        if &header[..Self::HEADER_MAGIC.len()] != Self::HEADER_MAGIC {
            return Err(OpenError::Magic);
        }
        let mut hpos = Self::HEADER_MAGIC.len();
        let v: [u8; 2] = (&header[hpos..hpos + Self::HEADER_VERSION.len()]).try_into().unwrap();
        if v != Self::HEADER_VERSION {
            return Err(OpenError::UnknownVersion(v));
        }
        hpos += Self::HEADER_VERSION.len();

        let s = crate::util::read_varint::<u64>(&backing, &mut hpos)?;
        if s as usize != options.spec_magic.len() {
            return Err(OpenError::SpecMagicLen {
                found: s as usize,
                expected: options.spec_magic.len(),
            });
        }
        if &backing[hpos..hpos + s as usize] != options.spec_magic {
            return Err(OpenError::SpecMagic {
                found: bstr::BString::new(backing[hpos..hpos + s as usize].to_owned()),
                expected: bstr::BString::new(options.spec_magic.to_owned()),
            });
        }
        hpos += s as usize;

        // This should not be possible to hit, but is kept to ensure that the reading checks
        // are kept in line with changes to the header size
        assert_eq!(hpos, header.len());

        let mut pos = hpos;
        let mut end = None;
        let mut gaps = Vec::new();
        while pos < backing.len() {
            let here = pos;
            let tag = MagicTag::read(&backing, &mut pos)?;
            match tag {
                MagicTag::End => {
                    end = Some(here);
                    let rest = &backing[pos..];
                    if let Some((idx, b)) = rest.iter().copied().enumerate().find(|(_, b)| *b != 0) {
                        return Err(OpenError::DataAfterEnd {
                            end: here,
                            first_data_at: pos + idx,
                            first_data: b,
                        });
                    }
                    break;
                }
                MagicTag::Writing { length } => match options.recovery_strategy {
                    RecoveryStrategy::Error => {
                        return Err(OpenError::PartialWrite {
                            position: here,
                            length: length as usize,
                        });
                    }
                    RecoveryStrategy::Rollback => {
                        let tag_len = pos - here;
                        MagicTag::Deleted { length }.write_exact(&mut backing, &mut { here }, tag_len)?;
                        backing[pos..pos + length as usize].fill(0);
                        backing.flush_range(here, tag_len + length as usize)?;
                        gaps.push(Gap {
                            at: here,
                            length: length as u32,
                            tag_len: tag_len as u8,
                        });
                        pos += length as usize;
                    }
                },
                MagicTag::Written { length } => {
                    pos += length as usize;
                }
                MagicTag::Deleted { length } => {
                    gaps.push(Gap {
                        at: here,
                        length: length as u32,
                        tag_len: (pos - here) as u8,
                    });
                    pos += length as usize;
                }
            }
        }
        let end = if let Some(end) = end {
            end
        } else {
            match options.recovery_strategy {
                RecoveryStrategy::Error => return Err(OpenError::NoEnd),
                RecoveryStrategy::Rollback => {
                    let end = pos;
                    MagicTag::End.write(&mut backing, &mut pos)?;
                    end
                }
            }
        };

        Ok(Self {
            backing,
            end,
            gaps,
            header_length: h_len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait Byteable {
        fn write_len(&self) -> usize;

        fn write(&self, bytes: &mut [u8], position: &mut usize);
    }

    impl Byteable for [u8] {
        fn write_len(&self) -> usize {
            self.len()
        }

        fn write(&self, bytes: &mut [u8], position: &mut usize) {
            bytes[*position..*position + self.len()].copy_from_slice(self);
            *position += self.len();
        }
    }

    impl Byteable for u8 {
        fn write_len(&self) -> usize {
            1
        }

        fn write(&self, bytes: &mut [u8], position: &mut usize) {
            bytes[*position] = *self;
            *position += 1;
        }
    }

    impl<const N: usize> Byteable for [u8; N] {
        fn write_len(&self) -> usize {
            N
        }

        fn write(&self, bytes: &mut [u8], position: &mut usize) {
            bytes[*position..*position + N].copy_from_slice(self);
            *position += N;
        }
    }

    impl<T: Byteable + ?Sized> Byteable for &T {
        fn write_len(&self) -> usize {
            <T as Byteable>::write_len(self)
        }

        fn write(&self, bytes: &mut [u8], position: &mut usize) {
            <T as Byteable>::write(self, bytes, position)
        }
    }

    impl Byteable for MagicTag {
        fn write_len(&self) -> usize {
            self.written_length()
        }

        fn write(&self, bytes: &mut [u8], position: &mut usize) {
            self.write_buffer(bytes, position);
        }
    }

    macro_rules! prepare_raw {
        ($($e:expr),* $(,)?) => {{
            let l = 0 $(+ Byteable::write_len(&$e))*;
            let mut bytes = vec![0_u8; l];
            let mut pos = 0;
            $(
            Byteable::write(&$e, &mut bytes, &mut pos);
            )*
            Backing::new_from_buffer(&bytes).unwrap()
        }};
    }

    macro_rules! prepare {
        ($($e:expr),* $(,)?) => {prepare_raw!(HEADER, 0, $($e,)* MagicTag::End)};
    }

    const HEADER: &[u8] = b"\x1FPLFmap\x00\x00";

    #[test]
    fn test_header() {
        let empty = [0; RawStore::HEADER_LENGTH];
        for l in 0..=RawStore::HEADER_LENGTH {
            let backing = Backing::new_from_buffer(&empty[..l]).unwrap();
            let e = RawStore::open(backing, Default::default()).unwrap_err();
            assert!(matches!(e, OpenError::TooSmall {found, ..} if found == l));
        }

        let e = RawStore::open(prepare_raw!(HEADER, 0), Default::default()).unwrap_err();
        assert!(matches!(e, OpenError::NoEnd), "{e:?}");
        let e = RawStore::open(
            prepare_raw!(HEADER, 1, b"A"),
            OpenStoreOptions::default().exact_spec_magic(b"A"),
        )
        .unwrap_err();
        assert!(matches!(e, OpenError::NoEnd), "{e:?}");

        assert_eq!(HEADER, &prepare_raw!(b"\x1FPLFmap", [0, 0]).0[..]);
        let e = RawStore::open(prepare_raw!(RawStore::HEADER_MAGIC, [0, 0], 0), Default::default()).unwrap_err();
        assert!(matches!(e, OpenError::NoEnd), "{e:?}");
        let false_magic = b"\x1FPLfmap";
        let e = RawStore::open(prepare_raw!(false_magic, [0, 0], 0), Default::default()).unwrap_err();
        assert!(matches!(e, OpenError::Magic), "{e:?}");
        let e = RawStore::open(prepare_raw!(RawStore::HEADER_MAGIC, [1, 0], 0), Default::default()).unwrap_err();
        assert!(matches!(e, OpenError::UnknownVersion([1, 0])), "{e:?}");

        RawStore::open(prepare!(), Default::default()).unwrap();
    }

    #[test]
    fn partial_write() {
        let backing = || prepare!(MagicTag::Writing { length: 10 }, [b'a'; 10]);
        let e = RawStore::open(backing(), Default::default()).unwrap_err();
        assert!(
            matches!(
                e,
                OpenError::PartialWrite {
                    position: l,
                    length: 10
                } if l == RawStore::HEADER_LENGTH + 1
            ),
            "{e:?}"
        );
        let s = RawStore::open(
            backing(),
            OpenStoreOptions::default().recovery_strategy(RecoveryStrategy::Rollback),
        )
        .unwrap();
        assert_eq!(
            *s.gaps.first().unwrap(),
            Gap {
                at: (HEADER.len() + 1) as _,
                length: 10,
                tag_len: 2,
            }
        );
    }
}
