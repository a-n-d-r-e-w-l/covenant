use std::{num::NonZeroU64, ops::ControlFlow};

use crate::{
    backing::{Backing, BackingInner},
    error::{Error, OpenError, RetainError},
    tag::MagicTag,
    Id,
};

#[cfg(feature = "debug_map")]
pub mod checker;

/// A "raw" `u64`-to-bytes store, either file-backed or entirely in memory.
///
/// This is "raw" in the sense that it makes no assumptions about what data is being stored, nor about
/// how that data will be retrieved. This makes it ideal to use to build other, more specialized
/// maps on top of, and less ergonomic to use directly.
///
/// If using [file-backed storage][Backing::new_file], a minimal amount of data is stored in memory,
/// and may potentially be reduced further in the future. No items are stored in memory.
// The path to reduction is by moving `Self.gaps` into another file-backed buffer, though given that
// Gap is fairly small (< 2*usize) this shouldn't be an issue for maps where either deletion is rare
// or new additions are common (as old gaps get filled) or both.
#[derive(Debug)]
pub struct RawStore {
    backing: BackingInner,
    end: usize,
    gaps: Vec<Gap>,
    header_length: usize,
}

impl RawStore {
    const HEADER_MAGIC: &'static [u8] = b"\x1FPLFmap";
    const HEADER_VERSION: [u8; 2] = [0x00, 0x00];
    const HEADER_LENGTH: usize = 9;

    // TODO: Unify `new` and `open` under OpenOptions, and properly document then (including a link
    //       back to Backing's safety requirements.
    /// # Header specialization
    ///
    /// As mentioned in the [type-level docs][Self], this map itself is "raw". To allow for other,
    /// more specialized maps built on top of this to detect if the wrong _kind_ of specialized
    /// map has written the file, you can specify "specialization magic bytes" that get included in
    /// the file header, and must match to be able to load it.
    pub fn new(backing: Backing, spec_magic: &[u8]) -> Result<Self, Error> {
        let mut backing = backing.0;
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

    /// # Header specialization
    ///
    /// Currently, the specialization magic bytes must match _exactly_ to be considered correct.
    /// This may be relaxed in the future to a `Fn(&[u8]) -> bool` or similar.
    pub fn open(backing: Backing, expected_spec_magic: &[u8]) -> Result<Self, OpenError> {
        let backing = backing.0;
        let spec_var_len = <u64 as varuint::VarintSizeHint>::varint_size(expected_spec_magic.len() as _);
        let h_len = Self::HEADER_LENGTH + spec_var_len + expected_spec_magic.len();
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
        if v[..] != Self::HEADER_VERSION {
            return Err(OpenError::UnknownVersion(v));
        }
        hpos += Self::HEADER_VERSION.len();

        let s = crate::util::read_varint::<u64>(&backing, &mut hpos)?;
        if s as usize != expected_spec_magic.len() {
            return Err(OpenError::SpecMagicLen {
                found: s as usize,
                expected: expected_spec_magic.len(),
            });
        }
        if &backing[hpos..hpos + s as usize] != expected_spec_magic {
            return Err(OpenError::SpecMagic {
                found: bstr::BString::new(backing[hpos..hpos + s as usize].to_owned()),
                expected: bstr::BString::new(expected_spec_magic.to_owned()),
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
                MagicTag::Writing { length } => {
                    return Err(OpenError::PartialWrite {
                        position: here,
                        length: length as usize,
                    });
                }
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
        let Some(end) = end else { return Err(OpenError::NoEnd) };

        Ok(Self {
            backing,
            end,
            gaps,
            header_length: h_len,
        })
    }

    /// Flush all outstanding changes and close the store.
    ///
    /// Returns the [`Backing`] used so that the store can be re-opened if desired.
    ///
    /// \*Technically*, while the [`Backing`] is not in active use after this returns, it is unwise
    /// to modify the underlying file until it drops. For more information about file safety, see
    /// [`Backing::new_file`]. This does not apply if the [`Backing`] was created using an anonymous map,
    /// as there is no underlying file to modify.
    pub fn close(self) -> Result<Backing, Error> {
        self.backing.flush()?;
        Ok(Backing(self.backing))
    }

    /// Store `bytes` and return the now-associated [`Id`].
    ///
    /// Currently, the maximum size of a single item is `134_217_727 B` (`= 128 MiB - 1 B`). This may
    /// change in the future, either by a factor of two down or to significantly higher, but such a
    /// change is unlikely.
    /// If storing items anywhere near that large, consider using this map as an index into some
    /// other storage solution better-suited to large items.
    ///
    /// # Panics
    ///
    /// Panics if attempting to store an item larger than `134_217_727 B`.
    pub fn add(&mut self, bytes: &[u8]) -> Result<Id, Error> {
        let (mut position, expected_tag, old_gap) = {
            fn satisfies_length(new: u32, old: u32) -> bool {
                new == old || new + 5 <= old
            }

            let required_length = MagicTag::Writing { length: bytes.len() as u64 }.written_length() + bytes.len();

            if let Some((idx, g)) = self
                .gaps
                .iter()
                .enumerate()
                .map(|(i, g)| (i, g.length + g.tag_len as u32))
                .filter(|(_, g)| satisfies_length(required_length as u32, *g))
                .take(8)
                .min_by_key(|(_, g)| *g)
            {
                let gap = self.gaps.swap_remove(idx);
                (
                    gap.at,
                    MagicTag::Deleted { length: gap.length as u64 },
                    if required_length as u32 == g { None } else { Some(gap) },
                )
            } else {
                (self.end, MagicTag::End, None)
            }
        };

        let existing_tag = MagicTag::read(&self.backing, &mut { position })?;
        assert_eq!(existing_tag, expected_tag);

        let start = position;
        MagicTag::Writing { length: bytes.len() as u64 }.write(&mut self.backing, &mut position)?;
        self.backing.write(bytes, &mut position)?;

        if let Some(old_gap) = old_gap {
            let total = old_gap.tag_len as usize + old_gap.length as usize;
            let used = position - start;
            let remaining = total - used;

            let (tag_len, new_len) = MagicTag::calc_tag_len(remaining);

            let new_at = position;
            MagicTag::Deleted { length: new_len as u64 }.write_exact(&mut self.backing, &mut position, tag_len as usize)?;
            position += new_len;
            assert_eq!(position, start + total);
            self.gaps.push(Gap {
                at: new_at,
                length: new_len as u32,
                tag_len,
            });
        }

        if expected_tag == MagicTag::End {
            self.end = position;
            MagicTag::End.write(&mut self.backing, &mut position)?;
        }
        let end = position;
        self.backing.flush_range(start, end)?;

        self.backing[start] ^= MagicTag::WRITING ^ MagicTag::WRITTEN;
        self.backing.map().flush_range(start, 1).map_err(Error::Flush)?;

        Ok(Id(NonZeroU64::new(start as u64).expect("cannot write to 0")))
    }

    /// Replaces the data stored at `at` with `with`, as long as the **new data has the same
    /// length as the old data**.
    ///
    /// `f` is given a view of the old data.
    pub fn replace<R>(&mut self, at: Id, with: &[u8], f: impl FnOnce(&[u8]) -> R) -> Result<R, Error> {
        let at = at.0.get() as usize;
        let mut position = at;
        let tag = MagicTag::read(&self.backing, &mut position)?;
        match tag {
            MagicTag::Written { length } => {
                if length as usize != with.len() {
                    Err(Error::MismatchedLengths {
                        position: at,
                        new: with.len(),
                        old: length as usize,
                    })
                } else {
                    let r = f(&self.backing[position..position + with.len()]);
                    self.backing[position..position + with.len()].copy_from_slice(with);
                    self.backing.map().flush_range(position, with.len()).map_err(Error::Flush)?;
                    Ok(r)
                }
            }
            MagicTag::Writing { .. } => Err(Error::EntryCorrupt { position: at }),
            MagicTag::Deleted { .. } => Err(Error::CannotReplaceDeleted { position: at }),
            other => Err(Error::IncorrectTag {
                position: at,
                found: other.into(),
                expected_kind: "Written",
            }),
        }
    }

    /// Gets the data stored at `at`, gives a view of it to `f`, and returns the result.
    ///
    /// `at` must be valid and correct _i.e._ in **this** map it must point to fully-written and
    /// non-deleted data.
    ///
    /// The only valid and correct [`Id`]s for this map are precisely those that have previously
    /// been returned by [`Self::add`] that have not subsequently been given to [`Self::remove`].
    /// Attempting to use an [`Id`] created through any other means is unwise, though will at worst
    /// result in a `panic!` or reception of `SIGBUS` (_i.e._ no UB), though returning
    /// bogus data is possible.
    pub fn get<R>(&self, at: Id, f: impl FnOnce(&[u8]) -> R) -> Result<R, Error> {
        let at = at.0.get() as usize;
        // TODO: Keys should include some marker to check the length to prevent overreads
        let mut position = at;
        let tag = MagicTag::read(&self.backing, &mut position)?;
        match tag {
            MagicTag::Writing { .. } => Err(Error::EntryCorrupt { position: at }),
            MagicTag::Written { length } => {
                let b = &self.backing[position..position + length as usize];
                Ok(f(b))
            }
            other => Err(Error::IncorrectTag {
                position: at,
                found: other.into(),
                expected_kind: "Written",
            }),
        }
    }

    /// Attempts to remove the data at `at`. This will return an error for partially-written data
    /// as well as already-deleted data.
    pub fn remove<R>(&mut self, at: Id, f: impl FnOnce(&[u8]) -> R) -> Result<R, Error> {
        let at = at.0.get() as usize;
        let mut position = at;
        let tag = MagicTag::read(&self.backing, &mut position)?;
        match tag {
            MagicTag::End => {
                panic!("cannot remove end tag")
            }
            MagicTag::Writing { .. } => Err(Error::EntryCorrupt { position: at }),
            MagicTag::Written { length } => {
                let ret = f(&self.backing[position..position + length as usize]);

                self.erase(&mut { at }, position - at, length as usize)?;

                Ok(ret)
            }
            MagicTag::Deleted { .. } => Err(Error::AlreadyDeleted { position: at }),
        }
    }

    fn erase(&mut self, position: &mut usize, tag_len: usize, length: usize) -> Result<(), Error> {
        let at = *position;
        let mut before = None;
        let mut after = None;
        for (i, gap) in self.gaps.iter().enumerate() {
            if gap.at + gap.length as usize + gap.tag_len as usize == at {
                assert!(before.is_none());
                before = Some(i);
            } else if *position + tag_len + length == gap.at {
                assert!(after.is_none());
                after = Some(i);
            }
        }

        let s = match (before, after) {
            (None, None) => None,
            (Some(b), None) => {
                let b = self.gaps.swap_remove(b);
                Some((b.at, *position + tag_len + length))
            }
            (None, Some(a)) => {
                let a = self.gaps.swap_remove(a);
                Some((at, a.at + a.tag_len as usize + a.length as usize))
            }
            (Some(b), Some(a)) => {
                let (b, a) = if b < a {
                    let a = self.gaps.swap_remove(a);
                    let b = self.gaps.swap_remove(b);
                    (b, a)
                } else {
                    let b = self.gaps.swap_remove(b);
                    let a = self.gaps.swap_remove(a);
                    (b, a)
                };
                Some((b.at, a.at + a.tag_len as usize + a.length as usize))
            }
        };

        if let Some((start, end)) = s {
            assert!(start < end);
            let gap_len = end - start;
            let (tag_len, len) = MagicTag::calc_tag_len(gap_len);
            *position = start;

            MagicTag::Deleted { length: len as u64 }.write_exact(&mut self.backing, position, tag_len as usize)?;
            assert_eq!(*position + len, end);

            self.backing[*position..end].fill(0);
            self.backing.flush_range(start, end)?;
            *position = end;

            self.gaps.push(Gap {
                at: start,
                length: len as u32,
                tag_len,
            });
        } else {
            self.backing[at] = MagicTag::DELETED | (self.backing[at] & !MagicTag::MASK);

            // After running some benchmarks, whether we clear deleted bytes or not doesn't seem to have a significant impact on performance.
            // Given how much easier it makes understanding the file, this will be left in for now.
            // (though of course the storage area of a deleted tag is still left unspecified, so this behaviour cannot be relied on)
            let end = at + tag_len + length;

            self.backing[at + tag_len..end].fill(0);
            self.backing.map().flush_range(at, tag_len + length).map_err(Error::Flush)?;
            *position = end;

            self.gaps.push(Gap {
                at,
                length: length as u32,
                tag_len: tag_len as u8,
            });
        }
        Ok(())
    }

    /// Removes any items not given by `known_ids`. `known_ids` mus be sorted in increasing order.
    ///
    /// **This is a dangerous operation** - any items not provided will be _permanently deleted_.
    /// This is intended to be used rarely, such as after repairing a store, and should only be used
    /// if you are _certain_ that you have every [`Id`] of this store you care about to hand.
    ///
    /// Should the scan stop - either because the next item was a [`ControlFlow::Break`] or because
    /// `known_ids` were not in increasing order, any items after the last [`Id`] encountered will
    /// not be changed.
    ///
    /// For example, assuming `1..=5` are valid keys:
    /// - `1, 2, 5` would leave only `3, 4` as valid keys
    /// - `1, 5, 3` would **delete `2, 3, 4`**, as the last increasing key encountered was `5`
    /// - `1, 3, <break>` would delete `2` and nothing else
    ///
    /// As you can see from the second example, leaving `known_ids` unsorted can result in deleting
    /// items that were included later - you should only call this when **absolutely certain** that
    /// it is in order. Also, make sure you take a backup beforehand, just in case.
    pub fn retain<E>(&mut self, known_ids: impl IntoIterator<Item = ControlFlow<E, Id>>) -> Result<Result<(), E>, RetainError> {
        let mut position = self.header_length;

        self.gaps.clear();
        let mut previous = None;
        for known in known_ids {
            let id = match known {
                ControlFlow::Continue(id) => id,
                ControlFlow::Break(e) => return Ok(Err(e)),
            };
            if let Some(p) = previous {
                if p >= id {
                    return Err(RetainError::UnsortedInputs(p, id));
                }
            }
            previous = Some(id);

            while position <= id.0.get() as usize {
                let here = position;
                let tag = MagicTag::read(&self.backing[..], &mut position)?;
                let tag_len = position - here;

                match tag {
                    MagicTag::End => {
                        return Err(RetainError::General(Error::IncorrectTag {
                            position: here,
                            found: tag.into(),
                            expected_kind: "Written",
                        }))
                    }
                    MagicTag::Deleted { length } => {
                        position += length as usize;
                        continue;
                    }
                    _ => {}
                }

                if id.0.get() as usize == here {
                    match tag {
                        MagicTag::Writing { .. } => return Err(RetainError::RetainPartial { position: here }),
                        MagicTag::Written { length } => {
                            position += length as usize;
                            continue;
                        }
                        MagicTag::Deleted { .. } | MagicTag::End => unreachable!(),
                    }
                } else {
                    match tag {
                        MagicTag::Writing { length } | MagicTag::Written { length } => {
                            position = here;
                            self.erase(&mut position, tag_len, length as usize)?;
                        }
                        MagicTag::Deleted { .. } | MagicTag::End => unreachable!(),
                    }
                }
            }
        }

        Ok(Ok(()))
    }

    /// Provides read-only access to the entire underlying bytes, header and post-end padding
    /// included.
    ///
    /// This is useful for debugging or taking snapshots of the map while in use. This should **not**
    /// be used to attempt to gain raw access to specific entries _i.e._ without passing through
    /// [`Self::get`] or [`Id`].
    pub fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&self.backing[..])
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct Gap {
    at: usize,
    length: u32,
    tag_len: u8,
}

#[cfg(feature = "debug_map")]
pub(crate) fn debug_map(map: &RawStore) -> Result<(), Error> {
    use bstr::{BStr, ByteSlice};
    use log::trace;

    trace!(" === BEGIN CHECK === ");
    let bytes = &map.backing[..];

    let header = &bytes[..RawStore::HEADER_LENGTH];
    assert_eq!(&header[..RawStore::HEADER_MAGIC.len()], RawStore::HEADER_MAGIC);
    let mut position = RawStore::HEADER_MAGIC.len();
    assert_eq!(&header[position..position + 2], &RawStore::HEADER_VERSION);
    position += 2;
    assert_eq!(position, header.len());
    let s = crate::util::read_varint::<u64>(bytes, &mut position)? as usize;
    trace!("Spec magic: {:?}", BStr::new(&bytes[position..position + s]));
    position += s;

    let mut ended = false;
    while position < bytes.len() {
        let tag = MagicTag::read(bytes, &mut position)?;
        match tag {
            MagicTag::End => {
                let b = bytes[position..].iter().find(|b| **b != 0x00);
                assert!(
                    b.is_none(),
                    "{:?} - {:?}",
                    b,
                    BStr::new(&bytes[position..].trim_end_with(|c| c == '\0'))
                );
                ended = true;
                break;
            }
            MagicTag::Writing { length } => {
                let b = &bytes[position..position + length as usize];
                position += length as usize;
                trace!("Writing - {:?}", BStr::new(b));
            }
            MagicTag::Written { length } => {
                let b = &bytes[position..position + length as usize];
                position += length as usize;
                trace!("Written - {:?}", BStr::new(b));
            }
            MagicTag::Deleted { length } => {
                position += length as usize;
                trace!("Deleted length {length}");
            }
        }
    }
    assert!(ended);
    trace!(" === END CHECK === \n");
    Ok(())
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
            let e = RawStore::open(backing, b"").unwrap_err();
            assert!(matches!(e, OpenError::TooSmall {found, ..} if found == l));
        }

        let e = RawStore::open(prepare_raw!(HEADER, 0), b"").unwrap_err();
        assert!(matches!(e, OpenError::NoEnd), "{e:?}");
        let e = RawStore::open(prepare_raw!(HEADER, 1, b"A"), b"A").unwrap_err();
        assert!(matches!(e, OpenError::NoEnd), "{e:?}");

        assert_eq!(HEADER, &prepare_raw!(b"\x1FPLFmap", [0, 0]).0[..]);
        let e = RawStore::open(prepare_raw!(RawStore::HEADER_MAGIC, [0, 0], 0), b"").unwrap_err();
        assert!(matches!(e, OpenError::NoEnd), "{e:?}");
        let false_magic = b"\x1FPLfmap";
        let e = RawStore::open(prepare_raw!(false_magic, [0, 0], 0), b"").unwrap_err();
        assert!(matches!(e, OpenError::Magic), "{e:?}");
        let e = RawStore::open(prepare_raw!(RawStore::HEADER_MAGIC, [1, 0], 0), b"").unwrap_err();
        assert!(matches!(e, OpenError::UnknownVersion([1, 0])), "{e:?}");

        RawStore::open(prepare!(), b"").unwrap();
    }

    #[test]
    fn partial_write() {
        // TODO: Add OpenOptions equivalent to configure attempting repairs
        let e = RawStore::open(prepare!(MagicTag::Writing { length: 10 }, [b'a'; 10]), b"").unwrap_err();
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
    }
}
