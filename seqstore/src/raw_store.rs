use crate::{
    backing::{Backing, BackingInner},
    error::Error,
    tag::MagicTag,
    Id,
};

#[cfg(feature = "debug_map")]
#[cfg_attr(docsrs, doc(cfg(feature = "debug_map")))]
pub mod checker;

mod open;
pub use open::{OpenStoreOptions, RecoveryStrategy};

/// A "raw" [`Id`]-to-bytes store, either file-backed or entirely in memory, where [`Id`] is
/// represented by an opaque (_i.e._ not corresponding to file offset) [`u64`].
///
/// This is "raw" in the sense that it makes no assumptions about what data is being stored, nor about
/// how that data will be retrieved. This makes it ideal to use to build other, more specialized
/// maps on top of, and less ergonomic to use directly.
///
/// If using [file-backed storage][Backing::new_file], only a minimal amount of data is stored in memory,
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

    /// Flush all outstanding changes and close the store.
    ///
    /// Returns the [`Backing`] used so that the store can be re-opened if desired.
    ///
    /// \*Technically*, while the [`Backing`] is not in active use after this returns, it is unwise
    /// to modify the underlying file until it drops. For more information about file safety, see
    /// [`Backing::new_file`]. This does not apply if the [`Backing`] was created using an anonymous map,
    /// as there is no underlying file to modify.
    pub fn close(mut self) -> Result<Backing, Error> {
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
        self.backing.flush_start_end(start, end)?;

        self.backing[start] ^= MagicTag::WRITING ^ MagicTag::WRITTEN;
        self.backing.flush_range(start, 1)?;

        Ok(Id::new(start, bytes.len()))
    }

    /// Replaces the data stored at `at` with `with`, as long as the **new data has the same
    /// length as the old data**.
    ///
    /// `f` is given a view of the old data.
    pub fn replace<R>(&mut self, at: Id, with: &[u8], f: impl FnOnce(&[u8]) -> R) -> Result<R, Error> {
        let mut position = at.at();
        let tag = MagicTag::read(&self.backing, &mut position)?;
        match tag {
            MagicTag::Written { length } => {
                at.verify(length)?;
                if length as usize != with.len() {
                    Err(Error::MismatchedLengths {
                        position: at.at(),
                        new: with.len(),
                        old: length as usize,
                    })
                } else {
                    let r = f(&self.backing[position..position + with.len()]);
                    self.backing[position..position + with.len()].copy_from_slice(with);
                    self.backing.flush_range(position, with.len())?;
                    Ok(r)
                }
            }
            MagicTag::Writing { .. } => Err(Error::EntryCorrupt { position: at.at() }),
            MagicTag::Deleted { .. } => Err(Error::CannotReplaceDeleted { position: at.at() }),
            other => Err(Error::IncorrectTag {
                position: at.at(),
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
        let mut position = at.at();
        let tag = MagicTag::read(&self.backing, &mut position)?;
        match tag {
            MagicTag::Writing { .. } => Err(Error::EntryCorrupt { position: at.at() }),
            MagicTag::Written { length } => {
                at.verify(length)?;
                let b = &self.backing[position..position + length as usize];
                Ok(f(b))
            }
            other => Err(Error::IncorrectTag {
                position: at.at(),
                found: other.into(),
                expected_kind: "Written",
            }),
        }
    }

    /// Attempts to remove the data at `at`. This will return an error for partially-written data
    /// as well as already-deleted data.
    pub fn remove<R>(&mut self, at: Id, f: impl FnOnce(&[u8]) -> R) -> Result<R, Error> {
        let mut position = at.at();
        let tag = MagicTag::read(&self.backing, &mut position)?;
        match tag {
            MagicTag::End => {
                panic!("cannot remove end tag")
            }
            MagicTag::Writing { .. } => Err(Error::EntryCorrupt { position: at.at() }),
            MagicTag::Written { length } => {
                at.verify(length)?;
                let ret = f(&self.backing[position..position + length as usize]);

                self.erase(&mut { at.at() }, position - at.at(), length as usize)?;

                Ok(ret)
            }
            MagicTag::Deleted { .. } => Err(Error::AlreadyDeleted { position: at.at() }),
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
            self.backing.flush_start_end(start, end)?;
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
            self.backing.flush_range(at, tag_len + length)?;
            *position = end;

            self.gaps.push(Gap {
                at,
                length: length as u32,
                tag_len: tag_len as u8,
            });
        }
        Ok(())
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

/// A function that can describe the contents of a [`RawStore`], intended for debugging when working
/// on this crate itself.
///
/// Requires a [`log`]-compatible logger to be setup.
#[cfg(feature = "debug_map")]
#[cfg_attr(docsrs, doc(cfg(feature = "debug_map")))]
pub fn debug_map(bytes: &[u8]) -> Result<(), Error> {
    use bstr::{BStr, ByteSlice};
    use log::trace;

    trace!(" === BEGIN CHECK === ");

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
