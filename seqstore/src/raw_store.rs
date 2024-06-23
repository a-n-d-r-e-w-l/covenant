use std::num::NonZeroU64;

use crate::{
    backing::Backing,
    error::{Error, OpenError},
    tag::MagicTag,
    Id,
};

#[cfg(feature = "debug_map")]
pub mod checker;

#[derive(Debug)]
pub struct RawStore {
    backing: Backing,
    end: usize,
    gaps: Vec<Gap>,
}

impl RawStore {
    const HEADER_MAGIC: &'static [u8] = b"PLFmap";
    const HEADER_VERSION: [u8; 2] = [0x00, 0x00];
    pub(crate) const HEADER_LENGTH: usize = 9;

    pub fn new(mut backing: Backing) -> Result<Self, Error> {
        let mut position = 0;
        MagicTag::Start.write(&mut backing, &mut position)?;
        backing.write(Self::HEADER_MAGIC, &mut position)?; // magic bytes
        backing.write(&Self::HEADER_VERSION, &mut position)?; // header version
        debug_assert_eq!(position, Self::HEADER_LENGTH);
        MagicTag::End.write(&mut backing, &mut position)?;
        backing.flush()?;
        Ok(Self {
            backing,
            end: Self::HEADER_LENGTH,
            gaps: vec![],
        })
    }

    pub fn open(backing: Backing) -> Result<Self, OpenError> {
        if backing.len() < Self::HEADER_LENGTH {
            return Err(OpenError::TooSmall(backing.len()));
        }
        let header = &backing[..Self::HEADER_LENGTH];
        let mut hpos = 0;
        let t = MagicTag::read(header, &mut hpos)?;
        if t != MagicTag::Start {
            return Err(OpenError::Start(t.into()));
        }
        if &header[hpos..hpos + Self::HEADER_MAGIC.len()] != Self::HEADER_MAGIC {
            return Err(OpenError::Magic);
        }
        hpos += Self::HEADER_MAGIC.len();
        let v: [u8; 2] = (&header[hpos..hpos + Self::HEADER_VERSION.len()]).try_into().unwrap();
        if v[..] != Self::HEADER_VERSION {
            return Err(OpenError::UnknownVersion(v));
        }
        hpos += Self::HEADER_VERSION.len();
        // This should not be possible to hit, but is kept to ensure that the reading checks
        // are kept in line with changes to the header size
        assert_eq!(hpos, header.len());

        let mut pos = Self::HEADER_LENGTH;
        let mut end = None;
        let mut gaps = Vec::new();
        while pos < backing.len() {
            let here = pos;
            let tag = MagicTag::read(&backing, &mut pos)?;
            match tag {
                MagicTag::Start => return Err(OpenError::FoundStart(here)),
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

        Ok(Self { backing, end, gaps })
    }

    pub fn close(self) -> Result<Backing, Error> {
        self.backing.flush()?;
        Ok(self.backing)
    }

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

    pub fn remove<R>(&mut self, at: Id, f: impl FnOnce(&[u8]) -> R) -> Result<R, Error> {
        let at = at.0.get() as usize;
        let mut position = at;
        let tag = MagicTag::read(&self.backing, &mut position)?;
        match tag {
            MagicTag::Start => {
                panic!("cannot remove start tag")
            }
            MagicTag::End => {
                panic!("cannot remove end tag")
            }
            MagicTag::Writing { .. } => Err(Error::EntryCorrupt { position: at }),
            MagicTag::Written { length } => {
                let ret = f(&self.backing[position..position + length as usize]);

                // TODO: Maybe wrap an inner function to minimise monomorphism?
                let mut before = None;
                let mut after = None;
                for (i, gap) in self.gaps.iter().enumerate() {
                    if gap.at + gap.length as usize + gap.tag_len as usize == at {
                        assert!(before.is_none());
                        before = Some(i);
                    } else if position + length as usize == gap.at {
                        assert!(after.is_none());
                        after = Some(i);
                    }
                }

                let s = match (before, after) {
                    (None, None) => None,
                    (Some(b), None) => {
                        let b = self.gaps.swap_remove(b);
                        Some((b.at, position + length as usize))
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
                    position = start;

                    MagicTag::Deleted { length: len as u64 }.write_exact(&mut self.backing, &mut position, tag_len as usize)?;
                    assert_eq!(position + len, end);

                    self.backing[position..end].fill(0);
                    self.backing.flush_range(start, end)?;

                    self.gaps.push(Gap {
                        at: start,
                        length: len as u32,
                        tag_len,
                    });
                } else {
                    self.backing[at] ^= MagicTag::WRITTEN ^ MagicTag::DELETED;

                    // After running some benchmarks, whether we clear deleted bytes or not doesn't seem to have a significant impact on performance.
                    // Given how much easier it makes understanding the file, this will be left in for now.
                    // (though of course the storage area of a deleted tag is still left unspecified, so this behaviour cannot be relied on)
                    self.backing[position..position + length as usize].fill(0);
                    self.backing
                        .map()
                        .flush_range(at, tag.written_length() + length as usize)
                        .map_err(Error::Flush)?;

                    self.gaps.push(Gap {
                        at,
                        length: length as u32,
                        tag_len: tag.written_length() as u8,
                    });
                }

                Ok(ret)
            }
            MagicTag::Deleted { .. } => Err(Error::AlreadyDeleted { position: at }),
        }
    }

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

    trace!("\n === BEGIN CHECK === ");
    let bytes = &map.backing[..];
    let header = &bytes[..RawStore::HEADER_LENGTH];
    let mut position = 0;
    let t = MagicTag::read(header, &mut position)?;
    assert_eq!(t, MagicTag::Start);
    assert_eq!(
        &header[position..position + RawStore::HEADER_MAGIC.len()],
        RawStore::HEADER_MAGIC
    );
    position += RawStore::HEADER_MAGIC.len();
    assert_eq!(&header[position..position + 2], &RawStore::HEADER_VERSION);
    position += 2;
    assert_eq!(position, header.len());
    let mut ended = false;
    while position < bytes.len() {
        let tag = MagicTag::read(bytes, &mut position)?;
        match tag {
            MagicTag::Start => {
                panic!("start tag encountered")
            }
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
                let b = &bytes[position..position + length as usize];
                position += length as usize;
                trace!("Deleted - {:?}", BStr::new(b));
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
        ($($e:expr),* $(,)?) => {prepare_raw!(HEADER, $($e,)* MagicTag::End)};
    }

    const HEADER: &[u8] = b"\x1FPLFmap\x00\x00";

    #[test]
    fn test_header() {
        let empty = [0; RawStore::HEADER_LENGTH];
        for l in 0..RawStore::HEADER_LENGTH {
            let backing = Backing::new_from_buffer(&empty[..l]).unwrap();
            let e = RawStore::open(backing).unwrap_err();
            assert!(matches!(e, OpenError::TooSmall(x) if x == l));
        }
        let backing = Backing::new_from_buffer(&empty[..RawStore::HEADER_LENGTH]).unwrap();
        let e = RawStore::open(backing).unwrap_err();
        assert!(!matches!(e, OpenError::TooSmall(_)));

        let backing = Backing::new_from_buffer(HEADER).unwrap();
        let e = RawStore::open(backing).unwrap_err();
        assert!(matches!(e, OpenError::NoEnd), "{e:?}");

        assert_eq!(HEADER, &prepare_raw!(MagicTag::Start, b"PLFmap", [0, 0])[..]);
        let e = RawStore::open(prepare_raw!(MagicTag::End, RawStore::HEADER_MAGIC, [0, 0])).unwrap_err();
        assert!(matches!(e, OpenError::Start(_)), "{e:?}");
        let e = RawStore::open(prepare_raw!(0b011_00000, RawStore::HEADER_MAGIC, [0, 0])).unwrap_err();
        assert!(
            matches!(
                e,
                OpenError::General(Error::UnknownTag {
                    position: 0,
                    byte: 0b011_00000,
                    ..
                })
            ),
            "{e:?}"
        );
        let false_magic = b"PLfmap";
        let e = RawStore::open(prepare_raw!(MagicTag::Start, false_magic, [0, 0])).unwrap_err();
        assert!(matches!(e, OpenError::Magic), "{e:?}");
        let e = RawStore::open(prepare_raw!(MagicTag::Start, RawStore::HEADER_MAGIC, [1, 0])).unwrap_err();
        assert!(matches!(e, OpenError::UnknownVersion([1, 0])), "{e:?}");

        RawStore::open(prepare!()).unwrap();
    }

    #[test]
    fn partial_write() {
        // TODO: Add OpenOptions equivalent to configure attempting repairs
        let e = RawStore::open(prepare!(MagicTag::Writing { length: 10 }, [b'a'; 10])).unwrap_err();
        assert!(matches!(
            e,
            OpenError::PartialWrite {
                position: RawStore::HEADER_LENGTH,
                length: 10
            }
        ));
    }
}
