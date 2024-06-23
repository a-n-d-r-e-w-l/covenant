use anyhow::anyhow;
use bstr::{BStr, ByteSlice};

use crate::{backing::Backing, tag::MagicTag};

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
    const HEADER_LENGTH: usize = 9;

    pub fn new(mut backing: Backing) -> anyhow::Result<Self> {
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

    pub fn open(backing: Backing) -> anyhow::Result<Self> {
        let header = &backing[..Self::HEADER_LENGTH];
        let mut hpos = 0;
        let t = MagicTag::read(header, &mut hpos)?;
        assert_eq!(t, MagicTag::Start);
        assert_eq!(&header[hpos..hpos + Self::HEADER_MAGIC.len()], Self::HEADER_MAGIC);
        hpos += Self::HEADER_MAGIC.len();
        assert_eq!(&header[hpos..hpos + Self::HEADER_VERSION.len()], Self::HEADER_VERSION);
        hpos += Self::HEADER_VERSION.len();
        assert_eq!(hpos, header.len());

        let mut pos = Self::HEADER_LENGTH;
        let mut end = None;
        let mut gaps = Vec::new();
        while pos < backing.len() {
            let here = pos;
            let tag = MagicTag::read(&backing, &mut pos)?;
            match tag {
                MagicTag::Start => {
                    panic!()
                }
                MagicTag::End => {
                    assert!(end.is_none());
                    end = Some(here);
                    let rest = &backing[pos..];
                    if !rest.iter().all(|&b| b == 0) {
                        return Err(anyhow!("data after end: {:?}", BStr::new(rest)));
                    }
                    break;
                }
                MagicTag::Writing { .. } => {
                    panic!()
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
        let Some(end) = end else { return Err(anyhow!("no end tag found")) };

        Ok(Self { backing, end, gaps })
    }

    pub fn close(self) -> anyhow::Result<Backing> {
        self.backing.flush()?;
        Ok(self.backing)
    }

    pub fn add(&mut self, bytes: &[u8]) -> anyhow::Result<u64> {
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
        self.backing.map().flush_range(start, 1)?;

        Ok(start as u64)
    }

    pub fn get(&self, at: u64) -> anyhow::Result<Vec<u8>> {
        // TODO: Keys should include some marker to check the length to prevent overreads
        let mut position = at as usize;
        let tag = MagicTag::read(&self.backing, &mut position)?;
        match tag {
            MagicTag::Writing { .. } => Err(anyhow!("previous writing attempt was incomplete: this entry is corrupt")),
            MagicTag::Written { length } => {
                let b = &self.backing[position..position + length as usize];
                Ok(b.to_owned())
            }
            _ => Err(anyhow!(
                "encountered incorrect tag {tag:?}, expecting MagicTag::Written {{ length: .. }}"
            )),
        }
    }

    pub fn remove(&mut self, at: u64) -> anyhow::Result<Vec<u8>> {
        let mut position = at as usize;
        let tag = MagicTag::read(&self.backing, &mut position)?;
        match tag {
            MagicTag::Start => {
                panic!("cannot remove start tag")
            }
            MagicTag::End => {
                panic!("cannot remove end tag")
            }
            MagicTag::Writing { .. } => Err(anyhow!("previous writing attempt was incomplete: this entry is corrupt")),
            MagicTag::Written { length } => {
                // TODO: Genericize extraction [this is RawMap]
                let segment = self.backing[position..position + length as usize].to_owned();

                let mut before = None;
                let mut after = None;
                for (i, gap) in self.gaps.iter().enumerate() {
                    if gap.at + gap.length as usize + gap.tag_len as usize == at as usize {
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
                        Some((at as usize, a.at + a.tag_len as usize + a.length as usize))
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
                    self.backing[at as usize] ^= MagicTag::WRITTEN ^ MagicTag::DELETED;

                    // After running some benchmarks, whether we clear deleted bytes or not doesn't seem to have a significant impact on performance.
                    // Given how much easier it makes understanding the file, this will be left in for now.
                    // (though of course the storage area of a deleted tag is still left unspecified, so this behaviour cannot be relied on)
                    self.backing[position..position + length as usize].fill(0);
                    self.backing.map().flush_range(at as usize, tag.written_length() + length as usize)?;

                    self.gaps.push(Gap {
                        at: at as usize,
                        length: length as u32,
                        tag_len: tag.written_length() as u8,
                    });
                }

                Ok(segment)
            }
            MagicTag::Deleted { .. } => Err(anyhow!("attempted to delete already-deleted item")),
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

pub(crate) fn debug_map(map: &RawStore) -> anyhow::Result<()> {
    println!("\n === BEGIN CHECK === ");
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
                println!("Writing - {:?}", BStr::new(b));
            }
            MagicTag::Written { length } => {
                let b = &bytes[position..position + length as usize];
                position += length as usize;
                println!("Written - {:?}", BStr::new(b));
            }
            MagicTag::Deleted { length } => {
                let b = &bytes[position..position + length as usize];
                position += length as usize;
                println!("Deleted - {:?}", BStr::new(b));
            }
        }
    }
    assert!(ended);
    println!(" === END CHECK === \n");
    Ok(())
}
