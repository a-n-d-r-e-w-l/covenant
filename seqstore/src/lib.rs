#![allow(clippy::unusual_byte_groupings)] // These are deliberate to make packed fields clearer

use std::ops::{Deref, DerefMut};

use anyhow::anyhow;
use bstr::{BStr, BString};
use varuint::Serializable;

#[derive(Debug)]
pub enum Backing {
    // TEMP: pub
    File { file: fs_err::File, map: memmap2::MmapMut },
    Anon(memmap2::MmapMut),
}

impl Deref for Backing {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::File { map, .. } => map,
            Self::Anon(map) => map,
        }
    }
}

impl DerefMut for Backing {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::File { map, .. } => map,
            Self::Anon(map) => map,
        }
    }
}

impl Backing {
    fn resize_to(&mut self, size: usize) -> anyhow::Result<()> {
        match self {
            Backing::File { file, map } => {
                file.set_len(size as u64)?;
                unsafe { map.remap(size, memmap2::RemapOptions::new().may_move(true))? };
            }
            Backing::Anon(map) => {
                unsafe { map.remap(size, memmap2::RemapOptions::new().may_move(true))? };
            }
        }
        Ok(())
    }

    fn write(&mut self, b: &[u8], position: &mut usize) -> anyhow::Result<()> {
        let req = *position + b.len();
        if self.len() <= req {
            self.resize_to(((req / 256) + 1) * 256)?;
        }
        let target = &mut self[*position..*position + b.len()];
        target.copy_from_slice(b);
        *position += b.len();
        Ok(())
    }

    fn map(&self) -> &memmap2::MmapMut {
        match self {
            Self::File { map, .. } => map,
            Self::Anon(map) => map,
        }
    }

    fn map_mut(&mut self) -> &mut memmap2::MmapMut {
        match self {
            Self::File { map, .. } => map,
            Self::Anon(map) => map,
        }
    }

    fn flush(&self) -> anyhow::Result<()> {
        self.map().flush()?;
        Ok(())
    }

    fn flush_range(&self, start: usize, end: usize) -> anyhow::Result<()> {
        assert!(start <= end);
        if start == end {
            return Ok(());
        }
        self.map().flush_range(start, end - start)?;
        Ok(())
    }
}

#[derive(Debug)]
struct GapTable {
    table_at: usize,
    gaps: Vec<Gap>,
}

#[derive(Debug, Copy, Clone)]
struct Gap {
    at: usize,
    length: usize,
    table_part: (u32, u8),
}

#[derive(Debug)]
pub struct FileMap {
    pub backing: Backing, // TEMP
    gap_table: Option<GapTable>,
    end: usize,
}

impl FileMap {
    const HEADER_MAGIC: &'static [u8] = b"PLFmap";
    const HEADER_VERSION: [u8; 2] = [0x00, 0x00];
    const HEADER_LENGTH: usize = 17;
    const GAP_TABLE_POINTER: usize = 9;

    pub fn new(file: Option<fs_err::File>) -> anyhow::Result<Self> {
        let mut backing = match file {
            Some(file) => Backing::File {
                map: unsafe { memmap2::MmapMut::map_mut(&file)? },
                file,
            },
            None => Backing::Anon(memmap2::MmapMut::map_anon(256)?),
        };

        let mut position = 0;
        MagicTag::Start.write(&mut backing, &mut position)?;
        backing.write(Self::HEADER_MAGIC, &mut position)?; // magic bytes
        backing.write(&Self::HEADER_VERSION, &mut position)?; // header version
        debug_assert_eq!(position, Self::GAP_TABLE_POINTER);
        let gap_table_at = 0_u64;
        backing.write(&gap_table_at.to_be_bytes(), &mut position)?;
        debug_assert_eq!(position, Self::HEADER_LENGTH);
        MagicTag::End.write(&mut backing, &mut position)?;
        backing.flush()?;
        Ok(Self {
            backing,
            gap_table: None,
            end: Self::HEADER_LENGTH,
        })
    }

    // pub fn open(mut backing: R) -> anyhow::Result<Self> {
    //     backing.seek(SeekFrom::Start(0))?;
    //     // TODO: Validate header
    //     todo!()
    // }

    pub fn add(&mut self, bytes: &[u8]) -> anyhow::Result<u64> {
        let written_len = MagicTag::Written { length: bytes.len() as u64 }.written_length() + bytes.len();
        let (mut position, gap) = 'find: {
            let Some(ref mut gt) = self.gap_table else {
                break 'find (self.end, None);
            };
            let mut chosen_gap = None;
            for (idx, gap) in gt.gaps.iter().enumerate() {
                if gap.length == written_len {
                    chosen_gap = Some(idx);
                    break;
                }
                if gap.length >= written_len + 3 {
                    chosen_gap = Some(idx);
                    break;
                }
            }
            if let Some(chosen_gap) = chosen_gap {
                let gap = gt.gaps.swap_remove(chosen_gap);
                (gap.at, Some(gap))
            } else {
                (self.end, None)
            }
        };

        let mut new_gap = None;
        let mut item_position = position;
        let tag = MagicTag::read(&self.backing, &mut { position })?;
        match tag {
            MagicTag::Start => {
                panic!("cannot write to start")
            }
            MagicTag::End => {
                assert!(gap.is_none());
                let start = item_position;
                MagicTag::Writing { length: bytes.len() as u64 }.write(&mut self.backing, &mut position)?;
                self.backing.write(bytes, &mut position)?;

                let new_end = position;
                MagicTag::End.write(&mut self.backing, &mut position)?;
                let end = position;
                self.backing.flush_range(start, end)?;

                self.backing[item_position] ^= MagicTag::WRITING ^ MagicTag::WRITTEN;
                self.backing.flush_range(start, end)?;
                self.end = new_end;
            }
            MagicTag::Writing { .. } => return Err(anyhow!("previous writing attempt was incomplete: this entry is corrupt")),
            MagicTag::Written { .. } => {
                panic!("cannot write to where data is [file is corrupt, rescan]")
            }
            MagicTag::Deleted { length } => {
                let gap = gap.unwrap();
                if length as usize + tag.written_length() == written_len {
                    // Replace
                    let start = item_position;
                    MagicTag::Writing { length: bytes.len() as u64 }.write(&mut self.backing, &mut position)?;
                    self.backing.write(bytes, &mut position)?;

                    let end = position;
                    self.backing.flush_range(start, end)?;

                    self.backing[item_position] ^= MagicTag::WRITING ^ MagicTag::WRITTEN;
                    self.backing.flush_range(start, end)?;
                } else {
                    // Shrink
                    let new_size = gap.length - written_len;
                    let ng = MagicTag::Deleted { length: new_size as u64 };

                    // assert_eq!(
                    //     ng.written_length() + new_size + written_len, // New deleted tag+space + new data tag+space
                    //     tag.written_length() + length as usize        // Old deleted tag+space
                    // );

                    let mut item_start = position + gap.length - written_len;
                    item_position = item_start;
                    MagicTag::Writing { length: bytes.len() as u64 }.write(&mut self.backing, &mut item_start)?;
                    self.backing.write(bytes, &mut item_start)?;
                    ng.write(&mut self.backing, &mut position)?;

                    let end = item_start;
                    self.backing.flush_range(gap.at, end)?;

                    self.backing[item_position] ^= MagicTag::WRITING ^ MagicTag::WRITTEN;
                    self.backing.flush_range(item_position, end)?;

                    new_gap = Some(Gap {
                        at: gap.at,
                        length: new_size,
                        table_part: (0, 0),
                    })
                }
            }
            MagicTag::GapTable { .. } => {
                panic!("cannot write to gap table")
            }
            MagicTag::OldGapTable { .. } => {
                // Take bite out of end
                // Any part not %128 gets a normal Gap added
                // Shrink OGT
                todo!()
            }
        }

        match (gap, new_gap) {
            (None, None) => {}
            (None, Some(new)) => {
                todo!("add new gap")
            }
            (Some(old), None) => {
                let gt_at = self.gap_table.as_ref().unwrap().table_at;
                let s = old.table_part.0 as usize + gt_at;
                let l = old.table_part.1 as usize;
                let range = s..s + l;
                self.backing[range].fill(0);
                self.backing.map().flush_range(s, l)?;
            }
            (Some(old), Some(new)) => {
                // TODO: First try to replace old gap

                let gt_at = self.gap_table.as_ref().unwrap().table_at;
                let s = old.table_part.0 as usize + gt_at;
                let l = old.table_part.1 as usize;
                let range = s..s + l;
                self.backing[range].fill(0);
                self.backing.map().flush_range(s, l)?; // TODO: Merge flushes

                let new_len = varuint::Varint(new.table_part.1 as u64).size_hint();
                if new_len <= l {
                    varuint::WriteVarint::<u64>::write_varint(
                        &mut std::io::Cursor::new(&mut self.backing[s..s + new_len]),
                        new.table_part.1 as u64,
                    )?;
                    self.backing.map().flush_range(s, l)?;
                } else {
                    todo!("find location to add to table")
                }

                self.gap_table.as_mut().unwrap().gaps.push(new);
            }
        }

        Ok(item_position as u64)
    }

    pub fn get(&mut self, at: u64) -> anyhow::Result<Vec<u8>> {
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
        let length = match tag {
            MagicTag::Start => {
                panic!("cannot remove start tag")
            }
            MagicTag::End => {
                panic!("cannot remove end tag")
            }
            MagicTag::Writing { .. } => return Err(anyhow!("previous writing attempt was incomplete: this entry is corrupt")),
            MagicTag::Written { length } => length,
            MagicTag::Deleted { .. } => return Err(anyhow!("attempted to delete already-deleted item")),
            MagicTag::GapTable { .. } => {
                panic!("cannot remove gap table")
            }
            MagicTag::OldGapTable { .. } => {
                panic!("cannot remove old gap table")
            }
        };
        // TODO: Genericize
        let data = self.backing[position..position + length as usize].to_owned();
        let segment = &mut self.backing[at as usize..position + length as usize];
        segment[0] ^= MagicTag::DELETED ^ MagicTag::WRITTEN;
        debug_assert_eq!(MagicTag::read(segment, &mut 0).unwrap(), MagicTag::Deleted { length });
        self.backing.flush()?;
        match self.gap_table {
            Some(ref mut gt) => {
                todo!()
            }
            None => {
                let end_tag = MagicTag::read(&self.backing, &mut { self.end })?;
                if end_tag != MagicTag::End {
                    return Err(anyhow!("end tag was not {:?} - {:?}", MagicTag::End, tag));
                }
                let mut position = self.end;
                let table_pos = position;
                MagicTag::GapTable { length: 128 }.write(&mut self.backing, &mut position)?;
                let mut body = [0_u8; 128];
                let int_len = varuint::WriteVarint::<u64>::write_varint(&mut std::io::Cursor::new(&mut body[1..]), at).unwrap();
                self.backing.write(&body, &mut position)?;
                self.gap_table = Some(GapTable {
                    table_at: table_pos,
                    gaps: vec![Gap {
                        at: at as usize,
                        length: tag.written_length() + length as usize,
                        table_part: (0, int_len as u8),
                    }],
                });
                let new_end = position;
                MagicTag::End.write(&mut self.backing, &mut position)?;
                self.backing.flush_range(table_pos, position)?;
                self.backing[Self::GAP_TABLE_POINTER..Self::GAP_TABLE_POINTER + 8].copy_from_slice(&table_pos.to_be_bytes());
                self.backing.flush_range(Self::GAP_TABLE_POINTER, Self::GAP_TABLE_POINTER + 8)?;
                self.end = new_end;
            }
        }
        Ok(data)
    }
}

// TEMP
pub fn debug_map(map: &FileMap) -> anyhow::Result<()> {
    println!("\n === BEGIN CHECK === ");
    let bytes = &map.backing[..];
    let header = &bytes[..FileMap::HEADER_LENGTH];
    let mut position = 0;
    let t = MagicTag::read(header, &mut position)?;
    assert_eq!(t, MagicTag::Start);
    assert_eq!(&header[position..position + FileMap::HEADER_MAGIC.len()], FileMap::HEADER_MAGIC);
    position += FileMap::HEADER_MAGIC.len();
    assert_eq!(&header[position..position + 2], &FileMap::HEADER_VERSION);
    position += 2;
    assert_eq!(position, FileMap::GAP_TABLE_POINTER);
    let gap_table_pointer = u64::from_be_bytes(<[u8; 8]>::try_from(&header[position..position + 8]).unwrap()) as usize;
    position += 8;
    if gap_table_pointer != 0 {
        assert!(gap_table_pointer >= FileMap::HEADER_LENGTH);
    }
    assert_eq!(position, header.len());
    let mut ended = false;
    while position < bytes.len() {
        let item_position = position;
        let tag = MagicTag::read(bytes, &mut position)?;
        match tag {
            MagicTag::Start => {
                panic!("start tag encountered")
            }
            MagicTag::End => {
                let b = bytes[position..].iter().find(|b| **b != 0x00);
                assert!(b.is_none(), "{:?} - {}", b, BStr::new(&bytes[position..]));
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
            MagicTag::GapTable { length } => {
                if item_position != gap_table_pointer {
                    panic!("expected gap table at {gap_table_pointer}, found at {item_position}")
                }
                let b = &bytes[position..position + length as usize];
                position += length as usize;
                let mut reader = std::io::Cursor::new(b);
                while reader.position() < b.len() as u64 {
                    let i = varuint::ReadVarint::<u64>::read_varint(&mut reader)?;
                    if i == 0 {
                        continue;
                    }
                    let (kind, length) = match MagicTag::read(bytes, &mut { i as usize })? {
                        MagicTag::Deleted { length } => ("deleted", length),
                        MagicTag::OldGapTable { length } => ("old table", length),
                        other => panic!("expected gap at {i}, found {other:?}"),
                    };
                    println!("Gap at {i} - {kind} {length}")
                }
            }
            MagicTag::OldGapTable { length } => {
                let b = &bytes[position..position + length as usize];
                position += length as usize;
                todo!()
            }
        }
    }
    assert!(ended);
    println!(" === END CHECK === \n");
    Ok(())
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MagicTag {
    // TEMP: pub
    Start,
    End,
    Writing { length: u64 },
    Written { length: u64 },
    Deleted { length: u64 },
    GapTable { length: u64 },
    OldGapTable { length: u64 },
}

impl MagicTag {
    const MASK: u8 = 0b111_00000;

    const START: u8 = 0b111_00000;
    const END: u8 = 0b110_00000;
    const WRITING: u8 = 0b101_00000;
    const WRITTEN: u8 = 0b100_00000;
    const DELETED: u8 = 0b011_00000;
    const GAP_TABLE: u8 = 0b010_00000;
    const OLD_GAP_TABLE: u8 = 0b001_00000;

    fn read(backing: &[u8], position: &mut usize) -> anyhow::Result<Self> {
        fn read_with_length(tag: u8, backing: &[u8], position: &mut usize) -> anyhow::Result<u64> {
            let extra_bits = tag & 0b000_00_111;
            let len_bytes = (tag & 0b000_11_000) >> 3;
            if len_bytes == 0 {
                return Ok(extra_bits as _);
            }

            let buffer = &backing[*position..*position + len_bytes as usize];
            *position += len_bytes as usize;
            let mut bytes = [0; 8];
            for (i, byte) in buffer.iter().copied().enumerate() {
                bytes[i + 8 - len_bytes as usize] = byte;
            }
            if extra_bits != 0 {
                bytes[7 - len_bytes as usize] = extra_bits;
            }
            let n = u64::from_be_bytes(bytes);
            Ok(n)
        }

        fn read_gap_table_length(tag: u8, backing: &[u8], position: &mut usize) -> anyhow::Result<u64> {
            let base_length = read_with_length(tag, backing, position)?;
            Ok((base_length + 1) * 128) // Multiples of 128 bytes, non-empty
        }

        let tag = backing[*position];
        *position += 1;
        match tag & Self::MASK {
            Self::START => Ok(Self::Start),
            Self::END => Ok(Self::End),
            Self::WRITING => Ok(Self::Writing {
                length: read_with_length(tag, backing, position)?,
            }),
            Self::WRITTEN => Ok(Self::Written {
                length: read_with_length(tag, backing, position)?,
            }),
            Self::DELETED => Ok(Self::Deleted {
                length: read_with_length(tag, backing, position)?,
            }),
            Self::GAP_TABLE => Ok(Self::GapTable {
                length: read_gap_table_length(tag, backing, position)?,
            }),
            Self::OLD_GAP_TABLE => Ok(Self::OldGapTable {
                length: read_gap_table_length(tag, backing, position)?,
            }),
            other => Err(anyhow!("unknown tag 0b{other:b}")),
        }
    }

    fn write(self, backing: &mut Backing, position: &mut usize) -> anyhow::Result<()> {
        fn write_with_length(backing: &mut Backing, position: &mut usize, length: u64, tag: u8) -> anyhow::Result<()> {
            if length != 0 {
                let needed_bits = 64 - length.leading_zeros();
                let needed_bytes = needed_bits.saturating_sub(3).div_ceil(8); // 3 bits can be stored in tag

                assert!(needed_bytes <= 0b11, "length is too large to store item [{length}]");
                let mut bytes = length.to_be_bytes();
                let tag_extra_bytes = (needed_bytes as u8) << 3;
                let bytes = if needed_bits % 8 <= 3 && needed_bits % 8 > 0 {
                    let tag_byte_idx = length.leading_zeros() as usize / 8;
                    assert_eq!(bytes[tag_byte_idx] & !0b111, 0, "tag overflowed its bounds: {length}");
                    bytes[tag_byte_idx] = bytes[tag_byte_idx] | tag | tag_extra_bytes;
                    &bytes[tag_byte_idx..]
                } else {
                    let tag_byte_idx = (length.leading_zeros() as usize / 8)
                        .checked_sub(1)
                        .expect("should be caught by size check");
                    debug_assert_eq!(bytes[tag_byte_idx], 0);
                    bytes[tag_byte_idx] = tag | tag_extra_bytes;
                    &bytes[tag_byte_idx..]
                };

                backing.write(bytes, position)
            } else {
                backing.write(&[tag], position)
            }
        }

        fn write_gap_table_length(backing: &mut Backing, position: &mut usize, length: u64, tag: u8) -> anyhow::Result<()> {
            assert_eq!(length % 128, 0);
            assert_ne!(length, 0);
            write_with_length(backing, position, length / 128 - 1, tag)
        }

        match self {
            Self::Start => backing.write(&[Self::START], position),
            Self::End => backing.write(&[Self::END], position),
            Self::Writing { length } => write_with_length(backing, position, length, Self::WRITING),
            Self::Written { length } => write_with_length(backing, position, length, Self::WRITTEN),
            Self::Deleted { length } => write_with_length(backing, position, length, Self::DELETED),
            Self::GapTable { length } => write_gap_table_length(backing, position, length, Self::GAP_TABLE),
            _ => todo!("{self:?}"),
        }
    }
    fn written_length(self) -> usize {
        match self {
            MagicTag::Start | MagicTag::End => 1,
            MagicTag::Writing { length }
            | MagicTag::Written { length }
            | MagicTag::Deleted { length }
            | MagicTag::GapTable { length }
            | MagicTag::OldGapTable { length } => {
                let needed_bits = 64 - length.leading_zeros();
                let needed_bytes = needed_bits.saturating_sub(3).div_ceil(8); // 3 bits can be stored in tag
                1 + needed_bytes as usize
            }
        }
    }

    // TEMP
    pub fn bytes(self) -> anyhow::Result<BString> {
        let mut backing = Backing::Anon(memmap2::MmapMut::map_anon(self.written_length())?);
        let mut pos = 0;
        self.write(&mut backing, &mut pos)?;
        Ok(BStr::new(&backing[..pos]).to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LENGTHS: &[u64] = &[0, 3, 6, 7, 8, 9, 0xFF, 0x7_FF, 0x8_FF, 0b1111111111, 0b10000000000, 0x7_FF_FF_FF];

    #[test]
    #[should_panic(expected = "length is too large to store item [134217728]")]
    fn max_size() {
        let max_size = 0x7_FF_FF_FF;
        assert_eq!(134217728, max_size + 1);
        let mut backing = Backing::Anon(memmap2::MmapMut::map_anon(32).unwrap());
        MagicTag::Writing { length: max_size }.write(&mut backing, &mut 0).unwrap();
        MagicTag::Writing { length: max_size + 1 }.write(&mut backing, &mut 0).unwrap();
    }

    #[test]
    fn test_writing_length() {
        for &length in LENGTHS {
            let mut backing = Backing::Anon(memmap2::MmapMut::map_anon(32).unwrap());
            MagicTag::Writing { length }.write(&mut backing, &mut 0).unwrap();
            let r = MagicTag::read(&backing, &mut 0).unwrap();
            assert_eq!(r, MagicTag::Writing { length });

            MagicTag::GapTable { length: (length + 1) * 128 }.write(&mut backing, &mut 0).unwrap();
            let r = MagicTag::read(&backing, &mut 0).unwrap();
            assert_eq!(r, MagicTag::GapTable { length: (length + 1) * 128 });
        }
    }

    #[test]
    fn test_no_further_length() {
        for &length in LENGTHS {
            let mut backing = Backing::Anon(memmap2::MmapMut::map_anon(32).unwrap());
            let mut written = 0;
            MagicTag::Writing { length }.write(&mut backing, &mut written).unwrap();
            let mut read = 0;
            MagicTag::read(&backing, &mut read).unwrap();
            assert_eq!(written, read);
        }
    }

    #[test]
    fn test_computed_length() {
        for &length in LENGTHS {
            let mut backing = Backing::Anon(memmap2::MmapMut::map_anon(32).unwrap());
            let tag = MagicTag::Writing { length };
            let mut position = 0;
            tag.write(&mut backing, &mut position).unwrap();
            assert_eq!(position, tag.written_length());
            position = 0;
            let tag2 = MagicTag::read(&backing, &mut position).unwrap();
            assert_eq!(position, tag.written_length());
            assert_eq!(tag, tag2);
        }
    }
}
