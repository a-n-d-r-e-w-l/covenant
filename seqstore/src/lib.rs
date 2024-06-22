#![allow(clippy::unusual_byte_groupings)] // These are deliberate to make packed fields clearer

use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use anyhow::anyhow;
use bstr::{BStr, BString, ByteSlice};

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
        self.resize_for(req)?;
        let target = &mut self[*position..*position + b.len()];
        target.copy_from_slice(b);
        *position += b.len();
        Ok(())
    }

    fn resize_for(&mut self, len: usize) -> anyhow::Result<()> {
        if self.len() <= len {
            self.resize_to(((len / 256) + 1) * 256)?;
        }
        Ok(())
    }

    fn map(&self) -> &memmap2::MmapMut {
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
pub struct FileMap {
    pub backing: Backing, // TEMP: pub
    end: usize,
    gaps: Vec<Gap>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct Gap {
    at: usize,
    length: u32,
    tag_len: u8,
}

impl FileMap {
    const HEADER_MAGIC: &'static [u8] = b"PLFmap";
    const HEADER_VERSION: [u8; 2] = [0x00, 0x00];
    const HEADER_LENGTH: usize = 9;

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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MagicTag {
    // TEMP: pub
    Start,
    End,
    Writing { length: u64 },
    Written { length: u64 },
    Deleted { length: u64 },
}

impl MagicTag {
    const MASK: u8 = 0b111_00000;

    const START: u8 = 0b000_00000;
    const END: u8 = 0b111_00000;
    const WRITING: u8 = 0b101_00000;
    const WRITTEN: u8 = 0b100_00000;
    const DELETED: u8 = 0b110_00000;

    // TEMP: pub
    pub fn read(backing: &[u8], position: &mut usize) -> anyhow::Result<Self> {
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

        let tag = backing[*position];
        *position += 1;
        let got = match tag & Self::MASK {
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
            other => Err(anyhow!(
                "unknown tag 0b{other:b} 0x{tag:02X} at position 0x{:02X} - {:?} {:?}",
                *position - 1,
                BStr::new(&backing[*position - 3..*position]),
                BStr::new(&backing[*position..*position + 3]),
            )),
        }?;
        Ok(got)
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

        match self {
            Self::Start => backing.write(&[Self::START | 0b11111], position),
            Self::End => backing.write(&[Self::END], position),
            Self::Writing { length } => write_with_length(backing, position, length, Self::WRITING),
            Self::Written { length } => write_with_length(backing, position, length, Self::WRITTEN),
            Self::Deleted { length } => write_with_length(backing, position, length, Self::DELETED),
        }?;

        Ok(())
    }

    fn write_exact(self, backing: &mut Backing, position: &mut usize, n: usize) -> anyhow::Result<()> {
        assert!(n <= 0b11 + 1, "length is too large to store item");
        let (tag, len) = match self {
            Self::Writing { length } => (Self::WRITING, length),
            Self::Written { length } => (Self::WRITTEN, length),
            Self::Deleted { length } => (Self::DELETED, length),
            _ => panic!("unsupported: {self:?}"),
        };

        let needed_bits = 64 - len.leading_zeros();
        let needed_bytes = needed_bits.saturating_sub(3).div_ceil(8); // 3 bits can be stored in tag
        if 1 + needed_bytes > n as _ {
            panic!("required {} bytes, have {}", 1 + needed_bytes, n)
        }

        if self.written_length() == n {
            return self.write(backing, position);
        }

        if len > 0b11111111_11111111_11111111 {
            // TODO: Is this not caught by the above check?
            let start = *position;
            self.write(backing, position)?;
            assert_eq!(*position, start + n);
        }

        // Otherwise, we don't _need_ to pack any bits in the tag
        let populated_bytes = needed_bits.div_ceil(8);
        assert!(populated_bytes <= 0b11, "length is too large to store item");
        let mut b = len.to_be_bytes();
        b[7 - populated_bytes as usize] = tag | ((populated_bytes as u8) << 3);
        backing.write(&b[7 - populated_bytes as usize..], position)?;
        Ok(())
    }

    fn written_length(self) -> usize {
        match self {
            MagicTag::Start | MagicTag::End => 1,
            MagicTag::Writing { length } | MagicTag::Written { length } | MagicTag::Deleted { length } => {
                let needed_bits = 64 - length.leading_zeros();
                let needed_bytes = needed_bits.saturating_sub(3).div_ceil(8); // 3 bits can be stored in tag
                1 + needed_bytes as usize
            }
        }
    }

    fn calc_tag_len(total_len: usize) -> (u8, usize) {
        let mut tag_len = 1;
        let new_len = loop {
            if tag_len > 4 {
                panic!("tag length overflow")
            }
            let new_len = total_len - tag_len;
            if (MagicTag::Writing { length: new_len as _ }).written_length() <= tag_len {
                break new_len;
            }
            tag_len += 1;
        };
        assert!(tag_len + new_len <= total_len);
        (tag_len as u8, total_len - tag_len)
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

    #[inline(always)]
    fn check_write_length(length: u64) {
        let mut backing = Backing::Anon(memmap2::MmapMut::map_anon(32).unwrap());
        MagicTag::Writing { length }.write(&mut backing, &mut 0).unwrap();
        let r = MagicTag::read(&backing, &mut 0).unwrap();
        assert_eq!(r, MagicTag::Writing { length });
    }

    #[inline(always)]
    fn check_write_exact(length: u64, n: usize) {
        if (MagicTag::Writing { length }).written_length() <= n {
            let mut backing = Backing::Anon(memmap2::MmapMut::map_anon(32).unwrap());
            MagicTag::Writing { length }.write_exact(&mut backing, &mut 0, n).unwrap();
            let r = MagicTag::read(&backing, &mut 0).unwrap();
            assert_eq!(r, MagicTag::Writing { length });
        }
    }

    #[test]
    fn test_writing_length() {
        for &length in LENGTHS {
            check_write_length(length);
        }
    }

    #[test]
    fn test_exact() {
        for &length in LENGTHS {
            for n in 0..=0b11 + 1 {
                check_write_exact(length, n);
            }
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
