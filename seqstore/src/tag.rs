#![allow(clippy::unusual_byte_groupings)] // These are deliberate to make packed fields clearer

use crate::{backing::BackingInner, error::Error};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum MagicTag {
    End,
    Writing { length: u64 },
    Written { length: u64 },
    Deleted { length: u64 },
}

impl MagicTag {
    pub(crate) const MASK: u8 = 0b111_00000;

    pub(crate) const END: u8 = 0b111_00000;
    pub(crate) const WRITING: u8 = 0b101_00000;
    pub(crate) const WRITTEN: u8 = 0b100_00000;
    pub(crate) const DELETED: u8 = 0b110_00000;

    pub(crate) fn read(backing: &[u8], position: &mut usize) -> Result<Self, Error> {
        fn read_with_length(tag: u8, backing: &[u8], position: &mut usize) -> Result<u64, Error> {
            let extra_bits = tag & 0b000_00_111;
            let len_bytes = (tag & 0b000_11_000) >> 3;
            if len_bytes == 0 {
                return Ok(extra_bits as _);
            }

            // TODO: Return error if does not fit
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
        match tag & Self::MASK {
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
            _ => {
                *position -= 1;
                Err(Error::UnknownTag {
                    position: *position,
                    byte: tag,
                })
            }
        }
    }

    pub(crate) fn write(self, backing: &mut BackingInner, position: &mut usize) -> Result<(), Error> {
        backing.resize_for(*position + self.written_length())?;
        self.write_buffer(backing, position);
        Ok(())
    }

    pub(crate) fn write_buffer(self, buffer: &mut [u8], position: &mut usize) {
        fn write_with_length(buffer: &mut [u8], position: &mut usize, length: u64, tag: u8) {
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

                buffer[*position..*position + bytes.len()].copy_from_slice(bytes);
                *position += bytes.len();
            } else {
                buffer[*position..*position + 1].copy_from_slice(&[tag]);
                *position += 1;
            }
        }

        match self {
            Self::End => {
                buffer[*position..*position + 1].copy_from_slice(&[Self::END]);
                *position += 1;
            }
            Self::Writing { length } => write_with_length(buffer, position, length, Self::WRITING),
            Self::Written { length } => write_with_length(buffer, position, length, Self::WRITTEN),
            Self::Deleted { length } => write_with_length(buffer, position, length, Self::DELETED),
        }
    }

    pub(crate) fn write_exact(self, backing: &mut BackingInner, position: &mut usize, tag_len: usize) -> Result<(), Error> {
        assert!(tag_len <= 0b11 + 1, "length is too large to store item");
        let (tag, len) = match self {
            Self::Writing { length } => (Self::WRITING, length),
            Self::Written { length } => (Self::WRITTEN, length),
            Self::Deleted { length } => (Self::DELETED, length),
            _ => panic!("unsupported: {self:?}"),
        };

        let needed_bits = 64 - len.leading_zeros();
        let needed_bytes = needed_bits.saturating_sub(3).div_ceil(8); // 3 bits can be stored in tag
        if 1 + needed_bytes > tag_len as _ {
            panic!("required {} bytes, have {}", 1 + needed_bytes, tag_len)
        }

        if self.written_length() == tag_len {
            return self.write(backing, position);
        }

        if len > 0b11111111_11111111_11111111 {
            // TODO: Is this not caught by the above check?
            let start = *position;
            self.write(backing, position)?;
            assert_eq!(*position, start + tag_len);
        }

        // Otherwise, we don't _need_ to pack any bits in the tag
        let populated_bytes = needed_bits.div_ceil(8);
        assert!(populated_bytes <= 0b11, "length is too large to store item");
        let mut b = len.to_be_bytes();
        b[7 - populated_bytes as usize] = tag | ((populated_bytes as u8) << 3);
        backing.write(&b[7 - populated_bytes as usize..], position)?;
        Ok(())
    }

    pub(crate) fn written_length(self) -> usize {
        match self {
            MagicTag::End => 1,
            MagicTag::Writing { length } | MagicTag::Written { length } | MagicTag::Deleted { length } => {
                let needed_bits = 64 - length.leading_zeros();
                let needed_bytes = needed_bits.saturating_sub(3).div_ceil(8); // 3 bits can be stored in tag
                1 + needed_bytes as usize
            }
        }
    }

    pub(crate) fn calc_tag_len(total_len: usize) -> (u8, usize) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backing::Backing;

    const LENGTHS: &[u64] = &[0, 3, 6, 7, 8, 9, 0xFF, 0x7_FF, 0x8_FF, 0b1111111111, 0b10000000000, 0x7_FF_FF_FF];

    #[test]
    fn no_overlap() {
        let items = [MagicTag::END, MagicTag::WRITING, MagicTag::WRITTEN, MagicTag::DELETED, 0];
        let iter = items.iter().copied().enumerate().flat_map(|(i, t)| {
            items
                .iter()
                .copied()
                .enumerate()
                .map(move |(j, s)| ((i, j), (t & MagicTag::MASK, s & MagicTag::MASK)))
        });

        for ((i, j), (t, s)) in iter {
            if i == j {
                continue;
            }
            assert_ne!(t, s)
        }
    }

    #[test]
    #[should_panic(expected = "length is too large to store item [134217728]")]
    fn max_size() {
        let max_size = 0x7_FF_FF_FF;
        assert_eq!(134217728, max_size + 1);
        let mut backing = Backing::new_anon().unwrap().0;
        MagicTag::Writing { length: max_size }.write(&mut backing, &mut 0).unwrap();
        MagicTag::Writing { length: max_size + 1 }.write(&mut backing, &mut 0).unwrap();
    }

    #[inline(always)]
    fn check_write_length(length: u64) {
        let mut backing = Backing::new_anon().unwrap().0;
        MagicTag::Writing { length }.write(&mut backing, &mut 0).unwrap();
        let r = MagicTag::read(&backing, &mut 0).unwrap();
        assert_eq!(r, MagicTag::Writing { length });
    }

    #[inline(always)]
    fn check_write_exact(length: u64, n: usize) {
        if (MagicTag::Writing { length }).written_length() <= n {
            let mut backing = Backing::new_anon().unwrap().0;
            MagicTag::Writing { length }.write_exact(&mut backing, &mut 0, n).unwrap();
            let r = MagicTag::read(&backing, &mut 0).unwrap();
            assert_eq!(r, MagicTag::Writing { length });
        }
    }

    #[test]
    fn test_buffer_write() {
        let mut buffer = [0; 4];
        let mut backing = Backing::new_anon().unwrap().0;
        for &length in LENGTHS {
            let o = MagicTag::Writing { length };
            o.write_buffer(&mut buffer, &mut 0);
            o.write(&mut backing, &mut 0).unwrap();
            let t = MagicTag::read(&buffer, &mut 0).unwrap();
            let r = MagicTag::read(&backing, &mut 0).unwrap();
            assert_eq!(t, o);
            assert_eq!(t, r);
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
            let mut backing = Backing::new_anon().unwrap().0;
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
            let mut backing = Backing::new_anon().unwrap().0;
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
