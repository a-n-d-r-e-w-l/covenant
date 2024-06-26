use std::fmt::{Debug, Display, Formatter};

use bstr::{BStr, BString};
use thiserror::Error;

use crate::Id;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    Resize(#[source] std::io::Error),
    Flush(#[source] std::io::Error),
    Map(#[source] std::io::Error),
    UnknownTag {
        position: usize,
        surrounding: [u8; 7],
        byte: u8,
    },
    IncorrectTag {
        position: usize,
        found: Tag,
        expected_kind: &'static str,
    },
    EntryCorrupt {
        position: usize,
    },
    AlreadyDeleted {
        position: usize,
    },
    CannotReplaceDeleted {
        position: usize,
    },
    MismatchedLengths {
        position: usize,
        new: usize,
        old: usize,
    },
    InvalidVarint {
        position: usize,
    },
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resize(e) => write!(f, "could not resize backing: {e}"),
            Self::Flush(e) => write!(f, "could not flush data: {e}"),
            Self::Map(e) => write!(f, "could not create memory map: {e}"),
            Self::UnknownTag { position, surrounding, byte } => write!(
                f,
                "unknown tag {byte:08b} at position 0x{position:X} - {:?}",
                BStr::new(surrounding)
            ),
            Self::IncorrectTag {
                position,
                found,
                expected_kind,
            } => {
                write!(f, "expected {expected_kind} tag at 0x{position:X}, found {found:?}")
            }
            Self::EntryCorrupt { position } => write!(f, "previous write at 0x{position:X} was interrupted, this entry is corrupt"),
            Self::AlreadyDeleted { position } => write!(f, "attempted to delete already deleted item at 0x{position:X}"),
            Self::CannotReplaceDeleted { position } => write!(f, "attempted to replace deleted item at 0x{position:X}"),
            Self::MismatchedLengths { position, new, old } => {
                write!(
                    f,
                    "cannot replace data of length {new} at 0x{position:X} with data of length {old}"
                )
            }
            Self::InvalidVarint { position } => {
                write!(f, "invalid packed integer or EOF at 0x{:X}", position)
            }
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenError {
    #[error(transparent)]
    General(#[from] Error),
    #[error("data of size {} is too small to hold header of size {}", .found, .expected)]
    TooSmall { found: usize, expected: usize },
    #[error("invalid magic bytes")]
    Magic,
    #[error("mismatch between spec magic: expected {} bytes, found {}", .expected, .found)]
    SpecMagicLen { found: usize, expected: usize },
    #[error("mismatch between spec magic: expected {:?}, found {:?}", .expected, .found)]
    SpecMagic { found: BString, expected: BString },
    #[error("unknown version {:?}", .0)]
    UnknownVersion([u8; 2]),
    #[error("found incomplete write of length {} at 0x{:X}", .length, .position)]
    PartialWrite { position: usize, length: usize },
    #[error("end tag is at 0x{end:X}, but 0b{first_data:08b} was found after it at 0x{first_data_at:X}")]
    DataAfterEnd { end: usize, first_data_at: usize, first_data: u8 },
    #[error("no end tag found")]
    NoEnd,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RetainError {
    #[error(transparent)]
    General(#[from] Error),
    #[error("expected inputs to be sorted: given {:?} before {:?}", .0, .1)]
    UnsortedInputs(Id, Id),
    #[error("attempted to retain partially-written data at 0x{:X}", .position)]
    RetainPartial { position: usize },
}

// This exists to prevent a `private_interfaces` warning without exposing MagicTag
#[derive(Copy, Clone)]
pub struct Tag(crate::tag::MagicTag);

impl Debug for Tag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <crate::tag::MagicTag as Debug>::fmt(&self.0, f)
    }
}

impl From<crate::tag::MagicTag> for Tag {
    fn from(value: crate::tag::MagicTag) -> Self {
        Self(value)
    }
}
