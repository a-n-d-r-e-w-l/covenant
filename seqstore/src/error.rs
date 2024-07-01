use std::fmt::{Debug, Display, Formatter};

use bstr::{BStr, BString};
use thiserror::Error;

use crate::Id;

/// Errors that can be encountered during general operations when using a
/// [`RawStore`][crate::raw_store::RawStore].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Failed to create a memory map.
    Map(#[source] std::io::Error),
    /// Failed to resize the underlying file/memory map.
    Resize(#[source] std::io::Error),
    /// Failed to flush the underlying memory map to disk.
    Flush(#[source] std::io::Error),
    /// Encountered an unknown tag.
    ///
    /// This almost certainly means that an incorrect or invalid [`Id`] was given as an argument.
    UnknownTag {
        position: usize,
        /// A few bytes surrounding the invalid tag, possibly zero-filled if at the start or
        /// end of the data.
        ///
        /// Intended only for debugging purposes - do not rely on this remaining of size 7.
        surrounding: [u8; 7],
        byte: u8,
    },
    /// Encountered an invalid tag for the desired operation.
    ///
    /// This most likely means that an incorrect [`Id`] has been given as an argument.
    IncorrectTag {
        position: usize,
        found: Tag,
        expected_kind: &'static str,
    },
    /// An [`Id`] did not match the data stored at its location. This normally means that the Id was
    /// constructed through invalid means (_i.e._ it did not originate from this map), or that its
    /// data was erased and replaced with another call to [`add(..)`][crate::raw_store::RawStore::add]
    IdCheck(Id),
    /// Encountered partially-written data.
    /// This should only be possible if the previous write operation at this location was interrupted
    /// by program termination.
    EntryCorrupt { position: usize },
    /// Attempted to delete an already-deleted item.
    AlreadyDeleted { position: usize },
    /// Attempted to [`replace(..)`][crate::raw_store::RawStore::replace] a deleted item.
    ///
    /// The `replace` operation should only be used to update written data - there is no functionality
    /// to request that **new** data be written at a specific location.
    CannotReplaceDeleted { position: usize },
    /// Attempted to [`replace(..)`][crate::raw_store::RawStore::replace] an item with data of a
    /// different length.
    ///
    /// The `replace` operation only supports replacing data in-place if the new data has the same length
    /// as the old data.
    MismatchedLengths { position: usize, new: usize, old: usize },
    /// A [varint][varuint]-encoded integer failed to read.
    ///
    /// As varints are (currently) only used in the header, this likely means that the file has been
    /// externally modified.
    InvalidVarint { position: usize },
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
            Self::IdCheck(id) => {
                write!(f, "{id:?} did not pass verification for referenced data")
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

/// Errors that can be encountered while [open][crate::raw_store::OpenStoreOptions::open]ing a map.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenError {
    /// Any normal error.
    #[error(transparent)]
    General(#[from] Error),
    /// The [`Backing`][crate::backing::Backing] was too small to possibly be valid.
    #[error("data of size {} is too small to hold header of size {}", .found, .expected)]
    TooSmall { found: usize, expected: usize },
    /// The header's magic bytes were invalid.
    #[error("invalid magic bytes")]
    Magic,
    /// The header's specialized magic bytes did not match the expected length.
    ///
    /// For more detail about header specialization, see the section in
    /// [`OpenStoreOptions`][crate::raw_store::OpenStoreOptions#header-specialization].
    #[error("mismatch between spec magic: expected {} bytes, found {}", .expected, .found)]
    SpecMagicLen { found: usize, expected: usize },
    /// The header's specialized magic bytes were invalid.
    ///
    /// For more detail about header specialization, see the section in
    /// [`OpenStoreOptions`][crate::raw_store::OpenStoreOptions#header-specialization].
    #[error("mismatch between spec magic: expected {:?}, found {:?}", .expected, .found)]
    SpecMagic { found: BString, expected: BString },
    /// The header version is unknown.
    #[error("unknown version {:?}", .0)]
    UnknownVersion([u8; 2]),
    /// See [`Error::EntryCorrupt`].
    #[error("found incomplete write of length {} at 0x{:X}", .length, .position)]
    PartialWrite { position: usize, length: usize },
    /// Data was encountered after the end tag.
    ///
    /// This is only possible if the file has been externally modified.
    #[error("end tag is at 0x{end:X}, but 0b{first_data:08b} was found after it at 0x{first_data_at:X}")]
    DataAfterEnd { end: usize, first_data_at: usize, first_data: u8 },
    /// The end tag was not found.
    ///
    /// This most likely means that the file has been externally modified.
    #[error("no end tag found")]
    NoEnd,
}

// This exists to prevent a `private_interfaces` warning without exposing MagicTag
/// An opaque representation of the internal tag structure.
///
/// Only used for error reporting.
#[derive(Copy, Clone)]
#[repr(transparent)]
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
