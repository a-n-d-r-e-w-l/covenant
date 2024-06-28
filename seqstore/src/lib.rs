#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(not(target_pointer_width = "64"))]
compile_error!("only available on 64-bit targets");

pub use backing::Backing;
pub use id::{Id, PackedId};

pub(crate) mod backing;
mod id;
pub(crate) mod tag;
pub(crate) mod util;

pub mod error;
pub mod raw_store;

// We want this to be accessible to all maps (if/when we have a private trait on all maps)
// but we don't want to make the fields of `raw_store::RawStore` pub(crate)
/// A function that can describe the contents of a [`RawStore`][raw_store::RawStore], intended for debugging
/// when working on this crate itself.
///
/// Requires a [`log`]-compatible logger to be setup.
#[cfg(feature = "debug_map")]
#[cfg_attr(docsrs, doc(cfg(feature = "debug_map")))]
pub fn debug_map(map: &raw_store::RawStore) -> Result<(), error::Error> {
    raw_store::debug_map(map)
}
