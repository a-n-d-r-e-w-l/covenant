pub use backing::Backing;
pub use error::Error;

pub(crate) mod backing;
pub(crate) mod tag;

mod error;
pub mod raw_store;

// We want this to be accessible to all maps (when we have a private trait on all maps)
// but we don't want to make the fields of `raw_map::FileMap` pub(crate)
#[cfg(feature = "debug_map")]
pub fn debug_map(map: &raw_store::RawStore) -> Result<(), Error> {
    raw_store::debug_map(map)
}
