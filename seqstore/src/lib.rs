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
