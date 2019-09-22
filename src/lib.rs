//! Hadoop WebHDFS API for Rust

#[macro_use] 
mod error;
mod rest_client;
mod natmap;
mod uri_tools;
mod op;
pub mod config;
pub mod datatypes;
pub mod async_client;
pub mod sync_client;

pub use natmap::NatMap;
pub use error::{Error, Result};
pub use datatypes::*;
pub use op::*;
pub use async_client::{HdfsClient, HdfsClientBuilder};
pub use sync_client::{SyncHdfsClient, SyncHdfsClientBuilder};
pub use http::Uri;