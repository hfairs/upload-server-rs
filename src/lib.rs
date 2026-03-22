//! Streaming upload server backed by S3-compatible object storage.

pub mod adapters;
pub mod applications;
pub mod domain;
pub mod infrastructure;

#[cfg(any(test, feature = "test-support"))]
pub mod testing;
