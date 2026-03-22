use std::fmt;

use bytes::Bytes;

/// A named binary object stored in object storage.
pub struct Object {
    pub name: String,
    pub data: Bytes,
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Object {{ name: {}, size: {} }}",
            self.name,
            self.data.len()
        )
    }
}
