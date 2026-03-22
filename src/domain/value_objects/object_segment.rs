use std::fmt;

use bytes::Bytes;

/// A chunk of a multipart upload.
pub struct ObjectSegment {
    pub name: String,
    pub part_num: u32,
    pub data: Bytes,
}

impl fmt::Display for ObjectSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ObjectSegment {{ name: {}, part: {} }}",
            self.name, self.part_num
        )
    }
}
