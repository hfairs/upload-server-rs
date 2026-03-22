use std::fmt;

use crate::domain::value_objects::{Error, Result};

/// A validated storage path (prefix/directory) within a bucket.
///
/// Rules:
/// - Non-empty
/// - No leading or trailing slashes
/// - Only printable ASCII, no control characters
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoragePath(String);

impl StoragePath {
    pub fn new(path: &str) -> Result<Self> {
        let path = path.trim().trim_matches('/');
        if path.is_empty() {
            return Err(Error::ValidationError(
                "Storage path must not be empty.".into(),
            ));
        }
        if !path.chars().all(|c| c.is_ascii_graphic() || c == ' ') {
            return Err(Error::ValidationError(
                "Storage path must only contain printable ASCII characters.".into(),
            ));
        }
        Ok(Self(path.to_string()))
    }
}

impl AsRef<str> for StoragePath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StoragePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_paths() {
        assert!(StoragePath::new("abc").is_ok());
        assert!(StoragePath::new("a/b/c").is_ok());
        assert!(StoragePath::new("models/v1").is_ok());
    }

    #[test]
    fn strips_slashes() {
        let p = StoragePath::new("/leading/trailing/").unwrap();
        assert_eq!(p.as_ref(), "leading/trailing");
    }

    #[test]
    fn empty_is_invalid() {
        assert!(StoragePath::new("").is_err());
        assert!(StoragePath::new("  ").is_err());
        assert!(StoragePath::new("/").is_err());
    }

    #[test]
    fn control_chars_rejected() {
        assert!(StoragePath::new("abc\x00def").is_err());
    }

    #[test]
    fn as_ref_and_display() {
        let p = StoragePath::new("foo/bar").unwrap();
        assert_eq!(p.as_ref(), "foo/bar");
        assert_eq!(p.to_string(), "foo/bar");
    }
}
