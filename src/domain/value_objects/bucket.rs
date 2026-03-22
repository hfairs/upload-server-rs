use std::fmt;

use crate::domain::value_objects::{Error, Result};

/// A validated object storage bucket name.
///
/// Rules (aligned with S3/MinIO):
/// - 3 to 63 characters
/// - Only lowercase alphanumeric, hyphens, and dots
/// - Must start and end with a letter or digit
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Bucket(String);

impl Bucket {
    pub fn new(name: &str) -> Result<Self> {
        let name = name.trim();
        if name.len() < 3 || name.len() > 63 {
            return Err(Error::ValidationError(
                "Bucket name must be between 3 and 63 characters.".into(),
            ));
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
        {
            return Err(Error::ValidationError(
                "Bucket name must only contain lowercase letters, digits, hyphens, and dots."
                    .into(),
            ));
        }
        if !name.starts_with(|c: char| c.is_ascii_alphanumeric())
            || !name.ends_with(|c: char| c.is_ascii_alphanumeric())
        {
            return Err(Error::ValidationError(
                "Bucket name must start and end with a letter or digit.".into(),
            ));
        }
        Ok(Self(name.to_string()))
    }
}

impl AsRef<str> for Bucket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_bucket() {
        assert!(Bucket::new("my-bucket").is_ok());
        assert!(Bucket::new("abc").is_ok());
        assert!(Bucket::new("bucket.name.123").is_ok());
    }

    #[test]
    fn too_short() {
        assert!(Bucket::new("ab").is_err());
    }

    #[test]
    fn invalid_chars() {
        assert!(Bucket::new("My-Bucket").is_err());
        assert!(Bucket::new("bucket_name").is_err());
    }

    #[test]
    fn bad_start_end() {
        assert!(Bucket::new("-bucket").is_err());
        assert!(Bucket::new("bucket-").is_err());
    }

    #[test]
    fn as_ref_and_display() {
        let b = Bucket::new("test-bucket").unwrap();
        assert_eq!(b.as_ref(), "test-bucket");
        assert_eq!(b.to_string(), "test-bucket");
    }
}
