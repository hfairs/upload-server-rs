use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use url::Url;

use crate::domain::value_objects::{
    Bucket, Object, ObjectSegment, ObjectStorageClientConfig, Result, StoragePath,
};

/// Defines how the platform interacts with object storage services.
#[async_trait]
pub trait ObjectStorageSpi: Send + Sync {
    /// Create a client instance from config.
    async fn from_config(config: Arc<ObjectStorageClientConfig>) -> Result<Self>
    where
        Self: Sized;

    /// Check if a bucket exists.
    async fn bucket_exists(&self, bucket: &Bucket) -> Result<bool>;

    /// Create a bucket if it does not exist.
    async fn create_bucket_if_not_exists(&self, bucket: &Bucket) -> Result<()>;

    /// Check if a path in the given bucket is empty.
    async fn path_empty(&self, bucket: &Bucket, path: &StoragePath) -> Result<bool>;

    /// Get a temporary URL for uploading objects.
    async fn get_temp_url(&self) -> Result<Url>;

    /// Upload multiple objects to the given bucket and path.
    async fn upload_objects(
        &self,
        bucket: &Bucket,
        path: &StoragePath,
        objects: Vec<Object>,
    ) -> Result<()>;

    /// Upload a single binary object to the given bucket at the specified key.
    async fn upload_object(&self, bucket: &Bucket, key: &str, content: Bytes) -> Result<String>;

    /// Start a streaming upload for a single object.
    async fn upload_object_stream(
        &self,
        bucket: &Bucket,
        key: &str,
    ) -> Result<(Sender<ObjectSegment>, JoinHandle<()>)>;

    /// Start a streaming upload for multiple objects.
    async fn upload_objects_stream(
        &self,
        bucket: &Bucket,
        path: &StoragePath,
    ) -> Result<(Sender<ObjectSegment>, JoinHandle<()>)>;

    /// Download an object by key. Returns `(key, content)`.
    async fn download_object(&self, bucket: &Bucket, key: &str) -> Result<(String, Bytes)>;

    /// List object names under the given bucket and path.
    async fn list_objects(&self, bucket: &Bucket, path: &StoragePath) -> Result<Vec<String>>;

    /// Copy all objects from source to destination. Set `force` to overwrite existing objects.
    async fn copy_objects(
        &self,
        source_bucket: &Bucket,
        source_path: &StoragePath,
        dest_bucket: &Bucket,
        dest_path: &StoragePath,
        force: bool,
    ) -> Result<()>;

    /// Move all objects from source to destination. Set `force` to overwrite existing objects.
    async fn move_objects(
        &self,
        source_bucket: &Bucket,
        source_path: &StoragePath,
        dest_bucket: &Bucket,
        dest_path: &StoragePath,
        force: bool,
    ) -> Result<()>;

    /// Delete all objects under the given bucket and path.
    /// Returns `true` if objects were deleted, `false` if none were found.
    async fn delete_objects(&self, bucket: &Bucket, path: &StoragePath) -> Result<bool>;

    /// Delete a single object by key.
    async fn delete_object(&self, bucket: &Bucket, key: &str) -> Result<String>;
}
