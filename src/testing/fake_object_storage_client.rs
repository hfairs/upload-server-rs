use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinHandle;
use url::Url;

use crate::domain::ports::spis::ObjectStorageSpi;
use crate::domain::value_objects::{
    Bucket, Error, Object, ObjectSegment, ObjectStorageClientConfig, Result, StoragePath,
};

/// In-memory [`ObjectStorageSpi`] for testing. Stores object names only (no data).
pub struct FakeObjectStorageClient {
    buckets: Arc<DashMap<String, Vec<String>>>,
    objects: Arc<DashMap<(String, String), Vec<String>>>,
}

impl FakeObjectStorageClient {
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(DashMap::new()),
            objects: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl ObjectStorageSpi for FakeObjectStorageClient {
    async fn from_config(_config: Arc<ObjectStorageClientConfig>) -> Result<Self> {
        Ok(Self::new())
    }

    async fn bucket_exists(&self, bucket: &Bucket) -> Result<bool> {
        Ok(self.buckets.contains_key(bucket.as_ref()))
    }

    async fn create_bucket_if_not_exists(&self, bucket: &Bucket) -> Result<()> {
        if !self.buckets.contains_key(bucket.as_ref()) {
            self.buckets.insert(bucket.to_string(), Vec::new());
        }
        Ok(())
    }

    async fn path_empty(&self, bucket: &Bucket, path: &StoragePath) -> Result<bool> {
        let objects = self.objects.get(&(bucket.to_string(), path.to_string()));
        Ok(objects.is_none_or(|v| v.is_empty()))
    }

    async fn get_temp_url(&self) -> Result<Url> {
        Ok(Url::parse("s3://test-bucket/abcd1234").unwrap())
    }

    async fn upload_objects(
        &self,
        bucket: &Bucket,
        path: &StoragePath,
        objects: Vec<Object>,
    ) -> Result<()> {
        self.create_bucket_if_not_exists(bucket).await?;
        self.objects
            .entry((bucket.to_string(), path.to_string()))
            .or_default()
            .extend(objects.into_iter().map(|obj| obj.name));
        Ok(())
    }

    async fn upload_object(&self, bucket: &Bucket, key: &str, _: Bytes) -> Result<String> {
        self.create_bucket_if_not_exists(bucket).await?;
        self.objects
            .entry((bucket.to_string(), String::new()))
            .or_default()
            .push(key.to_string());
        Ok(key.to_string())
    }

    async fn upload_object_stream(
        &self,
        _bucket: &Bucket,
        _key: &str,
    ) -> Result<(Sender<ObjectSegment>, JoinHandle<()>)> {
        todo!()
    }

    async fn upload_objects_stream(
        &self,
        bucket: &Bucket,
        path: &StoragePath,
    ) -> Result<(Sender<ObjectSegment>, JoinHandle<()>)> {
        let (sender, mut receiver) = mpsc::channel::<ObjectSegment>(1024);
        let mut file_names = HashSet::new();
        let collect_handle = tokio::spawn(async move {
            while let Some(chunk) = receiver.recv().await {
                file_names.insert(chunk.name.clone());
            }
            file_names
        });
        let file_names = collect_handle.await.unwrap();
        self.buckets
            .entry(bucket.to_string())
            .or_default()
            .push(path.to_string());
        self.objects
            .entry((bucket.to_string(), path.to_string()))
            .or_default()
            .extend(file_names);
        let handle = tokio::spawn(async {});
        Ok((sender, handle))
    }

    async fn download_object(&self, bucket: &Bucket, key: &str) -> Result<(String, Bytes)> {
        let object_key = (bucket.to_string(), String::new());
        if let Some(objects) = self.objects.get(&object_key)
            && objects.contains(&key.to_string())
        {
            return Ok((key.to_string(), Bytes::new()));
        }
        Err(Error::NotFound(format!(
            "Object '{}' not found in bucket '{}'",
            key, bucket
        )))
    }

    async fn list_objects(&self, bucket: &Bucket, path: &StoragePath) -> Result<Vec<String>> {
        Ok(self
            .objects
            .get(&(bucket.to_string(), path.to_string()))
            .map(|v| v.clone())
            .unwrap_or_default())
    }

    async fn copy_objects(
        &self,
        source_bucket: &Bucket,
        source_path: &StoragePath,
        dest_bucket: &Bucket,
        dest_path: &StoragePath,
        force: bool,
    ) -> Result<()> {
        if !force && !self.path_empty(dest_bucket, dest_path).await? {
            return Err(Error::AlreadyExistsError(format!(
                "Destination '{}/{}' is not empty",
                dest_bucket, dest_path
            )));
        }
        self.create_bucket_if_not_exists(dest_bucket).await?;
        self.buckets
            .entry(dest_bucket.to_string())
            .or_default()
            .push(dest_path.to_string());
        self.objects
            .entry((dest_bucket.to_string(), dest_path.to_string()))
            .or_default()
            .extend(
                self.objects
                    .get(&(source_bucket.to_string(), source_path.to_string()))
                    .map(|v| v.clone())
                    .unwrap_or_default(),
            );
        Ok(())
    }

    async fn move_objects(
        &self,
        source_bucket: &Bucket,
        source_path: &StoragePath,
        dest_bucket: &Bucket,
        dest_path: &StoragePath,
        force: bool,
    ) -> Result<()> {
        self.copy_objects(source_bucket, source_path, dest_bucket, dest_path, force)
            .await?;
        self.delete_objects(source_bucket, source_path).await?;
        Ok(())
    }

    async fn delete_objects(&self, bucket: &Bucket, path: &StoragePath) -> Result<bool> {
        self.objects.remove(&(bucket.to_string(), path.to_string()));
        self.buckets.entry(bucket.to_string()).and_modify(|paths| {
            paths.retain(|p| p != path.as_ref());
        });
        Ok(true)
    }

    async fn delete_object(&self, bucket: &Bucket, key: &str) -> Result<String> {
        let object_key = (bucket.to_string(), key.to_string());
        if self.objects.remove(&object_key).is_some() {
            Ok(key.to_string())
        } else {
            Err(Error::NotFound(format!(
                "Object '{}' not found in bucket '{}'",
                key, bucket
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(name: &str) -> Bucket {
        Bucket::new(name).unwrap()
    }

    fn path(p: &str) -> StoragePath {
        StoragePath::new(p).unwrap()
    }

    #[tokio::test]
    async fn bucket_lifecycle() {
        let client = FakeObjectStorageClient::new();
        let b = bucket("test-bucket");

        assert!(!client.bucket_exists(&b).await.unwrap());
        client.create_bucket_if_not_exists(&b).await.unwrap();
        assert!(client.bucket_exists(&b).await.unwrap());
    }

    #[tokio::test]
    async fn upload_and_list() {
        let client = FakeObjectStorageClient::new();
        let b = bucket("test-bucket");
        let p = path("uploads");

        let objects = vec![
            Object {
                name: "a.txt".into(),
                data: Bytes::from("hello"),
            },
            Object {
                name: "b.txt".into(),
                data: Bytes::from("world"),
            },
        ];
        client.upload_objects(&b, &p, objects).await.unwrap();

        let listed = client.list_objects(&b, &p).await.unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed.contains(&"a.txt".to_string()));
        assert!(listed.contains(&"b.txt".to_string()));
    }

    #[tokio::test]
    async fn upload_and_download_single() {
        let client = FakeObjectStorageClient::new();
        let b = bucket("test-bucket");

        client
            .upload_object(&b, "key1", Bytes::from("data"))
            .await
            .unwrap();
        let (key, _) = client.download_object(&b, "key1").await.unwrap();
        assert_eq!(key, "key1");
    }

    #[tokio::test]
    async fn download_missing_returns_error() {
        let client = FakeObjectStorageClient::new();
        let b = bucket("test-bucket");
        client.create_bucket_if_not_exists(&b).await.unwrap();

        let result = client.download_object(&b, "nope").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn copy_and_delete() {
        let client = FakeObjectStorageClient::new();
        let src_b = bucket("src-bucket");
        let dst_b = bucket("dst-bucket");
        let src_p = path("src");
        let dst_p = path("dst");

        let objects = vec![Object {
            name: "file.bin".into(),
            data: Bytes::new(),
        }];
        client
            .upload_objects(&src_b, &src_p, objects)
            .await
            .unwrap();

        client
            .copy_objects(&src_b, &src_p, &dst_b, &dst_p, false)
            .await
            .unwrap();
        let listed = client.list_objects(&dst_b, &dst_p).await.unwrap();
        assert_eq!(listed, vec!["file.bin"]);

        client.delete_objects(&dst_b, &dst_p).await.unwrap();
        assert!(client.path_empty(&dst_b, &dst_p).await.unwrap());
    }

    #[tokio::test]
    async fn move_objects_removes_source() {
        let client = FakeObjectStorageClient::new();
        let src_b = bucket("src-bucket");
        let dst_b = bucket("dst-bucket");
        let src_p = path("src");
        let dst_p = path("dst");

        let objects = vec![Object {
            name: "file.bin".into(),
            data: Bytes::new(),
        }];
        client
            .upload_objects(&src_b, &src_p, objects)
            .await
            .unwrap();

        client
            .move_objects(&src_b, &src_p, &dst_b, &dst_p, false)
            .await
            .unwrap();
        assert!(client.path_empty(&src_b, &src_p).await.unwrap());
        assert!(!client.path_empty(&dst_b, &dst_p).await.unwrap());
    }

    #[tokio::test]
    async fn copy_without_force_fails_if_dest_not_empty() {
        let client = FakeObjectStorageClient::new();
        let b = bucket("test-bucket");
        let p = path("data");

        let objects = vec![Object {
            name: "f.bin".into(),
            data: Bytes::new(),
        }];
        client.upload_objects(&b, &p, objects).await.unwrap();

        let result = client.copy_objects(&b, &p, &b, &p, false).await;
        assert!(result.is_err());
    }
}
