use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use minio::s3::builders::{CopySource, ObjectContent, ObjectToDelete};
use minio::s3::creds::StaticProvider;
use minio::s3::error::{Error::S3Error, ErrorCode};
use minio::s3::http::BaseUrl;
use minio::s3::response::BucketExistsResponse;
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::{PartInfo, S3Api, ToStream};
use minio::s3::{Client, ClientBuilder};
use tokio::sync::mpsc::{self, Sender};
use tokio::task::{self, JoinHandle, JoinSet};
use tracing::{debug, error, info, instrument};
use url::Url;
use uuid::Uuid;

use crate::domain::ports::spis::ObjectStorageSpi;
use crate::domain::value_objects::{
    Bucket, Error, Object, ObjectSegment, ObjectStorageClientConfig, Result, StoragePath,
};

const OBJECT_STORAGE_URL_SCHEME: &str = "s3";

/// [`ObjectStorageSpi`] implementation backed by MinIO / S3.
pub struct ObjectStorageMinIOAdapter {
    pub config: Arc<ObjectStorageClientConfig>,
    pub client: Arc<Client>,
}

impl ObjectStorageMinIOAdapter {
    #[instrument(skip_all, fields(endpoint = %config.endpoint), err)]
    async fn init_minio_s3_client(config: &ObjectStorageClientConfig) -> Result<Client> {
        let protocol = if config.secure { "https" } else { "http" };

        let base_url = format!("{}://{}", protocol, config.endpoint);
        let base_url = base_url
            .parse::<BaseUrl>()
            .map_err(|e| Error::InvalidInput(format!("Invalid MinIO endpoint: {}", e)))?;

        let stat = StaticProvider::new(&config.access_key, &config.secret_key, None);

        let client = ClientBuilder::new(base_url)
            .provider(Some(Box::new(stat)))
            .build()
            .map_err(|e| Error::MinIOError(format!("Failed to initialize MinIO client: {}", e)))?;
        info!("MinIO client initialized");
        Ok(client)
    }
}

#[async_trait]
impl ObjectStorageSpi for ObjectStorageMinIOAdapter {
    async fn from_config(config: Arc<ObjectStorageClientConfig>) -> Result<Self> {
        let client = ObjectStorageMinIOAdapter::init_minio_s3_client(config.as_ref()).await?;
        let client = Arc::new(client);
        Ok(ObjectStorageMinIOAdapter { config, client })
    }

    #[instrument(skip(self), ret, err)]
    async fn bucket_exists(&self, bucket: &Bucket) -> Result<bool> {
        let resp: BucketExistsResponse = self
            .client
            .bucket_exists(bucket.as_ref())
            .send()
            .await
            .map_err(|e| {
                Error::MinIOError(format!(
                    "Failed to check if bucket '{}' exist: {}",
                    bucket, e
                ))
            })?;
        Ok(resp.exists)
    }

    #[instrument(skip(self), err)]
    async fn create_bucket_if_not_exists(&self, bucket: &Bucket) -> Result<()> {
        let bucket_exists = self.bucket_exists(bucket).await?;
        if !bucket_exists {
            info!("creating bucket");
            self.client
                .create_bucket(bucket.as_ref())
                .send()
                .await
                .map_err(|e| {
                    Error::MinIOError(format!("Failed to create bucket '{}': {}", bucket, e))
                })?;
        };
        Ok(())
    }

    #[instrument(skip(self), ret, err)]
    async fn path_empty(&self, bucket: &Bucket, path: &StoragePath) -> Result<bool> {
        let bucket_exists = self.bucket_exists(bucket).await?;
        if bucket_exists {
            let objects = self.list_objects(bucket, path).await?;
            return Ok(objects.is_empty());
        }
        Ok(true)
    }

    async fn get_temp_url(&self) -> Result<Url> {
        let bucket = Bucket::new(&self.config.bucket)?;
        self.create_bucket_if_not_exists(&bucket).await?;
        let path = Uuid::new_v4().to_string();
        let url = Url::parse(&format!(
            "{}://{}/{}",
            OBJECT_STORAGE_URL_SCHEME, bucket, path
        ))
        .map_err(|e| Error::InternalError(format!("Failed to parse temp URL '{}': {}", path, e)))?;
        Ok(url)
    }

    #[instrument(skip(self, objects), fields(count = objects.len()), err)]
    async fn upload_objects(
        &self,
        bucket: &Bucket,
        path: &StoragePath,
        objects: Vec<Object>,
    ) -> Result<()> {
        self.create_bucket_if_not_exists(bucket).await?;

        let mut handles = JoinSet::new();
        for object in objects {
            let client = self.client.clone();
            let bucket = bucket.to_string();
            let object_name = format!("{}/{}", path, object.name);
            let content = ObjectContent::from(object.data);
            handles.spawn(
                client
                    .put_object_content(bucket.clone(), object_name.clone(), content)
                    .send(),
            );
        }

        let mut completed = vec![];
        let mut errors = vec![];
        while let Some(result) = handles.join_next().await {
            match result {
                Ok(Ok(resp)) => completed.push(resp.object),
                Ok(Err(e)) => {
                    error!(error = %e, "object upload failed");
                    errors.push(Error::MinIOError(format!("object upload failed: {:?}", e)));
                }
                Err(e) => {
                    error!(error = %e, "task failed");
                    errors.push(Error::MinIOError(format!("Task failed: {:?}", e)));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::MinIOError(format!(
                "Failed to upload some objects: {:?}",
                errors
            )))
        }
    }

    #[instrument(skip(self, content), fields(size = content.len()), ret, err)]
    async fn upload_object(&self, bucket: &Bucket, key: &str, content: Bytes) -> Result<String> {
        self.create_bucket_if_not_exists(bucket).await?;

        let object_content = ObjectContent::from(content);
        self.client
            .put_object_content(bucket.as_ref(), key, object_content)
            .send()
            .await
            .map_err(|e| {
                Error::MinIOError(format!(
                    "Failed to upload object '{}' to bucket '{}': {}",
                    key, bucket, e
                ))
            })?;
        Ok(key.to_string())
    }

    #[instrument(skip(self), err)]
    async fn upload_object_stream(
        &self,
        bucket: &Bucket,
        key: &str,
    ) -> Result<(Sender<ObjectSegment>, JoinHandle<()>)> {
        let (sender, mut receiver) = mpsc::channel::<ObjectSegment>(1024);
        let client = self.client.clone();
        let chunk_size = self.config.chunk_size;
        let bucket = bucket.to_string();
        let key = key.to_string();

        let resp = client
            .create_multipart_upload(&bucket, &key)
            .send()
            .await
            .unwrap();

        let upload_id = resp.upload_id;

        let handle = task::spawn(async move {
            let mut part_info = vec![];
            let mut part_num: u16 = 1;
            let mut segment: SegmentedBytes = SegmentedBytes::new();
            while let Some(chunk) = receiver.recv().await {
                segment.append(chunk.data);

                if segment.len() >= chunk_size {
                    let size = segment.len() as u64;
                    let resp = client
                        .upload_part(&bucket, &key, &upload_id, part_num, segment)
                        .send()
                        .await
                        .unwrap();

                    part_info.push(PartInfo {
                        number: part_num,
                        etag: resp.etag,
                        size,
                    });

                    part_num += 1;
                    segment = SegmentedBytes::new();
                }
            }

            if !segment.is_empty() {
                let size = segment.len() as u64;
                let resp = client
                    .upload_part(&bucket, &key, &upload_id, part_num, segment)
                    .send()
                    .await
                    .unwrap();
                part_info.push(PartInfo {
                    number: part_num,
                    etag: resp.etag,
                    size,
                });
            }

            debug!(parts = part_info.len(), "completing multipart upload");
            let _resp = client
                .complete_multipart_upload(bucket, key, upload_id, part_info)
                .send()
                .await
                .unwrap();
        });

        Ok((sender, handle))
    }

    async fn upload_objects_stream(
        &self,
        bucket: &Bucket,
        path: &StoragePath,
    ) -> Result<(Sender<ObjectSegment>, JoinHandle<()>)> {
        let (sender, mut receiver) = mpsc::channel::<ObjectSegment>(1024);
        let zelf = Arc::new(Self {
            config: self.config.clone(),
            client: self.client.clone(),
        });
        let bucket = bucket.clone();
        let path = path.to_string();

        let handle = task::spawn(async move {
            let mut key_map = HashMap::new();
            while let Some(chunk) = receiver.recv().await {
                let key = format!("{}/{}", path, chunk.name);

                if !key_map.contains_key(&key) {
                    let (sender, handle) = zelf.upload_object_stream(&bucket, &key).await.unwrap();
                    key_map.insert(key.clone(), (sender, handle));
                }

                let (sender, _) = key_map.get(&key).unwrap();
                sender.send(chunk).await.unwrap();
            }

            for (_, (sender, handle)) in key_map {
                drop(sender);
                handle.await.unwrap();
            }
        });

        Ok((sender, handle))
    }

    #[instrument(skip(self), err)]
    async fn download_object(&self, bucket: &Bucket, key: &str) -> Result<(String, Bytes)> {
        self.create_bucket_if_not_exists(bucket).await?;

        let response = self
            .client
            .get_object(bucket.as_ref(), key)
            .send()
            .await
            .map_err(|e| {
                if let S3Error(err) = &e
                    && ErrorCode::NoSuchKey == err.code
                {
                    return Error::NotFound(format!(
                        "Object '{}' not found in bucket '{}'",
                        key, bucket
                    ));
                }
                Error::MinIOError(format!(
                    "Failed to download object '{}' from bucket '{}': {}",
                    key, bucket, e
                ))
            })?;

        let content = response
            .content
            .to_segmented_bytes()
            .await
            .map_err(|e| {
                Error::MinIOError(format!(
                    "Failed to read object content from '{}' in bucket '{}': {}",
                    key, bucket, e
                ))
            })?
            .to_bytes();
        Ok((key.to_string(), content))
    }

    #[instrument(skip(self), ret, err)]
    async fn list_objects(&self, bucket: &Bucket, path: &StoragePath) -> Result<Vec<String>> {
        let path = path.as_ref();
        let mut response = self
            .client
            .list_objects(bucket.as_ref())
            .prefix(Some(path.to_string()))
            .recursive(true)
            .to_stream()
            .await;

        let mut objects = vec![];
        while let Some(result) = response.next().await {
            match result {
                Ok(resp) => {
                    for item in resp.contents {
                        let objectname = item.name.strip_prefix(path).unwrap_or(&item.name);
                        let objectname = objectname.trim_start_matches('/');
                        objects.push(objectname.to_string());
                    }
                }
                Err(e) => return Err(Error::MinIOError(format!("List item error: {:?}", e))),
            }
        }
        Ok(objects)
    }

    #[instrument(skip(self), err)]
    async fn copy_objects(
        &self,
        source_bucket: &Bucket,
        source_path: &StoragePath,
        dest_bucket: &Bucket,
        dest_path: &StoragePath,
        force: bool,
    ) -> Result<()> {
        self.create_bucket_if_not_exists(dest_bucket).await?;

        if !force {
            let dest_path_empty = self.path_empty(dest_bucket, dest_path).await?;
            if !dest_path_empty {
                return Err(Error::MinIOError(format!(
                    "Destination path '{}' in bucket '{}' already contains objects. Use force=true to overwrite.",
                    dest_path, dest_bucket
                )));
            }
        }

        let mut handles = vec![];
        let objects = self.list_objects(source_bucket, source_path).await?;
        for object in objects {
            let source_object = CopySource::new(
                source_bucket.as_ref(),
                &format!("{}/{}", source_path, object),
            )
            .map_err(|e| Error::InternalError(e.to_string()))?;
            let dest_object = format!("{}/{}", dest_path, object);
            handles.push(task::spawn(
                self.client
                    .copy_object(dest_bucket.as_ref(), dest_object)
                    .source(source_object)
                    .send(),
            ));
        }

        for handle in handles {
            handle
                .await
                .map_err(|e| Error::InternalError(format!("Failed to complete copy task: {}", e)))?
                .map_err(|e| Error::MinIOError(format!("Failed to copy object: {}", e)))?;
        }

        Ok(())
    }

    #[instrument(skip(self), err)]
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

    #[instrument(skip(self), ret, err)]
    async fn delete_objects(&self, bucket: &Bucket, path: &StoragePath) -> Result<bool> {
        let objects = self.list_objects(bucket, path).await?;
        let objects = objects
            .into_iter()
            .map(|object| format!("{}/{}", path, object))
            .map(ObjectToDelete::from)
            .collect::<Vec<_>>();
        if objects.is_empty() {
            return Ok(false);
        }

        self.client
            .delete_objects::<&str, ObjectToDelete>(bucket.as_ref(), objects.clone())
            .send()
            .await
            .map_err(|e| {
                Error::MinIOError(format!("Failed to delete directory: {}: {}", path, e))
            })?;
        Ok(true)
    }

    #[instrument(skip(self), ret, err)]
    async fn delete_object(&self, bucket: &Bucket, key: &str) -> Result<String> {
        self.create_bucket_if_not_exists(bucket).await?;

        self.client
            .delete_object(bucket.as_ref(), key)
            .send()
            .await
            .map_err(|e| {
                if let S3Error(err) = &e
                    && ErrorCode::NoSuchKey == err.code
                {
                    return Error::NotFound(format!(
                        "Object '{}' not found in bucket '{}'",
                        key, bucket
                    ));
                }
                Error::MinIOError(format!(
                    "Failed to delete object '{}' from bucket '{}': {}",
                    key, bucket, e
                ))
            })?;
        Ok(key.to_string())
    }
}
