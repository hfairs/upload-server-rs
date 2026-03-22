use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc::{Sender, channel};
use tokio::task::{JoinHandle, spawn};
use tracing::{error, instrument};
use url::Url;

use crate::domain::ports::services::UploadService;
use crate::domain::ports::spis::ObjectStorageSpi;
use crate::domain::value_objects::{Bucket, Error, ObjectSegment, Result, StoragePath};

/// Orchestrates streaming uploads via [`ObjectStorageSpi`].
pub struct Uploader {
    object_storage_client: Arc<dyn ObjectStorageSpi>,
}

impl Uploader {
    pub fn new(object_storage_client: Arc<dyn ObjectStorageSpi>) -> Self {
        Self {
            object_storage_client,
        }
    }
}

#[async_trait]
impl UploadService for Uploader {
    #[instrument(skip_all, err)]
    async fn start_stream(&self) -> Result<(Url, Sender<ObjectSegment>, JoinHandle<()>)> {
        let object_storage_client = Arc::clone(&self.object_storage_client);
        let url = self.object_storage_client.get_temp_url().await?;
        let host = url
            .host_str()
            .ok_or_else(|| Error::InternalError("Temp URL has no host".into()))?;
        let bucket = Bucket::new(host)?;
        let path = StoragePath::new(url.path().trim_start_matches('/'))?;

        let (sender, mut receiver) = channel::<ObjectSegment>(1024);
        let handle = spawn(async move {
            let (tx, join) = match object_storage_client
                .upload_objects_stream(&bucket, &path)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    error!(%e, "failed to start upload stream");
                    return;
                }
            };

            while let Some(chunk) = receiver.recv().await {
                if let Err(e) = tx.send(chunk).await {
                    error!(%e, "failed to send chunk");
                    return;
                }
            }

            drop(tx);
            if let Err(e) = join.await {
                error!(%e, "upload task join error");
            }
        });

        Ok((url, sender, handle))
    }
}
