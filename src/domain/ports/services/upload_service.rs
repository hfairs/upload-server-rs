use async_trait::async_trait;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use url::Url;

use crate::domain::value_objects::{ObjectSegment, Result};

/// Handles object uploads (model binaries, datasets, etc.) to temporary storage.
#[async_trait]
pub trait UploadService: Send + Sync {
    /// Start a streaming upload, returning a temporary URL, a channel sender, and a join handle.
    async fn start_stream(&self) -> Result<(Url, Sender<ObjectSegment>, JoinHandle<()>)>;
}
