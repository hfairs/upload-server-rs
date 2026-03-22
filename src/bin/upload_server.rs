use std::env;
use std::sync::Arc;

use anyhow::{Context, Result};
use dotenv::dotenv;
use tokio::main;
use tracing::info;
use tracing_subscriber::EnvFilter;

use upload_server_rs::adapters::incoming::UploadHttpAdapter;
use upload_server_rs::adapters::outgoing::ObjectStorageMinIOAdapter;
use upload_server_rs::applications::Uploader;
use upload_server_rs::domain::ports::spis::ObjectStorageSpi;
use upload_server_rs::domain::value_objects::{Bucket, ObjectStorageClientConfig};

#[main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = Arc::new(ObjectStorageClientConfig {
        endpoint: required_env("S3_ENDPOINT")?,
        access_key: required_env("S3_ACCESS_KEY")?,
        secret_key: required_env("S3_SECRET_KEY")?,
        bucket: required_env("S3_BUCKET")?,
        secure: required_env("S3_SECURE")? == "true",
        chunk_size: required_env("S3_CHUNK_SIZE")?.parse()?,
    });

    let object_storage_client = ObjectStorageMinIOAdapter::from_config(config.clone()).await?;
    let bucket = Bucket::new(&config.bucket)?;
    object_storage_client
        .create_bucket_if_not_exists(&bucket)
        .await?;
    let object_storage_client = Arc::new(object_storage_client);

    let app = Arc::new(Uploader::new(object_storage_client));

    let http_adapter = UploadHttpAdapter::new(app);

    let port = required_env("PORT")?;
    let addr = format!("0.0.0.0:{port}");
    info!("server listening on {addr}");
    http_adapter.serve(&addr).await?;

    Ok(())
}

fn required_env(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("Missing environment variable: {key}"))
}
