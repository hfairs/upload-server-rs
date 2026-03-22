/// Connection configuration for an object storage service.
#[derive(Debug, Clone)]
pub struct ObjectStorageClientConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub secure: bool,
    pub chunk_size: usize,
}
