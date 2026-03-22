# upload-server-rs

A high-performance, streaming file upload server built in Rust, purpose-designed for transferring large binary assets such as machine learning model artifacts, datasets, and checkpoints. Uploads are streamed directly to the backend with minimal memory overhead — no full-file buffering.

## Architecture

The system follows **Domain-Driven Design (DDD)** with a **Ports & Adapters (Hexagonal Architecture)** pattern, cleanly separating core domain logic from infrastructure concerns. Both the inbound transport layer and the storage backend are defined as ports with swappable adapter implementations, making the system straightforward to extend:

```
adapters/incoming   → HTTP (Axum)
applications        → Uploader (orchestration)
domain/ports        → UploadService (service), ObjectStorageSpi (SPI)
domain/value_objects→ Bucket, StoragePath, Object, Error, etc.
adapters/outgoing   → MinIO adapter (S3-compatible)
infrastructure      → HTTP error mapping
testing             → In-memory fake client
```

Additional adapters — such as gRPC or SFTP implementing the incoming port (UploadService), or alternative object stores and local filesystem implementing the outgoing port (ObjectStorageSpi) — can be introduced without touching domain logic.

## Local MinIO setup

```bash
docker run -d --name minio \
  -p 9100:9000 -p 9101:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Console: http://localhost:9101 (login: `minioadmin` / `minioadmin`)

## Running

```bash
cp .env.template .env   # configure env vars
cargo run --bin upload_server
```

See [`.env.template`](.env.template) for required environment variables and defaults.

## Testing

```bash
cargo test --features test-support
```

## Upload endpoint

```
POST /upload  (multipart/form-data)
→ 200 OK with JSON temporary S3 URL
```

### Example

```bash
curl --request POST \
  --url http://localhost:3030/upload \
  --header 'Content-Type: multipart/form-data' \
  --form =@./model.safetensors.index.json \
  --form =@./config.json \
  --form =@./generation_config.json \
  --form =@./model-00001-of-00004.safetensors \
  --form =@./model-00002-of-00004.safetensors \
  --form =@./model-00003-of-00004.safetensors \
  --form =@./model-00004-of-00004.safetensors
```

Response:

```
"s3://tmp/cd871a79-5f3b-4240-87ce-ebca565e1efc"
```

Each upload session receives a unique UUID path. Original filenames are
preserved from the multipart `Content-Disposition` header; fields without a
filename fall back to `file-0`, `file-1`, etc.
