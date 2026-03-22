# upload-server-rs

A high-performance, streaming file upload server built in Rust, purpose-designed for transferring large binary assets such as AI model artifacts, datasets, and checkpoints. It handles uploads with minimal memory overhead by streaming data directly to the backend without buffering entire files in memory.

The architecture follows Domain-Driven Design (DDD) with a Ports and Adapters (Hexagonal Architecture) pattern, cleanly separating core domain logic from infrastructure concerns. This makes the system highly extensible: both the inbound transport layer and the storage backend are defined as ports with swappable adapter implementations.

Currently supported adapters:

HTTP — inbound transport for streaming multipart and chunked uploads
MinIO / S3 — outbound storage backend via the S3-compatible API

Additional adapters — such as gRPC or SFTP on the ingress side, or alternative object stores and local filesystem targets on the egress side — can be introduced without touching domain logic.

## Architecture

Hexagonal (ports & adapters):

```
adapters/incoming   → HTTP (Axum)
applications        → Uploader (orchestration)
domain/ports        → UploadService (service), ObjectStorageSpi (SPI)
domain/value_objects→ Bucket, StoragePath, Object, Error, etc.
adapters/outgoing   → MinIO adapter
infrastructure      → HTTP error mapping
testing             → In-memory fake client
```

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
