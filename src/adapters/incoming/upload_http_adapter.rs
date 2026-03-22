use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Multipart, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use tokio::net::TcpListener;
use tracing::{info, instrument};
use url::Url;

use crate::domain::ports::services::UploadService;
use crate::domain::value_objects::{Error, ObjectSegment};
use crate::infrastructure::http::{into_rest_error, rest_error};

struct AppState {
    upload_service: Arc<dyn UploadService>,
}

/// HTTP adapter exposing the upload service via REST endpoints.
pub struct UploadHttpAdapter {
    upload_service: Arc<dyn UploadService>,
}

impl UploadHttpAdapter {
    pub fn new(upload_service: Arc<dyn UploadService>) -> Self {
        Self { upload_service }
    }

    #[instrument(skip_all)]
    async fn upload_objects(
        State(state): State<Arc<AppState>>,
        mut multipart: Multipart,
    ) -> Result<(StatusCode, Json<Url>), (StatusCode, String)> {
        let (url, sender, handle) = state
            .upload_service
            .start_stream()
            .await
            .map_err(into_rest_error)?;

        let mut file_count: usize = 0;
        let mut total_bytes: u64 = 0;
        while let Some(field) = multipart
            .next_field()
            .await
            .map_err(|e| rest_error(StatusCode::BAD_REQUEST, e))?
        {
            let file_name = field
                .file_name()
                .map(|n| n.to_string())
                .unwrap_or_else(|| format!("file-{file_count}"));
            file_count += 1;
            let mut stream = field;
            let mut part_num = 0;
            while let Some(chunk) = stream
                .chunk()
                .await
                .map_err(|e| rest_error(StatusCode::BAD_REQUEST, e))?
            {
                total_bytes += chunk.len() as u64;
                sender
                    .send(ObjectSegment {
                        name: file_name.clone(),
                        part_num,
                        data: chunk,
                    })
                    .await
                    .map_err(|e| rest_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
                part_num += 1;
            }
        }

        drop(sender);
        handle
            .await
            .map_err(|e| rest_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;

        info!(
            %url,
            file_count,
            total_bytes,
            "upload session completed"
        );
        Ok((StatusCode::OK, Json(url)))
    }

    pub fn router(&self) -> Router {
        let state = Arc::new(AppState {
            upload_service: Arc::clone(&self.upload_service),
        });

        Router::new()
            .route("/upload", post(Self::upload_objects))
            .layer(DefaultBodyLimit::max(500 * 1024 * 1024 * 1024))
            .with_state(state)
    }

    pub async fn serve(&self, addr: &str) -> Result<(), Error> {
        let app = self.router();
        let listener = TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}
