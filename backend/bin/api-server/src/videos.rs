use axum::extract::{Path as AxumPath, State};
use axum::response::IntoResponse;
use axum::Json;
use uuid::Uuid;

use crate::error::AppError;
use crate::state::{AppState, VideoState};

pub async fn list_videos(State(state): State<AppState>) -> Json<Vec<VideoState>> {
    let videos = state.videos.read().await;
    let mut list: Vec<VideoState> = videos.values().cloned().collect();
    list.sort_by(|a, b| b.uploaded_at.cmp(&a.uploaded_at));
    Json(list)
}

pub async fn video_status(
    State(state): State<AppState>,
    AxumPath(video_id): AxumPath<Uuid>,
) -> Result<Json<VideoState>, AppError> {
    let videos = state.videos.read().await;
    videos
        .get(&video_id)
        .cloned()
        .map(Json)
        .ok_or_else(|| AppError::not_found("video not found"))
}

pub async fn download_video(
    State(state): State<AppState>,
    AxumPath((video_id, resolution)): AxumPath<(Uuid, String)>,
) -> Result<impl IntoResponse, AppError> {
    {
        let videos = state.videos.read().await;
        let video = videos
            .get(&video_id)
            .ok_or_else(|| AppError::not_found("video not found"))?;
        let resolution_state = video
            .resolutions
            .get(&resolution)
            .ok_or_else(|| AppError::bad_request("unknown resolution"))?;
        if !resolution_state.completed {
            return Err(AppError::bad_request("resolution not ready yet"));
        }
    }

    let path = state
        .storage_root
        .join("output")
        .join(video_id.to_string())
        .join(format!("{resolution}.ts"));

    let file = tokio::fs::File::open(&path)
        .await
        .map_err(|_| AppError::not_found("output file not found"))?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = axum::body::Body::from_stream(stream);

    let filename = format!("{video_id}-{resolution}.ts");
    Ok((
        [
            (axum::http::header::CONTENT_TYPE, "video/mp2t".to_string()),
            (
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        body,
    ))
}
