use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use common::ProcessingEvent;
use rskafka::client::Client;
use serde::Serialize;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub videos: Arc<RwLock<HashMap<Uuid, VideoState>>>,
    pub storage_root: PathBuf,
    pub kafka_client: Arc<Client>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VideoState {
    pub video_id: Uuid,
    pub original_filename: String,
    pub uploaded_at: DateTime<Utc>,
    pub upload_bytes: u64,
    pub total_chunks: Option<u32>,
    pub resolutions: HashMap<String, ResolutionState>,
    pub failed: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ResolutionState {
    pub chunks_done: u32,
    pub completed: bool,
    pub error: Option<String>,
}

impl VideoState {
    pub fn new(video_id: Uuid, original_filename: String, upload_bytes: u64) -> Self {
        let resolutions = common::RESOLUTIONS
            .iter()
            .map(|r| (r.to_string(), ResolutionState::default()))
            .collect();
        Self {
            video_id,
            original_filename,
            uploaded_at: Utc::now(),
            upload_bytes,
            total_chunks: None,
            resolutions,
            failed: None,
        }
    }
}

impl AppState {
    pub fn new(storage_root: PathBuf, kafka_client: Client) -> Self {
        Self {
            videos: Arc::new(RwLock::new(HashMap::new())),
            storage_root,
            kafka_client: Arc::new(kafka_client),
        }
    }

    pub async fn insert_video(&self, video: VideoState) {
        self.videos.write().await.insert(video.video_id, video);
    }

    pub async fn mark_segmentation_completed(&self, video_id: Uuid, total_chunks: u32) {
        if let Some(video) = self.videos.write().await.get_mut(&video_id) {
            video.total_chunks = Some(total_chunks);
        }
    }

    pub async fn mark_failed(&self, video_id: Uuid, error: String) {
        if let Some(video) = self.videos.write().await.get_mut(&video_id) {
            video.failed = Some(error);
        }
    }

    pub async fn apply_processing_event(&self, event: ProcessingEvent) {
        let mut videos = self.videos.write().await;
        let Some(video) = videos.get_mut(&event.video_id) else {
            return;
        };
        match event.event {
            common::EventKind::UploadStarted | common::EventKind::SegmentationCompleted => {}
            common::EventKind::ChunkProcessed => {
                if let Some(resolution) = &event.resolution {
                    let state = video.resolutions.entry(resolution.clone()).or_default();
                    state.chunks_done += 1;
                }
            }
            common::EventKind::ResolutionCompleted => {
                if let Some(resolution) = &event.resolution {
                    let state = video.resolutions.entry(resolution.clone()).or_default();
                    state.completed = true;
                }
            }
            common::EventKind::Failed => {
                if let Some(resolution) = &event.resolution {
                    let state = video.resolutions.entry(resolution.clone()).or_default();
                    state.error = event.error.clone();
                } else {
                    video.failed = event.error.clone();
                }
            }
        }
    }
}
