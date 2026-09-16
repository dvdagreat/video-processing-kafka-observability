use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    UploadStarted,
    SegmentationCompleted,
    ChunkProcessed,
    ResolutionCompleted,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingEvent {
    pub video_id: Uuid,
    pub resolution: Option<String>,
    pub event: EventKind,
    pub chunk_index: Option<u32>,
    pub total_chunks: Option<u32>,
    pub error: Option<String>,
}
