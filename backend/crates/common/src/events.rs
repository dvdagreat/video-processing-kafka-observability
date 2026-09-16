use anyhow::{Context, Result};
use chrono::Utc;
use rskafka::client::partition::{Compression, PartitionClient};
use rskafka::record::Record;
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

pub async fn publish(client: &PartitionClient, event: &ProcessingEvent) -> Result<()> {
    let value = serde_json::to_vec(event).context("failed to serialize processing event")?;
    let record = Record {
        key: Some(event.video_id.as_bytes().to_vec()),
        value: Some(value),
        headers: Default::default(),
        timestamp: Utc::now(),
    };
    client
        .produce(vec![record], Compression::NoCompression)
        .await
        .context("failed to publish processing event")?;
    Ok(())
}
