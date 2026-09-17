mod remux;
mod writer;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::{ChunkMeta, EventKind, ProcessingEvent};
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::partition::PartitionClient;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;
use writer::WriterState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _tracer_provider = common::telemetry::init_tracing("final-consumer")?;
    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9100);
    common::telemetry::spawn_metrics_server(metrics_port);

    let storage_root = std::env::var("STORAGE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("../storage"));

    let kafka_client = common::kafka::client().await?;
    common::kafka::ensure_topic(&kafka_client, common::PROCESSED_VIDEO_CHUNKS_TOPIC, 1).await?;
    common::kafka::ensure_topic(&kafka_client, common::PROCESSING_EVENTS_TOPIC, 1).await?;

    let events_client =
        common::kafka::partition_client(&kafka_client, common::PROCESSING_EVENTS_TOPIC, 0).await?;

    loop {
        if let Err(err) = run(&kafka_client, &events_client, &storage_root).await {
            tracing::error!(?err, "final consumer loop failed, retrying");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run(
    kafka_client: &rskafka::client::Client,
    events_client: &PartitionClient,
    storage_root: &PathBuf,
) -> anyhow::Result<()> {
    let processed_client = Arc::new(
        common::kafka::partition_client(kafka_client, common::PROCESSED_VIDEO_CHUNKS_TOPIC, 0)
            .await?,
    );
    let mut stream = StreamConsumerBuilder::new(processed_client, StartOffset::Earliest)
        .with_max_wait_ms(500)
        .with_max_batch_size(20_000_000)
        .build();

    let mut writers: HashMap<(Uuid, String), WriterState> = HashMap::new();

    while let Some(result) = stream.next().await {
        let (record_and_offset, _high_watermark) = result?;
        let record = record_and_offset.record;
        let Some(value) = record.value else {
            continue;
        };
        let Ok(meta) = ChunkMeta::from_headers(&record.headers) else {
            tracing::warn!("skipping processed chunk with invalid headers");
            continue;
        };
        let Some(resolution) = meta.resolution.clone() else {
            tracing::warn!("skipping processed chunk without resolution header");
            continue;
        };

        let parent_cx = common::propagation::extract(&record.headers);
        let span = tracing::info_span!(
            "write_chunk",
            video_id = %meta.video_id,
            resolution = %resolution,
            chunk_index = meta.chunk_index,
        );
        span.set_parent(parent_cx);
        let _entered = span.enter();

        let output_dir = storage_root
            .join("output")
            .join(meta.video_id.to_string());
        if let Err(err) = tokio::fs::create_dir_all(&output_dir).await {
            tracing::error!(?err, "failed to create output directory");
            continue;
        }
        let output_path = output_dir.join(format!("{resolution}.ts"));

        let key = (meta.video_id, resolution.clone());
        let state = writers.entry(key.clone()).or_default();

        match state
            .accept(&output_path, meta.chunk_index, meta.is_last, value)
            .await
        {
            Ok(completed) => {
                if completed {
                    writers.remove(&key);
                    let mp4_path = output_dir.join(format!("{resolution}.mp4"));
                    match remux::remux_to_mp4(&output_path, &mp4_path).await {
                        Ok(()) => {
                            common::events::publish(
                                events_client,
                                &ProcessingEvent {
                                    video_id: meta.video_id,
                                    resolution: Some(resolution),
                                    event: EventKind::ResolutionCompleted,
                                    chunk_index: Some(meta.chunk_index),
                                    total_chunks: None,
                                    error: None,
                                },
                            )
                            .await
                            .ok();
                        }
                        Err(err) => {
                            tracing::error!(?err, "failed to remux to mp4");
                            common::events::publish(
                                events_client,
                                &ProcessingEvent {
                                    video_id: meta.video_id,
                                    resolution: Some(resolution),
                                    event: EventKind::Failed,
                                    chunk_index: Some(meta.chunk_index),
                                    total_chunks: None,
                                    error: Some(err.to_string()),
                                },
                            )
                            .await
                            .ok();
                        }
                    }
                }
            }
            Err(err) => {
                tracing::error!(?err, "failed to write chunk to disk");
                common::events::publish(
                    events_client,
                    &ProcessingEvent {
                        video_id: meta.video_id,
                        resolution: Some(resolution),
                        event: EventKind::Failed,
                        chunk_index: Some(meta.chunk_index),
                        total_chunks: None,
                        error: Some(err.to_string()),
                    },
                )
                .await
                .ok();
            }
        }
    }

    Ok(())
}
