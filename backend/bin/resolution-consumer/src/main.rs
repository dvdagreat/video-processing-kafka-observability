mod metrics;
mod transcode;

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use common::{ChunkMeta, EventKind, ProcessingEvent};
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::partition::{Compression, PartitionClient};
use rskafka::record::Record;

use metrics::Metrics;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let resolution = std::env::var("RESOLUTION").expect("RESOLUTION env var must be set");
    let height = transcode::target_height(&resolution)?;

    let service_name = format!("resolution-consumer-{resolution}");
    let _tracer_provider = common::telemetry::init_tracing(&service_name)?;
    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9100);
    common::telemetry::spawn_metrics_server(metrics_port);

    let metrics = Metrics::new(&resolution)?;

    let kafka_client = common::kafka::client().await?;
    common::kafka::ensure_topic(&kafka_client, common::RAW_VIDEO_CHUNKS_TOPIC, 1).await?;
    common::kafka::ensure_topic(&kafka_client, common::PROCESSED_VIDEO_CHUNKS_TOPIC, 1).await?;
    common::kafka::ensure_topic(&kafka_client, common::PROCESSING_EVENTS_TOPIC, 1).await?;

    let processed_client =
        common::kafka::partition_client(&kafka_client, common::PROCESSED_VIDEO_CHUNKS_TOPIC, 0)
            .await?;
    let events_client =
        common::kafka::partition_client(&kafka_client, common::PROCESSING_EVENTS_TOPIC, 0).await?;

    loop {
        if let Err(err) = run(
            &kafka_client,
            &processed_client,
            &events_client,
            &resolution,
            height,
            &metrics,
        )
        .await
        {
            tracing::error!(?err, "resolution consumer loop failed, retrying");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run(
    kafka_client: &rskafka::client::Client,
    processed_client: &PartitionClient,
    events_client: &PartitionClient,
    resolution: &str,
    height: u32,
    metrics: &Metrics,
) -> anyhow::Result<()> {
    let raw_client = Arc::new(
        common::kafka::partition_client(kafka_client, common::RAW_VIDEO_CHUNKS_TOPIC, 0).await?,
    );
    let mut stream = StreamConsumerBuilder::new(raw_client, StartOffset::Earliest)
        .with_max_wait_ms(500)
        .build();

    while let Some(result) = stream.next().await {
        let (record_and_offset, _high_watermark) = result?;
        let record = record_and_offset.record;
        let Some(value) = record.value else {
            continue;
        };
        let Ok(meta) = ChunkMeta::from_headers(&record.headers) else {
            tracing::warn!("skipping raw chunk with invalid headers");
            continue;
        };

        let span = tracing::info_span!(
            "transcode_chunk",
            video_id = %meta.video_id,
            chunk_index = meta.chunk_index,
            resolution = resolution,
        );
        let _entered = span.enter();

        let started = Instant::now();
        match transcode::transcode_chunk(value, height).await {
            Ok(transcoded) => {
                metrics
                    .transcode_duration_seconds
                    .observe(started.elapsed().as_secs_f64());
                metrics.chunks_processed_total.inc();

                let out_meta = ChunkMeta {
                    video_id: meta.video_id,
                    chunk_index: meta.chunk_index,
                    is_last: meta.is_last,
                    resolution: Some(resolution.to_string()),
                };
                let out_record = Record {
                    key: Some(meta.video_id.as_bytes().to_vec()),
                    value: Some(transcoded),
                    headers: out_meta.to_headers(),
                    timestamp: Utc::now(),
                };
                if let Err(err) = processed_client
                    .produce(vec![out_record], Compression::NoCompression)
                    .await
                {
                    tracing::error!(?err, "failed to publish processed chunk");
                    continue;
                }

                common::events::publish(
                    events_client,
                    &ProcessingEvent {
                        video_id: meta.video_id,
                        resolution: Some(resolution.to_string()),
                        event: EventKind::ChunkProcessed,
                        chunk_index: Some(meta.chunk_index),
                        total_chunks: None,
                        error: None,
                    },
                )
                .await
                .ok();
            }
            Err(err) => {
                metrics.chunk_errors_total.inc();
                tracing::error!(?err, "failed to transcode chunk");
                common::events::publish(
                    events_client,
                    &ProcessingEvent {
                        video_id: meta.video_id,
                        resolution: Some(resolution.to_string()),
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
