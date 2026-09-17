mod metrics;
mod watermark;

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use common::{ChunkMeta, EventKind, ProcessingEvent};
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::partition::{Compression, PartitionClient};
use rskafka::record::Record;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use metrics::Metrics;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _tracer_provider = common::telemetry::init_tracing("watermark-consumer")?;
    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9100);
    common::telemetry::spawn_metrics_server(metrics_port);

    let metrics = Metrics::new()?;

    let kafka_client = common::kafka::client().await?;
    common::kafka::ensure_topic(&kafka_client, common::RAW_VIDEO_CHUNKS_TOPIC, 1).await?;
    common::kafka::ensure_topic(&kafka_client, common::WATERMARKED_VIDEO_CHUNKS_TOPIC, 1).await?;
    common::kafka::ensure_topic(&kafka_client, common::PROCESSING_EVENTS_TOPIC, 1).await?;

    let watermarked_client = common::kafka::partition_client(
        &kafka_client,
        common::WATERMARKED_VIDEO_CHUNKS_TOPIC,
        0,
    )
    .await?;
    let events_client =
        common::kafka::partition_client(&kafka_client, common::PROCESSING_EVENTS_TOPIC, 0).await?;

    loop {
        if let Err(err) = run(
            &kafka_client,
            &watermarked_client,
            &events_client,
            &metrics,
        )
        .await
        {
            tracing::error!(?err, "watermark consumer loop failed, retrying");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run(
    kafka_client: &rskafka::client::Client,
    watermarked_client: &PartitionClient,
    events_client: &PartitionClient,
    metrics: &Metrics,
) -> anyhow::Result<()> {
    let raw_client = Arc::new(
        common::kafka::partition_client(kafka_client, common::RAW_VIDEO_CHUNKS_TOPIC, 0).await?,
    );
    let mut stream = StreamConsumerBuilder::new(raw_client, StartOffset::Earliest)
        .with_max_wait_ms(500)
        .with_max_batch_size(20_000_000)
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

        let parent_cx = common::propagation::extract(&record.headers);
        let span = tracing::info_span!(
            "watermark_chunk",
            video_id = %meta.video_id,
            chunk_index = meta.chunk_index,
        );
        span.set_parent(parent_cx);
        let _entered = span.enter();

        let started = Instant::now();
        match watermark::apply_watermark(value).await {
            Ok(watermarked) => {
                metrics
                    .watermark_duration_seconds
                    .observe(started.elapsed().as_secs_f64());
                metrics.chunks_processed_total.inc();

                let out_meta = ChunkMeta {
                    video_id: meta.video_id,
                    chunk_index: meta.chunk_index,
                    is_last: meta.is_last,
                    resolution: None,
                };
                let mut headers = out_meta.to_headers();
                common::propagation::inject(&mut headers);
                let out_record = Record {
                    key: Some(meta.video_id.as_bytes().to_vec()),
                    value: Some(watermarked),
                    headers,
                    timestamp: Utc::now(),
                };
                if let Err(err) = watermarked_client
                    .produce(vec![out_record], Compression::NoCompression)
                    .await
                {
                    metrics.chunk_errors_total.inc();
                    tracing::error!(?err, "failed to publish watermarked chunk");
                    common::events::publish(
                        events_client,
                        &ProcessingEvent {
                            video_id: meta.video_id,
                            resolution: None,
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
            Err(err) => {
                metrics.chunk_errors_total.inc();
                tracing::error!(?err, "failed to watermark chunk");
                common::events::publish(
                    events_client,
                    &ProcessingEvent {
                        video_id: meta.video_id,
                        resolution: None,
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
