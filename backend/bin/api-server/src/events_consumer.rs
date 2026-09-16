use std::sync::Arc;
use std::time::Duration;

use common::ProcessingEvent;
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};

use crate::state::AppState;

pub async fn run(state: AppState) {
    loop {
        if let Err(err) = consume_once(&state).await {
            tracing::error!(?err, "processing-events consumer failed, retrying");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn consume_once(state: &AppState) -> anyhow::Result<()> {
    let partition_client = Arc::new(
        common::kafka::partition_client(&state.kafka_client, common::PROCESSING_EVENTS_TOPIC, 0)
            .await?,
    );
    let mut stream = StreamConsumerBuilder::new(partition_client, StartOffset::Earliest)
        .with_max_wait_ms(500)
        .build();

    while let Some(result) = stream.next().await {
        let (record_and_offset, _high_watermark) = result?;
        let Some(value) = record_and_offset.record.value else {
            continue;
        };
        match serde_json::from_slice::<ProcessingEvent>(&value) {
            Ok(event) => state.apply_processing_event(event).await,
            Err(err) => tracing::warn!(?err, "failed to decode processing event"),
        }
    }
    Ok(())
}
