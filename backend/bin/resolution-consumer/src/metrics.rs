use anyhow::Result;
use prometheus::{register_counter, register_histogram, Counter, Histogram};

pub struct Metrics {
    pub transcode_duration_seconds: Histogram,
    pub chunks_processed_total: Counter,
    pub chunk_errors_total: Counter,
}

impl Metrics {
    pub fn new(resolution: &str) -> Result<Self> {
        Ok(Self {
            transcode_duration_seconds: register_histogram!(
                format!("chunk_transcode_duration_seconds_{resolution}"),
                "Time to transcode a single chunk"
            )?,
            chunks_processed_total: register_counter!(
                format!("chunks_processed_total_{resolution}"),
                "Number of chunks transcoded successfully"
            )?,
            chunk_errors_total: register_counter!(
                format!("chunk_errors_total_{resolution}"),
                "Number of chunks that failed to transcode"
            )?,
        })
    }
}
