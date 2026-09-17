use anyhow::Result;
use prometheus::{register_counter, register_histogram, Counter, Histogram};

pub struct Metrics {
    pub watermark_duration_seconds: Histogram,
    pub chunks_processed_total: Counter,
    pub chunk_errors_total: Counter,
}

impl Metrics {
    pub fn new() -> Result<Self> {
        Ok(Self {
            watermark_duration_seconds: register_histogram!(
                "watermark_chunk_duration_seconds",
                "Time to apply a watermark to a single chunk"
            )?,
            chunks_processed_total: register_counter!(
                "watermark_chunks_processed_total",
                "Number of chunks watermarked successfully"
            )?,
            chunk_errors_total: register_counter!(
                "watermark_chunk_errors_total",
                "Number of chunks that failed to watermark"
            )?,
        })
    }
}
