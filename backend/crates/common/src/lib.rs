pub mod chunk;
pub mod events;
pub mod ffmpeg;
pub mod kafka;
pub mod propagation;
pub mod telemetry;

pub use chunk::ChunkMeta;
pub use events::{EventKind, ProcessingEvent};

pub const RAW_VIDEO_CHUNKS_TOPIC: &str = "raw-video-chunks";
pub const WATERMARKED_VIDEO_CHUNKS_TOPIC: &str = "watermarked-video-chunks";
pub const PROCESSED_VIDEO_CHUNKS_TOPIC: &str = "processed-video-chunks";
pub const PROCESSING_EVENTS_TOPIC: &str = "processing-events";

pub const RESOLUTIONS: [&str; 4] = ["240p", "360p", "480p", "720p"];
