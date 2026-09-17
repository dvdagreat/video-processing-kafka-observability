use anyhow::{anyhow, Result};

pub fn target_height(resolution: &str) -> Result<u32> {
    match resolution {
        "240p" => Ok(240),
        "360p" => Ok(360),
        "480p" => Ok(480),
        "720p" => Ok(720),
        other => Err(anyhow!("unsupported resolution {other}")),
    }
}

pub async fn transcode_chunk(input: Vec<u8>, height: u32) -> Result<Vec<u8>> {
    let scale = format!("scale=-2:{height}");
    common::ffmpeg::transcode_chunk(
        input,
        &[
            "-vf", &scale, "-c:v", "libx264", "-preset", "veryfast", "-c:a", "aac",
        ],
    )
    .await
}
