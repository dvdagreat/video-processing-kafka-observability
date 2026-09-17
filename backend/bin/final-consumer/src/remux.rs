use std::path::Path;
use std::process::Stdio;

use anyhow::{anyhow, Context, Result};
use tokio::process::Command;

/// Remuxes a complete MPEG-TS file into an MP4 container without
/// re-encoding, then removes the source file.
pub async fn remux_to_mp4(ts_path: &Path, mp4_path: &Path) -> Result<()> {
    let output = Command::new("ffmpeg")
        .arg("-y")
        .arg("-i")
        .arg(ts_path)
        .args(["-c", "copy"])
        .arg(mp4_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("failed to spawn ffmpeg")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("ffmpeg exited with {}: {stderr}", output.status));
    }

    tokio::fs::remove_file(ts_path).await.ok();
    Ok(())
}
