use std::process::Stdio;

use anyhow::{anyhow, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

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
    let mut child = Command::new("ffmpeg")
        .args(["-copyts", "-i", "-"])
        .args(["-vf", &format!("scale=-2:{height}")])
        .args(["-c:v", "libx264", "-preset", "veryfast"])
        .args(["-c:a", "aac"])
        .args(["-muxdelay", "0", "-muxpreload", "0"])
        .args(["-f", "mpegts", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn ffmpeg")?;

    let mut stdin = child.stdin.take().expect("stdin was piped");
    let mut stdout = child.stdout.take().expect("stdout was piped");
    let mut stderr = child.stderr.take().expect("stderr was piped");

    let write_task = tokio::spawn(async move {
        let result = stdin.write_all(&input).await;
        drop(stdin);
        result
    });

    let mut output = Vec::new();
    let read_task = tokio::spawn(async move {
        stdout.read_to_end(&mut output).await?;
        Ok::<_, std::io::Error>(output)
    });

    let mut stderr_output = String::new();
    let stderr_task = tokio::spawn(async move {
        stderr.read_to_string(&mut stderr_output).await.ok();
        stderr_output
    });

    write_task.await.context("stdin writer task panicked")??;
    let output = read_task.await.context("stdout reader task panicked")??;
    let status = child.wait().await.context("failed to wait for ffmpeg")?;
    let stderr_text = stderr_task.await.unwrap_or_default();

    if !status.success() {
        return Err(anyhow!("ffmpeg exited with {status}: {stderr_text}"));
    }
    if output.is_empty() {
        return Err(anyhow!("ffmpeg produced no output: {stderr_text}"));
    }

    Ok(output)
}
