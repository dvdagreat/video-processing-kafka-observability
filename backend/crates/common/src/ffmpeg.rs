use std::process::Stdio;

use anyhow::{anyhow, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

/// Pipes `input` through ffmpeg with `-copyts -i -` on stdin and
/// `-muxdelay 0 -muxpreload 0 -f mpegts -` on stdout, inserting `codec_args`
/// in between. `-copyts` preserves the source timestamps carried by the
/// chunk so that chunks processed independently and later concatenated
/// still play back as one continuous stream.
pub async fn transcode_chunk(input: Vec<u8>, codec_args: &[&str]) -> Result<Vec<u8>> {
    let mut command = Command::new("ffmpeg");
    command.args(["-copyts", "-i", "-"]).args(codec_args).args([
        "-muxdelay",
        "0",
        "-muxpreload",
        "0",
        "-f",
        "mpegts",
        "-",
    ]);

    let mut child = command
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
