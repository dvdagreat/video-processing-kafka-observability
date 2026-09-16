use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use axum::extract::{Multipart, State};
use axum::Json;
use chrono::Utc;
use common::ChunkMeta;
use rskafka::client::partition::{Compression, PartitionClient};
use rskafka::record::Record;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use uuid::Uuid;

use crate::error::AppError;
use crate::state::{AppState, VideoState};

const SEGMENT_SECONDS: &str = "5";

#[derive(Serialize)]
pub struct UploadResponse {
    video_id: Uuid,
}

pub async fn upload_video(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, AppError> {
    let mut field = loop {
        match multipart.next_field().await? {
            Some(field) if field.name() == Some("file") => break field,
            Some(_) => continue,
            None => return Err(AppError::bad_request("missing file field")),
        }
    };

    let original_filename = field
        .file_name()
        .unwrap_or("upload")
        .to_string();
    let video_id = Uuid::new_v4();

    let video_dir = state.storage_root.join("uploads").join(video_id.to_string());
    tokio::fs::create_dir_all(&video_dir).await?;
    let input_path = video_dir.join("original");

    let mut file = tokio::fs::File::create(&input_path).await?;
    let mut upload_bytes = 0u64;
    while let Some(chunk) = field.chunk().await? {
        upload_bytes += chunk.len() as u64;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;

    state
        .insert_video(VideoState::new(video_id, original_filename, upload_bytes))
        .await;

    let state = state.clone();
    tokio::spawn(async move {
        if let Err(err) = run_segmentation(&state, video_id, &input_path).await {
            tracing::error!(%video_id, ?err, "segmentation failed");
            state.mark_failed(video_id, err.to_string()).await;
        }
    });

    Ok(Json(UploadResponse { video_id }))
}

#[tracing::instrument(skip(state, input_path), fields(video_id = %video_id))]
async fn run_segmentation(state: &AppState, video_id: Uuid, input_path: &Path) -> Result<()> {
    let segments_dir = input_path
        .parent()
        .ok_or_else(|| anyhow!("input path has no parent"))?
        .join("segments");
    tokio::fs::create_dir_all(&segments_dir).await?;

    let mut child = Command::new("ffmpeg")
        .arg("-y")
        .arg("-i")
        .arg(input_path)
        .args(["-c", "copy", "-map", "0"])
        .args(["-f", "segment", "-segment_time", SEGMENT_SECONDS])
        .arg(segments_dir.join("chunk_%05d.ts"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn ffmpeg")?;

    let stderr = child.stderr.take().expect("stderr was piped");
    let stderr_task = tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut lines = BufReader::new(stderr).lines();
        let mut collected = String::new();
        while let Ok(Some(line)) = lines.next_line().await {
            collected.push_str(&line);
            collected.push('\n');
        }
        collected
    });

    let raw_topic_client = common::kafka::partition_client(
        &state.kafka_client,
        common::RAW_VIDEO_CHUNKS_TOPIC,
        0,
    )
    .await?;

    let mut next_index = 0u32;
    let total_chunks = loop {
        let child_running = child.try_wait()?.is_none();

        loop {
            let candidate = segment_path(&segments_dir, next_index);
            let next_candidate = segment_path(&segments_dir, next_index + 1);
            let ready = next_candidate.exists() || (!child_running && candidate.exists());
            if !ready {
                break;
            }
            let is_last = !child_running && !next_candidate.exists();
            publish_chunk(&raw_topic_client, video_id, next_index, is_last, &candidate).await?;
            tokio::fs::remove_file(&candidate).await.ok();
            next_index += 1;
            if is_last {
                break;
            }
        }

        if !child_running {
            break next_index;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let status = child.wait().await.context("failed to wait for ffmpeg")?;
    if !status.success() {
        let stderr_output = stderr_task.await.unwrap_or_default();
        return Err(anyhow!("ffmpeg exited with {status}: {stderr_output}"));
    }
    stderr_task.abort();

    if total_chunks == 0 {
        return Err(anyhow!("ffmpeg produced no segments"));
    }

    state
        .mark_segmentation_completed(video_id, total_chunks)
        .await;
    tokio::fs::remove_file(input_path).await.ok();
    tokio::fs::remove_dir_all(&segments_dir).await.ok();

    Ok(())
}

fn segment_path(dir: &Path, index: u32) -> PathBuf {
    dir.join(format!("chunk_{index:05}.ts"))
}

#[tracing::instrument(skip(client, path), fields(video_id = %video_id, chunk_index, is_last))]
async fn publish_chunk(
    client: &PartitionClient,
    video_id: Uuid,
    chunk_index: u32,
    is_last: bool,
    path: &Path,
) -> Result<()> {
    let bytes = tokio::fs::read(path).await?;
    let meta = ChunkMeta {
        video_id,
        chunk_index,
        is_last,
        resolution: None,
    };
    let record = Record {
        key: Some(video_id.as_bytes().to_vec()),
        value: Some(bytes),
        headers: meta.to_headers(),
        timestamp: Utc::now(),
    };
    client
        .produce(vec![record], Compression::NoCompression)
        .await
        .context("failed to produce raw video chunk")?;
    Ok(())
}
