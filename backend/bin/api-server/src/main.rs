mod error;
mod events_consumer;
mod state;
mod upload;
mod videos;

use std::path::PathBuf;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;

use state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _tracer_provider = common::telemetry::init_tracing("api-server")?;
    common::telemetry::spawn_metrics_server(9100);

    let storage_root = std::env::var("STORAGE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("../storage"));
    tokio::fs::create_dir_all(&storage_root).await?;

    let kafka_client = common::kafka::client().await?;
    common::kafka::ensure_topic(&kafka_client, common::RAW_VIDEO_CHUNKS_TOPIC, 1).await?;
    common::kafka::ensure_topic(&kafka_client, common::PROCESSED_VIDEO_CHUNKS_TOPIC, 1).await?;
    common::kafka::ensure_topic(&kafka_client, common::PROCESSING_EVENTS_TOPIC, 1).await?;

    let state = AppState::new(storage_root, kafka_client);

    tokio::spawn(events_consumer::run(state.clone()));

    let app = Router::new()
        .route("/api/videos", post(upload::upload_video))
        .route("/api/videos", get(videos::list_videos))
        .route("/api/videos/:video_id", get(videos::video_status))
        .route(
            "/api/videos/:video_id/download/:resolution",
            get(videos::download_video),
        )
        .layer(DefaultBodyLimit::disable())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listen_addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    tracing::info!(%listen_addr, "api-server listening");
    axum::serve(listener, app).await?;

    common::telemetry::shutdown_tracing();
    Ok(())
}
