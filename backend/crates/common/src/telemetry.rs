use std::time::Duration;

use anyhow::Result;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace as sdktrace, Resource};
use prometheus::{Encoder, TextEncoder};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init_tracing(service_name: &str) -> Result<sdktrace::TracerProvider> {
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4318".to_string());

    let exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(3));

    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(sdktrace::Config::default().with_resource(Resource::new(vec![
            KeyValue::new("service.name", service_name.to_string()),
        ])))
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let tracer = tracer_provider.tracer(service_name.to_string());
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer().json())
        .with(otel_layer)
        .init();

    Ok(tracer_provider)
}

pub fn shutdown_tracing() {
    opentelemetry::global::shutdown_tracer_provider();
}

pub fn spawn_metrics_server(port: u16) {
    let app = axum::Router::new().route("/metrics", axum::routing::get(metrics_handler));
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{port}");
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .expect("failed to bind metrics listener");
        tracing::info!(%addr, "metrics server listening");
        axum::serve(listener, app)
            .await
            .expect("metrics server failed");
    });
}

async fn metrics_handler() -> axum::response::Response {
    use axum::response::IntoResponse;

    let metric_families = prometheus::gather();
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .expect("failed to encode metrics");
    (
        axum::http::StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            encoder.format_type().to_string(),
        )],
        buffer,
    )
        .into_response()
}
