# Video Processing Pipeline with Kafka and Observability

A video upload and transcoding pipeline. A React frontend uploads a video to
a Rust backend, which segments it and streams the segments through Kafka to
four parallel consumers, each transcoding to a fixed resolution (240p,
360p, 480p, 720p). Processed segments are reassembled to disk. The pipeline
is instrumented end to end with Prometheus, Grafana, Loki, and Zipkin.

## Prerequisites

- Rust
- Node.js and npm
- Docker and Docker Compose
- ffmpeg

## Getting started

Start Kafka, the observability stack, and the backend services:

```
cd infra
docker compose up -d
```

Start the frontend:

```
cd frontend
npm install
npm run dev
```

Open the frontend at `http://localhost:5173`.

Grafana is at `http://localhost:3000` (admin/admin), Zipkin at
`http://localhost:9411`, Prometheus at `http://localhost:9090`.
