import { useEffect, useState } from 'react'
import { downloadUrl, listVideos } from '../api'

const RESOLUTIONS = ['240p', '360p', '480p', '720p']

function resolutionPercent(video, resolution) {
  const state = video.resolutions[resolution]
  if (!state) return 0
  if (state.completed) return 100
  if (!video.total_chunks) return 0
  return Math.min(100, Math.round((state.chunks_done / video.total_chunks) * 100))
}

function VideoRow({ video }) {
  return (
    <div className="video-row">
      <div className="video-header">
        <span className="video-name">{video.original_filename}</span>
        <span className="video-date">{new Date(video.uploaded_at).toLocaleString()}</span>
      </div>

      {video.failed && <p className="error">Upload failed: {video.failed}</p>}

      {!video.failed && !video.total_chunks && <p className="status-note">Segmenting video...</p>}

      <div className="resolution-grid">
        {RESOLUTIONS.map((resolution) => {
          const state = video.resolutions[resolution]
          const percent = resolutionPercent(video, resolution)
          return (
            <div className="resolution-cell" key={resolution}>
              <div className="resolution-label">{resolution}</div>
              <div className="progress-bar small">
                <div className="progress-bar-fill" style={{ width: `${percent}%` }} />
              </div>
              {state?.error && <p className="error small">{state.error}</p>}
              {state?.completed ? (
                <a href={downloadUrl(video.video_id, resolution)}>Download</a>
              ) : (
                <span className="percent">{percent}%</span>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

function LibraryPage() {
  const [videos, setVideos] = useState([])
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false

    async function poll() {
      try {
        const data = await listVideos()
        if (!cancelled) {
          setVideos(data)
          setError(null)
        }
      } catch (err) {
        if (!cancelled) setError(err.message)
      }
    }

    poll()
    const interval = setInterval(poll, 3000)
    return () => {
      cancelled = true
      clearInterval(interval)
    }
  }, [])

  return (
    <div className="page">
      <h1>Videos</h1>
      {error && <p className="error">{error}</p>}
      {videos.length === 0 && !error && <p className="status-note">No videos uploaded yet.</p>}
      <div className="video-list">
        {videos.map((video) => (
          <VideoRow key={video.video_id} video={video} />
        ))}
      </div>
    </div>
  )
}

export default LibraryPage
