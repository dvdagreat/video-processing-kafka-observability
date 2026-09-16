import { useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { uploadVideo } from '../api'

function UploadPage() {
  const [file, setFile] = useState(null)
  const [progress, setProgress] = useState(0)
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState(null)
  const fileInputRef = useRef(null)
  const navigate = useNavigate()

  async function handleUpload() {
    if (!file) return
    setUploading(true)
    setError(null)
    setProgress(0)
    try {
      await uploadVideo(file, setProgress)
      navigate('/library')
    } catch (err) {
      setError(err.message)
      setUploading(false)
    }
  }

  return (
    <div className="page">
      <h1>Upload a video</h1>
      <div className="upload-box">
        <input
          ref={fileInputRef}
          type="file"
          accept="video/*"
          disabled={uploading}
          onChange={(event) => setFile(event.target.files?.[0] ?? null)}
        />
        <button type="button" disabled={!file || uploading} onClick={handleUpload}>
          {uploading ? 'Uploading...' : 'Upload'}
        </button>
      </div>

      {uploading && (
        <div className="progress-bar">
          <div className="progress-bar-fill" style={{ width: `${progress}%` }} />
          <span>{progress}%</span>
        </div>
      )}

      {error && <p className="error">{error}</p>}
    </div>
  )
}

export default UploadPage
