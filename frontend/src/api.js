const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8080'

export function uploadVideo(file, onProgress) {
  return new Promise((resolve, reject) => {
    const formData = new FormData()
    formData.append('file', file)

    const request = new XMLHttpRequest()
    request.open('POST', `${API_BASE_URL}/api/videos`)

    request.upload.onprogress = (event) => {
      if (event.lengthComputable && onProgress) {
        onProgress(Math.round((event.loaded / event.total) * 100))
      }
    }

    request.onload = () => {
      if (request.status >= 200 && request.status < 300) {
        resolve(JSON.parse(request.responseText))
      } else {
        reject(new Error(request.responseText || 'upload failed'))
      }
    }
    request.onerror = () => reject(new Error('upload failed'))
    request.send(formData)
  })
}

export async function listVideos() {
  const response = await fetch(`${API_BASE_URL}/api/videos`)
  if (!response.ok) {
    throw new Error('failed to list videos')
  }
  return response.json()
}

export async function getVideoStatus(videoId) {
  const response = await fetch(`${API_BASE_URL}/api/videos/${videoId}`)
  if (!response.ok) {
    throw new Error('failed to fetch video status')
  }
  return response.json()
}

export function downloadUrl(videoId, resolution) {
  return `${API_BASE_URL}/api/videos/${videoId}/download/${resolution}`
}
