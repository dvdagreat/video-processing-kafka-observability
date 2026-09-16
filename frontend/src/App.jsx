import { NavLink, Route, Routes } from 'react-router-dom'
import UploadPage from './pages/UploadPage'
import LibraryPage from './pages/LibraryPage'

function App() {
  return (
    <div className="app">
      <nav className="nav">
        <NavLink to="/" end>
          Upload
        </NavLink>
        <NavLink to="/library">Library</NavLink>
      </nav>
      <main>
        <Routes>
          <Route path="/" element={<UploadPage />} />
          <Route path="/library" element={<LibraryPage />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
