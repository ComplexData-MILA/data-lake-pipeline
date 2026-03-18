import { Routes, Route, NavLink } from 'react-router-dom'
import { Dashboard } from './pages/Dashboard'
import { LandingZone } from './pages/LandingZone'
import { QueueMonitor } from './pages/QueueMonitor'
import { Manifests } from './pages/Manifests'
import { DataExplorer } from './pages/DataExplorer'

function App() {
  return (
    <div className="min-h-screen bg-background">
      <nav className="border-b">
        <div className="flex h-16 items-center px-4 gap-6">
          <span className="font-semibold text-lg">Data Lake Viewer</span>
          <NavLink to="/" className={({ isActive }) => isActive ? 'text-primary' : 'text-muted-foreground'}>Dashboard</NavLink>
          <NavLink to="/landing" className={({ isActive }) => isActive ? 'text-primary' : 'text-muted-foreground'}>Landing Zone</NavLink>
          <NavLink to="/queue" className={({ isActive }) => isActive ? 'text-primary' : 'text-muted-foreground'}>Queue</NavLink>
          <NavLink to="/manifests" className={({ isActive }) => isActive ? 'text-primary' : 'text-muted-foreground'}>Manifests</NavLink>
          <NavLink to="/explorer" className={({ isActive }) => isActive ? 'text-primary' : 'text-muted-foreground'}>Explorer</NavLink>
        </div>
      </nav>
      <main className="p-6">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/landing" element={<LandingZone />} />
          <Route path="/queue" element={<QueueMonitor />} />
          <Route path="/manifests" element={<Manifests />} />
          <Route path="/explorer" element={<DataExplorer />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
