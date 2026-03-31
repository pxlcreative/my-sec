import { Routes, Route } from 'react-router-dom'
import { Layout } from './components/Layout'
import { ErrorBoundary } from './components/ErrorBoundary'
import { ToastProvider } from './components/Toast'
import Search from './pages/Search'
import FirmDetail from './pages/FirmDetail'
import BulkMatch from './pages/BulkMatch'
import Platforms from './pages/Platforms'
import PlatformDetail from './pages/PlatformDetail'
import Alerts from './pages/Alerts'
import Export from './pages/Export'
import Sync from './pages/Sync'

export default function App() {
  return (
    <ToastProvider>
      <Routes>
        <Route element={<Layout />}>
          <Route
            path="/"
            element={
              <ErrorBoundary>
                <Search />
              </ErrorBoundary>
            }
          />
          <Route
            path="/firms/:crd"
            element={
              <ErrorBoundary>
                <FirmDetail />
              </ErrorBoundary>
            }
          />
          <Route
            path="/match"
            element={
              <ErrorBoundary>
                <BulkMatch />
              </ErrorBoundary>
            }
          />
          <Route
            path="/platforms"
            element={
              <ErrorBoundary>
                <Platforms />
              </ErrorBoundary>
            }
          />
          <Route
            path="/platforms/:id"
            element={
              <ErrorBoundary>
                <PlatformDetail />
              </ErrorBoundary>
            }
          />
          <Route
            path="/alerts"
            element={
              <ErrorBoundary>
                <Alerts />
              </ErrorBoundary>
            }
          />
          <Route
            path="/export"
            element={
              <ErrorBoundary>
                <Export />
              </ErrorBoundary>
            }
          />
          <Route
            path="/sync"
            element={
              <ErrorBoundary>
                <Sync />
              </ErrorBoundary>
            }
          />
        </Route>
      </Routes>
    </ToastProvider>
  )
}
