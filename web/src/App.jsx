import { Loader2, Search, Trash2, X } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'

const SEARCH_LIMIT = 100

function App() {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [selected, setSelected] = useState(null)
  const [deleting, setDeleting] = useState(false)
  const [health, setHealth] = useState(null)

  const trimmedQuery = query.trim()
  const servedHostLabel = health?.allHosts ? 'all hosts' : health?.hostname

  useEffect(() => {
    let ignore = false
    async function loadHealth() {
      try {
        const response = await fetch('/api/health')
        const payload = await response.json()
        if (!ignore && response.ok) {
          setHealth(payload)
        }
      } catch {
        if (!ignore) {
          setHealth(null)
        }
      }
    }
    loadHealth()
    return () => {
      ignore = true
    }
  }, [])

  useEffect(() => {
    if (!trimmedQuery) {
      setResults([])
      setError('')
      setLoading(false)
      return
    }

    const controller = new AbortController()
    const timer = window.setTimeout(async () => {
      setLoading(true)
      setError('')
      setNotice('')
      try {
        const response = await fetch(`/api/search?q=${encodeURIComponent(trimmedQuery)}&limit=${SEARCH_LIMIT}`, {
          signal: controller.signal,
        })
        const payload = await response.json()
        if (!response.ok) {
          throw new Error(payload.error || 'Search failed')
        }
        setResults(payload)
      } catch (err) {
        if (err.name !== 'AbortError') {
          setError(err.message)
          setResults([])
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false)
        }
      }
    }, 250)

    return () => {
      controller.abort()
      window.clearTimeout(timer)
    }
  }, [trimmedQuery])

  const totalSize = useMemo(() => {
    return results.reduce((sum, file) => sum + (file.size || 0), 0)
  }, [results])

  async function confirmDelete() {
    if (!selected) {
      return
    }

    setDeleting(true)
    setError('')
    setNotice('')
    try {
      const response = await fetch(`/api/files/${selected.id}/delete`, { method: 'POST' })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(payload.error || 'Delete failed')
      }
      setResults((current) => current.filter((file) => file.id !== selected.id))
      setNotice(payload.alreadyMissing ? 'Database row removed; file was already missing.' : 'File and database row removed.')
      setSelected(null)
    } catch (err) {
      setError(err.message)
    } finally {
      setDeleting(false)
    }
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <h1>Deduplicator Files</h1>
          <div className="subline">
            {results.length} results · {formatBytes(totalSize)}
            {servedHostLabel ? ` · ${servedHostLabel}` : ''}
            {health && !health.deleteEnabled ? ' · read-only' : ''}
          </div>
        </div>
        <label className="search-box">
          <Search size={18} aria-hidden="true" />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Partial filepath"
            autoFocus
          />
          {loading ? <Loader2 className="spin" size={18} aria-hidden="true" /> : null}
        </label>
      </header>

      {error ? <div className="message error">{error}</div> : null}
      {notice ? <div className="message success">{notice}</div> : null}
      {health && !health.deleteEnabled ? <div className="message warning">{health.deleteDisabledReason}</div> : null}

      <section className="table-wrap" aria-label="Search results">
        <table>
          <thead>
            <tr>
              <th>Path</th>
              <th>Root</th>
              <th>Size</th>
              <th>Hash</th>
              <th className="action-col">Action</th>
            </tr>
          </thead>
          <tbody>
            {results.map((file) => (
              <tr key={file.id}>
                <td>
                  <div className="path-cell">{file.path}</div>
                  <div className="full-path">{file.fullPath || file.path}</div>
                </td>
                <td>{file.rootFolder}</td>
                <td>{formatBytes(file.size)}</td>
                <td><span className="hash">{file.hash || 'null'}</span></td>
                <td className="action-col">
                  <button
                    className="icon-button danger"
                    onClick={() => setSelected(file)}
                    disabled={health && !health.deleteEnabled}
                    aria-label={`Delete ${file.path}`}
                  >
                    <Trash2 size={17} aria-hidden="true" />
                    Delete
                  </button>
                </td>
              </tr>
            ))}
            {!results.length ? (
              <tr>
                <td colSpan="5" className="empty">
                  {trimmedQuery ? 'No matches' : 'Enter a filepath fragment'}
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </section>

      {selected ? (
        <div className="modal-backdrop" role="presentation">
          <div className="modal" role="dialog" aria-modal="true" aria-labelledby="delete-title">
            <div className="modal-head">
              <h2 id="delete-title">Confirm deletion</h2>
              <button className="close-button" onClick={() => setSelected(null)} aria-label="Close">
                <X size={18} aria-hidden="true" />
              </button>
            </div>
            <div className="delete-path">{selected.fullPath || selected.path}</div>
            <div className="modal-actions">
              <button className="secondary" onClick={() => setSelected(null)} disabled={deleting}>Cancel</button>
              <button className="danger solid" onClick={confirmDelete} disabled={deleting}>
                {deleting ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <Trash2 size={17} aria-hidden="true" />}
                Delete
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </main>
  )
}

function formatBytes(value) {
  if (!value) {
    return '0 B'
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  let size = value
  let unit = 0
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024
    unit += 1
  }
  return `${size.toFixed(size >= 10 || unit === 0 ? 0 : 1)} ${units[unit]}`
}

export default App
