import { useState } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import {
  Bell,
  Building2,
  ChevronLeft,
  ChevronRight,
  Download,
  FileQuestion,
  GitMerge,
  RefreshCw,
  Settings,
  Tag,
} from 'lucide-react'

const navItems = [
  { label: 'Firm Search', icon: Building2, to: '/', end: true },
  { label: 'Bulk Match', icon: GitMerge, to: '/match' },
  { label: 'Platforms', icon: Tag, to: '/platforms' },
  { label: 'Questionnaires', icon: FileQuestion, to: '/questionnaires' },
  { label: 'Alerts', icon: Bell, to: '/alerts' },
  { label: 'Export', icon: Download, to: '/export' },
  { label: 'Sync', icon: RefreshCw, to: '/sync' },
  { label: 'Settings', icon: Settings, to: '/settings' },
]

export function Layout() {
  const [collapsed, setCollapsed] = useState(false)

  return (
    <div className="flex h-screen overflow-hidden bg-gray-50">
      {/* Sidebar */}
      <aside className={`${collapsed ? 'w-14' : 'w-60'} flex-shrink-0 bg-brand-900 flex flex-col h-full transition-all duration-200`}>
        {/* Logo */}
        <div className={`px-4 py-5 border-b border-brand-800 flex items-center ${collapsed ? 'justify-center' : 'justify-between'}`}>
          {!collapsed && (
            <div>
              <p className="text-white font-semibold text-sm leading-tight">MySEC</p>
              <p className="text-blue-300 text-xs mt-0.5">Database & Analytics</p>
            </div>
          )}
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="text-blue-300 hover:text-white transition-colors flex-shrink-0"
            aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
          </button>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-2 py-4 space-y-0.5 overflow-y-auto">
          {navItems.map((item) => {
            const Icon = item.icon
            return (
              <NavLink
                key={`${item.label}-${item.to}`}
                to={item.to}
                end={item.end}
                title={collapsed ? item.label : undefined}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                    collapsed ? 'justify-center' : ''
                  } ${
                    isActive
                      ? 'bg-brand-900 text-white ring-1 ring-white/20'
                      : 'text-blue-200 hover:text-white hover:bg-white/10'
                  }`
                }
              >
                <Icon className="w-4 h-4 flex-shrink-0" />
                {!collapsed && item.label}
              </NavLink>
            )
          })}
        </nav>

        {/* Footer */}
        {!collapsed && (
          <div className="px-4 py-3 border-t border-brand-800">
            <p className="text-blue-300 text-xs">MySEC</p>
            <p className="text-blue-400 text-xs mt-0.5">v0.1.0</p>
          </div>
        )}
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto">
        <div className="p-6">
          <Outlet />
        </div>
      </main>
    </div>
  )
}
