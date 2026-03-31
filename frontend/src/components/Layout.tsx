import { NavLink, Outlet } from 'react-router-dom'
import {
  Home,
  Building2,
  GitMerge,
  Tag,
  Bell,
  Download,
  RefreshCw,
} from 'lucide-react'

const navItems = [
  { label: 'Search', icon: Home, to: '/', end: true },
  { label: 'Firm Search', icon: Building2, to: '/', end: true },
  { label: 'Bulk Match', icon: GitMerge, to: '/match' },
  { label: 'Platforms', icon: Tag, to: '/platforms' },
  { label: 'Alerts', icon: Bell, to: '/alerts' },
  { label: 'Export', icon: Download, to: '/export' },
  { label: 'Sync', icon: RefreshCw, to: '/sync' },
]

export function Layout() {
  return (
    <div className="flex h-screen overflow-hidden bg-gray-50">
      {/* Sidebar */}
      <aside className="w-60 flex-shrink-0 bg-brand-900 flex flex-col h-full">
        {/* Logo */}
        <div className="px-4 py-5 border-b border-brand-800">
          <p className="text-white font-semibold text-sm leading-tight">SEC Adviser Platform</p>
          <p className="text-blue-300 text-xs mt-0.5">Database & Analytics</p>
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
                className={({ isActive }) =>
                  `flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                    isActive
                      ? 'bg-brand-900 text-white ring-1 ring-white/20'
                      : 'text-blue-200 hover:text-white hover:bg-white/10'
                  }`
                }
              >
                <Icon className="w-4 h-4 flex-shrink-0" />
                {item.label}
              </NavLink>
            )
          })}
        </nav>

        {/* Footer */}
        <div className="px-4 py-3 border-t border-brand-800">
          <p className="text-blue-300 text-xs">SEC Adviser Platform</p>
          <p className="text-blue-400 text-xs mt-0.5">v0.1.0</p>
        </div>
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
