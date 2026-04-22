import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StatusBadge } from '../components/StatusBadge'

describe('StatusBadge', () => {
  it('renders the status text with underscores replaced by spaces', () => {
    render(<StatusBadge status="no_match" />)
    expect(screen.getByText('no match')).toBeInTheDocument()
  })

  it('applies green styling to "Registered"', () => {
    render(<StatusBadge status="Registered" />)
    const badge = screen.getByText('Registered')
    expect(badge.className).toContain('bg-green-100')
  })

  it('applies red styling to "Withdrawn"', () => {
    render(<StatusBadge status="Withdrawn" />)
    const badge = screen.getByText('Withdrawn')
    expect(badge.className).toContain('bg-red-100')
  })

  it('applies yellow styling to "pending"', () => {
    render(<StatusBadge status="pending" />)
    const badge = screen.getByText('pending')
    expect(badge.className).toContain('bg-yellow-100')
  })

  it('falls back to gray for unknown statuses', () => {
    render(<StatusBadge status="something_new" />)
    const badge = screen.getByText('something new')
    expect(badge.className).toMatch(/bg-gray-\d+/)
  })

  it('passes className through', () => {
    render(<StatusBadge status="Registered" className="custom" />)
    const badge = screen.getByText('Registered')
    expect(badge.className).toContain('custom')
  })
})
