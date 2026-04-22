import { describe, it, expect } from 'vitest'
import { formatAum, formatDate, formatDuration, US_STATES } from '../utils'

describe('formatAum', () => {
  it('returns em dash for null / undefined', () => {
    expect(formatAum(null)).toBe('—')
    expect(formatAum(undefined)).toBe('—')
  })

  it('renders billions as $XB', () => {
    expect(formatAum(1_200_000_000)).toBe('$1.2B')
    expect(formatAum(25_000_000_000)).toBe('$25.0B')
  })

  it('renders millions as $XM', () => {
    expect(formatAum(500_000_000)).toBe('$500.0M')
    expect(formatAum(1_000_000)).toBe('$1.0M')
  })

  it('renders thousands as $XK', () => {
    expect(formatAum(5_000)).toBe('$5.0K')
  })

  it('renders small numbers with toLocaleString', () => {
    expect(formatAum(500)).toBe('$500')
  })

  it('treats zero as $0 (not em dash)', () => {
    expect(formatAum(0)).toBe('$0')
  })
})

describe('formatDate', () => {
  it('returns em dash for falsy input', () => {
    expect(formatDate(null)).toBe('—')
    expect(formatDate('')).toBe('—')
  })

  it('formats an ISO string in US short form', () => {
    // "Mar 15, 2026" — exact formatting depends on locale, so assert on the year.
    const out = formatDate('2026-03-15')
    expect(out).toContain('2026')
  })
})

describe('formatDuration', () => {
  it('returns em dash if either side missing', () => {
    expect(formatDuration(null, '2026-01-01')).toBe('—')
    expect(formatDuration('2026-01-01', null)).toBe('—')
  })

  it('renders sub-minute in seconds', () => {
    expect(formatDuration('2026-01-01T00:00:00Z', '2026-01-01T00:00:45Z')).toBe('45s')
  })

  it('renders over-minute as Xm Ys', () => {
    expect(formatDuration('2026-01-01T00:00:00Z', '2026-01-01T00:05:30Z')).toBe('5m 30s')
  })
})

describe('US_STATES', () => {
  it('has 50 states plus DC and 3 territories', () => {
    expect(US_STATES).toHaveLength(54)
    expect(US_STATES).toContain('CA')
    expect(US_STATES).toContain('DC')
    expect(US_STATES).toContain('PR')
  })
})
