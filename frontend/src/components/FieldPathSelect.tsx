import { useState, useEffect } from 'react'
import type { FieldDefOut } from '../types'

interface Props {
  value: string
  onChange: (value: string) => void
  fields: Record<string, FieldDefOut>
  className?: string
  placeholder?: string
}

export function FieldPathSelect({ value, onChange, fields, className = '', placeholder = '— none —' }: Props) {
  const isRawAdv = value.startsWith('raw_adv.') && value !== 'raw_adv.*'
  const selectValue = isRawAdv ? 'raw_adv.*' : value
  const [customPath, setCustomPath] = useState(isRawAdv ? value : '')

  useEffect(() => {
    if (isRawAdv) setCustomPath(value)
  }, [value, isRawAdv])

  const categories: Record<string, Array<[string, FieldDefOut]>> = {}
  Object.entries(fields).forEach(([path, def]) => {
    if (path === 'raw_adv.*') return
    ;(categories[def.category] ??= []).push([path, def])
  })

  function handleSelectChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const v = e.target.value
    if (v === 'raw_adv.*') {
      setCustomPath('')
      onChange('raw_adv.*')
    } else {
      onChange(v)
    }
  }

  function handleCustomPathBlur(e: React.FocusEvent<HTMLInputElement>) {
    const v = e.target.value.trim()
    if (v) onChange(v)
  }

  return (
    <div className="space-y-1">
      <select
        value={selectValue}
        onChange={handleSelectChange}
        className={`w-full text-sm border border-gray-300 rounded-md px-2 py-2 focus:ring-2 focus:ring-brand-600 outline-none ${className}`}
      >
        <option value="">{placeholder}</option>
        {Object.entries(categories).map(([cat, entries]) => (
          <optgroup key={cat} label={cat}>
            {entries.map(([path, def]) => (
              <option key={path} value={path}>{def.label}</option>
            ))}
          </optgroup>
        ))}
        <optgroup label="Raw ADV">
          <option value="raw_adv.*">Raw ADV dot-path (type below)</option>
        </optgroup>
      </select>
      {selectValue === 'raw_adv.*' && (
        <input
          value={customPath}
          onChange={(e) => setCustomPath(e.target.value)}
          onBlur={handleCustomPathBlur}
          placeholder="e.g. raw_adv.FormInfo.Part1A.Item5F.Q5F2C"
          className="w-full text-xs border border-gray-300 rounded-md px-2 py-1 font-mono focus:ring-2 focus:ring-brand-600 outline-none"
        />
      )}
    </div>
  )
}
