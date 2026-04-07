import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Plus,
  Trash2,
  ChevronUp,
  ChevronDown,
  FileQuestion,
  Save,
  X,
  Edit2,
} from 'lucide-react'
import { Button } from '../components/Button'
import { Skeleton } from '../components/Skeleton'
import { useToast } from '../components/Toast'
import {
  getQuestionnaires,
  getQuestionnaire,
  createQuestionnaire,
  updateQuestionnaire,
  deleteQuestionnaire,
  getQuestionnaireFields,
  addQuestion,
  updateQuestion,
  deleteQuestion,
  reorderQuestions,
} from '../api/client'
import type { QuestionnaireTemplateDetailOut, QuestionnaireQuestionOut, FieldDefOut } from '../types'

const STYLE_TYPES = [
  { value: 'custom',      label: 'Custom' },
  { value: 'initial_rfi', label: 'Initial RFI / Due Diligence' },
  { value: 'annual_cert', label: 'Annual Certification' },
]

// ---------------------------------------------------------------------------
// QuestionRow — inline editor for a single question
// ---------------------------------------------------------------------------
function QuestionRow({
  q,
  templateId,
  index,
  total,
  fields,
  onMoveUp,
  onMoveDown,
  onDeleted,
  onSaved,
}: {
  q: QuestionnaireQuestionOut
  templateId: number
  index: number
  total: number
  fields: Record<string, FieldDefOut>
  onMoveUp: () => void
  onMoveDown: () => void
  onDeleted: () => void
  onSaved: (updated: QuestionnaireQuestionOut) => void
}) {
  const { addToast } = useToast()
  const queryClient = useQueryClient()
  const [editing, setEditing] = useState(false)
  const [text, setText] = useState(q.question_text)
  const [section, setSection] = useState(q.section)
  const [fieldPath, setFieldPath] = useState(q.answer_field_path ?? '')
  const [hint, setHint] = useState(q.answer_hint ?? '')
  const [notesEnabled, setNotesEnabled] = useState(q.notes_enabled)

  const saveMutation = useMutation({
    mutationFn: () =>
      updateQuestion(templateId, q.id, {
        section,
        question_text: text,
        answer_field_path: fieldPath || null,
        answer_hint: hint || null,
        notes_enabled: notesEnabled,
      }),
    onSuccess: (updated) => {
      setEditing(false)
      onSaved(updated)
      queryClient.invalidateQueries({ queryKey: ['questionnaire', templateId] })
    },
    onError: () => addToast('Failed to save question', 'error'),
  })

  const deleteMutation = useMutation({
    mutationFn: () => deleteQuestion(templateId, q.id),
    onSuccess: () => {
      onDeleted()
      queryClient.invalidateQueries({ queryKey: ['questionnaire', templateId] })
    },
    onError: () => addToast('Failed to delete question', 'error'),
  })

  // Group fields by category for the dropdown
  const categories: Record<string, Array<[string, FieldDefOut]>> = {}
  Object.entries(fields).forEach(([path, def]) => {
    if (path === 'raw_adv.*') return  // handled separately
    ;(categories[def.category] ??= []).push([path, def])
  })

  if (!editing) {
    return (
      <tr className="hover:bg-gray-50 group">
        <td className="px-3 py-2 text-xs text-gray-400 w-8">{index + 1}</td>
        <td className="px-3 py-2">
          <span className="text-xs font-medium text-brand-700 bg-brand-50 px-1.5 py-0.5 rounded">
            {q.section}
          </span>
        </td>
        <td className="px-3 py-2 text-sm text-gray-800">{q.question_text}</td>
        <td className="px-3 py-2 text-xs text-gray-500 font-mono">
          {q.answer_field_path
            ? <span className="bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded">{q.answer_field_path}</span>
            : q.answer_hint
              ? <span className="text-gray-400 italic">{q.answer_hint}</span>
              : <span className="text-gray-300">—</span>}
        </td>
        <td className="px-3 py-2 text-center">
          {q.notes_enabled
            ? <span className="text-green-600 text-xs">✓</span>
            : <span className="text-gray-300 text-xs">—</span>}
        </td>
        <td className="px-3 py-2 w-32">
          <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
            <button onClick={onMoveUp} disabled={index === 0} className="p-0.5 text-gray-400 hover:text-gray-700 disabled:opacity-30">
              <ChevronUp size={14} />
            </button>
            <button onClick={onMoveDown} disabled={index === total - 1} className="p-0.5 text-gray-400 hover:text-gray-700 disabled:opacity-30">
              <ChevronDown size={14} />
            </button>
            <button onClick={() => setEditing(true)} className="p-0.5 text-gray-400 hover:text-blue-600">
              <Edit2 size={14} />
            </button>
            <button
              onClick={() => { if (confirm('Delete this question?')) deleteMutation.mutate() }}
              className="p-0.5 text-gray-400 hover:text-red-600"
            >
              <Trash2 size={14} />
            </button>
          </div>
        </td>
      </tr>
    )
  }

  // Edit mode — spans full row
  return (
    <tr className="bg-blue-50">
      <td colSpan={6} className="px-3 py-3">
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Question text</label>
            <textarea
              value={text}
              onChange={e => setText(e.target.value)}
              rows={2}
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Section</label>
            <input
              value={section}
              onChange={e => setSection(e.target.value)}
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">
              Auto-populate from field
              <span className="text-gray-400 ml-1 font-normal">(optional)</span>
            </label>
            <select
              value={fieldPath}
              onChange={e => setFieldPath(e.target.value)}
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
            >
              <option value="">— none (manual answer) —</option>
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
            {fieldPath === 'raw_adv.*' && (
              <input
                placeholder="e.g. raw_adv.FormInfo.Part1A.Item5F.Q5F2C"
                className="mt-1 w-full text-xs border border-gray-300 rounded px-2 py-1 font-mono"
                onBlur={e => setFieldPath(e.target.value)}
              />
            )}
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">
              Answer hint
              <span className="text-gray-400 ml-1 font-normal">(shown when no auto-value)</span>
            </label>
            <input
              value={hint}
              onChange={e => setHint(e.target.value)}
              placeholder="e.g. See ADV Item 5D"
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
            />
          </div>
        </div>
        <div className="flex items-center gap-4 mt-2">
          <label className="flex items-center gap-1.5 text-sm text-gray-600 cursor-pointer">
            <input
              type="checkbox"
              checked={notesEnabled}
              onChange={e => setNotesEnabled(e.target.checked)}
              className="rounded"
            />
            Analyst notes column
          </label>
          <div className="ml-auto flex gap-2">
            <Button size="sm" variant="outline" onClick={() => setEditing(false)}>
              <X size={12} className="mr-1" /> Cancel
            </Button>
            <Button
              size="sm"
              onClick={() => saveMutation.mutate()}
              disabled={!text.trim() || saveMutation.isPending}
            >
              <Save size={12} className="mr-1" /> Save
            </Button>
          </div>
        </div>
      </td>
    </tr>
  )
}

// ---------------------------------------------------------------------------
// AddQuestionRow — inline form at bottom of table
// ---------------------------------------------------------------------------
function AddQuestionRow({
  templateId,
  fields,
  onAdded,
}: {
  templateId: number
  fields: Record<string, FieldDefOut>
  onAdded: () => void
}) {
  const { addToast } = useToast()
  const queryClient = useQueryClient()
  const [open, setOpen] = useState(false)
  const [text, setText] = useState('')
  const [section, setSection] = useState('General')
  const [fieldPath, setFieldPath] = useState('')
  const [hint, setHint] = useState('')

  const mutation = useMutation({
    mutationFn: () =>
      addQuestion(templateId, {
        section,
        question_text: text,
        answer_field_path: fieldPath || null,
        answer_hint: hint || null,
      }),
    onSuccess: () => {
      setText('')
      setSection('General')
      setFieldPath('')
      setHint('')
      setOpen(false)
      onAdded()
      queryClient.invalidateQueries({ queryKey: ['questionnaire', templateId] })
    },
    onError: () => addToast('Failed to add question', 'error'),
  })

  const categories: Record<string, Array<[string, FieldDefOut]>> = {}
  Object.entries(fields).forEach(([path, def]) => {
    if (path === 'raw_adv.*') return
    ;(categories[def.category] ??= []).push([path, def])
  })

  if (!open) {
    return (
      <tr>
        <td colSpan={6} className="px-3 py-2">
          <button
            onClick={() => setOpen(true)}
            className="flex items-center gap-1 text-sm text-brand-600 hover:text-brand-800"
          >
            <Plus size={14} /> Add question
          </button>
        </td>
      </tr>
    )
  }

  return (
    <tr className="bg-green-50">
      <td colSpan={6} className="px-3 py-3">
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Question text</label>
            <textarea
              value={text}
              onChange={e => setText(e.target.value)}
              rows={2}
              placeholder="Enter question..."
              autoFocus
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Section</label>
            <input
              value={section}
              onChange={e => setSection(e.target.value)}
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Auto-populate from field</label>
            <select
              value={fieldPath}
              onChange={e => setFieldPath(e.target.value)}
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
            >
              <option value="">— none —</option>
              {Object.entries(categories).map(([cat, entries]) => (
                <optgroup key={cat} label={cat}>
                  {entries.map(([path, def]) => (
                    <option key={path} value={path}>{def.label}</option>
                  ))}
                </optgroup>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Answer hint</label>
            <input
              value={hint}
              onChange={e => setHint(e.target.value)}
              placeholder="e.g. See ADV Item 5D"
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
            />
          </div>
        </div>
        <div className="flex gap-2 mt-2 justify-end">
          <Button size="sm" variant="outline" onClick={() => setOpen(false)}>Cancel</Button>
          <Button
            size="sm"
            onClick={() => mutation.mutate()}
            disabled={!text.trim() || mutation.isPending}
          >
            <Plus size={12} className="mr-1" /> Add
          </Button>
        </div>
      </td>
    </tr>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------
export default function Questionnaires() {
  const { addToast } = useToast()
  const queryClient = useQueryClient()
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [newStyle, setNewStyle] = useState('custom')
  const [editingMeta, setEditingMeta] = useState(false)
  const [editName, setEditName] = useState('')
  const [editDesc, setEditDesc] = useState('')
  const [editStyle, setEditStyle] = useState('custom')

  const { data: templates, isLoading } = useQuery({
    queryKey: ['questionnaires'],
    queryFn: getQuestionnaires,
  })

  const { data: template, isLoading: templateLoading } = useQuery({
    queryKey: ['questionnaire', selectedId],
    queryFn: () => getQuestionnaire(selectedId!),
    enabled: selectedId !== null,
  })

  const { data: fields = {} } = useQuery({
    queryKey: ['questionnaire-fields'],
    queryFn: getQuestionnaireFields,
  })

  const createMutation = useMutation({
    mutationFn: () => createQuestionnaire({ name: newName.trim(), description: newDesc.trim() || undefined, style_type: newStyle }),
    onSuccess: (t) => {
      queryClient.invalidateQueries({ queryKey: ['questionnaires'] })
      addToast(`Template "${t.name}" created`, 'success')
      setCreating(false)
      setNewName('')
      setNewDesc('')
      setNewStyle('custom')
      setSelectedId(t.id)
    },
    onError: (e: any) => addToast(e?.response?.data?.detail || 'Failed to create template', 'error'),
  })

  const updateMutation = useMutation({
    mutationFn: () => updateQuestionnaire(selectedId!, { name: editName, description: editDesc || undefined, style_type: editStyle }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['questionnaires'] })
      queryClient.invalidateQueries({ queryKey: ['questionnaire', selectedId] })
      addToast('Template saved', 'success')
      setEditingMeta(false)
    },
    onError: (e: any) => addToast(e?.response?.data?.detail || 'Failed to update template', 'error'),
  })

  const deleteMutation = useMutation({
    mutationFn: () => deleteQuestionnaire(selectedId!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['questionnaires'] })
      addToast('Template deleted', 'success')
      setSelectedId(null)
    },
    onError: (e: any) => addToast(e?.response?.data?.detail || 'Cannot delete — firm responses exist', 'error'),
  })

  const handleMoveUp = async (questions: QuestionnaireTemplateDetailOut['questions'], idx: number) => {
    if (idx === 0) return
    const ids = questions.map(q => q.id)
    ;[ids[idx - 1], ids[idx]] = [ids[idx], ids[idx - 1]]
    await reorderQuestions(selectedId!, ids)
    queryClient.invalidateQueries({ queryKey: ['questionnaire', selectedId] })
  }

  const handleMoveDown = async (questions: QuestionnaireTemplateDetailOut['questions'], idx: number) => {
    if (idx === questions.length - 1) return
    const ids = questions.map(q => q.id)
    ;[ids[idx], ids[idx + 1]] = [ids[idx + 1], ids[idx]]
    await reorderQuestions(selectedId!, ids)
    queryClient.invalidateQueries({ queryKey: ['questionnaire', selectedId] })
  }

  const startEditMeta = () => {
    if (!template) return
    setEditName(template.name)
    setEditDesc(template.description ?? '')
    setEditStyle(template.style_type)
    setEditingMeta(true)
  }

  return (
    <div className="flex h-full min-h-0">
      {/* Left panel — template list */}
      <aside className="w-72 border-r border-gray-200 flex flex-col bg-gray-50 shrink-0">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
          <h2 className="font-semibold text-gray-800">Templates</h2>
          <Button size="xs" onClick={() => setCreating(true)}>
            <Plus size={12} className="mr-1" /> New
          </Button>
        </div>

        {isLoading ? (
          <div className="p-4"><Skeleton /></div>
        ) : !templates?.length ? (
          <div className="flex-1 flex items-center justify-center p-6 text-center">
            <div>
              <FileQuestion size={36} className="mx-auto text-gray-300 mb-2" />
              <p className="text-sm text-gray-500">No templates yet.</p>
              <p className="text-xs text-gray-400 mt-1">Create one or run <code>make seed</code>.</p>
            </div>
          </div>
        ) : (
          <ul className="flex-1 overflow-y-auto">
            {templates.map(t => (
              <li key={t.id}>
                <button
                  onClick={() => setSelectedId(t.id)}
                  className={`w-full text-left px-4 py-3 border-b border-gray-100 hover:bg-white transition-colors ${selectedId === t.id ? 'bg-white border-l-2 border-l-brand-600' : ''}`}
                >
                  <div className="font-medium text-sm text-gray-800 truncate">{t.name}</div>
                  <div className="text-xs text-gray-400 mt-0.5">
                    {t.question_count} question{t.question_count !== 1 ? 's' : ''} · {t.style_type}
                  </div>
                </button>
              </li>
            ))}
          </ul>
        )}

        {/* Create new template form */}
        {creating && (
          <div className="border-t border-gray-200 p-4 bg-white">
            <p className="text-xs font-semibold text-gray-600 mb-2">New template</p>
            <input
              autoFocus
              value={newName}
              onChange={e => setNewName(e.target.value)}
              placeholder="Template name"
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 mb-2 focus:outline-none focus:ring-1 focus:ring-brand-500"
            />
            <textarea
              value={newDesc}
              onChange={e => setNewDesc(e.target.value)}
              placeholder="Description (optional)"
              rows={2}
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 mb-2 focus:outline-none focus:ring-1 focus:ring-brand-500"
            />
            <select
              value={newStyle}
              onChange={e => setNewStyle(e.target.value)}
              className="w-full text-sm border border-gray-300 rounded px-2 py-1 mb-2"
            >
              {STYLE_TYPES.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
            </select>
            <div className="flex gap-2">
              <Button size="xs" variant="outline" onClick={() => setCreating(false)} className="flex-1">Cancel</Button>
              <Button
                size="xs"
                onClick={() => createMutation.mutate()}
                disabled={!newName.trim() || createMutation.isPending}
                className="flex-1"
              >
                Create
              </Button>
            </div>
          </div>
        )}
      </aside>

      {/* Right panel — template editor */}
      <main className="flex-1 overflow-y-auto p-6">
        {!selectedId ? (
          <div className="flex flex-col items-center justify-center h-full text-center">
            <FileQuestion size={48} className="text-gray-200 mb-3" />
            <p className="text-gray-500">Select a template to view and edit its questions.</p>
            <p className="text-sm text-gray-400 mt-1">Or create a new template using the button on the left.</p>
          </div>
        ) : templateLoading ? (
          <Skeleton />
        ) : !template ? (
          <p className="text-gray-500">Template not found.</p>
        ) : (
          <div>
            {/* Template metadata header */}
            <div className="flex items-start justify-between mb-6">
              <div className="flex-1">
                {editingMeta ? (
                  <div className="space-y-2 max-w-xl">
                    <input
                      value={editName}
                      onChange={e => setEditName(e.target.value)}
                      className="w-full text-lg font-semibold border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
                    />
                    <textarea
                      value={editDesc}
                      onChange={e => setEditDesc(e.target.value)}
                      rows={2}
                      className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500"
                    />
                    <select
                      value={editStyle}
                      onChange={e => setEditStyle(e.target.value)}
                      className="text-sm border border-gray-300 rounded px-2 py-1"
                    >
                      {STYLE_TYPES.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
                    </select>
                    <div className="flex gap-2">
                      <Button size="sm" variant="outline" onClick={() => setEditingMeta(false)}>Cancel</Button>
                      <Button size="sm" onClick={() => updateMutation.mutate()} disabled={!editName.trim() || updateMutation.isPending}>
                        <Save size={12} className="mr-1" /> Save
                      </Button>
                    </div>
                  </div>
                ) : (
                  <>
                    <h1 className="text-xl font-semibold text-gray-900">{template.name}</h1>
                    {template.description && <p className="text-sm text-gray-500 mt-0.5">{template.description}</p>}
                    <p className="text-xs text-gray-400 mt-1">Style: {template.style_type} · {template.questions.length} questions</p>
                  </>
                )}
              </div>
              {!editingMeta && (
                <div className="flex gap-2 ml-4">
                  <Button size="sm" variant="outline" onClick={startEditMeta}>
                    <Edit2 size={12} className="mr-1" /> Edit
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      if (confirm(`Delete template "${template.name}"? This cannot be undone.`)) {
                        deleteMutation.mutate()
                      }
                    }}
                    disabled={deleteMutation.isPending}
                    className="text-red-600 border-red-200 hover:bg-red-50"
                  >
                    <Trash2 size={12} className="mr-1" /> Delete
                  </Button>
                </div>
              )}
            </div>

            {/* Questions table */}
            <div className="border border-gray-200 rounded-lg overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50 border-b border-gray-200">
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 w-8">#</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Section</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Question</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Field / Hint</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Notes</th>
                    <th className="px-3 py-2 w-32"></th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {template.questions.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="px-3 py-8 text-center text-sm text-gray-400">
                        No questions yet. Add your first question below.
                      </td>
                    </tr>
                  ) : (
                    template.questions.map((q, idx) => (
                      <QuestionRow
                        key={q.id}
                        q={q}
                        templateId={template.id}
                        index={idx}
                        total={template.questions.length}
                        fields={fields}
                        onMoveUp={() => handleMoveUp(template.questions, idx)}
                        onMoveDown={() => handleMoveDown(template.questions, idx)}
                        onDeleted={() => queryClient.invalidateQueries({ queryKey: ['questionnaire', selectedId] })}
                        onSaved={() => queryClient.invalidateQueries({ queryKey: ['questionnaire', selectedId] })}
                      />
                    ))
                  )}
                  <AddQuestionRow
                    templateId={template.id}
                    fields={fields}
                    onAdded={() => queryClient.invalidateQueries({ queryKey: ['questionnaire', selectedId] })}
                  />
                </tbody>
              </table>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
