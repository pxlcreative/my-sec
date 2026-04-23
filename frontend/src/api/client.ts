import axios from 'axios'
import type {
  PaginatedFirms,
  FirmDetail,
  FirmHistoryResponse,
  AumHistoryResponse,
  BrochureMeta,
  BusinessProfile,
  DisclosuresSummary,
  PlatformOut,
  FirmSummary,
  FirmPlatformTag,
  BulkMatchSyncResponse,
  BulkMatchAsyncResponse,
  MatchJobStatus,
  ExportJobOut,
  ExportTemplateOut,
  AlertRuleOut,
  AlertEventOut,
  AlertTestResponse,
  SyncStatusEntry,
  CronScheduleOut,
  StorageSettingsOut,
  StorageTestResult,
  ReductoSettingsOut,
  ReductoTestResult,
  BrochureParseResult,
  BrochureParsedContent,
  QuestionnaireTemplateOut,
  QuestionnaireTemplateDetailOut,
  QuestionnaireQuestionOut,
  QuestionnaireResponseOut,
  FirmQuestionnaireListItem,
  FieldDefOut,
} from '../types'

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
  paramsSerializer: (params) =>
    Object.entries(params)
      .flatMap(([k, v]) =>
        Array.isArray(v)
          ? v.map((item) => `${encodeURIComponent(k)}=${encodeURIComponent(item)}`)
          : v != null ? [`${encodeURIComponent(k)}=${encodeURIComponent(v)}`] : []
      )
      .join('&'),
})

// Firms
export async function searchFirms(
  q: string,
  params?: Record<string, unknown>
): Promise<PaginatedFirms> {
  const response = await api.get('/firms', { params: { q, ...params } })
  return response.data
}

export async function getFirm(crd: number): Promise<FirmDetail> {
  const response = await api.get(`/firms/${crd}`)
  return response.data
}

export async function getFirmHistory(crd: number): Promise<FirmHistoryResponse> {
  const response = await api.get(`/firms/${crd}/history`)
  return response.data
}

export async function getFirmAumHistory(crd: number): Promise<AumHistoryResponse> {
  const response = await api.get(`/firms/${crd}/aum-history`)
  return response.data
}

export async function getFirmBrochures(crd: number): Promise<BrochureMeta[]> {
  const response = await api.get(`/firms/${crd}/brochures`)
  return response.data
}

export async function getFirmDisclosures(crd: number): Promise<DisclosuresSummary> {
  const response = await api.get(`/firms/${crd}/disclosures`)
  return response.data
}

export async function getFirmBusinessProfile(crd: number): Promise<BusinessProfile> {
  const response = await api.get(`/firms/${crd}/business-profile`)
  return response.data
}

export async function refreshFirm(crd: number): Promise<{ changed: boolean; num_changes: number; fields_changed: string[]; last_iapd_refresh_at: string | null }> {
  const response = await api.post(`/firms/${crd}/refresh`)
  return response.data
}

// Platforms
export async function getPlatforms(): Promise<PlatformOut[]> {
  const response = await api.get('/platforms')
  return response.data
}

export async function createPlatform(data: {
  name: string
  description?: string
  save_brochures?: boolean
}): Promise<PlatformOut> {
  const response = await api.post('/platforms', data)
  return response.data
}

export async function updatePlatform(id: number, data: { save_brochures: boolean }): Promise<PlatformOut> {
  const response = await api.patch(`/platforms/${id}`, data)
  return response.data
}

export async function deletePlatform(id: number): Promise<void> {
  await api.delete(`/platforms/${id}`)
}

export async function getPlatformFirms(id: number): Promise<FirmSummary[]> {
  const response = await api.get(`/platforms/${id}/firms`)
  return response.data.results
}

export async function getFirmPlatforms(crd: number): Promise<FirmPlatformTag[]> {
  const response = await api.get(`/firms/${crd}/platforms`)
  return response.data
}

export async function addFirmPlatform(
  crd: number,
  platformId: number
): Promise<FirmPlatformTag> {
  const response = await api.post(`/firms/${crd}/platforms`, { platform_id: platformId })
  return response.data
}

export async function removeFirmPlatform(crd: number, platformId: number): Promise<void> {
  await api.delete(`/firms/${crd}/platforms/${platformId}`)
}

// Match
export async function bulkMatch(data: Record<string, unknown>): Promise<BulkMatchSyncResponse | BulkMatchAsyncResponse> {
  const response = await api.post('/match/bulk', data)
  return response.data
}

export async function getMatchJob(jobId: number): Promise<MatchJobStatus> {
  const response = await api.get(`/match/jobs/${jobId}`)
  return response.data
}

export async function bulkTag(data: Record<string, unknown>): Promise<unknown> {
  const response = await api.post('/match/bulk-tag', data)
  return response.data
}

// Export
export async function startExport(data: Record<string, unknown>): Promise<unknown> {
  const response = await api.post('/export/firms', data)
  return response.data
}

export async function getExportJob(jobId: string): Promise<ExportJobOut> {
  const response = await api.get(`/export/jobs/${jobId}`)
  return response.data
}

export async function getExportTemplates(): Promise<ExportTemplateOut[]> {
  const response = await api.get('/export/templates')
  return response.data
}

export async function createExportTemplate(data: Record<string, unknown>): Promise<ExportTemplateOut> {
  const response = await api.post('/export/templates', data)
  return response.data
}

// Alerts
export async function getAlertRules(): Promise<AlertRuleOut[]> {
  const response = await api.get('/alerts/rules')
  return response.data
}

export async function createAlertRule(data: Record<string, unknown>): Promise<AlertRuleOut> {
  const response = await api.post('/alerts/rules', data)
  return response.data
}

export async function updateAlertRule(
  id: number,
  data: Record<string, unknown>
): Promise<AlertRuleOut> {
  const response = await api.patch(`/alerts/rules/${id}`, data)
  return response.data
}

export async function deleteAlertRule(id: number): Promise<void> {
  await api.delete(`/alerts/rules/${id}`)
}

export async function getAlertEvents(
  params?: Record<string, unknown>
): Promise<AlertEventOut[]> {
  const response = await api.get('/alerts/events', { params })
  return response.data
}

export async function testAlertRule(id: number): Promise<AlertTestResponse> {
  const response = await api.post(`/alerts/rules/${id}/test`)
  return response.data
}

export async function evaluateAlertRule(id: number): Promise<{ rule_id: number; fired: number; rule_type: string; label: string }> {
  const response = await api.post(`/alerts/rules/${id}/evaluate`)
  return response.data
}

// Sync
export async function getSyncStatus(): Promise<SyncStatusEntry[]> {
  const response = await api.get('/sync/status')
  return response.data
}

export async function getSyncJobs(limit = 50): Promise<SyncStatusEntry[]> {
  const response = await api.get('/sync/jobs', { params: { limit } })
  return response.data
}

export async function getSyncJob(jobId: number): Promise<SyncStatusEntry> {
  const response = await api.get(`/sync/jobs/${jobId}`)
  return response.data
}

export async function cancelSyncJob(jobId: number): Promise<unknown> {
  const response = await api.post(`/sync/jobs/${jobId}/cancel`)
  return response.data
}

export async function triggerSync(): Promise<unknown> {
  const response = await api.post('/sync/trigger')
  return response.data
}

// Schedules
export async function getSchedules(): Promise<CronScheduleOut[]> {
  const response = await api.get('/schedules')
  return response.data
}

export async function patchSchedule(
  id: number,
  data: Partial<CronScheduleOut>
): Promise<CronScheduleOut> {
  const response = await api.patch(`/schedules/${id}`, data)
  return response.data
}

export async function triggerSchedule(id: number): Promise<{ status: string; task_id: string }> {
  const response = await api.post(`/schedules/${id}/trigger`)
  return response.data
}

// Settings
export async function getStorageSettings(): Promise<StorageSettingsOut> {
  const response = await api.get('/settings/storage')
  return response.data
}

export async function updateStorageSettings(
  data: Partial<StorageSettingsOut>
): Promise<StorageSettingsOut> {
  const response = await api.patch('/settings/storage', data)
  return response.data
}

export async function testStorageConnection(): Promise<StorageTestResult> {
  const response = await api.post('/settings/storage/test')
  return response.data
}

// Reducto settings
export async function getReductoSettings(): Promise<ReductoSettingsOut> {
  const response = await api.get('/settings/reducto')
  return response.data
}

export async function updateReductoSettings(
  data: Partial<ReductoSettingsOut>
): Promise<ReductoSettingsOut> {
  const response = await api.patch('/settings/reducto', data)
  return response.data
}

export async function testReductoConnection(): Promise<ReductoTestResult> {
  const response = await api.post('/settings/reducto/test')
  return response.data
}

// Brochure parsing
export async function parseBrochure(
  crd: number,
  versionId: number
): Promise<BrochureParseResult> {
  const response = await api.post(`/firms/${crd}/brochures/${versionId}/parse`)
  return response.data
}

export async function getBrochureParsed(
  crd: number,
  versionId: number
): Promise<BrochureParsedContent> {
  const response = await api.get(`/firms/${crd}/brochures/${versionId}/parsed`)
  return response.data
}

// Questionnaires — template management
export async function getQuestionnaires(): Promise<QuestionnaireTemplateOut[]> {
  const response = await api.get('/questionnaires')
  return response.data
}

export async function createQuestionnaire(data: {
  name: string
  description?: string
  style_type?: string
}): Promise<QuestionnaireTemplateOut> {
  const response = await api.post('/questionnaires', data)
  return response.data
}

export async function getQuestionnaire(id: number): Promise<QuestionnaireTemplateDetailOut> {
  const response = await api.get(`/questionnaires/${id}`)
  return response.data
}

export async function updateQuestionnaire(
  id: number,
  data: { name: string; description?: string; style_type?: string }
): Promise<QuestionnaireTemplateOut> {
  const response = await api.put(`/questionnaires/${id}`, data)
  return response.data
}

export async function deleteQuestionnaire(id: number): Promise<void> {
  await api.delete(`/questionnaires/${id}`)
}

export async function getQuestionnaireFields(): Promise<Record<string, FieldDefOut>> {
  const response = await api.get('/questionnaires/fields')
  return response.data
}

// Questionnaires — question management
export async function addQuestion(
  templateId: number,
  data: {
    section?: string
    question_text: string
    answer_field_path?: string | null
    answer_hint?: string | null
    notes_enabled?: boolean
    order_index?: number
  }
): Promise<QuestionnaireQuestionOut> {
  const response = await api.post(`/questionnaires/${templateId}/questions`, data)
  return response.data
}

export async function updateQuestion(
  templateId: number,
  questionId: number,
  data: {
    section?: string
    question_text?: string
    answer_field_path?: string | null
    answer_hint?: string | null
    notes_enabled?: boolean
  }
): Promise<QuestionnaireQuestionOut> {
  const response = await api.put(`/questionnaires/${templateId}/questions/${questionId}`, data)
  return response.data
}

export async function deleteQuestion(templateId: number, questionId: number): Promise<void> {
  await api.delete(`/questionnaires/${templateId}/questions/${questionId}`)
}

export async function reorderQuestions(
  templateId: number,
  orderedIds: number[]
): Promise<void> {
  await api.put(`/questionnaires/${templateId}/questions/reorder`, { ordered_ids: orderedIds })
}

// Questionnaires — firm responses
export async function getFirmQuestionnaires(crd: number): Promise<FirmQuestionnaireListItem[]> {
  const response = await api.get(`/firms/${crd}/questionnaires`)
  return response.data
}

export async function getFirmQuestionnaire(
  crd: number,
  templateId: number
): Promise<QuestionnaireResponseOut> {
  const response = await api.get(`/firms/${crd}/questionnaires/${templateId}`)
  return response.data
}

export async function regenerateFirmQuestionnaire(
  crd: number,
  templateId: number
): Promise<QuestionnaireResponseOut> {
  const response = await api.post(`/firms/${crd}/questionnaires/${templateId}/regenerate`)
  return response.data
}

export async function updateFirmQuestionnaireAnswers(
  crd: number,
  templateId: number,
  data: {
    answers?: Record<string, string>
    analyst_notes?: Record<string, string>
    status?: string
  }
): Promise<QuestionnaireResponseOut> {
  const response = await api.patch(`/firms/${crd}/questionnaires/${templateId}/answers`, data)
  return response.data
}

export default api
