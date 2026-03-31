import axios from 'axios'
import type {
  PaginatedFirms,
  FirmDetail,
  FirmHistoryResponse,
  AumHistoryResponse,
  BrochureMeta,
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
} from '../types'

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
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

// Platforms
export async function getPlatforms(): Promise<PlatformOut[]> {
  const response = await api.get('/platforms')
  return response.data
}

export async function createPlatform(data: {
  name: string
  description?: string
}): Promise<PlatformOut> {
  const response = await api.post('/platforms', data)
  return response.data
}

export async function getPlatformFirms(id: number): Promise<FirmSummary[]> {
  const response = await api.get(`/platforms/${id}/firms`)
  return response.data
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

// Sync
export async function getSyncStatus(): Promise<SyncStatusEntry[]> {
  const response = await api.get('/sync/status')
  return response.data
}

export async function triggerSync(monthStr?: string): Promise<unknown> {
  const params = monthStr ? { month_str: monthStr } : {}
  const response = await api.post('/sync/trigger', null, { params })
  return response.data
}

export default api
