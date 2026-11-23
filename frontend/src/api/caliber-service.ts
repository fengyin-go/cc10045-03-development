import { request } from './client'
import type {
  CaliberFetchLog,
  CaliberGroup,
  CaliberSnapshot,
  CaliberVersion,
} from '@/data/types'

const BASE = '/api/realtime-agg'

async function asJson<T>(response: Response): Promise<T> {
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

/** 发起一轮口径试算；不传 groups 时由后端按窗口现有事件实时聚合出候选。 */
export async function startTrial(
  windowId: number,
  payload: {
    caliber_code?: string
    label?: string
    groups?: Pick<CaliberGroup, 'dimension_key' | 'computed_value' | 'event_count'>[]
    force_fail?: boolean
    fail_reason?: string
  } = {},
): Promise<{ ok: boolean; message: string; version: CaliberVersion }> {
  const response = await request(`${BASE}/windows/${windowId}/trials`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return asJson(response)
}

/** 废弃待启用候选版本。 */
export async function discardCandidate(
  windowId: number,
): Promise<{ ok: boolean; message: string }> {
  const response = await request(`${BASE}/windows/${windowId}/candidate/discard`, {
    method: 'POST',
  })
  return asJson(response)
}

/** 启用候选口径，整轮分组原子生效。 */
export async function activateVersion(
  windowId: number,
  versionId?: number,
): Promise<{ ok: boolean; message: string; version: CaliberVersion; previous_caliber_code?: string | null }> {
  const response = await request(`${BASE}/windows/${windowId}/activate`, {
    method: 'POST',
    body: JSON.stringify({ version_id: versionId ?? null }),
  })
  return asJson(response)
}

/** 撤销当前启用口径，回退到上一已确认版本。 */
export async function revokeActive(
  windowId: number,
): Promise<{ ok: boolean; message: string; active_version: CaliberVersion | null }> {
  const response = await request(`${BASE}/windows/${windowId}/revoke`, { method: 'POST' })
  return asJson(response)
}

/** 列出窗口全部口径版本。 */
export async function listVersions(windowId: number): Promise<CaliberVersion[]> {
  const response = await request(`${BASE}/windows/${windowId}/versions`)
  const data = await asJson<{ ok: boolean; versions: CaliberVersion[] }>(response)
  return data.versions
}

/** 查询单个版本及其整轮分组原样数据。 */
export async function getVersion(versionId: number): Promise<CaliberVersion> {
  const response = await request(`${BASE}/versions/${versionId}`)
  const data = await asJson<{ ok: boolean; version: CaliberVersion }>(response)
  return data.version
}

/** 仪表盘/值班取数：读取当前启用口径的整轮分组，响应带口径标注。 */
export async function fetchSnapshot(
  windowId: number,
  consumer = 'dashboard',
): Promise<CaliberSnapshot> {
  const response = await request(`${BASE}/windows/${windowId}/snapshot?consumer=${encodeURIComponent(consumer)}`)
  return asJson(response)
}

/** 历史图：曾确认过的口径轮次。 */
export async function listHistory(windowId: number): Promise<CaliberVersion[]> {
  const response = await request(`${BASE}/windows/${windowId}/history`)
  const data = await asJson<{ ok: boolean; versions: CaliberVersion[] }>(response)
  return data.versions
}

/** 按版本回查历史聚合原样数据。 */
export async function fetchHistorySnapshot(
  windowId: number,
  versionId: number,
): Promise<CaliberSnapshot> {
  const response = await request(`${BASE}/windows/${windowId}/history/${versionId}`)
  return asJson(response)
}

/** 取数留痕。 */
export async function listFetchLogs(windowId: number): Promise<CaliberFetchLog[]> {
  const response = await request(`${BASE}/windows/${windowId}/fetch-logs`)
  const data = await asJson<{ ok: boolean; logs: CaliberFetchLog[] }>(response)
  return data.logs
}
