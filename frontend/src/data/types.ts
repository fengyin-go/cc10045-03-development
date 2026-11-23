/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

/** 口径版本分组（不可变整轮结果中的单个分组值）。 */
export type CaliberGroup = {
  dimension_key: string
  computed_value: number
  event_count: number
}

/** 一轮聚合口径版本及其状态。 */
export type CaliberVersion = {
  id: number
  window_id: number
  version_no: number
  caliber_code: string
  label: string
  status: '试算中' | '候选' | '试算失败' | '已启用' | '已确认' | '已撤销'
  fail_reason?: string | null
  window_start?: string | null
  window_end?: string | null
  group_count: number
  total_value: number
  created_at: string
  activated_at?: string | null
  revoked_at?: string | null
  is_active?: boolean
  groups?: CaliberGroup[]
}

/** 仪表盘/值班取数结果：整轮分组 + 明确的口径标注。 */
export type CaliberSnapshot = {
  ok: boolean
  active: boolean
  message: string
  window_id: number
  window_name: string
  caliber_code: string | null
  caliber_label?: string
  version_id: number | null
  version_no: number | null
  version_status?: string
  window_start?: string | null
  window_end?: string | null
  fetch_log_id?: number
  groups: CaliberGroup[]
  total_value?: number
}

/** 取数留痕记录，用于值班日报事后原样回查。 */
export type CaliberFetchLog = {
  id: number
  window_id: number
  version_id: number
  caliber_code: string
  consumer: string
  fetched_at: string
}

