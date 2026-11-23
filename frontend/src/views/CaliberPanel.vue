<template>
  <section class="caliber-panel" data-testid="caliber-panel">
    <header class="caliber-head">
      <div>
        <h3>聚合口径试算与回退</h3>
        <p class="page-desc">
          把现有事件数据聚合成候选结果，核对口径后整轮启用；撤销会原子回退到上一已确认轮次。
          每次取数都标注所用口径，并可按留痕原样回查。
        </p>
      </div>
      <div class="caliber-toolbar">
        <label class="filter-item">
          <span>聚合窗口</span>
          <select v-model.number="windowId" data-testid="caliber-window-select" @change="reloadAll">
            <option v-for="w in windows" :key="w.id" :value="w.id">
              {{ w.window_name }}（#{{ w.id }} · {{ w.status }}）
            </option>
          </select>
        </label>
        <button class="btn" type="button" data-testid="caliber-refresh" @click="reloadAll">刷新</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="caliber-error">{{ errorMessage }}</p>

    <!-- 当前启用口径取数 -->
    <article class="stat-card snapshot-card" data-testid="caliber-snapshot-card">
      <div class="snapshot-head">
        <h4>当前取数（仪表盘 / 值班日报）</h4>
        <button class="btn primary" type="button" data-testid="caliber-fetch" @click="doFetch">
          发起取数
        </button>
      </div>
      <template v-if="snapshot">
        <div v-if="!snapshot.active" class="empty-state">{{ snapshot.message }}</div>
        <template v-else>
          <p class="caliber-banner" data-testid="caliber-banner">
            本次取数使用口径
            <strong>{{ snapshot.caliber_code }}</strong>
            （第 {{ snapshot.version_no }} 轮 · {{ snapshot.version_status }}），
            取数留痕 #{{ snapshot.fetch_log_id }}
          </p>
          <table class="data-table">
            <thead>
              <tr><th>分组</th><th>聚合值</th><th>事件数</th><th>所属口径</th></tr>
            </thead>
            <tbody>
              <tr v-for="g in snapshot.groups" :key="g.dimension_key" :data-testid="`snap-row-${g.dimension_key}`">
                <td>{{ g.dimension_key }}</td>
                <td>{{ g.computed_value }}</td>
                <td>{{ g.event_count }}</td>
                <td><span class="caliber-tag">{{ snapshot.caliber_code }}</span></td>
              </tr>
            </tbody>
          </table>
        </template>
      </template>
    </article>

    <div class="caliber-columns">
      <!-- 版本流转 -->
      <article class="stat-card">
        <div class="snapshot-head">
          <h4>口径版本流转</h4>
          <div class="row-actions">
            <button class="btn primary" type="button" data-testid="caliber-new-trial" @click="openTrial">
              发起试算
            </button>
          </div>
        </div>
        <table class="data-table">
          <thead>
            <tr><th>轮次</th><th>口径编码</th><th>状态</th><th>分组数</th><th>合计</th><th>操作</th></tr>
          </thead>
          <tbody>
            <tr v-for="v in versions" :key="v.id" :data-testid="`version-row-${v.id}`" :data-status="v.status">
              <td>第{{ v.version_no }}轮</td>
              <td>{{ v.caliber_code }}<small v-if="v.is_active"> · 当前启用</small></td>
              <td>
                <span class="status-pill" :data-status="v.status">{{ v.status }}</span>
                <small v-if="v.fail_reason" :title="v.fail_reason">（{{ v.fail_reason }}）</small>
              </td>
              <td>{{ v.group_count }}</td>
              <td>{{ v.total_value }}</td>
              <td class="row-actions">
                <button
                  v-if="v.status === '候选'"
                  class="link" type="button"
                  :data-testid="`activate-${v.id}`"
                  @click="doActivate(v.id)"
                >启用</button>
                <button
                  v-if="v.status === '候选'"
                  class="link" type="button"
                  :data-testid="`discard-${v.id}`"
                  @click="doDiscard"
                >废弃</button>
                <button
                  v-if="v.is_active"
                  class="link" type="button"
                  :data-testid="`revoke-${v.id}`"
                  @click="doRevoke"
                >撤销</button>
                <button
                  v-if="['已启用', '已确认', '已撤销'].includes(v.status)"
                  class="link" type="button"
                  :data-testid="`inspect-${v.id}`"
                  @click="inspectVersion(v.id)"
                >原样回查</button>
              </td>
            </tr>
            <tr v-if="!versions.length">
              <td colspan="6" class="empty-state">暂无口径版本，先发起一轮试算</td>
            </tr>
          </tbody>
        </table>
      </article>

      <!-- 历史与留痕 -->
      <article class="stat-card">
        <h4>历史确认轮次（打开历史图）</h4>
        <table class="data-table">
          <thead><tr><th>轮次</th><th>口径</th><th>状态</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="h in history" :key="h.id" :data-testid="`history-row-${h.id}`">
              <td>第{{ h.version_no }}轮</td>
              <td>{{ h.caliber_code }}</td>
              <td><span class="status-pill" :data-status="h.status">{{ h.status }}</span></td>
              <td class="row-actions">
                <button class="link" type="button" :data-testid="`history-view-${h.id}`" @click="viewHistory(h.id)">
                  查看原样
                </button>
              </td>
            </tr>
            <tr v-if="!history.length">
              <td colspan="4" class="empty-state">还没有确认过的口径轮次</td>
            </tr>
          </tbody>
        </table>

        <h4 style="margin-top: 16px">取数留痕（值班日报回查）</h4>
        <table class="data-table">
          <thead><tr><th>时间</th><th>消费者</th><th>使用口径</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="log in logs" :key="log.id" :data-testid="`fetch-log-${log.id}`">
              <td>{{ log.fetched_at }}</td>
              <td>{{ log.consumer }}</td>
              <td><span class="caliber-tag">{{ log.caliber_code }}</span></td>
              <td class="row-actions">
                <button class="link" type="button" :data-testid="`log-trace-${log.id}`" @click="traceLog(log.version_id)">
                  回查原样
                </button>
              </td>
            </tr>
            <tr v-if="!logs.length">
              <td colspan="4" class="empty-state">暂无取数记录</td>
            </tr>
          </tbody>
        </table>
      </article>
    </div>

    <!-- 试算弹窗 -->
    <div v-if="showTrial" class="modal-backdrop" data-testid="trial-modal">
      <form class="modal" @submit.prevent="submitTrial">
        <h3>发起口径试算</h3>
        <label class="create-field">
          <span>口径编码（留空自动生成）</span>
          <input v-model="trialForm.caliber_code" data-testid="trial-code" placeholder="如 caliber-v2" />
        </label>
        <label class="create-field">
          <span>口径说明</span>
          <input v-model="trialForm.label" data-testid="trial-label" placeholder="如 新口径" />
        </label>
        <fieldset class="create-field">
          <legend>分组值（留空则按窗口现有事件实时聚合）</legend>
          <div v-for="(row, idx) in trialRows" :key="idx" class="trial-row">
            <input v-model="row.dimension_key" placeholder="分组，如 华北" :data-testid="`trial-group-key-${idx}`" />
            <input v-model.number="row.computed_value" type="number" placeholder="聚合值，如 9" :data-testid="`trial-group-value-${idx}`" />
            <input v-model.number="row.event_count" type="number" placeholder="事件数" :data-testid="`trial-group-count-${idx}`" />
            <button class="btn ghost" type="button" @click="trialRows.splice(idx, 1)">删除</button>
          </div>
          <button class="btn" type="button" data-testid="trial-add-row" @click="addRow">新增分组</button>
        </fieldset>
        <label class="create-field">
          <span>模拟试算失败（验证不影响启用口径）</span>
          <input v-model="trialForm.force_fail" type="checkbox" data-testid="trial-force-fail" />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showTrial = false">取消</button>
          <button class="btn primary" type="submit" data-testid="trial-submit">开始试算</button>
        </div>
      </form>
    </div>

    <!-- 原样回查弹窗 -->
    <div v-if="inspect" class="modal-backdrop" data-testid="inspect-modal">
      <div class="modal">
        <h3>口径原样回查：{{ inspect.caliber_code }}</h3>
        <p class="caliber-banner">
          第{{ inspect.version_no }}轮 · {{ inspect.status }}
          <template v-if="inspect.window_start"> · 窗口 {{ inspect.window_start }} ~ {{ inspect.window_end }}</template>
        </p>
        <table class="data-table">
          <thead><tr><th>分组</th><th>聚合值</th><th>事件数</th></tr></thead>
          <tbody>
            <tr v-for="g in inspect.groups" :key="g.dimension_key">
              <td>{{ g.dimension_key }}</td>
              <td>{{ g.computed_value }}</td>
              <td>{{ g.event_count }}</td>
            </tr>
          </tbody>
        </table>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="inspect = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  activateVersion,
  discardCandidate,
  fetchHistorySnapshot,
  fetchSnapshot,
  getVersion,
  listFetchLogs,
  listHistory,
  listVersions,
  revokeActive,
  startTrial,
} from '@/api/caliber-service'
import { listEntries } from '@/api/backend-service'
import type {
  CaliberFetchLog,
  CaliberSnapshot,
  CaliberVersion,
  EntryRow,
} from '@/data/types'

type WindowRow = EntryRow & { window_name: string }

const windows = ref<WindowRow[]>([])
const windowId = ref<number>(0)
const versions = ref<CaliberVersion[]>([])
const history = ref<CaliberVersion[]>([])
const logs = ref<CaliberFetchLog[]>([])
const snapshot = ref<CaliberSnapshot | null>(null)
const errorMessage = ref('')

const showTrial = ref(false)
const trialForm = ref({ caliber_code: '', label: '', force_fail: false })
const trialRows = ref<{ dimension_key: string; computed_value: number | null; event_count: number | null }[]>([])
const inspect = ref<CaliberVersion | null>(null)

function notify(message: string) {
  errorMessage.value = message
}

async function loadWindows() {
  const page = await listEntries('realtime_agg', {})
  windows.value = page.items as WindowRow[]
  if (!windowId.value && windows.value.length) {
    const preferred = windows.value.find((w) => w.status === '已启用') ?? windows.value[0]
    windowId.value = Number(preferred.id)
  }
}

async function reloadAll() {
  if (!windowId.value) return
  errorMessage.value = ''
  try {
    const [vs, hs, ls, snap] = await Promise.all([
      listVersions(windowId.value),
      listHistory(windowId.value),
      listFetchLogs(windowId.value),
      fetchSnapshot(windowId.value),
    ])
    versions.value = vs
    history.value = hs
    logs.value = ls
    snapshot.value = snap
  } catch (error) {
    notify(error instanceof Error ? error.message : '口径数据读取失败')
  }
}

function openTrial() {
  trialForm.value = { caliber_code: '', label: '', force_fail: false }
  trialRows.value = [
    { dimension_key: '华北', computed_value: null, event_count: null },
    { dimension_key: '华南', computed_value: null, event_count: null },
  ]
  inspect.value = null
  showTrial.value = true
}

function addRow() {
  trialRows.value.push({ dimension_key: '', computed_value: null, event_count: null })
}

async function submitTrial() {
  errorMessage.value = ''
  const filled = trialRows.value.filter(
    (r) => r.dimension_key.trim() && r.computed_value !== null,
  )
  try {
    const result = await startTrial(windowId.value, {
      caliber_code: trialForm.value.caliber_code || undefined,
      label: trialForm.value.label || undefined,
      force_fail: trialForm.value.force_fail,
      groups: filled.length
        ? filled.map((r) => ({
            dimension_key: r.dimension_key.trim(),
            computed_value: Number(r.computed_value),
            event_count: Number(r.event_count ?? 0),
          }))
        : undefined,
    })
    showTrial.value = false
    notify(result.message)
    await reloadAll()
  } catch (error) {
    notify(error instanceof Error ? error.message : '试算失败')
  }
}

async function doActivate(versionId: number) {
  errorMessage.value = ''
  try {
    const result = await activateVersion(windowId.value, versionId)
    notify(result.message)
    await reloadAll()
  } catch (error) {
    notify(error instanceof Error ? error.message : '启用失败')
  }
}

async function doDiscard() {
  errorMessage.value = ''
  try {
    const result = await discardCandidate(windowId.value)
    notify(result.message)
    await reloadAll()
  } catch (error) {
    notify(error instanceof Error ? error.message : '废弃失败')
  }
}

async function doRevoke() {
  errorMessage.value = ''
  try {
    const result = await revokeActive(windowId.value)
    notify(result.message)
    await reloadAll()
  } catch (error) {
    notify(error instanceof Error ? error.message : '撤销失败')
  }
}

async function doFetch() {
  errorMessage.value = ''
  try {
    snapshot.value = await fetchSnapshot(windowId.value, 'duty-report')
    notify(snapshot.value.message)
    await Promise.all([
      listFetchLogs(windowId.value).then((l) => (logs.value = l)),
      listVersions(windowId.value).then((v) => (versions.value = v)),
    ])
  } catch (error) {
    notify(error instanceof Error ? error.message : '取数失败')
  }
}

async function inspectVersion(versionId: number) {
  errorMessage.value = ''
  try {
    inspect.value = await getVersion(versionId)
  } catch (error) {
    notify(error instanceof Error ? error.message : '回查失败')
  }
}

async function viewHistory(versionId: number) {
  errorMessage.value = ''
  try {
    const snap = await fetchHistorySnapshot(windowId.value, versionId)
    inspect.value = {
      id: snap.version_id as number,
      window_id: snap.window_id,
      version_no: snap.version_no as number,
      caliber_code: snap.caliber_code as string,
      label: snap.caliber_label ?? '',
      status: snap.version_status as CaliberVersion['status'],
      group_count: snap.groups.length,
      total_value: snap.total_value ?? 0,
      created_at: '',
      window_start: snap.window_start,
      window_end: snap.window_end,
      groups: snap.groups,
    }
  } catch (error) {
    notify(error instanceof Error ? error.message : '历史回查失败')
  }
}

async function traceLog(versionId: number) {
  await inspectVersion(versionId)
}

onMounted(async () => {
  try {
    await loadWindows()
    await reloadAll()
  } catch (error) {
    notify(error instanceof Error ? error.message : '初始化失败')
  }
})
</script>

<style scoped>
.caliber-panel {
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.caliber-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
  flex-wrap: wrap;
}
.caliber-toolbar {
  display: flex;
  align-items: flex-end;
  gap: 10px;
}
.snapshot-card { padding: 14px; }
.snapshot-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}
.caliber-banner {
  background: #eef4ff;
  border: 1px solid #c7dbff;
  border-radius: 6px;
  padding: 8px 10px;
  margin: 0 0 10px;
  font-size: 13px;
}
.caliber-columns {
  display: grid;
  grid-template-columns: 1.2fr 1fr;
  gap: 14px;
}
.caliber-columns .stat-card { padding: 12px; }
.caliber-tag {
  display: inline-block;
  background: #f1f5f9;
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 1px 8px;
  font-size: 12px;
}
.status-pill {
  display: inline-block;
  border-radius: 999px;
  padding: 1px 8px;
  font-size: 12px;
  background: #f1f5f9;
}
.status-pill[data-status='已启用'] { background: #dcfce7; color: #166534; }
.status-pill[data-status='候选'] { background: #fef9c3; color: #854d0e; }
.status-pill[data-status='已确认'] { background: #e0e7ff; color: #3730a3; }
.status-pill[data-status='已撤销'] { background: #fee2e2; color: #991b1b; }
.status-pill[data-status='试算失败'] { background: #fee2e2; color: #991b1b; }
.status-pill[data-status='试算中'] { background: #f1f5f9; }
.trial-row {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr auto;
  gap: 8px;
  margin-bottom: 8px;
}
.create-field fieldset,
.create-field { width: 100%; }
@media (max-width: 980px) {
  .caliber-columns { grid-template-columns: 1fr; }
}
</style>
