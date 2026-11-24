<template>
  <section class="page" data-module="funnel_analyzer">
    <header class="page-head">
      <div>
        <h2>漏斗分析器</h2>
        <p class="page-desc">
          按事件实际发生时间匹配转化机会：一笔付款只归一次浏览，必须晚于浏览且不超窗口；路径明细可回查原始记录。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="funnel-back" type="button" @click="router.push('/')">返回概览</button>
        <button class="btn primary" data-testid="funnel-recompute" type="button" :disabled="computing" @click="recompute">
          {{ computing ? '计算中…' : '重新计算' }}
        </button>
      </div>
    </header>

    <p v-if="message" class="error-text" data-testid="funnel-message">{{ message }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">当前版本</span>
        <strong class="stat-value" data-testid="funnel-run-no">{{ currentRun?.run_no ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">进入人数</span>
        <strong class="stat-value">{{ currentRun?.entry_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">转化人数</span>
        <strong class="stat-value" data-testid="funnel-converted">{{ currentRun?.converted_subjects ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">转化率</span>
        <strong class="stat-value">{{ conversionRateText }}</strong>
      </article>
    </div>

    <div class="filter-bar">
      <label class="filter-item">
        <span>漏斗</span>
        <select v-model.number="selectedFunnelId" data-testid="funnel-select" @change="loadFunnel">
          <option v-for="funnel in funnels" :key="funnel.id" :value="funnel.id">
            {{ funnel.funnel_name }}
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>运算版本</span>
        <select v-model.number="selectedRunId" data-testid="run-select" @change="loadRun">
          <option v-for="run in runs" :key="run.id" :value="run.id">
            第 {{ run.run_no }} 版 · {{ run.computed_at }}
          </option>
        </select>
      </label>
      <span class="version-hint" data-testid="version-hint">
        旧版本路径按 run_id 固定，迟到记录只进新版本
      </span>
    </div>

    <table class="data-table" data-testid="step-table">
      <thead>
        <tr>
          <th>步骤</th>
          <th>进入</th>
          <th>离开</th>
          <th>转化率</th>
          <th>流失解释</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="step in steps" :key="step.id" :data-testid="`step-${step.step_index}`">
          <td>{{ step.step_name }}</td>
          <td>{{ step.entry_count }}</td>
          <td>{{ step.exit_count }}</td>
          <td>{{ (step.conversion_rate * 100).toFixed(2) }}%</td>
          <td>
            <span v-if="!step.drop_reasons?.count">—</span>
            <span v-else data-testid="drop-breakdown">
              {{ step.drop_reasons.count }} 人：
              <span v-for="(count, reason) in step.drop_reasons.breakdown" :key="reason">
                {{ reason }} {{ count }}；
              </span>
              <small>样例 {{ (step.drop_reasons.sample_subjects || []).join('、') }}</small>
            </span>
          </td>
        </tr>
        <tr v-if="!steps.length">
          <td colspan="5" class="empty-state">该版本没有步骤结果</td>
        </tr>
      </tbody>
    </table>

    <div class="tabs">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="tab"
        :class="{ active: activeTab === tab.key }"
        :data-testid="`tab-${tab.key}`"
        type="button"
        @click="activeTab = tab.key"
      >
        {{ tab.label }}
      </button>
    </div>

    <!-- 转化明细 -->
    <div v-if="activeTab === 'converted'" data-testid="converted-panel">
      <p class="status-legend">
        归因分布：
        <span v-for="(count, campaign) in attributionSummary" :key="campaign" class="legend-item">
          {{ campaign }}：{{ count }}
        </span>
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>用户</th>
            <th>有效浏览机会</th>
            <th>浏览来源</th>
            <th>付款时刻</th>
            <th>候选浏览</th>
            <th>明细</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="path in converted" :key="path.id" :data-testid="`converted-${path.subject_key}`">
            <td>{{ path.subject_key }}</td>
            <td>{{ path.opportunity_at }}</td>
            <td>{{ path.opportunity_campaign || '未标注' }}</td>
            <td>{{ path.conversion_at }}</td>
            <td>
              <span v-for="cand in path.candidate_opportunities" :key="cand.event_id" class="candidate">
                {{ cand.occurred_at }} {{ cand.campaign || '未标注' }}
                <strong v-if="cand.selected">（已采用）</strong>
              </span>
            </td>
            <td>
              <button class="link" type="button" :data-testid="`drill-${path.id}`" @click="openDetail(path.id)">
                下钻
              </button>
            </td>
          </tr>
          <tr v-if="!converted.length">
            <td colspan="6" class="empty-state">该版本没有已转化路径</td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 未转化明细 -->
    <div v-else-if="activeTab === 'unconverted'" data-testid="unconverted-panel">
      <p class="status-legend">
        <span v-for="(count, reason) in unconvertedByReason" :key="reason" class="legend-item">
          {{ reason }}：{{ count }}
        </span>
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>用户</th>
            <th>结论</th>
            <th>说明</th>
            <th>浏览机会</th>
            <th>付款/尝试</th>
            <th>下一步</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="path in unconverted" :key="path.id" :data-testid="`unconverted-${path.subject_key}`">
            <td>{{ path.subject_key }}</td>
            <td>{{ path.failure_label }}</td>
            <td>{{ path.failure_reason }}</td>
            <td>{{ path.opportunity_at || '无' }}</td>
            <td>{{ path.attempt_at || '—' }}</td>
            <td>{{ path.next_action }}</td>
          </tr>
          <tr v-if="!unconverted.length">
            <td colspan="6" class="empty-state">该版本没有未转化路径</td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 原始事件时间线 -->
    <div v-else data-testid="timeline-panel">
      <div class="filter-bar">
        <label class="filter-item">
          <span>用户</span>
          <input v-model="timelineSubject" data-testid="timeline-subject" placeholder="输入 user_id" />
        </label>
        <button class="btn" data-testid="timeline-query" type="button" @click="loadTimeline">查时间线</button>
      </div>
      <table class="data-table">
        <thead>
          <tr><th>事件</th><th>发生时刻</th><th>入库时刻</th><th>活动</th><th>来源</th></tr>
        </thead>
        <tbody>
          <tr v-for="evt in timeline" :key="evt.id" :data-testid="`timeline-${evt.id}`">
            <td>{{ evt.event_name }}</td>
            <td>{{ evt.occurred_at }}</td>
            <td>{{ evt.ingested_at }}</td>
            <td>{{ evt.attrs?.campaign || '—' }}</td>
            <td>{{ evt.attrs?.source || '—' }}</td>
          </tr>
          <tr v-if="!timeline.length"><td colspan="5" class="empty-state">输入用户后查询</td></tr>
        </tbody>
      </table>
    </div>

    <!-- 下钻详情 -->
    <div v-if="detail" class="modal-backdrop" data-testid="path-detail">
      <div class="modal detail-modal">
        <h3>路径 {{ detail.path.id }} · {{ detail.path.subject_key }}</h3>
        <dl class="detail-grid">
          <div><dt>结论</dt><dd>{{ detail.path.outcome }}<span v-if="detail.failure_label"> · {{ detail.failure_label }}</span></dd></div>
          <div><dt>所属版本</dt><dd>第 {{ detail.run?.run_no }} 版（run_id={{ detail.run?.id }}）</dd></div>
          <div><dt>有效浏览</dt><dd>{{ detail.path.opportunity_at || '无' }} · {{ detail.path.opportunity_campaign || '未标注' }}</dd></div>
          <div><dt>付款</dt><dd>{{ detail.path.conversion_at || detail.path.attempt_at || '—' }}</dd></div>
          <div><dt>窗口</dt><dd>{{ detail.path.window_seconds }} 秒</dd></div>
          <div><dt>说明</dt><dd>{{ detail.path.failure_reason || '已转化，付款归给最近一次有效浏览' }}</dd></div>
        </dl>

        <h4>本次运算保存的路径明细（event_id → 原始记录）</h4>
        <table class="data-table">
          <thead><tr><th>步骤</th><th>event_id</th><th>发生时刻</th><th>入库时刻</th><th>活动</th></tr></thead>
          <tbody>
            <tr v-for="step in detail.steps" :key="step.event_id" :data-testid="`detail-step-${step.event_id}`">
              <td>{{ step.step_name }}</td>
              <td>{{ step.event_id }}</td>
              <td>{{ step.occurred_at }}</td>
              <td>{{ step.ingested_at }}</td>
              <td>{{ step.campaign || '—' }}</td>
            </tr>
          </tbody>
        </table>

        <h4>候选浏览归因（各活动独立核对，不互相复制）</h4>
        <table class="data-table">
          <thead><tr><th>event_id</th><th>发生时刻</th><th>活动</th><th>来源</th><th>是否采用</th></tr></thead>
          <tbody>
            <tr v-for="cand in detail.candidate_opportunities" :key="cand.event_id">
              <td>{{ cand.event_id }}</td>
              <td>{{ cand.occurred_at }}</td>
              <td>{{ cand.campaign || '—' }}</td>
              <td>{{ cand.source || '—' }}</td>
              <td>{{ cand.selected ? '是' : '否' }}</td>
            </tr>
          </tbody>
        </table>

        <h4>原始记录</h4>
        <pre class="raw-records">{{ rawRecordText }}</pre>

        <div class="page-actions">
          <button class="btn" type="button" data-testid="close-detail" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'

import { listEntries } from '@/api/backend-service'
import {
  computeFunnel,
  fetchConversions,
  fetchPathDetail,
  fetchResults,
  fetchSubjectTimeline,
  fetchUnconverted,
  listRuns,
} from '@/api/funnel-service'
import type { FunnelPath, FunnelRun, PathDetail, StepResult } from '@/api/funnel-service'

const router = useRouter()
const funnels = ref<{ id: number; funnel_name: string }[]>([])
const selectedFunnelId = ref<number>(0)
const runs = ref<FunnelRun[]>([])
const selectedRunId = ref<number>(0)
const currentRun = ref<FunnelRun | null>(null)
const steps = ref<StepResult[]>([])
const converted = ref<FunnelPath[]>([])
const unconverted = ref<FunnelPath[]>([])
const attributionSummary = ref<Record<string, number>>({})
const unconvertedByReason = ref<Record<string, number>>({})
const detail = ref<PathDetail | null>(null)
const timeline = ref<{ id: number; event_name: string; occurred_at: string; ingested_at: string; attrs: Record<string, unknown> }[]>([])
const timelineSubject = ref('')
const activeTab = ref('converted')
const computing = ref(false)
const message = ref('')

const tabs = [
  { key: 'converted', label: '转化明细' },
  { key: 'unconverted', label: '未转化解释' },
  { key: 'timeline', label: '原始事件时间线' },
]

const conversionRateText = computed(() => {
  const run = currentRun.value
  if (!run) return '—'
  const entry = run.entry_count || 0
  return `${((run.converted_subjects || 0) / Math.max(entry, 1) * 100).toFixed(2)}%`
})

const rawRecordText = computed(() => {
  if (!detail.value) return ''
  return JSON.stringify(detail.value.raw_records, null, 2)
})

async function loadFunnels() {
  const payload = await listEntries('funnel_analyzer')
  funnels.value = payload.items.map((item) => ({
    id: Number(item.id),
    funnel_name: String(item.funnel_name ?? item['漏斗名称'] ?? `漏斗 ${item.id}`),
  }))
  if (funnels.value.length && !selectedFunnelId.value) {
    selectedFunnelId.value = funnels.value[0].id
  }
  await loadFunnel()
}

async function loadFunnel() {
  if (!selectedFunnelId.value) return
  const payload = await listRuns(selectedFunnelId.value)
  runs.value = payload.items
  selectedRunId.value = runs.value[0]?.id ?? 0
  await loadRun()
}

async function loadRun() {
  if (!selectedFunnelId.value || !selectedRunId.value) return
  const results = await fetchResults(selectedFunnelId.value, selectedRunId.value)
  currentRun.value = results.run
  steps.value = results.steps_detail
  const conv = await fetchConversions(selectedFunnelId.value, selectedRunId.value)
  converted.value = conv.converted
  attributionSummary.value = conv.attribution_summary
  const unconv = await fetchUnconverted(selectedFunnelId.value, selectedRunId.value)
  unconverted.value = unconv.unconverted
  unconvertedByReason.value = unconv.by_reason
}

async function recompute() {
  if (!selectedFunnelId.value) return
  computing.value = true
  message.value = ''
  try {
    const result = await computeFunnel(selectedFunnelId.value)
    if (!result.ok) throw new Error(result.message || '计算失败')
    await loadFunnel()
    message.value = `已生成第 ${result.run_no} 版；旧版本路径仍可回查`
  } catch (error) {
    message.value = error instanceof Error ? error.message : '重新计算失败'
  } finally {
    computing.value = false
  }
}

async function openDetail(pathId: number) {
  if (!selectedFunnelId.value) return
  detail.value = await fetchPathDetail(selectedFunnelId.value, pathId, selectedRunId.value)
}

async function loadTimeline() {
  if (!selectedFunnelId.value || !selectedRunId.value || !timelineSubject.value) return
  const result = await fetchSubjectTimeline(
    selectedFunnelId.value, timelineSubject.value, selectedRunId.value)
  timeline.value = result.events
}

onMounted(async () => {
  try {
    await loadFunnels()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '漏斗数据读取失败'
  }
})
</script>

<style scoped>
.tabs { display: flex; gap: 6px; margin: 14px 0 10px; }
.tab { border: 1px solid var(--border); background: #fff; border-radius: 6px; padding: 6px 12px; cursor: pointer; }
.tab.active { background: var(--brand); border-color: var(--brand); color: #fff; }
.version-hint { font-size: 12px; color: var(--muted); align-self: center; }
.candidate { display: block; font-size: 12px; }
.detail-modal { width: min(860px, 100%); }
.detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 16px; margin: 10px 0; }
.detail-grid dt { font-size: 12px; color: var(--muted); }
.detail-grid dd { margin: 0; font-size: 13px; }
.raw-records { background: #0f172a; color: #e2e8f0; border-radius: 6px; padding: 10px; font-size: 12px; max-height: 200px; overflow: auto; }
</style>
