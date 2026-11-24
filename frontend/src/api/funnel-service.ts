import { request } from './client'

export type FunnelRun = {
  id: number
  funnel_id: number
  run_no: number
  conversion_window_hours: number
  steps_snapshot: string[]
  status: string
  entry_count: number
  converted_count: number
  converted_subjects: number
  parameters: Record<string, unknown>
  computed_at: string
}

export type StepEvent = {
  step_index: number
  step_name: string
  event_id: number
  event_name: string
  occurred_at: string
  ingested_at: string
  campaign: string | null
  source: string | null
  event?: RawEvent | null
}

export type RawEvent = {
  id: number
  event_name: string
  event_attrs: string
  occurred_at: string
  ingested_at: string
  attrs: Record<string, unknown>
}

export type CandidateOpportunity = {
  event_id: number
  occurred_at: string
  campaign: string | null
  source: string | null
  selected: boolean
  event?: RawEvent | null
}

export type FunnelPath = {
  id: number
  run_id: number
  funnel_id: number
  subject_key: string
  outcome: string
  reached_step: number
  step_count: number
  opportunity_event_id: number | null
  opportunity_at: string | null
  attempt_event_id: number | null
  attempt_at: string | null
  conversion_event_id: number | null
  conversion_at: string | null
  window_seconds: number
  failure_code: string | null
  failure_reason: string | null
  failure_label: string | null
  missing_step_index: number | null
  missing_step_name: string | null
  opportunity_campaign: string | null
  opportunity_source: string | null
  candidate_opportunities: CandidateOpportunity[]
  attribution_campaign: string | null
  attribution_source: string | null
  attribution_events: StepEvent[]
  step_events: Record<string, StepEvent>
  next_action?: string
  all_opportunity_paths?: BriefPath[]
}

export type BriefPath = {
  path_id: number
  outcome: string
  opportunity_event_id: number | null
  opportunity_at: string | null
  opportunity_campaign: string | null
  attempt_event_id: number | null
  attempt_at: string | null
  failure_code: string | null
  failure_label: string | null
}

export type PathDetail = {
  path: FunnelPath
  run: FunnelRun | null
  failure_label: string | null
  steps: StepEvent[]
  attribution: StepEvent[]
  candidate_opportunities: CandidateOpportunity[]
  raw_records: { role: string; event: RawEvent }[]
}

export type StepResult = {
  id: number
  run_id: number
  step_index: number
  step_name: string
  entry_count: number
  exit_count: number
  conversion_rate: number
  drop_reasons: {
    count?: number
    breakdown?: Record<string, number>
    sample_subjects?: string[]
  }
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}，数据未更新`)
  return (await response.json()) as T
}

export async function computeFunnel(funnelId: number): Promise<{ ok: boolean; message?: string; run_no?: number }> {
  const response = await request(`/api/pipeline/funnels/${funnelId}/compute`, { method: 'POST' })
  return (await response.json()) as { ok: boolean; message?: string; run_no?: number }
}

export function listRuns(funnelId: number): Promise<{ items: FunnelRun[]; total: number }> {
  return getJson(`/api/pipeline/funnels/${funnelId}/runs`)
}

export function fetchResults(
  funnelId: number,
  runId?: number,
): Promise<{ run: FunnelRun; steps_detail: StepResult[] }> {
  const suffix = runId ? `?run_id=${runId}` : ''
  return getJson(`/api/pipeline/funnels/${funnelId}/results${suffix}`)
}

export function fetchPaths(
  funnelId: number,
  runId: number,
  outcome?: string,
  subjectKey?: string,
): Promise<{ items: FunnelPath[]; total: number; run: FunnelRun }> {
  const query = new URLSearchParams({ run_id: String(runId) })
  if (outcome) query.set('outcome', outcome)
  if (subjectKey) query.set('subject_key', subjectKey)
  return getJson(`/api/pipeline/funnels/${funnelId}/paths?${query.toString()}`)
}

export function fetchPathDetail(
  funnelId: number,
  pathId: number,
  runId?: number,
): Promise<PathDetail> {
  const suffix = runId ? `?run_id=${runId}` : ''
  return getJson(`/api/pipeline/funnels/${funnelId}/paths/${pathId}${suffix}`)
}

export function fetchUnconverted(
  funnelId: number,
  runId: number,
): Promise<{ unconverted: FunnelPath[]; by_reason: Record<string, number>; run: FunnelRun }> {
  return getJson(`/api/pipeline/funnels/${funnelId}/unconverted?run_id=${runId}`)
}

export function fetchConversions(
  funnelId: number,
  runId: number,
): Promise<{ converted: FunnelPath[]; attribution_summary: Record<string, number>; run: FunnelRun }> {
  return getJson(`/api/pipeline/funnels/${funnelId}/conversions?run_id=${runId}`)
}

export function fetchSubjectTimeline(
  funnelId: number,
  subjectKey: string,
  runId: number,
): Promise<{ events: RawEvent[]; paths: FunnelPath[] }> {
  return getJson(
    `/api/pipeline/funnels/${funnelId}/subjects/${encodeURIComponent(subjectKey)}/timeline?run_id=${runId}`,
  )
}
