"""数据分析管道路由：事件采集、聚合执行、指标快照、分群计算等核心操作入口。"""
from __future__ import annotations

from fastapi import APIRouter

from app.database import db
from app.pipeline import (
    check_alert, compute_aggregation, compute_cohort_comparison,
    compute_forecast, compute_funnel, compute_metric_snapshot,
    compute_retention, compute_segment, generate_report,
    ingest_events, precompute_cube, seed_real_data,
)
from app.services import service_for

router = APIRouter(prefix="/api/pipeline", tags=["分析管线"])


@router.post("/seed")
def api_seed_real_data() -> dict:
    """初始化全部种子数据：事件、指标、聚合窗口、分群、漏斗等。"""
    counts = seed_real_data()
    return {"ok": True, "counts": counts}


@router.post("/events/ingest")
def api_ingest_events() -> dict:
    """对所有已启用的事件定义批量摄入模拟事件。"""
    svc = service_for("event_tracker")
    result = svc.ingest_all_enabled()
    return result


@router.post("/aggregation/run-all")
def api_run_all_aggregations() -> dict:
    """对所有已启用聚合窗口执行计算。"""
    svc = service_for("realtime_agg")
    result = svc.run_all_active()
    return result


@router.post("/aggregation/{window_id}/run")
def api_run_aggregation(window_id: int) -> dict:
    """对指定聚合窗口执行单次计算。"""
    svc = service_for("realtime_agg")
    return svc.run_aggregation(window_id)


@router.post("/metrics/compute-all")
def api_compute_all_metrics() -> dict:
    """对所有指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_all()


@router.post("/metrics/{metric_id}/snapshot")
def api_compute_snapshot(metric_id: int) -> dict:
    """对指定指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_snapshot(metric_id)


@router.post("/segments/{segment_id}/compute")
def api_compute_segment(segment_id: int) -> dict:
    """对指定分群执行计算。"""
    svc = service_for("segment_engine")
    return svc.run_computation(segment_id)


@router.post("/funnels/{funnel_id}/compute")
def api_compute_funnel(
    funnel_id: int,
    as_of: str | None = None,
    lookback_hours: float | None = None,
) -> dict:
    """对指定漏斗执行分析计算，生成新的不可变版本。"""
    svc = service_for("funnel_analyzer")
    return svc.run_computation(funnel_id, as_of=as_of, lookback_hours=lookback_hours)


@router.get("/funnels/{funnel_id}/runs")
def api_funnel_runs(funnel_id: int) -> dict:
    """列出漏斗的所有运算版本，旧版本保留可回查。"""
    svc = service_for("funnel_analyzer")
    runs = svc.list_runs(funnel_id)
    return {"ok": True, "funnel_id": funnel_id, "items": runs, "total": len(runs)}


@router.get("/funnels/{funnel_id}/results")
def api_funnel_results(funnel_id: int, run_id: int | None = None) -> dict:
    """读取某次运算的步骤明细；不传 run_id 取最新版本。"""
    svc = service_for("funnel_analyzer")
    run = svc.resolve_run(funnel_id, run_id)
    if run is None:
        return {"ok": False, "message": f"漏斗 {funnel_id} 没有可用的运算版本"}
    return {
        "ok": True,
        "run": svc._public_run(run),
        "steps_detail": svc.funnel_results(funnel_id, run["id"]),
    }


@router.get("/funnels/{funnel_id}/paths")
def api_funnel_paths(
    funnel_id: int,
    run_id: int | None = None,
    outcome: str | None = None,
    subject_key: str | None = None,
    step_index: int | None = None,
    page: int = 1,
    size: int = 50,
) -> dict:
    """按运算版本列出路径明细。"""
    svc = service_for("funnel_analyzer")
    return {"ok": True, **svc.list_paths(
        funnel_id, run_id=run_id, outcome=outcome, subject_key=subject_key,
        step_index=step_index, page=page, size=size,
    )}


@router.get("/funnels/{funnel_id}/paths/{path_id}")
def api_funnel_path_detail(funnel_id: int, path_id: int, run_id: int | None = None) -> dict:
    """下钻单条路径，关联 events 原始记录核对事件时刻。"""
    svc = service_for("funnel_analyzer")
    detail = svc.path_detail(funnel_id, path_id, run_id=run_id)
    if detail is None:
        return {"ok": False, "message": f"路径 {path_id} 不属于漏斗 {funnel_id}"}
    return {"ok": True, **detail}


@router.get("/funnels/{funnel_id}/conversions")
def api_funnel_conversions(funnel_id: int, run_id: int | None = None) -> dict:
    """转化明细：已转化路径及每条路径的浏览机会归因。"""
    svc = service_for("funnel_analyzer")
    return {"ok": True, **svc.conversion_detail(funnel_id, run_id=run_id)}


@router.get("/funnels/{funnel_id}/unconverted")
def api_funnel_unconverted(
    funnel_id: int, run_id: int | None = None, subject_key: str | None = None,
) -> dict:
    """未转化明细：解释少了一步还是顺序对不上。"""
    svc = service_for("funnel_analyzer")
    return {"ok": True, **svc.unconverted_detail(
        funnel_id, run_id=run_id, subject_key=subject_key)}


@router.get("/funnels/{funnel_id}/subjects/{subject_key}/timeline")
def api_funnel_subject_timeline(
    funnel_id: int, subject_key: str, run_id: int | None = None,
) -> dict:
    """某主体的原始事件时间线与本次运算的归属结果。"""
    svc = service_for("funnel_analyzer")
    return {"ok": True, **svc.subject_timeline(funnel_id, subject_key, run_id=run_id)}


@router.get("/funnels/{funnel_id}/drops")
def api_funnel_drops(funnel_id: int, run_id: int | None = None) -> dict:
    """某版本各步骤流失原因分析。"""
    svc = service_for("funnel_analyzer")
    return {"ok": True, **svc.drop_analysis(funnel_id, run_id=run_id)}


@router.post("/retention/{task_id}/compute")
def api_compute_retention(task_id: int) -> dict:
    """对指定留存任务执行计算。"""
    svc = service_for("retention_analysis")
    return svc.run_computation(task_id)


@router.post("/forecast/{task_id}/compute")
def api_compute_forecast(task_id: int) -> dict:
    """对指定预测任务执行计算。"""
    svc = service_for("forecast_model")
    return svc.run_forecast(task_id)


@router.post("/comparison/{comparison_id}/compute")
def api_compute_comparison(comparison_id: int) -> dict:
    """对指定分群对比执行计算。"""
    svc = service_for("cohort_compare")
    return svc.run_comparison(comparison_id)


@router.post("/cube/{cube_id}/precompute")
def api_precompute_cube(cube_id: int) -> dict:
    """对指定Cube执行预计算。"""
    svc = service_for("cube_builder")
    return svc.run_precomputation(cube_id)


@router.post("/report/{report_id}/generate")
def api_generate_report(report_id: int) -> dict:
    """对指定报告模板执行生成。"""
    svc = service_for("data_report")
    return svc.generate(report_id)


@router.post("/alerts/check-all")
def api_check_all_alerts() -> dict:
    """检查所有已生效告警规则。"""
    svc = service_for("alert_policy")
    return svc.check_all()


@router.get("/analytics/full-pipeline")
def api_full_pipeline() -> dict:
    """执行完整分析链路：事件采集→聚合→指标→分群→漏斗→留存→预测→报告。"""
    steps = {}

    # 1. 事件采集
    svc_events = service_for("event_tracker")
    steps["events"] = svc_events.ingest_all_enabled()

    # 2. 聚合
    svc_agg = service_for("realtime_agg")
    steps["aggregation"] = svc_agg.run_all_active()

    # 3. 指标快照
    svc_metrics = service_for("metric_store")
    steps["metrics"] = svc_metrics.compute_all()

    # 4. 分群
    segments = db.fetchall("SELECT id FROM segments LIMIT 6")
    seg_results = []
    for s in segments:
        r = service_for("segment_engine").run_computation(s["id"])
        seg_results.append(r)
    steps["segments"] = {"processed": len(segments), "results": seg_results}

    # 5. 漏斗
    funnels = db.fetchall("SELECT id FROM funnels LIMIT 3")
    funnel_results = []
    for f in funnels:
        r = service_for("funnel_analyzer").run_computation(f["id"])
        funnel_results.append(r)
    steps["funnels"] = {"processed": len(funnels), "results": funnel_results}

    # 6. 留存
    ret_tasks = db.fetchall("SELECT id FROM retention_tasks LIMIT 3")
    ret_results = []
    for t in ret_tasks:
        r = service_for("retention_analysis").run_computation(t["id"])
        ret_results.append(r)
    steps["retention"] = {"processed": len(ret_tasks), "results": ret_results}

    # 7. 预测
    forecast_tasks = db.fetchall("SELECT id FROM forecast_tasks LIMIT 2")
    fc_results = []
    for t in forecast_tasks:
        r = service_for("forecast_model").run_forecast(t["id"])
        fc_results.append(r)
    steps["forecast"] = {"processed": len(forecast_tasks), "results": fc_results}

    # 8. 报告
    reports = db.fetchall("SELECT id FROM reports LIMIT 2")
    report_results = []
    for rep in reports:
        r = service_for("data_report").generate(rep["id"])
        report_results.append(r)
    steps["reports"] = {"processed": len(reports), "results": report_results}

    return {"ok": True, "pipeline": steps}