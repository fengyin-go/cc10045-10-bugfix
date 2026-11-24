"""漏斗分析器领域服务：定义多步骤行为漏斗并计算各步转化率。

真实数据流：漏斗步骤序列 → pipeline.compute_funnel_run 按事件发生时间匹配转化机会
→ 每次运算生成不可变 run 与路径明细 funnel_paths → 列表/下钻/归因/流失解析都读路径明细，
event_id 指回 events 原始记录，可核对真实事件时刻与归属。
"""
from __future__ import annotations

import json
from typing import Any

from app.database import db
from app.pipeline import FUNNEL_FAILURE_LABELS, compute_funnel_run
from app.services.base import BaseModuleService


class FunnelAnalyzerService(BaseModuleService):
    MODULE = "funnel_analyzer"
    ENTITY = "漏斗定义"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["漏斗名称", "步骤序列", "转化窗口小时", "分群条件", "最近转化率"]
    REQUIRED_FIELDS = ["漏斗名称", "步骤序列", "转化窗口小时"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用漏斗": "已启用", "停用漏斗": "已停用"}
    NEGATIVE_ACTIONS = []
    COLUMN_OVERRIDES = {"分群条件": "segment_condition"}

    # ================================================================
    # 计算
    # ================================================================

    def run_computation(
        self,
        funnel_id: int,
        *,
        as_of: str | None = None,
        lookback_hours: float | None = None,
    ) -> dict[str, Any]:
        """执行一次漏斗运算，生成新的不可变版本。"""
        funnel = self._load_funnel(funnel_id)
        if funnel is None:
            return {"ok": False, "message": f"漏斗 {funnel_id} 不存在"}

        computed = compute_funnel_run(funnel, as_of=as_of, lookback_hours=lookback_hours)
        if not computed.get("ok"):
            return {"ok": False, "message": f"漏斗 {funnel_id} 没有可计算的步骤序列"}

        steps = computed["steps_detail"]
        overall_rate = computed["overall_conversion_rate"]
        self._record_latest_rate(funnel_id, overall_rate)

        return {
            "ok": True,
            "funnel_id": funnel_id,
            "funnel_name": funnel["funnel_name"],
            "run_id": computed["run_id"],
            "run_no": computed["run_no"],
            "steps": len(steps),
            "entry_count": computed["entry_count"],
            "converted_count": computed["converted_count"],
            "converted_paths": computed.get("converted_paths", 0),
            "converted_subjects": computed["converted_subjects"],
            "overall_conversion_rate": overall_rate,
            "window_seconds": computed["window_seconds"],
            "parameters": computed["parameters"],
            "steps_detail": steps,
        }

    def _load_funnel(self, funnel_id: int) -> dict[str, Any] | None:
        return db.fetchone("SELECT * FROM funnels WHERE id=?", [funnel_id])

    def _record_latest_rate(self, funnel_id: int, rate: float) -> None:
        # funnels 表没有独立转化率列，列表读取时由 list_entries 用最新 run 覆盖「最近转化率」。
        return None

    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        """漏斗列表：把「最近转化率」替换为最新运算版本的真实值。"""
        rows, total = super().list_entries(**kwargs)
        for row in rows:
            latest = db.fetchone(
                "SELECT converted_subjects, entry_count FROM funnel_runs "
                "WHERE funnel_id=? ORDER BY run_no DESC LIMIT 1",
                [row["id"]],
            )
            if latest is None:
                row["最近转化率"] = "未计算"
                continue
            entry_count = int(latest["entry_count"] or 0)
            rate = round(int(latest["converted_subjects"] or 0) / max(entry_count, 1), 4)
            row["最近转化率"] = f"{rate:.2%}"
        return rows, total

    # ================================================================
    # 版本与结果
    # ================================================================

    def list_runs(self, funnel_id: int) -> list[dict[str, Any]]:
        """列出某漏斗的所有运算版本，含已经发出的旧版本。"""
        rows = db.fetchall(
            "SELECT * FROM funnel_runs WHERE funnel_id=? ORDER BY run_no DESC", [funnel_id]
        )
        for row in rows:
            row["parameters"] = _safe_json(row.get("parameters"), {})
            row["steps_snapshot"] = _safe_json(row.get("steps_snapshot"), [])
        return rows

    def latest_run(self, funnel_id: int) -> dict[str, Any] | None:
        row = db.fetchone(
            "SELECT * FROM funnel_runs WHERE funnel_id=? ORDER BY run_no DESC LIMIT 1", [funnel_id]
        )
        if row:
            row["parameters"] = _safe_json(row.get("parameters"), {})
        return row

    def resolve_run(self, funnel_id: int, run_id: int | None = None) -> dict[str, Any] | None:
        """按 run_id 取版本；不传则取最新版本。保证旧报告能把 run_id 钉在原版本上。"""
        if run_id is not None:
            return db.fetchone(
                "SELECT * FROM funnel_runs WHERE id=? AND funnel_id=?", [run_id, funnel_id]
            )
        return self.latest_run(funnel_id)

    def funnel_results(self, funnel_id: int, run_id: int | None = None) -> list[dict[str, Any]]:
        """获取某次运算的步骤结果；不传 run_id 取最新版本。"""
        run = self.resolve_run(funnel_id, run_id)
        rows = db.fetchall(
            "SELECT * FROM funnel_results WHERE funnel_id=? AND run_id=? ORDER BY step_index ASC",
            [funnel_id, run["id"] if run else run_id],
        )
        for row in rows:
            row["drop_reasons"] = _safe_json(row.get("drop_reasons"), {})
        return rows

    # ================================================================
    # 路径明细 / 下钻
    # ================================================================

    def list_paths(
        self,
        funnel_id: int,
        *,
        run_id: int | None = None,
        outcome: str | None = None,
        subject_key: str | None = None,
        step_index: int | None = None,
        page: int = 1,
        size: int = 50,
    ) -> dict[str, Any]:
        """按运算版本列出转化/未转化路径，支持按主体和步骤筛选。"""
        run = self.resolve_run(funnel_id, run_id)
        if run is None:
            return {"run": None, "items": [], "total": 0, "page": page, "size": size,
                    "summary": self._empty_summary()}

        where = ["run_id=?"]
        params: list[Any] = [run["id"]]
        if outcome:
            where.append("outcome=?")
            params.append(outcome)
        if subject_key:
            where.append("subject_key=?")
            params.append(subject_key)
        if step_index is not None:
            where.append("reached_step>=?")
            params.append(step_index)
        clause = " AND ".join(where)
        total = db.count("funnel_paths", clause, params)
        rows = db.fetchall(
            f"SELECT * FROM funnel_paths WHERE {clause} ORDER BY id ASC LIMIT ? OFFSET ?",
            params + [size, (page - 1) * size],
        )
        return {
            "run": self._public_run(run),
            "items": [self._public_path(row) for row in rows],
            "total": total,
            "page": page,
            "size": size,
            "summary": self._summary(run["id"]),
        }

    def path_detail(
        self,
        funnel_id: int,
        path_id: int,
        run_id: int | None = None,
    ) -> dict[str, Any] | None:
        """下钻一条路径：用本次运算保存的 event_id 关联 events 原始记录。"""
        path = db.fetchone(
            "SELECT * FROM funnel_paths WHERE id=? AND funnel_id=?", [path_id, funnel_id]
        )
        if path is None:
            return None
        if run_id is not None and int(path["run_id"]) != int(run_id):
            return None
        run = db.fetchone("SELECT * FROM funnel_runs WHERE id=?", [path["run_id"]])
        if run is not None and run.get("status") != "已完成":
            return None

        step_events = _safe_json(path.get("step_events"), {})
        attribution = _safe_json(path.get("attribution_events"), [])
        candidates = _safe_json(path.get("candidate_opportunities"), [])

        return {
            "path": self._public_path(path),
            "run": self._public_run(run) if run else None,
            "failure_label": (
                FUNNEL_FAILURE_LABELS.get(path["failure_code"], path["failure_code"])
                if path.get("failure_code") else None
            ),
            "steps": self._resolve_events(step_events),
            "attribution": self._resolve_events(attribution),
            "candidate_opportunities": [
                {**cand, "event": self._resolve_event_id(cand.get("event_id"))}
                for cand in candidates
            ],
            "raw_records": self._raw_step_records(path),
        }

    def conversion_detail(
        self, funnel_id: int, *, run_id: int | None = None,
    ) -> dict[str, Any]:
        """转化明细：已转化路径 + 每条路径的候选浏览归因，供值班员核对来源。"""
        run = self.resolve_run(funnel_id, run_id)
        if run is None:
            return {"run": None, "converted": [], "by_subject": [], "attribution_summary": {}}

        paths = db.fetchall(
            "SELECT * FROM funnel_paths WHERE run_id=? AND outcome='已转化' ORDER BY id ASC",
            [run["id"]],
        )
        converted = []
        attribution_counts: dict[str, int] = {}
        for path in paths:
            public = self._public_path(path)
            candidates = [
                {**cand, "event": self._resolve_event_id(cand.get("event_id"))}
                for cand in public["candidate_opportunities"]
            ]
            public["candidate_opportunities"] = candidates
            converted.append(public)
            campaign = path.get("opportunity_campaign") or "未标注"
            attribution_counts[campaign] = attribution_counts.get(campaign, 0) + 1

        return {
            "run": self._public_run(run),
            "converted": converted,
            "attribution_summary": attribution_counts,
            "summary": self._summary(run["id"]),
            # 旧报告/旧快照保存 run_id 即可一直回查当时的路径，不随后续重算变化。
            "version_anchor": {
                "funnel_id": funnel_id,
                "run_id": run["id"],
                "run_no": run["run_no"],
                "paths_query": f"/api/pipeline/funnels/{funnel_id}/paths?run_id={run['id']}",
            },
        }

    def unconverted_detail(
        self,
        funnel_id: int,
        *,
        run_id: int | None = None,
        subject_key: str | None = None,
    ) -> dict[str, Any]:
        """未转化明细：逐人给出一个结论，说明是「少了一步」还是「顺序对不上」。"""
        run = self.resolve_run(funnel_id, run_id)
        if run is None:
            return {"run": None, "unconverted": [], "by_reason": {}, "subjects": []}

        params: list[Any] = [run["id"]]
        clause = "run_id=? AND outcome='未转化'"
        if subject_key:
            clause += " AND subject_key=?"
            params.append(subject_key)
        paths = db.fetchall(f"SELECT * FROM funnel_paths WHERE {clause} ORDER BY id ASC", params)

        # 一个人可能有多条浏览机会路径；取最近一次机会作为这个人的结论，避免重复计数。
        primary: dict[str, dict[str, Any]] = {}
        for path in paths:
            subject = path["subject_key"]
            key = (str(path.get("opportunity_at") or "9999"), int(path.get("id") or 0))
            current = primary.get(subject)
            if current is None:
                primary[subject] = path
                continue
            current_key = (str(current.get("opportunity_at") or "9999"), int(current.get("id") or 0))
            if key >= current_key:
                primary[subject] = path

        by_reason: dict[str, int] = {}
        items = []
        for subject in sorted(primary):
            path = primary[subject]
            label = FUNNEL_FAILURE_LABELS.get(path.get("failure_code"), path.get("failure_code") or "未知")
            by_reason[label] = by_reason.get(label, 0) + 1
            public = self._public_path(path)
            public["failure_label"] = label
            public["next_action"] = self._next_action(path, label)
            public["all_opportunity_paths"] = [
                self._brief_path(row) for row in paths if row["subject_key"] == subject
            ]
            items.append(public)

        return {
            "run": self._public_run(run),
            "unconverted": items,
            "by_reason": by_reason,
            "subjects": sorted(primary),
        }

    @staticmethod
    def _next_action(path: dict[str, Any], label: str) -> str:
        if path.get("failure_code") == "ORDER_MISMATCH":
            return "核对付款与浏览的先后顺序：付款发生在浏览之前，不能被算作已完成转化"
        if path.get("failure_code") == "WINDOW_EXCEEDED":
            return f"付款超出 {path.get('window_seconds')} 秒转化窗口，不能归到这次浏览"
        if path.get("failure_code") == "SUPERSEDED":
            return "付款已归给另一次有效浏览，本次浏览单独留档、不重复计入"
        if path.get("missing_step_name"):
            return f"补齐步骤「{path['missing_step_name']}」"
        return "补齐缺失步骤"

    def _brief_path(self, path: dict[str, Any]) -> dict[str, Any]:
        return {
            "path_id": path["id"],
            "outcome": path["outcome"],
            "opportunity_event_id": path.get("opportunity_event_id"),
            "opportunity_at": path.get("opportunity_at"),
            "opportunity_campaign": path.get("opportunity_campaign"),
            "attempt_event_id": path.get("attempt_event_id"),
            "attempt_at": path.get("attempt_at"),
            "failure_code": path.get("failure_code"),
            "failure_label": FUNNEL_FAILURE_LABELS.get(path.get("failure_code"), path.get("failure_code")),
        }

    def subject_timeline(
        self,
        funnel_id: int,
        subject_key: str,
        *,
        run_id: int | None = None,
    ) -> dict[str, Any]:
        """某主体的完整行为时间线：原始事件时刻 + 它在本次运算里的归属。"""
        run = self.resolve_run(funnel_id, run_id)
        if run is None:
            return {"run": None, "subject_key": subject_key, "events": [], "paths": []}

        step_names = _safe_json(run.get("steps_snapshot"), [])
        rows = db.fetchall(
            "SELECT id,event_name,event_attrs,occurred_at,ingested_at FROM events "
            "WHERE event_name IN ({}) ORDER BY occurred_at ASC, id ASC".format(
                ",".join("?" for _ in step_names) or "''"
            ),
            step_names,
        )
        events = []
        for row in rows:
            attrs = _safe_json(row.get("event_attrs"), {})
            if str(attrs.get("user_id", "")) != subject_key:
                continue
            events.append({
                "event_id": row["id"],
                "event_name": row["event_name"],
                "occurred_at": row["occurred_at"],
                "ingested_at": row["ingested_at"],
                "campaign": attrs.get("campaign") or attrs.get("activity"),
                "source": attrs.get("source") or attrs.get("channel"),
                "attrs": attrs,
            })

        paths = db.fetchall(
            "SELECT * FROM funnel_paths WHERE run_id=? AND subject_key=? ORDER BY id ASC",
            [run["id"], subject_key],
        )
        return {
            "run": self._public_run(run),
            "subject_key": subject_key,
            "events": events,
            "paths": [self._public_path(row) for row in paths],
        }

    # ================================================================
    # 旧接口兼容
    # ================================================================

    def compare_funnels(self, funnel_id_a: int, funnel_id_b: int) -> dict[str, Any]:
        """对比两个漏斗最新版本的转化率差异。"""
        results_a = self.run_computation(funnel_id_a)
        results_b = self.run_computation(funnel_id_b)

        funnel_a = db.fetchone("SELECT funnel_name FROM funnels WHERE id=?", [funnel_id_a])
        funnel_b = db.fetchone("SELECT funnel_name FROM funnels WHERE id=?", [funnel_id_b])

        steps_a = results_a.get("steps_detail", [])
        steps_b = results_b.get("steps_detail", [])

        comparison = []
        for sa, sb in zip(steps_a, steps_b):
            comparison.append({
                "step": sa["step_index"],
                "rate_a": sa["conversion_rate"],
                "rate_b": sb["conversion_rate"],
                "diff": round(sa["conversion_rate"] - sb["conversion_rate"], 4),
            })

        return {
            "funnel_a": funnel_a["funnel_name"] if funnel_a else "?",
            "funnel_b": funnel_b["funnel_name"] if funnel_b else "?",
            "overall_diff": round(
                results_a.get("overall_conversion_rate", 0)
                - results_b.get("overall_conversion_rate", 0), 4),
            "comparison": comparison,
        }

    def drop_analysis(self, funnel_id: int, run_id: int | None = None) -> dict[str, Any]:
        """分析某版本各步骤的流失原因。"""
        rows = self.funnel_results(funnel_id, run_id)
        max_drop_step = None
        max_drop_count = 0
        step_analysis = []

        for r in rows:
            if r["step_index"] > 0:
                drop = r["entry_count"] - r["exit_count"]
                step_analysis.append({
                    "step": r["step_name"],
                    "entry": r["entry_count"],
                    "exit": r["exit_count"],
                    "drop": drop,
                    "drop_rate": round(drop / max(r["entry_count"], 1), 4),
                    "reasons": r.get("drop_reasons", {}),
                })
                if drop > max_drop_count:
                    max_drop_count = drop
                    max_drop_step = r["step_name"]

        return {
            "funnel_id": funnel_id,
            "run_id": rows[0]["run_id"] if rows else run_id,
            "steps": step_analysis,
            "max_drop_step": max_drop_step,
            "max_drop_count": max_drop_count,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_funnels"] = db.count("funnels")
        snapshot["total_results"] = db.count("funnel_results")
        snapshot["total_runs"] = db.count("funnel_runs")
        snapshot["total_paths"] = db.count("funnel_paths")
        return snapshot

    # ================================================================
    # 内部工具
    # ================================================================

    def _public_run(self, run: dict[str, Any] | None) -> dict[str, Any] | None:
        if run is None:
            return None
        data = dict(run)
        data["parameters"] = _safe_json(data.get("parameters"), {})
        data["steps_snapshot"] = _safe_json(data.get("steps_snapshot"), [])
        return data

    def _public_path(self, path: dict[str, Any]) -> dict[str, Any]:
        data = dict(path)
        data["step_events"] = _safe_json(data.get("step_events"), {})
        data["attribution_events"] = _safe_json(data.get("attribution_events"), [])
        data["candidate_opportunities"] = _safe_json(data.get("candidate_opportunities"), [])
        data["failure_label"] = (
            FUNNEL_FAILURE_LABELS.get(data.get("failure_code"), data.get("failure_code"))
            if data.get("failure_code") else None
        )
        return data

    def _summary(self, run_id: int) -> dict[str, Any]:
        total = db.count("funnel_paths", "run_id=?", [run_id])
        converted = db.count("funnel_paths", "run_id=? AND outcome='已转化'", [run_id])
        unconverted = db.count("funnel_paths", "run_id=? AND outcome='未转化'", [run_id])
        return {
            "paths": total,
            "converted": converted,
            "unconverted": unconverted,
        }

    @staticmethod
    def _empty_summary() -> dict[str, Any]:
        return {"paths": 0, "converted": 0, "unconverted": 0}

    def _resolve_event_id(self, event_id: Any) -> dict[str, Any] | None:
        if event_id in (None, ""):
            return None
        row = db.fetchone(
            "SELECT id,event_name,event_attrs,occurred_at,ingested_at FROM events WHERE id=?",
            [int(event_id)],
        )
        if row is None:
            return None
        row["attrs"] = _safe_json(row.get("event_attrs"), {})
        return row

    def _resolve_events(self, snapshots: Any) -> list[dict[str, Any]]:
        if isinstance(snapshots, dict):
            snapshots = list(snapshots.values())
        if not isinstance(snapshots, list):
            return []
        resolved = []
        for snap in snapshots:
            if not isinstance(snap, dict):
                continue
            resolved.append({**snap, "event": self._resolve_event_id(snap.get("event_id"))})
        return resolved

    def _raw_step_records(self, path: dict[str, Any]) -> list[dict[str, Any]]:
        records = []
        for key in ("opportunity_event_id", "attempt_event_id", "conversion_event_id"):
            event = self._resolve_event_id(path.get(key))
            if event is not None:
                records.append({"role": key, "event": event})
        return records


def _safe_json(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default if not isinstance(default, dict) else dict(default)
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return default if not isinstance(default, dict) else dict(default)
