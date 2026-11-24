"""漏斗口径回归测试：直接跑计算管道与领域服务，不依赖 Web 服务。

关注四件事：
1. 后浏览后付款的 07:25 付款只归给最近一次有效浏览，两次活动各自留归因。
2. 先付款后浏览的人不能进入已完成转化，并被解释为顺序对不上。
3. 迟到补录的事件进入重新计算后，旧版本路径仍然可回查。
4. 付款只归一次机会，窗口和先后顺序都按 occurred_at 判断。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)


@pytest.fixture()
def fresh_db(monkeypatch):
    tmpdir = tempfile.mkdtemp(prefix="funnel-tests-")
    monkeypatch.setenv("CC10045_DB_DIR", tmpdir)
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    from app.database import db  # noqa: WPS433

    yield db


def _add_event(db, event_name, occurred_at, **attrs):
    return db.insert("events", {
        "event_name": event_name,
        "event_attrs": json.dumps(attrs, ensure_ascii=False),
        "source_module": "test",
        "occurred_at": occurred_at,
    })


def _make_funnel(db, steps, window_hours=1):
    from app.database import db as _db  # noqa: WPS433

    funnel_id = _db.insert("funnels", {
        "funnel_name": "测试漏斗",
        "steps": json.dumps(steps, ensure_ascii=False),
        "conversion_window_hours": window_hours,
        "segment_condition": "all",
        "status": "已启用",
    })
    return _db.fetchone("SELECT * FROM funnels WHERE id=?", [funnel_id])


def test_later_payment_uses_most_recent_browse(fresh_db):
    from app.pipeline import compute_funnel_run

    db = fresh_db
    funnel = _make_funnel(db, ["浏览商品", "支付成功"])
    _add_event(db, "浏览商品", "2026-11-24T07:00:00", user_id="A", campaign="拉新活动")
    _add_event(db, "浏览商品", "2026-11-24T07:20:00", user_id="A", campaign="召回活动")
    _add_event(db, "支付成功", "2026-11-24T07:25:00", user_id="A")

    run = compute_funnel_run(funnel)
    assert run["entry_count"] == 1
    assert run["converted_count"] == 1
    assert run["converted_subjects"] == 1

    converted = [p for p in run["paths"] if p["outcome"] == "已转化"]
    assert len(converted) == 1
    path = converted[0]
    assert path["opportunity_at"] == "2026-11-24T07:20:00"
    assert path["opportunity_campaign"] == "召回活动"
    assert path["conversion_at"] == "2026-11-24T07:25:00"

    # 两次浏览都带自己的活动，各自可核对，不互相复制
    campaigns = {
        cand["event_id"]: cand["campaign"] for cand in path["candidate_opportunities"]
    }
    assert set(campaigns.values()) == {"拉新活动", "召回活动"}
    selected = [c for c in path["candidate_opportunities"] if c["selected"]]
    assert len(selected) == 1
    assert selected[0]["campaign"] == "召回活动"

    superseded = [
        p for p in run["paths"]
        if p["opportunity_campaign"] == "拉新活动" and p["outcome"] == "未转化"
    ]
    assert superseded and superseded[0]["failure_code"] == "SUPERSEDED"


def test_payment_before_browse_is_not_converted(fresh_db):
    from app.pipeline import compute_funnel_run

    db = fresh_db
    funnel = _make_funnel(db, ["浏览商品", "支付成功"])
    _add_event(db, "支付成功", "2026-11-24T06:50:00", user_id="B")
    _add_event(db, "浏览商品", "2026-11-24T07:10:00", user_id="B", campaign="自然流量")

    run = compute_funnel_run(funnel)
    assert run["converted_count"] == 0
    assert run["converted_subjects"] == 0
    assert run["entry_count"] == 1

    path = next(p for p in run["paths"] if p["subject_key"] == "B")
    assert path["outcome"] == "未转化"
    assert path["failure_code"] == "ORDER_MISMATCH"
    assert path["attempt_at"] == "2026-11-24T06:50:00"


def test_late_event_creates_new_version_and_old_path_remains(fresh_db):
    from app.pipeline import compute_funnel_run

    db = fresh_db
    funnel = _make_funnel(db, ["浏览商品", "支付成功"])
    _add_event(db, "浏览商品", "2026-11-24T07:00:00", user_id="A", campaign="拉新活动")
    _add_event(db, "浏览商品", "2026-11-24T07:20:00", user_id="A", campaign="召回活动")
    _add_event(db, "支付成功", "2026-11-24T07:25:00", user_id="A")

    first = compute_funnel_run(funnel)
    first_run_id = first["run_id"]
    first_paths = db.fetchall("SELECT * FROM funnel_paths WHERE run_id=?", [first_run_id])
    assert len(first_paths) == 2

    # 事后才到库的 07:23 浏览
    _add_event(db, "浏览商品", "2026-11-24T07:23:00", user_id="A", campaign="迟到补录活动")
    second = compute_funnel_run(funnel)
    assert second["run_id"] != first_run_id
    assert second["run_no"] == first["run_no"] + 1

    new_converted = [p for p in second["paths"] if p["outcome"] == "已转化"]
    assert new_converted[0]["opportunity_at"] == "2026-11-24T07:23:00"

    # 旧版本路径与活动归属原样保留
    old_converted = db.fetchall(
        "SELECT * FROM funnel_paths WHERE run_id=? AND outcome='已转化'", [first_run_id])
    assert old_converted[0]["opportunity_at"] == "2026-11-24T07:20:00"
    assert old_converted[0]["opportunity_campaign"] == "召回活动"


def test_payment_assigned_once_even_with_two_payments(fresh_db):
    from app.pipeline import compute_funnel_run

    db = fresh_db
    funnel = _make_funnel(db, ["浏览商品", "支付成功"])
    _add_event(db, "浏览商品", "2026-11-24T08:00:00", user_id="F", campaign="A1")
    _add_event(db, "支付成功", "2026-11-24T08:02:00", user_id="F")
    _add_event(db, "浏览商品", "2026-11-24T08:20:00", user_id="F", campaign="A2")
    _add_event(db, "支付成功", "2026-11-24T08:22:00", user_id="F")

    run = compute_funnel_run(funnel)
    converted = [p for p in run["paths"] if p["outcome"] == "已转化"]
    assert len(converted) == 2
    assert {p["opportunity_campaign"] for p in converted} == {"A1", "A2"}
    assert len({p["conversion_event_id"] for p in converted}) == 2


def test_window_exceeded_is_explained(fresh_db):
    from app.pipeline import compute_funnel_run

    db = fresh_db
    funnel = _make_funnel(db, ["浏览商品", "支付成功"], window_hours=1)
    _add_event(db, "浏览商品", "2026-11-24T08:00:00", user_id="W")
    _add_event(db, "支付成功", "2026-11-24T09:30:00", user_id="W")

    run = compute_funnel_run(funnel)
    path = next(p for p in run["paths"] if p["subject_key"] == "W")
    assert path["outcome"] == "未转化"
    assert path["failure_code"] == "WINDOW_EXCEEDED"


def test_service_drilldown_links_raw_records(fresh_db):
    from app.services import service_for

    db = fresh_db
    funnel = _make_funnel(db, ["浏览商品", "支付成功"])
    _add_event(db, "浏览商品", "2026-11-24T07:20:00", user_id="A", campaign="召回活动")
    _add_event(db, "支付成功", "2026-11-24T07:25:00", user_id="A")

    svc = service_for("funnel_analyzer")
    result = svc.run_computation(funnel["id"])
    assert result["ok"] is True

    paths = svc.list_paths(funnel["id"], run_id=result["run_id"], outcome="已转化")
    assert paths["total"] == 1
    path_id = paths["items"][0]["id"]

    detail = svc.path_detail(funnel["id"], path_id, run_id=result["run_id"])
    assert detail is not None
    raw_roles = {rec["role"]: rec["event"]["occurred_at"] for rec in detail["raw_records"]}
    assert raw_roles["opportunity_event_id"] == "2026-11-24T07:20:00"
    assert raw_roles["conversion_event_id"] == "2026-11-24T07:25:00"
    assert detail["steps"][0]["event"]["attrs"]["campaign"] == "召回活动"

    # 用旧版本 run_id 不能取到新版本路径
    newer = svc.run_computation(funnel["id"])
    assert svc.path_detail(funnel["id"], path_id, run_id=newer["run_id"]) is None


def test_unconverted_detail_explains_missing_step(fresh_db):
    from app.services import service_for

    db = fresh_db
    funnel = _make_funnel(db, ["浏览商品", "加入购物车", "支付成功"])
    _add_event(db, "浏览商品", "2026-11-24T08:00:00", user_id="C", campaign="搜索")
    _add_event(db, "支付成功", "2026-11-24T08:10:00", user_id="C")

    svc = service_for("funnel_analyzer")
    result = svc.run_computation(funnel["id"])
    detail = svc.unconverted_detail(funnel["id"], run_id=result["run_id"])
    row = next(item for item in detail["unconverted"] if item["subject_key"] == "C")
    assert row["failure_code"] == "MISSING_STEP"
    assert row["missing_step_name"] == "加入购物车"
    assert "补齐步骤" in row["next_action"]


def test_late_event_earlier_than_old_as_of_does_not_mutate_old_run(fresh_db):
    """迟到事件的 occurred_at 落在旧版本 as_of 之前，也不改旧版本确认过的链。"""
    from app.pipeline import compute_funnel_run

    db = fresh_db
    funnel = _make_funnel(db, ["浏览商品", "支付成功"])
    _add_event(db, "浏览商品", "2026-11-24T07:00:00", user_id="A", campaign="拉新活动")
    _add_event(db, "浏览商品", "2026-11-24T07:20:00", user_id="A", campaign="召回活动")
    _add_event(db, "支付成功", "2026-11-24T07:25:00", user_id="A")

    first = compute_funnel_run(funnel, as_of="2026-11-24T08:00:00")
    snapshot_before = db.fetchall(
        "SELECT id,opportunity_event_id,opportunity_at,opportunity_campaign,conversion_event_id "
        "FROM funnel_paths WHERE run_id=? ORDER BY id",
        [first["run_id"]],
    )

    _add_event(db, "浏览商品", "2026-11-24T07:23:00", user_id="A", campaign="迟到补录活动")
    second = compute_funnel_run(funnel, as_of="2026-11-24T08:00:00")
    assert second["run_id"] != first["run_id"]

    snapshot_after = db.fetchall(
        "SELECT id,opportunity_event_id,opportunity_at,opportunity_campaign,conversion_event_id "
        "FROM funnel_paths WHERE run_id=? ORDER BY id",
        [first["run_id"]],
    )
    assert snapshot_before == snapshot_after

    new_converted = [p for p in second["paths"] if p["outcome"] == "已转化"]
    assert new_converted[0]["opportunity_at"] == "2026-11-24T07:23:00"
