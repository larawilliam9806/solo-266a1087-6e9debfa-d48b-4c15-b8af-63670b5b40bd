"""针对“确认时才准备统计”改造的行为验证（补充测试，不替代 tests/test_app.py）。"""
from __future__ import annotations

import io
import json
import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app  # noqa: E402
from app import service  # noqa: E402
from app.db import get_db  # noqa: E402


@pytest.fixture()
def app(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "DATABASE": str(tmp_path / "lazy.sqlite3"),
            "SECRET_KEY": "test",
        }
    )
    yield app


@pytest.fixture()
def client(app):
    return app.test_client()


def make_csv(rows, header="渠道,日期,金额,转化数"):
    lines = [header]
    for row in rows:
        lines.append(",".join("" if value is None else str(value) for value in row))
    return "\n".join(lines).encode("utf-8")


def upload(client, url, csv_bytes, filename="data.csv", month=None):
    data = {"csv": (io.BytesIO(csv_bytes), filename)}
    if month is not None:
        data["month"] = month
    return client.post(
        url, data=data, content_type="multipart/form-data", follow_redirects=True
    )


def base_rows():
    rows = []
    for channel in ("A", "B"):
        for day in range(1, 9):
            rows.append((channel, f"2026-11-{day:02d}", day * 10, day))
    return rows


def seed(app):
    c = app.test_client()
    upload(c, "/baseline", make_csv(base_rows()), month="2026-11")
    return c


def candidate(app, rows):
    c = app.test_client()
    upload(c, "/months/2026-11/candidate", make_csv(rows))
    with app.app_context():
        row = get_db().execute(
            "SELECT * FROM batches WHERE status='pending' ORDER BY id DESC"
        ).fetchone()
    return row


def reconcile(app, batch_id, **th):
    thresholds = {
        "max_key_missing_ratio": th.get("key", 0.0),
        "max_null_rate_diff": th.get("null", 0.0),
        "max_bin_share_diff": th.get("share", 0.0),
    }
    with app.app_context():
        return service.reconcile(batch_id, thresholds)


# ---------------------------------------------------------------------------
# 1. 上传不再算候选统计
# ---------------------------------------------------------------------------


def test_upload_candidate_stores_no_channel_stats(app):
    seed(app)
    cand = candidate(app, base_rows())
    with app.app_context():
        n = get_db().execute(
            "SELECT COUNT(*) AS c FROM batch_channel_stats WHERE batch_id = ?",
            (cand["id"],),
        ).fetchone()["c"]
        assert n == 0
        # 键仍然随上传预存
        k = get_db().execute(
            "SELECT COUNT(*) AS c FROM batch_keys WHERE batch_id = ?",
            (cand["id"],),
        ).fetchone()["c"]
        assert k == len(base_rows())


def test_upload_does_not_call_summarize(app, monkeypatch):
    seed(app)
    calls = []
    original = service._summarize_baseline

    def spy(parsed):
        calls.append(len(parsed.rows))
        return original(parsed)

    monkeypatch.setattr(service, "_summarize_baseline", spy)
    candidate(app, base_rows())
    assert calls == []


# ---------------------------------------------------------------------------
# 2. 确认时按原口径准备统计
# ---------------------------------------------------------------------------


def test_confirm_prepares_original_methodology_stats(app):
    client = seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])
    client.post(f"/reports/{report['report_no']}/confirm", follow_redirects=True)

    with app.app_context():
        rows = get_db().execute(
            "SELECT channel, amount_bins FROM batch_channel_stats"
            " WHERE batch_id = ? ORDER BY channel",
            (cand["id"],),
        ).fetchall()
        assert [r["channel"] for r in rows] == ["A", "B"]
        amount_bins = json.loads(rows[0]["amount_bins"])
        assert amount_bins["cuts"] == [20.0, 40.0, 60.0]
        active = service.get_active_batch("2026-11")
        assert active["id"] == cand["id"]


def test_next_round_uses_prepared_stats(app):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])
    with app.app_context():
        service.confirm_report(report["report_no"])

    second = candidate(app, base_rows())
    r2 = reconcile(app, second["id"])
    assert r2["verdict"] == "passed"

    # 带偏差的候选在新一轮仍能被统计口径抓到
    rows = []
    for ch, day, amount, conv in base_rows():
        if ch == "A":
            amount *= 100
        rows.append((ch, day, amount, conv))
    third = candidate(app, rows)
    r3 = reconcile(app, third["id"], share=0.05)
    assert r3["verdict"] == "rejected"


# ---------------------------------------------------------------------------
# 3. 旧库已存候选统计直接复用
# ---------------------------------------------------------------------------


def test_legacy_stored_stats_are_reused_without_recompute(app, monkeypatch):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])

    sentinel_bins = json.dumps({"sentinel": "legacy"}, ensure_ascii=False)
    with app.app_context():
        db = get_db()
        for ch in ("A", "B"):
            db.execute(
                "INSERT INTO batch_channel_stats (batch_id, channel, key_count,"
                " amount_nonempty, amount_null_rate, amount_bins,"
                " conversions_nonempty, conversions_null_rate, conversions_bins)"
                " VALUES (?, ?, 8, 8, 0, ?, 8, 0, ?)",
                (cand["id"], ch, sentinel_bins, sentinel_bins),
            )

    def boom(*a, **k):
        raise AssertionError("legacy stats must be reused, not recomputed")

    monkeypatch.setattr(service, "_summarize_baseline", boom)

    with app.app_context():
        outcome = service.confirm_report(report["report_no"])
        assert outcome["ok"] is True
        row = get_db().execute(
            "SELECT amount_bins FROM batch_channel_stats"
            " WHERE batch_id = ? AND channel = 'A'",
            (cand["id"],),
        ).fetchone()
        assert json.loads(row["amount_bins"]) == {"sentinel": "legacy"}


# ---------------------------------------------------------------------------
# 4. 准备统计期间的并发性
# ---------------------------------------------------------------------------


def test_prep_does_not_block_other_month_registration(app, monkeypatch):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])
    original = service._summarize_baseline
    done = {}
    state = {"ran": False}

    def during_prep(*a, **k):
        if state["ran"]:
            return original(*a, **k)
        state["ran"] = True

        def work():
            c = app.test_client()
            r = upload(
                c,
                "/baseline",
                make_csv([("X", "2026-12-01", 1, 1)]),
                month="2026-12",
            )
            done["text"] = r.get_data(as_text=True)

        t = threading.Thread(target=work)
        t.start()
        t.join(timeout=20)
        done["finished"] = not t.is_alive()
        return original(*a, **k)

    monkeypatch.setattr(service, "_summarize_baseline", during_prep)
    with app.app_context():
        outcome = service.confirm_report(report["report_no"])
    assert outcome["ok"] is True
    assert done.get("finished") is True
    assert "已验收登记" in done.get("text", "")
    with app.app_context():
        assert service.get_active_batch("2026-12") is not None


def test_prep_keeps_serving_downstream(app, monkeypatch):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])
    original = service._summarize_baseline
    seen = {}

    def during_prep(*a, **k):
        c = app.test_client()
        resp = c.get("/months/2026-11/active.csv")
        seen["status"] = resp.status_code
        seen["body"] = resp.get_data(as_text=True)
        return original(*a, **k)

    monkeypatch.setattr(service, "_summarize_baseline", during_prep)
    with app.app_context():
        service.confirm_report(report["report_no"])
    assert seen["status"] == 200
    # 准备期间供出的仍是 V1（含原始 10/20... 数值）
    assert ",10," in seen["body"]


def test_prep_allows_same_month_other_candidate_to_confirm_first(app, monkeypatch):
    seed(app)
    first = candidate(app, base_rows())
    r1 = reconcile(app, first["id"])
    second = candidate(app, base_rows())
    r2 = reconcile(app, second["id"])

    original = service._summarize_baseline
    second_outcome = {}
    state = {"ran": False}

    def during_first_prep(*a, **k):
        if state["ran"]:
            return original(*a, **k)
        state["ran"] = True

        def work():
            with app.app_context():
                second_outcome["value"] = service.confirm_report(r2["report_no"])

        t = threading.Thread(target=work)
        t.start()
        t.join(timeout=20)
        second_outcome["finished"] = not t.is_alive()
        return original(*a, **k)

    monkeypatch.setattr(service, "_summarize_baseline", during_first_prep)

    with app.app_context():
        outcome = service.confirm_report(r1["report_no"])

    assert second_outcome.get("finished") is True
    assert second_outcome["value"]["ok"] is True
    # 首个确认在生效点发现基线已换版 -> 软冲突，提示重新核对
    assert outcome["ok"] is False
    assert "基线已换版" in outcome["message"]

    with app.app_context():
        active = service.get_active_batch("2026-11")
        assert active["id"] == second["id"]
        assert get_db().execute(
            "SELECT status FROM batches WHERE id = ?", (first["id"],)
        ).fetchone()["status"] == "pending"
        # 落败的确认不得写入候选统计（统计与生效必须同一事务）
        assert get_db().execute(
            "SELECT COUNT(*) AS c FROM batch_channel_stats WHERE batch_id = ?",
            (first["id"],),
        ).fetchone()["c"] == 0
        assert service.get_report(r1["report_no"])["status"] == "stale"

        # 重新核对后可再次确认
        new_report = service.reconcile(
            first["id"],
            {
                "max_key_missing_ratio": 0.0,
                "max_null_rate_diff": 0.0,
                "max_bin_share_diff": 0.0,
            },
        )
        assert new_report["verdict"] == "passed"
        again = service.confirm_report(new_report["report_no"])
        assert again["ok"] is True


def test_race_lost_via_http_prompts_re_reconcile(app, monkeypatch):
    seed(app)
    first = candidate(app, base_rows())
    r1 = reconcile(app, first["id"])
    second = candidate(app, base_rows())
    r2 = reconcile(app, second["id"])

    original = service._summarize_baseline
    state = {"ran": False}

    def during_first_prep(*a, **k):
        if state["ran"]:
            return original(*a, **k)
        state["ran"] = True
        c = app.test_client()
        resp = c.post(
            f"/reports/{r2['report_no']}/confirm", follow_redirects=True
        )
        assert resp.status_code == 200
        return original(*a, **k)

    monkeypatch.setattr(service, "_summarize_baseline", during_first_prep)
    client = app.test_client()
    resp = client.post(f"/reports/{r1['report_no']}/confirm", follow_redirects=True)
    text = resp.get_data(as_text=True)
    assert "基线已换版" in text
    assert "重新核对" in text


# ---------------------------------------------------------------------------
# 5. 失败与重试
# ---------------------------------------------------------------------------


def test_baseline_changed_before_confirm_skips_prep(app, monkeypatch):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])

    with app.app_context():
        db = get_db()
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE batches SET status='superseded' WHERE id=1")
        db.execute(
            "INSERT INTO batches (month, version, status, row_count, channel_count,"
            " created_at) VALUES ('2026-11', 9, 'active', 1, 1, 'now')"
        )
        db.execute("COMMIT")

    def boom(*a, **k):
        raise AssertionError("prep must be skipped once baseline is already changed")

    monkeypatch.setattr(service, "_summarize_baseline", boom)
    with app.app_context():
        outcome = service.confirm_report(report["report_no"])
        assert outcome["ok"] is False
        assert "基线已换版" in outcome["message"]
        assert get_db().execute(
            "SELECT COUNT(*) AS c FROM batch_channel_stats WHERE batch_id = ?",
            (cand["id"],),
        ).fetchone()["c"] == 0


def test_prep_failure_propagates_and_allows_retry(app, monkeypatch):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])

    def boom(*a, **k):
        raise RuntimeError("prep exploded")

    monkeypatch.setattr(service, "_summarize_baseline", boom)
    with app.app_context():
        with pytest.raises(RuntimeError):
            service.confirm_report(report["report_no"])
        assert service.get_active_batch("2026-11")["version"] == 1
        assert service.get_report(report["report_no"])["status"] == "passed"
        assert get_db().execute(
            "SELECT COUNT(*) AS c FROM batch_channel_stats WHERE batch_id = ?",
            (cand["id"],),
        ).fetchone()["c"] == 0

    monkeypatch.undo()
    with app.app_context():
        outcome = service.confirm_report(report["report_no"])
        assert outcome["ok"] is True
        assert service.get_report(report["report_no"])["status"] == "confirmed"


def test_prep_failure_route_is_friendly_and_retryable(app, monkeypatch):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])

    calls = {"n": 0}
    original = service._summarize_baseline

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("temporary prep failure")
        return original(*a, **k)

    monkeypatch.setattr(service, "_summarize_baseline", flaky)
    client = app.test_client()

    resp = client.post(f"/reports/{report['report_no']}/confirm", follow_redirects=True)
    assert resp.status_code == 200
    text = resp.get_data(as_text=True)
    assert "确认生效未完成" in text
    assert "再次确认" in text
    # 页面仍是待确认，表单还在
    assert "确认生效" in text

    with app.app_context():
        assert service.get_active_batch("2026-11")["version"] == 1

    resp2 = client.post(f"/reports/{report['report_no']}/confirm", follow_redirects=True)
    assert "现行批已切换" in resp2.get_data(as_text=True)


def test_write_failure_then_retry_succeeds(app):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])

    with app.app_context():
        db = get_db()
        db.execute(
            "CREATE TRIGGER abort_confirm BEFORE UPDATE OF status ON reports"
            " WHEN NEW.status = 'confirmed'"
            " BEGIN SELECT RAISE(ABORT, 'simulated failure'); END"
        )
        with pytest.raises(Exception):
            service.confirm_report(report["report_no"])
        assert service.get_report(report["report_no"])["status"] == "passed"
        db.execute("DROP TRIGGER abort_confirm")
        outcome = service.confirm_report(report["report_no"])
        assert outcome["ok"] is True
        assert service.get_active_batch("2026-11")["version"] == 2


def test_stats_write_failure_rolls_back_switch(app):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])

    with app.app_context():
        db = get_db()
        db.execute(
            "CREATE TRIGGER abort_stats BEFORE INSERT ON batch_channel_stats"
            " BEGIN SELECT RAISE(ABORT, 'stats failure'); END"
        )
        with pytest.raises(Exception):
            service.confirm_report(report["report_no"])
        active = service.get_active_batch("2026-11")
        assert active["version"] == 1
        assert get_db().execute(
            "SELECT status FROM batches WHERE id = ?", (cand["id"],)
        ).fetchone()["status"] == "pending"
        assert service.get_report(report["report_no"])["status"] == "passed"


# ---------------------------------------------------------------------------
# 6. 核对仍然只读
# ---------------------------------------------------------------------------


def test_reconcile_still_does_not_create_candidate_stats(app):
    seed(app)
    cand = candidate(app, base_rows())
    report = reconcile(app, cand["id"])
    assert report["verdict"] == "passed"
    with app.app_context():
        assert get_db().execute(
            "SELECT COUNT(*) AS c FROM batch_channel_stats WHERE batch_id = ?",
            (cand["id"],),
        ).fetchone()["c"] == 0
        assert get_db().execute(
            "SELECT status FROM batches WHERE id = ?", (cand["id"],)
        ).fetchone()["status"] == "pending"
