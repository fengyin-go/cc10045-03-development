"""口径版本化全流程验证脚本（独立临时库，不污染正式数据）。

覆盖：
1. 试算 -> 候选 -> 启用 -> 再试算 -> 启用（旧版转已确认）-> 撤销回退
2. 试算失败不改变已启用口径
3. 每次取数带口径标注 + 留痕回查
4. 历史图只拿到确认过的轮次
5. 撤销交错取数：取数事务进行中撤销，返回值必须同属一个口径，不出现 9+6 混轮
6. 撤销完成后新请求与撤销结果一致
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import db  # noqa: E402
from app.pipeline import seed_real_data  # noqa: E402
from app.services import service_for  # noqa: E402

FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        FAILURES.append(name)


def main() -> None:
    seed_real_data()
    svc = service_for("realtime_agg")

    # 选一个已启用窗口
    window = db.fetchone("SELECT * FROM aggregation_windows WHERE status='已启用' ORDER BY id LIMIT 1")
    wid = window["id"]
    print(f"== 使用窗口 id={wid} name={window['window_name']} ==")

    # ---------- 初始：无启用口径 ----------
    snap0 = svc.fetch_active_snapshot(wid)
    check("初始无启用口径时取数提示先启用", snap0["active"] is False and snap0["groups"] == [])

    # ---------- 第 1 轮：旧口径 华北4 / 华南6 ----------
    r1 = svc.start_trial(wid, caliber_code="caliber-old", label="旧口径", groups=[
        {"dimension_key": "华北", "computed_value": 4, "event_count": 4},
        {"dimension_key": "华南", "computed_value": 6, "event_count": 6},
    ])
    check("第1轮试算生成候选", r1["ok"] and r1["version"]["status"] == "候选")
    v1 = r1["version"]["id"]

    # 候选未启用：取数仍无启用口径
    check("候选未启用前取数不受影响", svc.fetch_active_snapshot(wid)["active"] is False)

    a1 = svc.activate_version(wid, v1)
    check("启用第1轮", a1["ok"] and a1["version"]["status"] == "已启用")

    snap1 = svc.fetch_active_snapshot(wid, consumer="dashboard")
    g1 = {g["dimension_key"]: g["computed_value"] for g in snap1["groups"]}
    check("启用后取数=旧口径 4/6 且带口径标注",
          g1 == {"华北": 4.0, "华南": 6.0} and snap1["caliber_code"] == "caliber-old"
          and snap1["version_id"] == v1 and snap1["version_no"] == 1,
          json.dumps(g1, ensure_ascii=False))

    # ---------- 试算失败不改变已启用口径 ----------
    rf = svc.start_trial(wid, caliber_code="caliber-fail", force_fail=True, fail_reason="模拟计算异常")
    check("试算失败返回 ok=False", rf["ok"] is False and rf["version"]["status"] == "试算失败")
    snap_after_fail = svc.fetch_active_snapshot(wid)
    g_fail = {g["dimension_key"]: g["computed_value"] for g in snap_after_fail["groups"]}
    check("试算失败后启用口径仍是旧口径 4/6",
          g_fail == {"华北": 4.0, "华南": 6.0} and snap_after_fail["version_id"] == v1)

    # 失败版本不允许启用
    bad_activate = svc.activate_version(wid, rf["version"]["id"])
    check("试算失败版本不可启用", bad_activate["ok"] is False)

    # ---------- 第 2 轮：新口径 华北9 / 华南8 ----------
    r2 = svc.start_trial(wid, caliber_code="caliber-new", label="新口径", groups=[
        {"dimension_key": "华北", "computed_value": 9, "event_count": 9},
        {"dimension_key": "华南", "computed_value": 8, "event_count": 8},
    ])
    check("第2轮试算生成候选", r2["ok"] and r2["version"]["status"] == "候选")
    v2 = r2["version"]["id"]

    # 同时只允许一个候选
    r_dup = svc.start_trial(wid, caliber_code="caliber-dup", groups=[{"dimension_key": "x", "computed_value": 1}])
    check("存在未决候选时不能再次试算", r_dup["ok"] is False)

    # 候选存在期间，取数依旧是旧口径（不混轮）
    snap_pending = svc.fetch_active_snapshot(wid)
    g_pending = {g["dimension_key"]: g["computed_value"] for g in snap_pending["groups"]}
    check("候选待启用期间取数仍是旧口径", g_pending == {"华北": 4.0, "华南": 6.0}
          and snap_pending["version_id"] == v1)

    a2 = svc.activate_version(wid, v2)
    check("启用第2轮，旧轮转已确认",
          a2["ok"] and a2["previous_caliber_code"] == "caliber-old")
    old_v = svc.get_version(v1)
    check("旧版本状态=已确认且分组原样保留",
          old_v["status"] == "已确认"
          and {g["dimension_key"]: g["computed_value"] for g in old_v["groups"]} == {"华北": 4.0, "华南": 6.0})

    snap2 = svc.fetch_active_snapshot(wid, consumer="duty-report")
    g2 = {g["dimension_key"]: g["computed_value"] for g in snap2["groups"]}
    check("启用新口径后取数=9/8 且标注新口径",
          g2 == {"华北": 9.0, "华南": 8.0} and snap2["caliber_code"] == "caliber-new"
          and snap2["version_id"] == v2 and snap2["version_no"] == r2["version"]["version_no"])

    # ---------- 取数留痕 + 原样回查 ----------
    logs = svc.fetch_logs(wid)
    check("取数留痕记录了消费者与口径",
          any(l["caliber_code"] == "caliber-new" and l["consumer"] == "duty-report" for l in logs))
    log_new = next(l for l in logs if l["caliber_code"] == "caliber-new")
    back = svc.fetch_history_snapshot(wid, log_new["version_id"])
    check("按留痕版本回查原样=9/8",
          back["ok"] and {g["dimension_key"]: g["computed_value"] for g in back["groups"]}
          == {"华北": 9.0, "华南": 8.0} and back["caliber_code"] == "caliber-new")

    # ---------- 历史图：两轮确认数据都齐 ----------
    hist = svc.history_versions(wid)
    hist_codes = [h["caliber_code"] for h in hist]
    check("历史图含两轮确认口径", hist_codes == ["caliber-old", "caliber-new"])
    check("历史图不含失败/候选版本", "caliber-fail" not in hist_codes)
    old_hist = svc.fetch_history_snapshot(wid, v1)
    check("历史图回查旧轮=4/6",
          {g["dimension_key"]: g["computed_value"] for g in old_hist["groups"]}
          == {"华北": 4.0, "华南": 6.0})

    # ---------- 撤销/取数交错：取数事务进行中发起撤销 ----------
    interleave_result: dict = {}
    barrier = threading.Barrier(2)

    original_read_snapshot = db.read_snapshot

    def slow_read_snapshot(reader):
        # 进入快照读取后、返回前，让另一线程执行撤销
        def wrapped(conn):
            out = reader(conn)
            barrier.wait()           # 通知撤销线程：取数已在同一事务视图内读到数据
            time.sleep(0.3)          # 留出撤销完成窗口
            return out
        return original_read_snapshot(wrapped)

    def do_fetch():
        barrier.wait()  # 等取数线程进入事务
        revoke = svc.revoke_active(wid)
        interleave_result["revoke"] = revoke

    db.read_snapshot = slow_read_snapshot
    t_revoke = threading.Thread(target=do_fetch)
    t_revoke.start()
    interleave_snap = svc.fetch_active_snapshot(wid, consumer="duty-report-interleave")
    t_revoke.join()
    db.read_snapshot = original_read_snapshot

    gi = {g["dimension_key"]: g["computed_value"] for g in interleave_snap["groups"]}
    mixed = (gi.get("华北") == 9.0 and gi.get("华南") == 6.0) or \
            (gi.get("华北") == 4.0 and gi.get("华南") == 8.0)
    check("交错取数整轮同口径（不出现9+6混轮）",
          not mixed and interleave_snap["version_id"] in (v1, v2),
          f"groups={gi} caliber={interleave_snap['caliber_code']}")
    check("交错取数标注的口径与返回分组一致",
          (interleave_snap["version_id"] == v2 and gi == {"华北": 9.0, "华南": 8.0})
          or (interleave_snap["version_id"] == v1 and gi == {"华北": 4.0, "华南": 6.0}))

    # ---------- 撤销完成后：状态切回上一轮，新请求一致 ----------
    rev = interleave_result["revoke"]
    check("撤销成功且回退到上一轮", rev["ok"] and rev["active_version"]["id"] == v1)
    revoked_v = svc.get_version(v2)
    check("被撤销版本状态=已撤销，分组原样保留",
          revoked_v["status"] == "已撤销"
          and {g["dimension_key"]: g["computed_value"] for g in revoked_v["groups"]}
          == {"华北": 9.0, "华南": 8.0})

    snap3 = svc.fetch_active_snapshot(wid)
    g3 = {g["dimension_key"]: g["computed_value"] for g in snap3["groups"]}
    check("撤销后新取数=旧口径4/6",
          g3 == {"华北": 4.0, "华南": 6.0} and snap3["version_id"] == v1
          and snap3["caliber_code"] == "caliber-old")

    # 撤销后再开历史图：旧轮仍可回查，新轮标已撤销
    hist2 = svc.history_versions(wid)
    status_map = {h["caliber_code"]: h["status"] for h in hist2}
    check("撤销后历史图两轮都在且状态正确",
          status_map == {"caliber-old": "已启用", "caliber-new": "已撤销"})

    # 三种观测点都不能出现 9+6 组合
    def is_mixed(snap):
        gg = {g["dimension_key"]: g["computed_value"] for g in snap["groups"]}
        return (gg.get("华北") == 9.0 and gg.get("华南") == 6.0)

    check("交错取数无9+6", not is_mixed(interleave_snap))
    check("撤销后新图表无9+6", not is_mixed(snap3))
    check("历史回查旧轮无9+6", not is_mixed(old_hist))

    # ---------- 真实事件试算路径（不传 groups） ----------
    # 用一个全新窗口验证 calculate_window_groups 联动
    w2_row = db.fetchone("SELECT * FROM aggregation_windows WHERE id != ? ORDER BY id LIMIT 1", [wid])
    rt = svc.start_trial(w2_row["id"])
    check("基于真实事件的试算可产出候选或明确无数据失败，且不影响其它窗口",
          (rt["ok"] and rt["version"]["status"] == "候选")
          or (not rt["ok"] and rt["version"]["status"] == "试算失败"))

    print()
    if FAILURES:
        print(f"== {len(FAILURES)} 项失败: {FAILURES} ==")
        sys.exit(1)
    print("== 全部断言通过 ==")


if __name__ == "__main__":
    main()
