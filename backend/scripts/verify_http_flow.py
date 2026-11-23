"""HTTP 端到端验证：通过真实 REST 接口走完整口径流转。仅用标准库 urllib。"""
from __future__ import annotations

import json
import os
import sys
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8020")
FAILURES: list[str] = []


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def check(name: str, cond: bool, detail="") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        FAILURES.append(name)


def main() -> None:
    windows = call("GET", "/api/realtime_agg?size=100")
    wid = next(w["id"] for w in windows["items"] if w["status"] == "已启用")
    print(f"== 窗口 {wid} ==")

    # 初始版本列表
    vers = call("GET", f"/api/realtime-agg/windows/{wid}/versions")["versions"]
    check("初始无口径版本", vers == [])

    # 试算旧口径
    t1 = call("POST", f"/api/realtime-agg/windows/{wid}/trials", {
        "caliber_code": "http-old", "label": "旧口径",
        "groups": [
            {"dimension_key": "华北", "computed_value": 4, "event_count": 4},
            {"dimension_key": "华南", "computed_value": 6, "event_count": 6},
        ],
    })
    check("试算旧口径->候选", t1["ok"] and t1["version"]["status"] == "候选")
    v1 = t1["version"]["id"]

    # 启用
    a1 = call("POST", f"/api/realtime-agg/windows/{wid}/activate", {"version_id": v1})
    check("启用旧口径", a1["ok"])

    # 取数
    s1 = call("GET", f"/api/realtime-agg/windows/{wid}/snapshot?consumer=analyst")
    g1 = {g["dimension_key"]: g["computed_value"] for g in s1["groups"]}
    check("取数标注旧口径且4/6",
          s1["caliber_code"] == "http-old" and g1 == {"华北": 4.0, "华南": 6.0}
          and s1["fetch_log_id"] is not None)

    # 试算失败
    tf = call("POST", f"/api/realtime-agg/windows/{wid}/trials",
              {"caliber_code": "http-fail", "force_fail": True, "fail_reason": "boom"})
    check("试算失败接口返回 ok=False", tf["ok"] is False and tf["version"]["status"] == "试算失败")
    s_keep = call("GET", f"/api/realtime-agg/windows/{wid}/snapshot")
    check("失败后取数仍是旧口径", s_keep["caliber_code"] == "http-old")

    # 新口径试算+启用
    t2 = call("POST", f"/api/realtime-agg/windows/{wid}/trials", {
        "caliber_code": "http-new", "label": "新口径",
        "groups": [
            {"dimension_key": "华北", "computed_value": 9, "event_count": 9},
            {"dimension_key": "华南", "computed_value": 8, "event_count": 8},
        ],
    })
    v2 = t2["version"]["id"]
    a2 = call("POST", f"/api/realtime-agg/windows/{wid}/activate", {"version_id": v2})
    check("启用新口径，旧版确认", a2["ok"] and a2["previous_caliber_code"] == "http-old")

    s2 = call("POST", f"/api/realtime-agg/windows/{wid}/snapshot", {"consumer": "duty-daily"})
    g2 = {g["dimension_key"]: g["computed_value"] for g in s2["groups"]}
    check("值班取数=新口径9/8且带标注",
          s2["caliber_code"] == "http-new" and g2 == {"华北": 9.0, "华南": 8.0}
          and s2["version_id"] == v2)

    # 版本详情原样回查
    detail = call("GET", f"/api/realtime-agg/versions/{v2}")
    check("版本详情含整轮分组",
          {g["dimension_key"]: g["computed_value"] for g in detail["version"]["groups"]}
          == {"华北": 9.0, "华南": 8.0})

    # 撤销
    rev = call("POST", f"/api/realtime-agg/windows/{wid}/revoke", {})
    check("撤销回退旧口径", rev["ok"] and rev["active_version"]["id"] == v1)

    s3 = call("GET", f"/api/realtime-agg/windows/{wid}/snapshot")
    g3 = {g["dimension_key"]: g["computed_value"] for g in s3["groups"]}
    check("撤销后新请求=4/6", s3["caliber_code"] == "http-old" and g3 == {"华北": 4.0, "华南": 6.0})
    check("撤销后无9+6混轮", not (g3.get("华北") == 9.0 and g3.get("华南") == 6.0))

    # 历史
    hist = call("GET", f"/api/realtime-agg/windows/{wid}/history")["versions"]
    codes = {h["caliber_code"]: h["status"] for h in hist}
    check("历史两轮状态正确",
          codes == {"http-old": "已启用", "http-new": "已撤销"})
    old_snap = call("GET", f"/api/realtime-agg/windows/{wid}/history/{v1}")
    check("历史回查旧轮4/6",
          {g["dimension_key"]: g["computed_value"] for g in old_snap["groups"]}
          == {"华北": 4.0, "华南": 6.0})

    # 取数留痕
    logs = call("GET", f"/api/realtime-agg/windows/{wid}/fetch-logs")["logs"]
    check("留痕含值班日报消费者", any(l["consumer"] == "duty-daily" and l["caliber_code"] == "http-new" for l in logs))

    # active 接口
    active = call("GET", f"/api/realtime-agg/windows/{wid}/active")
    check("active 接口指向旧口径", active["version"]["caliber_code"] == "http-old")

    print()
    if FAILURES:
        print(f"失败 {len(FAILURES)}: {FAILURES}")
        sys.exit(1)
    print("== HTTP 端到端全部通过 ==")


if __name__ == "__main__":
    main()
