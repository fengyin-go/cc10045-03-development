"""并发压测：启用/撤销切换与大量并发取数交错，断言每次取数整轮同口径、绝不混轮。"""
from __future__ import annotations

import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import db  # noqa: E402
from app.pipeline import seed_real_data  # noqa: E402
from app.services import service_for  # noqa: E402


def main() -> None:
    seed_real_data()
    svc = service_for("realtime_agg")
    window = db.fetchone("SELECT * FROM aggregation_windows WHERE status='已启用' ORDER BY id LIMIT 1")
    wid = window["id"]

    # 旧口径 4/6
    t_old = svc.start_trial(wid, caliber_code="old", groups=[
        {"dimension_key": "华北", "computed_value": 4, "event_count": 4},
        {"dimension_key": "华南", "computed_value": 6, "event_count": 6},
    ])["version"]["id"]
    svc.activate_version(wid, t_old)

    # 新口径 9/8：先启用一轮，之后反复 撤销->重新试算->启用 循环
    t_new = svc.start_trial(wid, caliber_code="new", groups=[
        {"dimension_key": "华北", "computed_value": 9, "event_count": 9},
        {"dimension_key": "华南", "computed_value": 8, "event_count": 8},
    ])["version"]["id"]

    valid = {
        "old": {"华北": 4.0, "华南": 6.0},
        "new": {"华北": 9.0, "华南": 8.0},
    }
    stop = threading.Event()
    errors: list[str] = []
    seen: dict[str, int] = {"old": 0, "new": 0, "none": 0}
    lock = threading.Lock()

    new_groups = [
        {"dimension_key": "华北", "computed_value": 9, "event_count": 9},
        {"dimension_key": "华南", "computed_value": 8, "event_count": 8},
    ]
    svc.activate_version(wid, t_new)

    def toggle():
        activated = True
        seq = 0
        while not stop.is_set():
            if activated:
                svc.revoke_active(wid)
                activated = False
            else:
                seq += 1
                r = svc.start_trial(wid, caliber_code=f"new-{seq}", groups=new_groups)
                if r["ok"]:
                    svc.activate_version(wid, r["version"]["id"])
                    activated = True

    def reader(idx: int):
        while not stop.is_set():
            snap = svc.fetch_active_snapshot(wid, consumer=f"stress-{idx}")
            if not snap.get("active"):
                with lock:
                    seen["none"] += 1
                continue
            g = {x["dimension_key"]: x["computed_value"] for x in snap["groups"]}
            code = "new" if snap["caliber_code"].startswith("new") else "old"
            expect = valid[code]
            with lock:
                if g != expect:
                    errors.append(f"混轮! caliber={snap['caliber_code']} groups={g}")
                    return
                seen[code] += 1

    threads = [threading.Thread(target=reader, args=(i,)) for i in range(6)]
    tg = threading.Thread(target=toggle)
    tg.start()
    for t in threads:
        t.start()

    stop.wait(4.0)
    stop.set()
    tg.join()
    for t in threads:
        t.join()

    print("取数统计:", seen)
    assert not errors, errors[:5]
    assert seen["old"] > 0 and seen["new"] > 0, seen
    print(f"== 并发压测通过：共 {sum(seen.values())} 次取数，0 次混轮 ==")


if __name__ == "__main__":
    main()
