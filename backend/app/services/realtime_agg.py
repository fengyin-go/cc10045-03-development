"""实时聚合器领域服务：管理聚合窗口配置并驱动真实聚合计算。

真实数据流：接收事件定义 → 按窗口维度分组 → 执行 COUNT/SUM/AVG/MAX/MIN 等聚合函数
→ 结果写入 aggregation_results 表 → 供下游指标存储和仪表盘消费。

窗口类型支持 tumbling（滚动）、sliding（滑动）、session（会话）三种，
每种类型有独立的时间语义和结果合并策略。

口径版本化（试算 → 启用 → 撤销）：
  每个窗口可以把现有事件数据聚合成一份「候选」结果（试算），核对口径后再「启用」；
  启用是整轮原子切换，旧口径转入「已确认」并完整保留；「撤销」当前启用版会把
  启用指针原子回退到上一已确认版本。仪表盘只消费启用版本的不可变分组快照，
  每次取数都带口径标注（caliber_code/version_id），并留痕支持事后原样回查。
  状态机全部在本服务内完成，路由层不做业务判断。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from app.database import db
from app.pipeline import calculate_window_groups, compute_aggregation
from app.services.base import BaseModuleService

# 口径版本状态机
VERSION_TRIALING = "试算中"
VERSION_CANDIDATE = "候选"
VERSION_FAILED = "试算失败"
VERSION_ACTIVE = "已启用"
VERSION_CONFIRMED = "已确认"   # 曾启用、后被新版本取代，但仍可回查/回退
VERSION_REVOKED = "已撤销"     # 当前启用版被撤销，指针已回退到上一轮


def _row(conn, sql: str, params: list[Any] = ()) -> dict[str, Any] | None:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    return dict(row) if row else None


def _rows(conn, sql: str, params: list[Any] = ()) -> list[dict[str, Any]]:
    cur = conn.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


class RealtimeAggService(BaseModuleService):
    MODULE = "realtime_agg"
    ENTITY = "聚合窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口名称", "数据源", "聚合维度", "聚合函数", "窗口大小"]
    REQUIRED_FIELDS = ["窗口名称", "数据源", "聚合维度"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用聚合": "已启用", "停用聚合": "已停用"}
    NEGATIVE_ACTIONS = ["停用聚合"]

    # 串行化所有口径状态流转（试算/启用/撤销），保证指针切换与版本状态在进程内原子。
    _version_lock = threading.RLock()

    # ================================================================
    # 扩展查询
    # ================================================================

    def list_windows(
        self, status: str | None = None, data_source: str | None = None,
        page: int = 1, size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """列出聚合窗口配置，支持按数据源筛选。"""
        where = "1=1"
        params: list[Any] = []
        if status:
            where += " AND status=?"
            params.append(status)
        if data_source:
            where += " AND data_source_module=?"
            params.append(data_source)

        total = db.count("aggregation_windows", where, params)
        rows = db.fetchall(
            f"SELECT * FROM aggregation_windows WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [size, (page - 1) * size],
        )
        return _sqlite_rows_to_dict(rows), total

    def results_by_window(
        self, window_id: int, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """查询指定窗口的聚合结果。按窗口起始时间倒序。"""
        rows = db.fetchall(
            "SELECT * FROM aggregation_results WHERE window_id=? ORDER BY window_start DESC LIMIT ?",
            [window_id, limit],
        )
        return _sqlite_rows_to_dict(rows)

    def latest_value(
        self, window_id: int, dimension_key: str = "__all__",
    ) -> dict[str, Any] | None:
        """获取指定窗口和维度的最新聚合值。"""
        return db.fetchone(
            "SELECT * FROM aggregation_results WHERE window_id=? AND dimension_key=? "
            "ORDER BY window_start DESC LIMIT 1",
            [window_id, dimension_key],
        )

    # ================================================================
    # 聚合执行
    # ================================================================

    def run_aggregation(self, window_id: int) -> dict[str, Any]:
        """对指定窗口执行实时聚合计算。

        1. 从 aggregation_windows 加载窗口配置
        2. 调用 pipeline.compute_aggregation 执行计算
        3. 返回本次计算的结果摘要
        """
        window = db.fetchone("SELECT * FROM aggregation_windows WHERE id=?", [window_id])
        if window is None:
            return {"ok": False, "message": f"窗口 {window_id} 不存在", "results": []}

        if window["status"] != "已启用":
            return {"ok": False, "message": f"窗口 {window['window_name']} 未启用，当前状态: {window['status']}", "results": []}

        results = compute_aggregation(dict(window))

        return {
            "ok": True,
            "message": f"窗口 {window['window_name']} 完成聚合，产出 {len(results)} 条结果",
            "results": results,
            "window_name": window["window_name"],
            "window_type": window["window_type"],
            "total_value": sum(r["computed_value"] for r in results),
        }

    def run_all_active(self) -> dict[str, Any]:
        """对所有已启用窗口执行聚合。"""
        windows = db.fetchall("SELECT * FROM aggregation_windows WHERE status='已启用'")
        summary = []
        total_results = 0
        for w in windows:
            r = self.run_aggregation(w["id"])
            summary.append({"window_name": w["window_name"], "result_count": len(r["results"])})
            total_results += len(r["results"])
        return {"ok": True, "windows_processed": len(windows), "total_results": total_results, "summary": summary}

    def time_range_analysis(
        self, window_id: int, start_time: str, end_time: str,
    ) -> dict[str, Any]:
        """对指定时间范围的聚合结果做趋势分析。"""
        rows = db.fetchall(
            "SELECT * FROM aggregation_results WHERE window_id=? AND window_start>=? AND window_start<=? "
            "ORDER BY window_start ASC",
            [window_id, start_time, end_time],
        )
        if not rows:
            return {"trend": "no_data", "points": []}

        values = [r["computed_value"] for r in rows]
        points = [{"time": r["window_start"], "value": r["computed_value"], "count": r["event_count"]} for r in rows]

        import statistics
        mean_v = statistics.mean(values)
        std_v = statistics.stdev(values) if len(values) > 1 else 0
        trend = "stable"
        if len(values) >= 2:
            if values[0] < values[-1] * 0.7:
                trend = "upward"
            elif values[0] > values[-1] * 1.3:
                trend = "downward"

        return {
            "trend": trend,
            "mean": round(mean_v, 4),
            "std": round(std_v, 4),
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "points": points,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        enabled = db.count("aggregation_windows", "status='已启用'")
        total_events = db.count("events")
        total_results = db.count("aggregation_results")
        snapshot["enabled_windows"] = enabled
        snapshot["total_ingested_events"] = total_events
        snapshot["total_agg_results"] = total_results
        snapshot["latest_batch"] = db.fetchone("SELECT batch_id, MAX(ingested_at) as ts FROM events")
        return snapshot

    # ================================================================
    # 口径版本化：试算 / 启用 / 撤销 / 取数 / 历史回查
    # ================================================================

    def _get_window(self, window_id: int) -> dict[str, Any] | None:
        return db.fetchone("SELECT * FROM aggregation_windows WHERE id=?", [window_id])

    def _ensure_window_state(self, conn, window_id: int) -> dict[str, Any] | None:
        """取窗口状态指针行，不存在则创建（active_version_id 为空）。"""
        state = _row(conn, "SELECT * FROM aggregation_window_states WHERE window_id=?", [window_id])
        if state is None:
            conn.execute(
                "INSERT INTO aggregation_window_states(window_id, active_version_id, updated_at) VALUES (?, NULL, ?)",
                [window_id, db.now()],
            )
            state = _row(conn, "SELECT * FROM aggregation_window_states WHERE window_id=?", [window_id])
        return state

    def start_trial(
        self,
        window_id: int,
        *,
        caliber_code: str | None = None,
        label: str = "",
        groups: list[dict[str, Any]] | None = None,
        window_start: str | None = None,
        window_end: str | None = None,
        force_fail: bool = False,
        fail_reason: str | None = None,
    ) -> dict[str, Any]:
        """对窗口发起一轮口径试算，把现有事件数据聚合成候选结果。

        - 正常路径：复用正式聚合同一套计算逻辑 calculate_window_groups，结果写入不可变分组表，
          版本置为「候选」，不影响当前启用口径。
        - groups 显式传入时（用于核定固定分组值/测试），跳过事件计算直接落候选。
        - force_fail 或计算异常：版本置为「试算失败」，不写分组、不动启用指针。
        同一窗口同时只允许一个未决（试算中/候选）版本。
        """
        with self._version_lock:
            window = self._get_window(window_id)
            if window is None:
                return {"ok": False, "message": f"窗口 {window_id} 不存在"}

            pending = db.fetchall(
                "SELECT id, status FROM aggregation_versions "
                "WHERE window_id=? AND status IN (?, ?)",
                [window_id, VERSION_TRIALING, VERSION_CANDIDATE],
            )
            if pending:
                return {
                    "ok": False,
                    "message": f"窗口 {window['window_name']} 已存在未决口径版本（{pending[0]['status']}），请先启用或废弃后再试算",
                }

            next_no_row = db.fetchone(
                "SELECT COALESCE(MAX(version_no), 0) + 1 AS n FROM aggregation_versions WHERE window_id=?",
                [window_id],
            )
            version_no = int(next_no_row["n"]) if next_no_row else 1
            code = caliber_code or f"caliber-v{version_no}"

            existed = db.fetchone(
                "SELECT id FROM aggregation_versions WHERE window_id=? AND caliber_code=?",
                [window_id, code],
            )
            if existed is not None:
                return {"ok": False, "message": f"口径编码 {code} 在该窗口已存在，请更换 caliber_code"}

            # 先建「试算中」版本
            version_id = db.insert("aggregation_versions", {
                "window_id": window_id,
                "version_no": version_no,
                "caliber_code": code,
                "label": label or f"第{version_no}轮口径",
                "status": VERSION_TRIALING,
                "group_count": 0,
                "total_value": 0.0,
            })

            try:
                if force_fail:
                    raise RuntimeError(fail_reason or "试算被显式标记为失败")

                if groups is not None:
                    calc = {
                        "window_start": window_start,
                        "window_end": window_end,
                        "groups": [
                            {
                                "dimension_key": str(g["dimension_key"]),
                                "computed_value": float(g["computed_value"]),
                                "event_count": int(g.get("event_count", 0)),
                            }
                            for g in groups
                        ],
                    }
                else:
                    calc = calculate_window_groups(dict(window))
                    if window_start:
                        calc["window_start"] = window_start
                    if window_end:
                        calc["window_end"] = window_end
                    if not calc["groups"]:
                        raise RuntimeError("窗口时间范围内没有可聚合的事件数据")

                # 整轮分组在一个事务里落库，保证候选结果完整可见
                with db.tx() as conn:
                    total = 0.0
                    for g in calc["groups"]:
                        conn.execute(
                            "INSERT INTO aggregation_version_groups(version_id, dimension_key, computed_value, event_count) "
                            "VALUES (?, ?, ?, ?)",
                            [version_id, g["dimension_key"], g["computed_value"], g["event_count"]],
                        )
                        total += float(g["computed_value"])
                    conn.execute(
                        "UPDATE aggregation_versions SET status=?, window_start=?, window_end=?, group_count=?, total_value=? WHERE id=?",
                        [VERSION_CANDIDATE, calc.get("window_start"), calc.get("window_end"),
                         len(calc["groups"]), round(total, 4), version_id],
                    )

                return {
                    "ok": True,
                    "message": f"窗口 {window['window_name']} 第{version_no}轮口径试算完成，候选结果 {len(calc['groups'])} 个分组，待启用",
                    "version": self.get_version(version_id),
                }
            except Exception as exc:  # 试算失败：只标记本版本，不动已启用口径
                db.update(
                    "aggregation_versions",
                    {"status": VERSION_FAILED, "fail_reason": str(exc)},
                    "id=?",
                    [version_id],
                )
                active = self.active_version(window_id)
                return {
                    "ok": False,
                    "message": f"口径试算失败：{exc}；当前启用口径保持不变",
                    "version": self.get_version(version_id),
                    "active_version": active,
                }

    def discard_candidate(self, window_id: int) -> dict[str, Any]:
        """废弃当前候选版本（候选 → 试算失败归档），便于重新试算；不影响启用口径。"""
        with self._version_lock:
            window = self._get_window(window_id)
            if window is None:
                return {"ok": False, "message": f"窗口 {window_id} 不存在"}
            candidate = db.fetchone(
                "SELECT * FROM aggregation_versions WHERE window_id=? AND status=? ORDER BY version_no DESC LIMIT 1",
                [window_id, VERSION_CANDIDATE],
            )
            if candidate is None:
                return {"ok": False, "message": "该窗口没有待启用的候选口径版本"}
            db.update(
                "aggregation_versions",
                {"status": VERSION_FAILED, "fail_reason": "候选被废弃，未启用"},
                "id=?",
                [candidate["id"]],
            )
            return {"ok": True, "message": f"候选口径 {candidate['caliber_code']} 已废弃，启用口径不变",
                    "version": self.get_version(candidate["id"])}

    def activate_version(self, window_id: int, version_id: int | None = None) -> dict[str, Any]:
        """启用候选口径：整轮分组原子生效。

        同一事务内：候选 → 已启用、原启用版 → 已确认、窗口指针切到新版本。
        一次读取只会命中指针指向的单一版本，因此不会混进上轮分组值。
        """
        with self._version_lock:
            window = self._get_window(window_id)
            if window is None:
                return {"ok": False, "message": f"窗口 {window_id} 不存在"}

            with db.tx() as conn:
                state = self._ensure_window_state(conn, window_id)
                if version_id is None:
                    candidate = _row(
                        conn,
                        "SELECT * FROM aggregation_versions WHERE window_id=? AND status=? ORDER BY version_no DESC LIMIT 1",
                        [window_id, VERSION_CANDIDATE],
                    )
                else:
                    candidate = _row(
                        conn, "SELECT * FROM aggregation_versions WHERE id=? AND window_id=?",
                        [version_id, window_id],
                    )
                if candidate is None:
                    return {"ok": False, "message": "没有可启用的候选口径版本"}
                if candidate["status"] != VERSION_CANDIDATE:
                    return {"ok": False, "message": f"版本 {candidate['caliber_code']} 当前状态为{candidate['status']}，仅候选版本可启用"}

                previous_active_id = state["active_version_id"]
                now_str = db.now()

                # 旧启用版转为已确认（历史仍可回查），候选版整轮启用
                if previous_active_id is not None:
                    conn.execute(
                        "UPDATE aggregation_versions SET status=? WHERE id=? AND status=?",
                        [VERSION_CONFIRMED, previous_active_id, VERSION_ACTIVE],
                    )
                conn.execute(
                    "UPDATE aggregation_versions SET status=?, activated_at=? WHERE id=?",
                    [VERSION_ACTIVE, now_str, candidate["id"]],
                )
                conn.execute(
                    "UPDATE aggregation_window_states SET active_version_id=?, updated_at=? WHERE window_id=?",
                    [candidate["id"], now_str, window_id],
                )
                prev_code = None
                if previous_active_id is not None:
                    prev = _row(conn, "SELECT caliber_code FROM aggregation_versions WHERE id=?", [previous_active_id])
                    prev_code = prev["caliber_code"] if prev else None

            return {
                "ok": True,
                "message": f"口径 {candidate['caliber_code']} 已整轮启用" +
                           (f"，原口径 {prev_code} 转为已确认并保留" if prev_code else ""),
                "version": self.get_version(candidate["id"]),
                "previous_caliber_code": prev_code,
            }

    def revoke_active(self, window_id: int) -> dict[str, Any]:
        """撤销当前启用口径，启用指针原子回退到上一已确认版本。

        撤销后：被撤销版本 → 已撤销（分组原样保留可回查），上一已确认版 → 已启用。
        若不存在上一已确认版本，则指针置空（窗口回到无启用口径）。
        """
        with self._version_lock:
            window = self._get_window(window_id)
            if window is None:
                return {"ok": False, "message": f"窗口 {window_id} 不存在"}

            with db.tx() as conn:
                state = self._ensure_window_state(conn, window_id)
                active_id = state["active_version_id"]
                if active_id is None:
                    active = _row(
                        conn,
                        "SELECT * FROM aggregation_versions WHERE window_id=? AND status=? ORDER BY version_no DESC LIMIT 1",
                        [window_id, VERSION_ACTIVE],
                    )
                    if active is None:
                        return {"ok": False, "message": "该窗口当前没有启用中的口径版本，无需撤销"}
                    active_id = active["id"]

                current = _row(conn, "SELECT * FROM aggregation_versions WHERE id=?", [active_id])
                if current is None:
                    return {"ok": False, "message": "启用指针指向的版本不存在，无法撤销"}

                predecessor = _row(
                    conn,
                    "SELECT * FROM aggregation_versions WHERE window_id=? AND status=? AND version_no < ? "
                    "ORDER BY version_no DESC LIMIT 1",
                    [window_id, VERSION_CONFIRMED, current["version_no"]],
                )
                now_str = db.now()
                conn.execute(
                    "UPDATE aggregation_versions SET status=?, revoked_at=? WHERE id=?",
                    [VERSION_REVOKED, now_str, current["id"]],
                )
                if predecessor is not None:
                    conn.execute(
                        "UPDATE aggregation_versions SET status=?, activated_at=COALESCE(activated_at, ?) WHERE id=?",
                        [VERSION_ACTIVE, now_str, predecessor["id"]],
                    )
                    new_active_id = predecessor["id"]
                else:
                    new_active_id = None
                conn.execute(
                    "UPDATE aggregation_window_states SET active_version_id=?, updated_at=? WHERE window_id=?",
                    [new_active_id, now_str, window_id],
                )

            restored = self.get_version(new_active_id) if new_active_id is not None else None
            return {
                "ok": True,
                "message": f"口径 {current['caliber_code']} 已撤销" +
                           (f"，已回退到上一轮口径 {restored['caliber_code']}" if restored else "，窗口已无启用口径"),
                "revoked_version": self.get_version(current["id"]),
                "active_version": restored,
            }

    # -------- 查询 --------

    def get_version(self, version_id: int) -> dict[str, Any] | None:
        version = db.fetchone("SELECT * FROM aggregation_versions WHERE id=?", [version_id])
        if version is None:
            return None
        groups = db.fetchall(
            "SELECT dimension_key, computed_value, event_count FROM aggregation_version_groups WHERE version_id=? ORDER BY id",
            [version_id],
        )
        version = dict(version)
        version["groups"] = groups
        return version

    def list_versions(self, window_id: int) -> list[dict[str, Any]]:
        """列出窗口全部口径版本（含试算/候选/启用/确认/撤销/失败），按轮次升序。"""
        rows = db.fetchall(
            "SELECT id, window_id, version_no, caliber_code, label, status, fail_reason, "
            "window_start, window_end, group_count, total_value, created_at, activated_at, revoked_at "
            "FROM aggregation_versions WHERE window_id=? ORDER BY version_no ASC",
            [window_id],
        )
        state = db.fetchone("SELECT active_version_id FROM aggregation_window_states WHERE window_id=?", [window_id])
        active_id = state["active_version_id"] if state else None
        for r in rows:
            r["is_active"] = (r["id"] == active_id)
        return rows

    def active_version(self, window_id: int) -> dict[str, Any] | None:
        """返回窗口当前启用版本（含整轮分组）；无启用版本返回 None。"""
        state = db.fetchone("SELECT active_version_id FROM aggregation_window_states WHERE window_id=?", [window_id])
        if not state or state["active_version_id"] is None:
            return None
        return self.get_version(state["active_version_id"])

    def fetch_active_snapshot(self, window_id: int, *, consumer: str = "dashboard") -> dict[str, Any]:
        """仪表盘/值班取数：在单一 WAL 快照里读取当前启用版本的整轮分组。

        返回每个分组及其值，并明确标注本次取数使用的口径（caliber_code/version_id/version_no），
        同时写入取数留痕，支持事后按 fetch 记录原样回查。撤销与取数交错时，本方法全程使用
        同一连接事务视图，因此返回的分组要么全属旧口径、要么全属新口径，绝不混轮。
        """
        window = self._get_window(window_id)
        if window is None:
            return {"ok": False, "message": f"窗口 {window_id} 不存在"}

        def _read(conn):
            state = _row(conn, "SELECT active_version_id FROM aggregation_window_states WHERE window_id=?", [window_id])
            active_id = state["active_version_id"] if state else None
            if active_id is None:
                return {"empty": True}
            version = _row(conn, "SELECT * FROM aggregation_versions WHERE id=?", [active_id])
            groups = _rows(
                conn,
                "SELECT dimension_key, computed_value, event_count FROM aggregation_version_groups WHERE version_id=? ORDER BY id",
                [active_id],
            )
            return {"empty": False, "version": version, "groups": groups}

        snap = db.read_snapshot(_read)
        if snap.get("empty"):
            return {
                "ok": True,
                "window_id": window_id,
                "window_name": window["window_name"],
                "active": False,
                "message": "该窗口尚无启用口径，请先完成试算并启用",
                "caliber_code": None,
                "version_id": None,
                "version_no": None,
                "groups": [],
            }

        version = snap["version"]
        groups = snap["groups"]
        # 留痕在快照读取之后写入，记录的就是本次实际交付的口径版本
        log_id = db.insert("aggregation_fetch_log", {
            "window_id": window_id,
            "version_id": version["id"],
            "caliber_code": version["caliber_code"],
            "consumer": consumer,
        })
        return {
            "ok": True,
            "window_id": window_id,
            "window_name": window["window_name"],
            "active": True,
            "message": f"取数使用口径 {version['caliber_code']}（第{version['version_no']}轮，{version['status']}）",
            "caliber_code": version["caliber_code"],
            "caliber_label": version["label"],
            "version_id": version["id"],
            "version_no": version["version_no"],
            "version_status": version["status"],
            "window_start": version["window_start"],
            "window_end": version["window_end"],
            "fetch_log_id": log_id,
            "groups": groups,
            "total_value": round(sum(float(g["computed_value"]) for g in groups), 4),
        }

    def history_versions(self, window_id: int) -> list[dict[str, Any]]:
        """历史图：返回所有曾确认过的口径轮次（已启用/已确认/已撤销），试算失败与候选不含在内。"""
        rows = db.fetchall(
            "SELECT id, version_no, caliber_code, label, status, window_start, window_end, "
            "group_count, total_value, activated_at, revoked_at, created_at "
            "FROM aggregation_versions WHERE window_id=? AND status IN (?, ?, ?) ORDER BY version_no ASC",
            [window_id, VERSION_ACTIVE, VERSION_CONFIRMED, VERSION_REVOKED],
        )
        state = db.fetchone("SELECT active_version_id FROM aggregation_window_states WHERE window_id=?", [window_id])
        active_id = state["active_version_id"] if state else None
        for r in rows:
            r["is_active"] = (r["id"] == active_id)
        return rows

    def fetch_history_snapshot(self, window_id: int, version_id: int) -> dict[str, Any]:
        """按指定版本回查历史聚合原样数据（打开历史图/值班日报复核用）。"""
        version = self.get_version(version_id)
        if version is None or version["window_id"] != window_id:
            return {"ok": False, "message": f"窗口 {window_id} 不存在口径版本 {version_id}"}
        if version["status"] in (VERSION_TRIALING, VERSION_CANDIDATE, VERSION_FAILED):
            return {"ok": False, "message": f"版本 {version['caliber_code']} 尚未确认启用，不属于历史确认数据"}
        return {
            "ok": True,
            "window_id": window_id,
            "caliber_code": version["caliber_code"],
            "caliber_label": version["label"],
            "version_id": version["id"],
            "version_no": version["version_no"],
            "version_status": version["status"],
            "window_start": version["window_start"],
            "window_end": version["window_end"],
            "groups": version["groups"],
            "total_value": round(sum(float(g["computed_value"]) for g in version["groups"]), 4),
        }

    def fetch_logs(self, window_id: int, limit: int = 50) -> list[dict[str, Any]]:
        """取数留痕：列出每次取数实际使用的口径，支撑值班日报的事后原样回查。"""
        return db.fetchall(
            "SELECT id, window_id, version_id, caliber_code, consumer, fetched_at "
            "FROM aggregation_fetch_log WHERE window_id=? ORDER BY id DESC LIMIT ?",
            [window_id, limit],
        )


def _sqlite_rows_to_dict(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return rows