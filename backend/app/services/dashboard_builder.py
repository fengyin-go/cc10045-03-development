"""仪表盘构建器领域服务：构建和配置数据可视化仪表盘。

真实数据流：配置仪表盘（图表配置/刷新间隔/数据范围）
→ 按刷新间隔从 aggregation_results 和 metric_values 拉取数据
→ 拼装为仪表盘数据 → 写入 dashboards 表。
"""
from __future__ import annotations

import json
from typing import Any

from app.database import db
from app.services.base import BaseModuleService


class DashboardBuilderService(BaseModuleService):
    MODULE = "dashboard_builder"
    ENTITY = "仪表盘"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["仪表盘名称", "包含图表", "刷新间隔", "数据范围", "共享状态"]
    REQUIRED_FIELDS = ["仪表盘名称", "包含图表", "刷新间隔"]
    STATUS_ORDER = ["草稿", "已发布", "已归档"]
    ACTION_RULES = {"发布面板": "已发布", "归档面板": "已归档"}
    NEGATIVE_ACTIONS = []

    def dashboard_data(self, dashboard_id: int) -> dict[str, Any]:
        """组装仪表盘的实时数据。

        聚合类图表只消费窗口「当前启用口径版本」的不可变整轮分组结果，每个数据点都带
        caliber_code/version_id 标注，保证一次取数内各分组同属一个启用状态，不混轮。
        尚未建立口径版本的窗口回退到旧 aggregation_results 表，保持向后兼容。
        """
        dashboard = db.fetchone("SELECT * FROM dashboards WHERE id=?", [dashboard_id])
        if dashboard is None:
            return {"ok": False, "message": "仪表盘不存在"}

        chart_configs_raw = dashboard.get("chart_configs", "[]")
        try:
            charts = json.loads(chart_configs_raw) if isinstance(chart_configs_raw, str) else chart_configs_raw
        except (json.JSONDecodeError, TypeError):
            charts = []

        data_scope = dashboard.get("data_scope", "")
        chart_data = []

        # 默认仪表盘数据：拉取启用口径聚合和指标
        if not charts or len(charts) == 0:
            agg_values = self._versioned_agg_points(limit=20)
            if not agg_values:
                agg_values = [
                    {
                        "x": r["dimension_key"],
                        "y": r["computed_value"],
                        "window_name": r["window_name"],
                        "caliber_code": None,
                        "version_id": None,
                    }
                    for r in db.fetchall(
                        "SELECT aw.window_name, ar.computed_value, ar.dimension_key "
                        "FROM aggregation_results ar JOIN aggregation_windows aw ON ar.window_id=aw.id "
                        "ORDER BY ar.window_start DESC LIMIT 20"
                    )
                ]
            metric_values = db.fetchall(
                "SELECT m.metric_name, mv.snapshot_value "
                "FROM metric_values mv JOIN metrics m ON mv.metric_id=m.id "
                "ORDER BY mv.snapshot_time DESC LIMIT 20"
            )

            chart_data = [
                {
                    "chart_type": "line",
                    "title": "聚合趋势（按启用口径）",
                    "data": [{"x": r["x"], "y": r["y"]} for r in agg_values],
                    "caliber_annotations": [
                        {"x": r["x"], "caliber_code": r.get("caliber_code"), "version_id": r.get("version_id")}
                        for r in agg_values
                    ],
                },
                {
                    "chart_type": "bar",
                    "title": "指标快照",
                    "data": [{"label": r["metric_name"], "value": r["snapshot_value"]} for r in metric_values],
                },
            ]
        else:
            for chart in charts:
                chart_type = chart.get("type", "line")
                chart_title = chart.get("title", "图表")
                module = chart.get("module", "events")

                if module == "aggregation":
                    window_id = chart.get("window_id")
                    if window_id:
                        snap = self._agg_svc().fetch_active_snapshot(int(window_id), consumer="dashboard")
                        rows = snap.get("groups", [])
                        chart_data.append({
                            "chart_type": chart_type,
                            "title": chart_title,
                            "caliber_code": snap.get("caliber_code"),
                            "version_id": snap.get("version_id"),
                            "version_no": snap.get("version_no"),
                            "data": rows,
                        })
                        continue
                    rows = db.fetchall(
                        "SELECT * FROM aggregation_results ORDER BY window_start DESC LIMIT 30"
                    )
                elif module == "metrics":
                    rows = db.fetchall(
                        "SELECT mv.snapshot_value, mv.snapshot_time, m.metric_name "
                        "FROM metric_values mv JOIN metrics m ON mv.metric_id=m.id "
                        "ORDER BY mv.snapshot_time DESC LIMIT 30"
                    )
                else:
                    rows = db.fetchall("SELECT event_name as name, COUNT(*) as cnt FROM events GROUP BY event_name LIMIT 20")

                chart_data.append({"chart_type": chart_type, "title": chart_title, "data": rows})

        return {
            "ok": True,
            "dashboard_name": dashboard["dashboard_name"],
            "refresh_interval": dashboard.get("refresh_interval_seconds", 30),
            "charts": chart_data,
        }

    def _agg_svc(self):
        from app.services import service_for
        return service_for("realtime_agg")

    def _versioned_agg_points(self, limit: int = 20) -> list[dict[str, Any]]:
        """跨窗口读取各窗口当前启用口径版本的分组点，全部带口径标注。"""
        windows = db.fetchall("SELECT id, window_name FROM aggregation_windows ORDER BY id")
        points: list[dict[str, Any]] = []
        for w in windows:
            snap = self._agg_svc().fetch_active_snapshot(w["id"], consumer="dashboard")
            if not snap.get("ok") or not snap.get("active"):
                continue
            for g in snap["groups"]:
                points.append({
                    "x": g["dimension_key"],
                    "y": g["computed_value"],
                    "window_name": w["window_name"],
                    "window_id": w["id"],
                    "caliber_code": snap["caliber_code"],
                    "version_id": snap["version_id"],
                    "version_no": snap["version_no"],
                })
                if len(points) >= limit:
                    return points
        return points

    def publish(self, dashboard_id: int) -> dict[str, Any]:
        """发布仪表盘。"""
        db.update("dashboards", {"status": "已发布"}, "id=?", [dashboard_id])
        return self.dashboard_data(dashboard_id)

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_dashboards"] = db.count("dashboards")
        snapshot["published"] = db.count("dashboards", "status='已发布'")
        return snapshot