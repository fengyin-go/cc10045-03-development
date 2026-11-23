"""实时聚合口径版本路由：试算、启用、撤销、取数与历史回查。

业务判断全部在 RealtimeAggService 内完成，路由层只做参数转发，保持
「状态流转只允许在 app/services 里改，路由层不做业务判断」的约定。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api/realtime-agg", tags=["聚合口径版本"])


class TrialGroup(BaseModel):
    dimension_key: str
    computed_value: float
    event_count: int = 0


class TrialPayload(BaseModel):
    caliber_code: str | None = None
    label: str | None = ""
    # 显式分组值：用于核定固定分组（如华北/华南）或测试，缺省则按窗口事件实时计算
    groups: list[TrialGroup] | None = None
    window_start: str | None = None
    window_end: str | None = None
    force_fail: bool = False
    fail_reason: str | None = None


class ActivatePayload(BaseModel):
    version_id: int | None = None


class FetchPayload(BaseModel):
    consumer: str = "dashboard"


def _svc():
    return service_for("realtime_agg")


@router.post("/windows/{window_id}/trials")
def start_trial(window_id: int, payload: TrialPayload) -> dict[str, Any]:
    """发起一轮口径试算，生成候选聚合结果（不改变当前启用口径）。"""
    groups = [g.model_dump() for g in payload.groups] if payload.groups is not None else None
    return _svc().start_trial(
        window_id,
        caliber_code=payload.caliber_code,
        label=payload.label or "",
        groups=groups,
        window_start=payload.window_start,
        window_end=payload.window_end,
        force_fail=payload.force_fail,
        fail_reason=payload.fail_reason,
    )


@router.post("/windows/{window_id}/candidate/discard")
def discard_candidate(window_id: int) -> dict[str, Any]:
    """废弃待启用候选版本。"""
    return _svc().discard_candidate(window_id)


@router.post("/windows/{window_id}/activate")
def activate(window_id: int, payload: ActivatePayload) -> dict[str, Any]:
    """启用候选口径，整轮分组原子生效。"""
    return _svc().activate_version(window_id, payload.version_id)


@router.post("/windows/{window_id}/revoke")
def revoke(window_id: int) -> dict[str, Any]:
    """撤销当前启用口径，回退到上一已确认版本。"""
    return _svc().revoke_active(window_id)


@router.get("/windows/{window_id}/versions")
def list_versions(window_id: int) -> dict[str, Any]:
    """列出窗口全部口径版本及状态。"""
    return {"ok": True, "window_id": window_id, "versions": _svc().list_versions(window_id)}


@router.get("/versions/{version_id}")
def get_version(version_id: int) -> dict[str, Any]:
    """查询单个口径版本及其整轮分组原样数据。"""
    version = _svc().get_version(version_id)
    if version is None:
        return {"ok": False, "message": f"口径版本 {version_id} 不存在"}
    return {"ok": True, "version": version}


@router.get("/windows/{window_id}/active")
def active_version(window_id: int) -> dict[str, Any]:
    """查询窗口当前启用的口径版本。"""
    version = _svc().active_version(window_id)
    return {"ok": True, "window_id": window_id, "version": version}


@router.get("/windows/{window_id}/snapshot")
def fetch_snapshot(window_id: int, consumer: str = "dashboard") -> dict[str, Any]:
    """仪表盘/值班取数：返回当前启用口径的整轮分组，响应明确标注所用口径。"""
    return _svc().fetch_active_snapshot(window_id, consumer=consumer)


@router.post("/windows/{window_id}/snapshot")
def fetch_snapshot_post(window_id: int, payload: FetchPayload) -> dict[str, Any]:
    """取数（POST 形式），用于值班日报等需要显式留痕消费者的场景。"""
    return _svc().fetch_active_snapshot(window_id, consumer=payload.consumer)


@router.get("/windows/{window_id}/history")
def history(window_id: int) -> dict[str, Any]:
    """历史图：列出所有曾确认过的口径轮次。"""
    return {"ok": True, "window_id": window_id, "versions": _svc().history_versions(window_id)}


@router.get("/windows/{window_id}/history/{version_id}")
def history_snapshot(window_id: int, version_id: int) -> dict[str, Any]:
    """按版本回查历史聚合原样数据。"""
    return _svc().fetch_history_snapshot(window_id, version_id)


@router.get("/windows/{window_id}/fetch-logs")
def fetch_logs(window_id: int, limit: int = 50) -> dict[str, Any]:
    """取数留痕：每次取数使用的口径版本，支持事后原样回查。"""
    return {"ok": True, "window_id": window_id, "logs": _svc().fetch_logs(window_id, limit)}
