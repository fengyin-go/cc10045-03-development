"""业务路由汇总：资源路由 + 管道路由 + 聚合口径版本路由。"""
from __future__ import annotations

from app.routers import resource
from app.routers import pipeline
from app.routers import aggregation_version

ROUTERS = [resource, aggregation_version]
PIPELINE_ROUTER = pipeline.router
