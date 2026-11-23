"""业务路由汇总：训练分包能力路由在前，通用资源路由按模块 key 分派。"""
from __future__ import annotations

from app.routers import resource, train_pack

# train_pack 的 /api/train-pack/* 具体路径必须先于 resource 的
# /api/{module}/{entry_id} 注册，否则会被动态段抢占。
ROUTERS = [train_pack, resource]
