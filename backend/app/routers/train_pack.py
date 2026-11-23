"""训练分区数据包能力接口：快照、导出、下载、回执、清单与补算核对。

路径用 train-pack（连字符），与通用资源模块 train_pack（下划线）区分；
本路由必须先于通用资源路由注册，避免被 /api/{module}/{entry_id} 抢占。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.train_pack import (
    PackageNotFound,
    PackageNotReady,
    PartitionNotFound,
    PipelineNotRegistered,
    SnapshotMissing,
    TrainPackError,
    TrainPackService,
)

router = APIRouter(prefix="/api/train-pack", tags=["训练分区数据包"])


def _service() -> TrainPackService:
    service = service_for("train_pack")
    if not isinstance(service, TrainPackService):  # pragma: no cover - 注册表固定
        raise HTTPException(status_code=500, detail="train_pack 服务未正确注册")
    return service


def _guard(call):
    try:
        return call()
    except (PartitionNotFound, PipelineNotRegistered, SnapshotMissing) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PackageNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PackageNotReady as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except TrainPackError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class IngestPayload(BaseModel):
    records: list[dict[str, Any]] = Field(default_factory=list)


class CorrectPayload(BaseModel):
    key: str
    values: dict[str, Any]


class ExportPayload(BaseModel):
    pipeline_no: str
    batch_no: str
    dates: list[str]
    interrupt: list[str] = Field(default_factory=list)
    force: bool = False


class ReceiptPayload(BaseModel):
    consumer: str = "training"


@router.get("/overview")
def train_pack_overview() -> dict[str, Any]:
    """引擎全景：分区、快照、数据包、存储清单与已覆盖区间。"""
    return _service().engine.state()


@router.post("/partitions/{date}/ingest")
def ingest_partition(date: str, payload: IngestPayload) -> dict[str, Any]:
    """特征存储按日期分区接入记录。"""
    service = _service()
    result = _guard(lambda: service.engine.ingest(date, payload.records))
    return {"ok": True, **result}


@router.post("/partitions/{date}/correct")
def correct_partition(date: str, payload: CorrectPayload) -> dict[str, Any]:
    """存储侧接到更正：行版次与分区修订号抬升，已签发的包不受影响。"""
    service = _service()
    result = _guard(lambda: service.engine.correct(date, payload.key, payload.values))
    return {"ok": True, **result}


@router.post("/partitions/{date}/snapshot")
def snapshot_partition(date: str) -> dict[str, Any]:
    """对日期分区取快照，冻结当下行值与版次。"""
    service = _service()
    result = _guard(lambda: service.engine.take_snapshot(date))
    return {"ok": True, **result}


@router.post("/export")
def export_batch(payload: ExportPayload) -> dict[str, Any]:
    """按训练批导出分区数据包；interrupt 用于模拟压缩被打断。"""
    service = _service()
    result = _guard(lambda: service.engine.export_batch(
        payload.pipeline_no,
        payload.batch_no,
        payload.dates,
        interrupt=set(payload.interrupt),
        force=payload.force,
    ))
    service.sync_from_engine()
    return {"ok": True, **result}


@router.get("/packages")
def list_packages() -> dict[str, Any]:
    """全部打包尝试及其状态；已签发的附索引。"""
    return {"items": _service().engine.state()["数据包"]}


@router.get("/packages/{pack_id}/index")
def package_index(pack_id: str) -> dict[str, Any]:
    """外附索引：管道、分区、来源版次、行数、编码、压缩与内容摘要。"""
    service = _service()
    return _guard(lambda: service.engine.read_index(pack_id))


@router.get("/packages/{pack_id}/download")
def package_download(pack_id: str) -> Response:
    """下载分区文件；压缩未结束的包一律 409。"""
    service = _service()
    blob = _guard(lambda: service.engine.download(pack_id))
    index = service.engine.read_index(pack_id)
    return Response(
        content=blob,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{pack_id}.bin"',
            "X-Pack-Compression": str(index.get("压缩", "")),
            "X-Pack-Encoding": str(index.get("编码", "")),
            "X-Pack-Digest": str(index.get("内容摘要", "")),
        },
    )


@router.get("/packages/{pack_id}/rows")
def package_rows(pack_id: str) -> dict[str, Any]:
    """训练侧依据索引打开数据包：校验摘要、解压、解码后的行值。"""
    service = _service()
    rows = _guard(lambda: service.engine.open_rows(pack_id))
    return {"包编号": pack_id, "行数": len(rows), "行": rows}


@router.post("/packages/{pack_id}/receipt")
def package_receipt(pack_id: str, payload: ReceiptPayload) -> dict[str, Any]:
    """训练侧回执：特征存储据此更新批次已覆盖区间。"""
    service = _service()
    result = _guard(lambda: service.engine.receipt(pack_id, payload.consumer))
    service.sync_from_engine()
    return {"ok": True, **result}


@router.get("/manifest")
def storage_manifest() -> dict[str, Any]:
    """存储清单：全部分区交付结果。"""
    return {"items": _service().engine.manifest()}


@router.get("/coverage")
def batch_coverage() -> dict[str, Any]:
    """各训练批已被回执覆盖的日期区间。"""
    return _service().engine.coverage()


@router.get("/backfill-check")
def backfill_check(batch_no: str, date: str) -> dict[str, Any]:
    """安排补算前核对交付版次与存储当前版次。"""
    return _service().engine.backfill_check(batch_no, date)
