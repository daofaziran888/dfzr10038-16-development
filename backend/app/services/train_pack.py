"""训练分区数据包领域服务：把特征存储的日期分区快照导出为可交付训练的不可变数据包。

业务规则（与题目路由层无关，全部收敛在本模块）：
- 快照冻结分区当下的行值与版次；之后的更正只抬升存储侧版次，绝不回写已签发的包，
  否则训练侧按旧索引复算会得到别的结果。
- 签发时索引与每份分区文件都对应真实来源版次；索引外附，文件内含特征身份与实际行值。
- 编码与压缩沿用 offline_exp 模块里登记的管道配置，不在导出时临时指定。
- 压缩未结束（打包中/打包失败）的分区没有交付结果，也不可下载。
- 打包失败后重试，只为本次实际得到的分区产生交付结果；已签发索引始终能打开原数据。
- 交付结果同步存储清单；特征存储按训练侧回执更新批次已覆盖区间，补算时可核对版次。
"""
from __future__ import annotations

import csv
import datetime
import hashlib
import io
import json
import zipfile
import gzip
from dataclasses import dataclass, field
from typing import Any, Callable

from app.services.base import BaseModuleService
from app.store import store

# ---------------------------------------------------------------------------
# 错误类型：路由层据此映射 HTTP 状态码
# ---------------------------------------------------------------------------


class TrainPackError(Exception):
    """训练分包领域错误基类。"""


class PartitionNotFound(TrainPackError):
    """特征存储里没有该日期分区。"""


class SnapshotMissing(TrainPackError):
    """分区尚未取快照，不能进入打包。"""


class PipelineNotRegistered(TrainPackError):
    """管道未在 offline_exp 登记，或配置缺少编码/压缩项。"""


class CompressionInterrupted(TrainPackError):
    """压缩过程被中断（模拟存储侧故障），本次打包失败。"""


class PackageNotFound(TrainPackError):
    """没有这份数据包。"""


class PackageNotReady(TrainPackError):
    """压缩未结束，数据包不可下载。"""


# ---------------------------------------------------------------------------
# 特征存储侧：日期分区 + 行版次 + 存储清单 + 批次覆盖区间
# ---------------------------------------------------------------------------


@dataclass
class PartitionRecord:
    """分区里的一行特征记录；version 随每次更正单调递增。"""

    key: str
    values: dict[str, Any]
    version: int = 1


@dataclass
class Snapshot:
    """某一日期分区在某个修订号上的冻结视图。"""

    date: str
    revision: int
    rows: list[dict[str, Any]]
    taken_at: str

    @property
    def source_revision(self) -> str:
        return f"r{self.revision}"


class FeatureStoreBackend:
    """特征存储的分区数据面： ingest / 更正 / 快照 / 清单 / 覆盖区间。"""

    def __init__(self) -> None:
        self.partitions: dict[str, dict[str, PartitionRecord]] = {}
        self.revisions: dict[str, int] = {}
        self.manifest: list[dict[str, Any]] = []
        self.coverage: dict[str, dict[str, dict[str, Any]]] = {}

    @staticmethod
    def _now() -> str:
        return datetime.datetime.now().isoformat()

    def ingest(self, date: str, records: list[dict[str, Any]]) -> dict[str, Any]:
        """按日期分区写入记录；重复主键视为更正。"""
        partition = self.partitions.setdefault(date, {})
        for item in records:
            key = str(item["记录"])
            values = dict(item["值"])
            if key in partition:
                rec = partition[key]
                rec.values = values
                rec.version += 1
            else:
                partition[key] = PartitionRecord(key=key, values=values)
        self.revisions[date] = self.revisions.get(date, 0) + 1
        return {
            "分区": date,
            "记录数": len(partition),
            "当前版次": f"r{self.revisions[date]}",
        }

    def correct(self, date: str, key: str, values: dict[str, Any]) -> dict[str, Any]:
        """更正分区里的一行：行版次与分区修订号同时抬升。"""
        partition = self.partitions.get(date)
        if partition is None or key not in partition:
            raise PartitionNotFound(f"分区 {date} 里没有记录 {key}")
        rec = partition[key]
        rec.values = dict(values)
        rec.version += 1
        self.revisions[date] += 1
        return {
            "分区": date,
            "记录": key,
            "行版次": rec.version,
            "当前版次": f"r{self.revisions[date]}",
        }

    def take_snapshot(self, date: str) -> Snapshot:
        """冻结分区当下视图；之后的更正不影响这份快照。"""
        partition = self.partitions.get(date)
        if partition is None:
            raise PartitionNotFound(f"特征存储里没有日期分区 {date}")
        revision = self.revisions.get(date, 0)
        rows = [
            {"记录": rec.key, "值": dict(rec.values), "版次": rec.version}
            for rec in partition.values()
        ]
        return Snapshot(date=date, revision=revision, rows=rows, taken_at=self._now())

    def record_delivery(self, batch_no: str, index: dict[str, Any]) -> None:
        """分区交付结果同步到存储清单。"""
        self.manifest.append({
            "批次": batch_no,
            "包编号": index["包编号"],
            "管道编号": index["管道编号"],
            "日期分区": index["日期分区"],
            "来源版次": index["来源版次"],
            "行数": index["行数"],
            "交付时间": self._now(),
        })

    def acknowledge(self, batch_no: str, date: str, index: dict[str, Any]) -> dict[str, Any]:
        """训练侧回执：把该分区记入批次已覆盖区间。"""
        covered = self.coverage.setdefault(batch_no, {})
        covered[date] = {
            "包编号": index["包编号"],
            "来源版次": index["来源版次"],
            "行数": index["行数"],
            "回执时间": self._now(),
        }
        return {"批次": batch_no, "已覆盖区间": sorted(covered)}

    def current_revision(self, date: str) -> int:
        return self.revisions.get(date, 0)


# ---------------------------------------------------------------------------
# 管道配置：编码与压缩沿用 offline_exp 登记项
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineConfig:
    pipeline_no: str
    encoding: str
    compression: str
    partition_strategy: str = ""
    batch_size: str = ""


def pipeline_config_from_row(row: dict[str, Any]) -> PipelineConfig:
    encoding = str(row.get("导出格式", "")).strip()
    compression = str(row.get("压缩方式", "")).strip()
    if not encoding or not compression:
        raise PipelineNotRegistered(
            f"管道 {row.get('管道编号', '?')} 缺少已登记的导出格式或压缩方式"
        )
    return PipelineConfig(
        pipeline_no=str(row.get("管道编号", "")),
        encoding=encoding,
        compression=compression,
        partition_strategy=str(row.get("分区策略", "")),
        batch_size=str(row.get("批次大小", "")),
    )


def resolve_registered_pipeline(pipeline_no: str) -> PipelineConfig:
    """从 offline_exp 模块的登记行解析管道配置。"""
    for row in store.rows("offline_exp"):
        if str(row.get("管道编号", "")) == pipeline_no:
            return pipeline_config_from_row(row)
    raise PipelineNotRegistered(f"管道 {pipeline_no} 未在离线导出模块登记")


# ---------------------------------------------------------------------------
# 编码 / 压缩编解码器：文件内含特征身份与实际行值
# ---------------------------------------------------------------------------


def _features_of(rows: list[dict[str, Any]]) -> list[str]:
    features: list[str] = []
    for row in rows:
        for name in row["值"]:
            if name not in features:
                features.append(name)
    return features


def encode_rows(encoding: str, rows: list[dict[str, Any]]) -> bytes:
    """把快照行编码为分区文件内容（含特征身份与行值）。"""
    fmt = encoding.upper()
    features = _features_of(rows)
    if fmt == "JSON":
        payload = {"特征": features, "行": rows}
        return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    if fmt == "CSV":
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(["记录", *features, "版次"])
        for row in rows:
            writer.writerow(
                [row["记录"], *[row["值"].get(f, "") for f in features], row["版次"]]
            )
        return buffer.getvalue().encode("utf-8")
    raise TrainPackError(f"导出格式 {encoding} 暂不支持编码")


def decode_rows(encoding: str, raw: bytes) -> list[dict[str, Any]]:
    """训练侧按索引里的编码还原行值。"""
    fmt = encoding.upper()
    if fmt == "JSON":
        payload = json.loads(raw.decode("utf-8"))
        return list(payload["行"])
    if fmt == "CSV":
        reader = csv.reader(io.StringIO(raw.decode("utf-8")))
        header = next(reader)
        features = header[1:-1]
        rows: list[dict[str, Any]] = []
        for line in reader:
            rows.append({
                "记录": line[0],
                "值": {f: v for f, v in zip(features, line[1:-1])},
                "版次": int(line[-1]),
            })
        return rows
    raise TrainPackError(f"导出格式 {encoding} 暂不支持解码")


def compress_bytes(mode: str, raw: bytes, inner_name: str) -> bytes:
    kind = mode.upper()
    if kind in ("NONE", "不压缩"):
        return raw
    if kind == "GZIP":
        return gzip.compress(raw)
    if kind == "ZIP":
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(inner_name, raw)
        return buffer.getvalue()
    raise TrainPackError(f"压缩方式 {mode} 暂不支持")


def decompress_bytes(mode: str, blob: bytes) -> bytes:
    kind = mode.upper()
    if kind in ("NONE", "不压缩"):
        return blob
    if kind == "GZIP":
        return gzip.decompress(blob)
    if kind == "ZIP":
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            names = zf.namelist()
            if len(names) != 1:
                raise TrainPackError("ZIP 包内文件数量异常")
            return zf.read(names[0])
    raise TrainPackError(f"压缩方式 {mode} 暂不支持")


# ---------------------------------------------------------------------------
# 数据包：尝试（attempt）与已签发包
# ---------------------------------------------------------------------------

PACK_STATUS_PACKING = "打包中"
PACK_STATUS_ISSUED = "已签发"
PACK_STATUS_FAILED = "打包失败"


@dataclass
class PackAttempt:
    """一次分区打包尝试；只有已签发的尝试才有索引与文件体。"""

    pack_id: str
    pipeline_no: str
    batch_no: str
    date: str
    status: str = PACK_STATUS_PACKING
    index: dict[str, Any] | None = None
    blob: bytes | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# 导出引擎
# ---------------------------------------------------------------------------


class TrainPackEngine:
    """快照 → 编码 → 压缩 → 签发 → 交付 → 回执 的全流程引擎。"""

    def __init__(
        self,
        pipeline_resolver: Callable[[str], PipelineConfig] = resolve_registered_pipeline,
        backend: FeatureStoreBackend | None = None,
    ) -> None:
        self.backend = backend or FeatureStoreBackend()
        self._resolve_pipeline = pipeline_resolver
        self.snapshots: dict[str, Snapshot] = {}
        self.attempts: dict[str, PackAttempt] = {}
        self._seq = 0

    # -- 特征存储侧代理 -------------------------------------------------

    def ingest(self, date: str, records: list[dict[str, Any]]) -> dict[str, Any]:
        return self.backend.ingest(date, records)

    def correct(self, date: str, key: str, values: dict[str, Any]) -> dict[str, Any]:
        return self.backend.correct(date, key, values)

    def take_snapshot(self, date: str) -> dict[str, Any]:
        snapshot = self.backend.take_snapshot(date)
        self.snapshots[date] = snapshot
        return {
            "分区": date,
            "来源版次": snapshot.source_revision,
            "行数": len(snapshot.rows),
            "快照时间": snapshot.taken_at,
        }

    # -- 打包与签发 -----------------------------------------------------

    def _next_pack_id(self) -> str:
        self._seq += 1
        return f"PACK-{self._seq:04d}"

    def _delivered_index(self, batch_no: str, date: str) -> dict[str, Any] | None:
        for attempt in self.attempts.values():
            if (
                attempt.batch_no == batch_no
                and attempt.date == date
                and attempt.status == PACK_STATUS_ISSUED
            ):
                return attempt.index
        return None

    def export_batch(
        self,
        pipeline_no: str,
        batch_no: str,
        dates: list[str],
        *,
        interrupt: set[str] | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """按训练批导出分区数据包。

        - 已在该批次交付过的分区默认跳过（重试不会重复交付）；
        - force=True 用于存储侧更正后的重导：先重新取快照再签发新包，
          旧包照旧可开；常规导出与失败重试一律使用批前冻结的快照，
          保证同一训练批各分区来自同一次冻结。
        - interrupt 模拟压缩被存储侧打断：该分区本次打包失败，不产生交付。
        """
        config = self._resolve_pipeline(pipeline_no)
        interrupt = interrupt or set()
        results: list[dict[str, Any]] = []

        for date in dates:
            if not force and self._delivered_index(batch_no, date) is not None:
                results.append({"分区": date, "结果": "已交付", "包编号": None,
                                "说明": "本批次已交付，重试跳过"})
                continue
            if force:
                snapshot = self.backend.take_snapshot(date)
                self.snapshots[date] = snapshot
            snapshot = self.snapshots.get(date)
            if snapshot is None:
                results.append({"分区": date, "结果": "失败", "包编号": None,
                                "说明": f"分区 {date} 尚未取快照"})
                continue

            attempt = PackAttempt(
                pack_id=self._next_pack_id(),
                pipeline_no=pipeline_no,
                batch_no=batch_no,
                date=date,
            )
            self.attempts[attempt.pack_id] = attempt
            try:
                raw = encode_rows(config.encoding, snapshot.rows)
                if date in interrupt:
                    raise CompressionInterrupted(f"分区 {date} 压缩被中断")
                blob = compress_bytes(config.compression, raw, f"{date}.data")
            except CompressionInterrupted as exc:
                attempt.status = PACK_STATUS_FAILED
                attempt.error = str(exc)
                results.append({"分区": date, "结果": "失败", "包编号": attempt.pack_id,
                                "说明": attempt.error})
                continue

            digest = hashlib.sha256(blob).hexdigest()
            index = {
                "包编号": attempt.pack_id,
                "管道编号": config.pipeline_no,
                "训练批次": batch_no,
                "日期分区": date,
                "来源版次": snapshot.source_revision,
                "行数": len(snapshot.rows),
                "编码": config.encoding,
                "压缩": config.compression,
                "签发时间": datetime.datetime.now().isoformat(),
                "内容摘要": digest,
            }
            attempt.status = PACK_STATUS_ISSUED
            attempt.index = index
            attempt.blob = blob
            self.backend.record_delivery(batch_no, index)
            results.append({"分区": date, "结果": "已签发", "包编号": attempt.pack_id,
                            "说明": f"来源版次 {index['来源版次']}，{index['行数']} 行"})

        issued = sum(1 for item in results if item["结果"] == "已签发")
        return {
            "训练批次": batch_no,
            "管道编号": pipeline_no,
            "结果": results,
            "本次交付": issued,
        }

    # -- 下载与读取 -----------------------------------------------------

    def _issued_attempt(self, pack_id: str) -> PackAttempt:
        attempt = self.attempts.get(pack_id)
        if attempt is None:
            raise PackageNotFound(f"数据包 {pack_id} 不存在")
        if attempt.status != PACK_STATUS_ISSUED or attempt.blob is None or attempt.index is None:
            raise PackageNotReady(f"数据包 {pack_id} 压缩未结束，不可下载")
        return attempt

    def read_index(self, pack_id: str) -> dict[str, Any]:
        attempt = self.attempts.get(pack_id)
        if attempt is None:
            raise PackageNotFound(f"数据包 {pack_id} 不存在")
        if attempt.status != PACK_STATUS_ISSUED or attempt.index is None:
            raise PackageNotReady(f"数据包 {pack_id} 尚未签发索引")
        return dict(attempt.index)

    def download(self, pack_id: str) -> bytes:
        """训练侧下载：只允许取走压缩已完成、已签发的文件体。"""
        return self._issued_attempt(pack_id).blob  # type: ignore[return-value]

    def open_rows(self, pack_id: str) -> list[dict[str, Any]]:
        """训练侧依据索引打开数据包：校验摘要后解压、解码。"""
        attempt = self._issued_attempt(pack_id)
        index = attempt.index or {}
        blob = attempt.blob or b""
        if hashlib.sha256(blob).hexdigest() != index.get("内容摘要"):
            raise TrainPackError(f"数据包 {pack_id} 内容摘要与索引不符")
        raw = decompress_bytes(str(index.get("压缩", "NONE")), blob)
        return decode_rows(str(index.get("编码", "JSON")), raw)

    # -- 回执 / 清单 / 覆盖区间 / 补算核对 -------------------------------

    def receipt(self, pack_id: str, consumer: str = "training") -> dict[str, Any]:
        """训练侧回执：特征存储据此更新批次已覆盖区间。"""
        attempt = self._issued_attempt(pack_id)
        index = attempt.index or {}
        covered = self.backend.acknowledge(
            str(index["训练批次"]), str(index["日期分区"]), index
        )
        return {
            "包编号": pack_id,
            "消费方": consumer,
            "训练批次": covered["批次"],
            "已覆盖区间": covered["已覆盖区间"],
        }

    def manifest(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self.backend.manifest]

    def coverage(self) -> dict[str, Any]:
        return {
            batch: {date: dict(info) for date, info in dates.items()}
            for batch, dates in self.backend.coverage.items()
        }

    def backfill_check(self, batch_no: str, date: str) -> dict[str, Any]:
        """安排补算前核对：交付版次 vs 存储当前版次。"""
        delivered = self.backend.coverage.get(batch_no, {}).get(date)
        current = f"r{self.backend.current_revision(date)}"
        if delivered is None:
            return {
                "训练批次": batch_no,
                "日期分区": date,
                "已交付版次": None,
                "当前版次": current,
                "版次一致": False,
                "建议": "该分区尚未交付，需按当前版次全量补算",
            }
        match = delivered["来源版次"] == current
        return {
            "训练批次": batch_no,
            "日期分区": date,
            "已交付版次": delivered["来源版次"],
            "当前版次": current,
            "版次一致": match,
            "建议": "交付版次与存储一致，无需补算" if match
            else "存储侧已有更新版次，安排补算时可对照交付包核对行版次",
        }

    def state(self) -> dict[str, Any]:
        return {
            "分区": {
                date: {
                    "记录数": len(records),
                    "当前版次": f"r{self.backend.revisions.get(date, 0)}",
                    "行版次": {key: rec.version for key, rec in records.items()},
                }
                for date, records in self.backend.partitions.items()
            },
            "快照": {
                date: {"来源版次": snap.source_revision, "行数": len(snap.rows)}
                for date, snap in self.snapshots.items()
            },
            "数据包": [
                {
                    "包编号": attempt.pack_id,
                    "训练批次": attempt.batch_no,
                    "日期分区": attempt.date,
                    "状态": attempt.status,
                    "索引": attempt.index,
                    "错误": attempt.error,
                }
                for attempt in self.attempts.values()
            ],
            "存储清单": self.manifest(),
            "已覆盖区间": self.coverage(),
        }


# ---------------------------------------------------------------------------
# 通用资源模块门面：交付结果在Workbench里可见，行数据由引擎同步
# ---------------------------------------------------------------------------


class TrainPackService(BaseModuleService):
    MODULE = "train_pack"
    ENTITY = "训练分区包"
    OPERATION_TERM = "分包"
    LIST_FIELDS = ["包编号", "管道编号", "训练批次", "日期分区", "来源版次", "行数", "压缩方式", "签发时间"]
    REQUIRED_FIELDS = ["包编号"]
    STATUS_ORDER = ["已签发", "已交付", "已作废"]
    ACTION_RULES = {"确认交付": "已交付", "重试打包": "已签发"}
    NEGATIVE_ACTIONS: list[str] = []

    def __init__(self) -> None:
        self.engine = TrainPackEngine()

    # -- 引擎 → 模块行同步 ----------------------------------------------

    def sync_from_engine(self) -> None:
        """把引擎的交付结果物化为模块行，供通用列表/导出接口消费。"""
        rows = store.rows(self.MODULE)
        by_pack = {str(row.get("包编号", "")): row for row in rows}
        for item in self.engine.manifest():
            pack_id = item["包编号"]
            row = by_pack.get(pack_id)
            if row is None:
                row = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
                rows.append(row)
                by_pack[pack_id] = row
            covered = self.engine.backend.coverage.get(item["批次"], {})
            delivered = item["日期分区"] in covered
            row.update({
                "包编号": pack_id,
                "管道编号": item["管道编号"],
                "训练批次": item["批次"],
                "日期分区": item["日期分区"],
                "来源版次": item["来源版次"],
                "行数": item["行数"],
                "压缩方式": self.engine.attempts[pack_id].index.get("压缩", "")
                if pack_id in self.engine.attempts and self.engine.attempts[pack_id].index
                else "",
                "签发时间": item["交付时间"],
                "status": "已交付" if delivered else "已签发",
                "pending": not delivered,
                "abnormal": False,
            })

    def list_entries(self, **kwargs: Any):
        self.sync_from_engine()
        return super().list_entries(**kwargs)

    def get_entry(self, entry_id: int):
        self.sync_from_engine()
        return super().get_entry(entry_id)

    def create_entry(self, values: dict[str, Any]):
        return None, ["训练分区包由离线导出引擎签发，请通过 /api/train-pack/export 导出"]

    # -- 动作：确认交付（回执）/ 重试打包（按当前存储版次重导） -------------

    def run_action(self, entry_id: int, action: str):
        self.sync_from_engine()
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        pack_id = str(entry.get("包编号", ""))
        if action == "确认交付":
            try:
                receipt = self.engine.receipt(pack_id)
            except TrainPackError as exc:
                return None, str(exc)
            self.sync_from_engine()
            entry = store.find(self.MODULE, entry_id)
            return entry, (
                f"{self.ENTITY} {pack_id} 已回执，"
                f"批次 {receipt['训练批次']} 已覆盖区间：{'、'.join(receipt['已覆盖区间'])}"
            )
        if action == "重试打包":
            try:
                result = self.engine.export_batch(
                    str(entry.get("管道编号", "")),
                    str(entry.get("训练批次", "")),
                    [str(entry.get("日期分区", ""))],
                    force=True,
                )
            except TrainPackError as exc:
                return None, str(exc)
            self.sync_from_engine()
            detail = result["结果"][0]
            if detail["结果"] != "已签发":
                return None, f"重试打包失败：{detail['说明']}"
            entry = store.find(self.MODULE, entry_id)
            return entry, f"已按当前存储版次重新签发 {detail['包编号']}，原包仍可按旧索引打开"
        return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
