"""场景验证：训练分区数据包的快照导出、更正隔离、失败重试与交付索引核对。

场景（与需求逐条对应）：
1. DAY-01 含记录 A、B，取完快照；DAY-02 快照后压缩被打断；
2. 存储此时接到 DAY-01 记录 B 的更正（行版次抬升）；
3. 训练侧依据索引打开 DAY-01 包，解压逐行核对 B 仍是快照时的版次；
4. 压缩未结束的 DAY-02 不可下载；
5. 补交第 2 日：重试只为实际得到的分区产生交付结果；
6. 核对两日交付索引（管道、分区、来源版次、行数）；
7. 回执更新批次已覆盖区间，补算时核对对应版次；
8. 已签发索引始终能打开原数据（更正不会覆盖旧包）。

运行：cd backend && .venv/bin/python tests/train_pack_scenario.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.offline_exp import OfflineExpService
from app.services.train_pack import (
    PackageNotReady,
    TrainPackEngine,
    TrainPackService,
    resolve_registered_pipeline,
)

PASS = "\033[32m通过\033[0m"
_checks = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global _checks
    if not condition:
        raise AssertionError(f"校验失败：{label} {detail}")
    _checks += 1
    print(f"  [{PASS}] {label}{(' —— ' + detail) if detail else ''}")


def main() -> None:
    print("== 0. 登记导出管道（编码/压缩沿用登记配置） ==")
    offline = OfflineExpService()
    entry, missing = offline.create_entry({
        "管道编号": "OFFL-SCEN-01",
        "特征范围": "user_age_7d,user_ctr_7d",
        "导出格式": "JSON",
        "分区策略": "按日分区",
        "批次大小": "100000",
        "压缩方式": "GZIP",
        "目标地址": "s3://train-exports/scenario/",
        "最后导出": "-",
    })
    check("管道登记成功", entry is not None and not missing, "OFFL-SCEN-01 JSON+GZIP")
    config = resolve_registered_pipeline("OFFL-SCEN-01")
    check("解析到登记配置", config.encoding == "JSON" and config.compression == "GZIP")

    service = TrainPackService()
    engine: TrainPackEngine = service.engine
    batch = "TRAIN-2026-11-23"

    print("\n== 1. DAY-01 含记录 A、B；两日取快照 ==")
    engine.ingest("DAY-01", [
        {"记录": "A", "值": {"user_age_7d": 31, "user_ctr_7d": 0.11}},
        {"记录": "B", "值": {"user_age_7d": 45, "user_ctr_7d": 0.07}},
    ])
    engine.ingest("DAY-02", [
        {"记录": "C", "值": {"user_age_7d": 28, "user_ctr_7d": 0.19}},
        {"记录": "D", "值": {"user_age_7d": 52, "user_ctr_7d": 0.05}},
    ])
    snap1 = engine.take_snapshot("DAY-01")
    snap2 = engine.take_snapshot("DAY-02")
    check("DAY-01 快照", snap1["来源版次"] == "r1" and snap1["行数"] == 2, str(snap1))
    check("DAY-02 快照", snap2["来源版次"] == "r1" and snap2["行数"] == 2, str(snap2))

    print("\n== 2. 首次导出：DAY-01 已签发，DAY-02 压缩被打断 ==")
    result = engine.export_batch("OFFL-SCEN-01", batch, ["DAY-01", "DAY-02"],
                                 interrupt={"DAY-02"})
    by_date = {item["分区"]: item for item in result["结果"]}
    check("DAY-01 已签发", by_date["DAY-01"]["结果"] == "已签发",
          f"包编号 {by_date['DAY-01']['包编号']}")
    check("DAY-02 打包失败", by_date["DAY-02"]["结果"] == "失败",
          by_date["DAY-02"]["说明"])
    pack_day1 = by_date["DAY-01"]["包编号"]
    pack_day2_failed = by_date["DAY-02"]["包编号"]
    check("本次仅 1 个分区交付", result["本次交付"] == 1)
    check("存储清单仅含 DAY-01", [m["日期分区"] for m in engine.manifest()] == ["DAY-01"])

    print("\n== 3. 存储接到 DAY-01 记录 B 的更正 ==")
    corr = engine.correct("DAY-01", "B", {"user_age_7d": 46, "user_ctr_7d": 0.09})
    check("B 行版次抬升为 2", corr["行版次"] == 2, str(corr))
    check("DAY-01 分区修订号抬升为 r2", corr["当前版次"] == "r2")

    print("\n== 4. 训练侧依据索引打开 DAY-01 包，逐行核对 B 的版次 ==")
    index1 = engine.read_index(pack_day1)
    check("索引注明管道", index1["管道编号"] == "OFFL-SCEN-01")
    check("索引注明分区", index1["日期分区"] == "DAY-01")
    check("索引注明来源版次", index1["来源版次"] == "r1", "快照之后的更正不回写旧包")
    check("索引注明行数", index1["行数"] == 2)
    check("索引注明编码与压缩", index1["编码"] == "JSON" and index1["压缩"] == "GZIP")
    rows = engine.open_rows(pack_day1)  # 校验摘要 → 解压 → 解码
    versions = {row["记录"]: row["版次"] for row in rows}
    check("解压后逐行核对：A 版次 1", versions.get("A") == 1)
    check("解压后逐行核对：B 版次 1（非更正后的 2）", versions.get("B") == 1,
          f"实际 {versions}")
    row_b = next(row for row in rows if row["记录"] == "B")
    check("B 的行值仍是快照值", row_b["值"] == {"user_age_7d": 45, "user_ctr_7d": 0.07})

    print("\n== 5. 压缩未结束的 DAY-02 不可下载 ==")
    try:
        engine.download(pack_day2_failed)
    except PackageNotReady as exc:
        check("下载被拒绝", True, str(exc))
    else:
        check("下载被拒绝", False, "压缩失败的包竟然可下载")

    print("\n== 6. 补交第 2 日：重试只为实际得到的分区产生交付结果 ==")
    retry = engine.export_batch("OFFL-SCEN-01", batch, ["DAY-01", "DAY-02"])
    by_date2 = {item["分区"]: item for item in retry["结果"]}
    check("DAY-01 已交付跳过", by_date2["DAY-01"]["结果"] == "已交付",
          "重试不重复交付")
    check("DAY-02 补交成功", by_date2["DAY-02"]["结果"] == "已签发",
          f"包编号 {by_date2['DAY-02']['包编号']}")
    pack_day2 = by_date2["DAY-02"]["包编号"]
    check("本次重试仅交付 1 个分区", retry["本次交付"] == 1)
    check("存储清单现有两日", [m["日期分区"] for m in engine.manifest()] == ["DAY-01", "DAY-02"])

    print("\n== 7. 核对两日交付索引 ==")
    index2 = engine.read_index(pack_day2)
    check("DAY-01 索引：管道/分区/版次/行数",
          (index1["管道编号"], index1["日期分区"], index1["来源版次"], index1["行数"])
          == ("OFFL-SCEN-01", "DAY-01", "r1", 2))
    check("DAY-02 索引：管道/分区/版次/行数",
          (index2["管道编号"], index2["日期分区"], index2["来源版次"], index2["行数"])
          == ("OFFL-SCEN-01", "DAY-02", "r1", 2))
    rows2 = engine.open_rows(pack_day2)
    check("DAY-02 解压行数与索引一致", len(rows2) == index2["行数"] == 2,
          f"记录 {sorted(row['记录'] for row in rows2)}")
    check("两日索引摘要各自对应真实文件",
          index1["内容摘要"] != index2["内容摘要"])

    print("\n== 8. 回执 → 批次已覆盖区间；模块清单同步 ==")
    receipt1 = engine.receipt(pack_day1)
    receipt2 = engine.receipt(pack_day2)
    check("批次已覆盖区间含两日",
          receipt2["已覆盖区间"] == ["DAY-01", "DAY-02"], str(receipt2["已覆盖区间"]))
    service.sync_from_engine()
    rows_mod, total = service.list_entries()
    check("训练分包清单 2 条", total == 2)
    check("两日均为已交付", all(row["status"] == "已交付" for row in rows_mod),
          str([row["status"] for row in rows_mod]))

    print("\n== 9. 安排补算时核对对应版次 ==")
    chk1 = engine.backfill_check(batch, "DAY-01")
    chk2 = engine.backfill_check(batch, "DAY-02")
    check("DAY-01 交付 r1 ≠ 当前 r2，建议补算",
          chk1["已交付版次"] == "r1" and chk1["当前版次"] == "r2" and not chk1["版次一致"],
          chk1["建议"])
    check("DAY-02 交付 r1 = 当前 r1，无需补算",
          chk2["版次一致"] is True, chk2["建议"])

    print("\n== 10. 更正后重导签发新包，旧索引仍能打开原数据 ==")
    redo = engine.export_batch("OFFL-SCEN-01", batch, ["DAY-01"], force=True)
    pack_day1_v2 = redo["结果"][0]["包编号"]
    index1_new = engine.read_index(pack_day1_v2)
    check("新包来源版次 r2", index1_new["来源版次"] == "r2")
    rows_new = engine.open_rows(pack_day1_v2)
    versions_new = {row["记录"]: row["版次"] for row in rows_new}
    check("新包内 B 版次 2", versions_new.get("B") == 2, f"实际 {versions_new}")
    rows_old = engine.open_rows(pack_day1)
    versions_old = {row["记录"]: row["版次"] for row in rows_old}
    check("旧包索引仍打开原数据：B 版次 1", versions_old.get("B") == 1,
          "训练结果可按旧索引复算")
    check("旧索引来源版次仍是 r1", engine.read_index(pack_day1)["来源版次"] == "r1")

    print(f"\n全部 {_checks} 项校验通过。")


if __name__ == "__main__":
    main()
