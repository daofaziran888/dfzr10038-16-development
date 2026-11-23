"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.feature_reg import FeatureRegService
from app.services.derive_rule import DeriveRuleService
from app.services.time_window import TimeWindowService
from app.services.backfill_job import BackfillJobService
from app.services.sample_strata import SampleStrataService
from app.services.label_task import LabelTaskService
from app.services.label_review import LabelReviewService
from app.services.consent_calc import ConsentCalcService
from app.services.feature_store import FeatureStoreService
from app.services.online_serv import OnlineServService
from app.services.offline_exp import OfflineExpService
from app.services.feature_line import FeatureLineService
from app.services.feature_mon import FeatureMonService
from app.services.version_mgr import VersionMgrService
from app.services.data_split import DataSplitService
from app.services.feat_catalog import FeatCatalogService
from app.services.quality_rule import QualityRuleService
from app.services.label_dict import LabelDictService
from app.services.batch_load import BatchLoadService
from app.services.report_task import ReportTaskService
from app.services.schedule_job import ScheduleJobService
from app.services.govern_audit import GovernAuditService
from app.services.train_pack import TrainPackService

SERVICES: dict[str, BaseModuleService] = {
    "feature_reg": FeatureRegService(),
    "derive_rule": DeriveRuleService(),
    "time_window": TimeWindowService(),
    "backfill_job": BackfillJobService(),
    "sample_strata": SampleStrataService(),
    "label_task": LabelTaskService(),
    "label_review": LabelReviewService(),
    "consent_calc": ConsentCalcService(),
    "feature_store": FeatureStoreService(),
    "online_serv": OnlineServService(),
    "offline_exp": OfflineExpService(),
    "feature_line": FeatureLineService(),
    "feature_mon": FeatureMonService(),
    "version_mgr": VersionMgrService(),
    "data_split": DataSplitService(),
    "feat_catalog": FeatCatalogService(),
    "quality_rule": QualityRuleService(),
    "label_dict": LabelDictService(),
    "batch_load": BatchLoadService(),
    "report_task": ReportTaskService(),
    "schedule_job": ScheduleJobService(),
    "govern_audit": GovernAuditService(),
    "train_pack": TrainPackService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
