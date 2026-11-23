"""熔炉引擎：跨模块数据融合流水线编排器。

负责执行数据源 -> Schema 映射 -> 宽表定义 -> 融合作业的完整链路，
并管理冲突仲裁、质量门禁、分区快照、水位追踪与增量归并。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.models import (
    BackfillTask,
    ColdStoreStrategy,
    ComparisonReport,
    CrossValidatePlan,
    DataDriftRecord,
    ExportPlan,
    FusionJob,
    FusionMonitorMetric,
    LineageEdge,
    MergeConflictRule,
    MetricAggrRule,
    PartitionRule,
    QualityGateRule,
    SchemaMapping,
    SnapshotPartition,
    SourceDef,
    WatermarkRecord,
    WideTableDef,
    WideTableVersion,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_code(prefix: str, index: int) -> str:
    return f"{prefix}-{index:04d}"


class FusionEngine:
    """熔炉引擎：编排跨模块数据融合流水线。"""

    def __init__(self) -> None:
        self._sources: dict[int, SourceDef] = {}
        self._schemas: dict[int, SchemaMapping] = {}
        self._wide_tables: dict[int, WideTableDef] = {}
        self._fusion_jobs: dict[int, FusionJob] = {}
        self._conflict_rules: dict[int, MergeConflictRule] = {}
        self._quality_gates: dict[int, QualityGateRule] = {}
        self._snapshots: dict[int, SnapshotPartition] = {}
        self._backfills: dict[int, BackfillTask] = {}
        self._cold_stores: dict[int, ColdStoreStrategy] = {}
        self._lineages: dict[int, LineageEdge] = {}
        self._metrics: dict[int, MetricAggrRule] = {}
        self._partitions: dict[int, PartitionRule] = {}
        self._watermarks: dict[int, WatermarkRecord] = {}
        self._exports: dict[int, ExportPlan] = {}
        self._cross_validates: dict[int, CrossValidatePlan] = {}
        self._drifts: dict[int, DataDriftRecord] = {}
        self._monitors: dict[int, FusionMonitorMetric] = {}
        self._versions: dict[int, WideTableVersion] = {}
        self._reports: dict[int, ComparisonReport] = {}
        self._next_id: int = 1

    def _next_seq(self) -> int:
        sid = self._next_id
        self._next_id += 1
        return sid

    # ---- 1. source_def -> schema_map -> wide_table_def -> fusion_job ----

    def register_source(self, source_code: str, source_name: str, source_type: str,
                        connection_string: str, auth_method: str = "jdbc",
                        charset: str = "UTF-8", timezone: str = "Asia/Shanghai") -> SourceDef:
        sid = self._next_seq()
        source = SourceDef(
            source_id=sid, source_code=source_code, source_name=source_name,
            source_type=source_type, connection_string=connection_string,
            auth_method=auth_method, charset=charset, timezone=timezone,
            access_status="待验证",
        )
        self._sources[sid] = source
        return source

    def create_schema_mapping(self, source_table: str, target_wide_table: str,
                              field_mapping_json: str, type_conversion: str = "",
                              missing_policy: str = "skip") -> SchemaMapping:
        sid = self._next_seq()
        mapping = SchemaMapping(
            mapping_id=sid, plan_code=_generate_code("SCHE", sid),
            source_table=source_table, target_wide_table=target_wide_table,
            field_mapping_json=field_mapping_json,
            type_conversion_rules=type_conversion,
            missing_value_policy=missing_policy,
            mapping_order="ASC", plan_status="草稿",
        )
        self._schemas[sid] = mapping
        return mapping

    def define_wide_table(self, table_code: str, table_name: str,
                          field_list: list[str], primary_key: str = "id",
                          partition_key: str = "dt", ttl_days: int = 90) -> WideTableDef:
        sid = self._next_seq()
        wt = WideTableDef(
            table_id=sid, table_code=table_code, table_name=table_name,
            field_list=field_list, primary_key=primary_key,
            partition_key=partition_key, ttl_days=ttl_days,
            source_count=0, last_updated=_now_iso(),
        )
        self._wide_tables[sid] = wt
        return wt

    def create_fusion_job(self, target_wide_table: str, participant_sources: list[str],
                          join_key: str, fusion_window: str = "1d",
                          watermark_field: str = "update_time") -> FusionJob:
        sid = self._next_seq()
        job = FusionJob(
            job_id=sid, job_code=_generate_code("FUSI", sid),
            target_wide_table=target_wide_table,
            participant_sources=participant_sources,
            join_key=join_key, fusion_window=fusion_window,
            watermark_field=watermark_field,
            output_rows=0, skipped_rows=0,
        )
        self._fusion_jobs[sid] = job
        return job

    # ---- 2. fusion_job -> merge_conflict -> quality_gate -> snapshot_mgr ----

    def create_conflict_rule(self, target_wide_table: str, conflict_fields: list[str],
                             arbitration_strategy: str = "source_priority",
                             priority_source: str = "", time_baseline: str = "") -> MergeConflictRule:
        sid = self._next_seq()
        rule = MergeConflictRule(
            rule_id=sid, rule_code=_generate_code("MERG", sid),
            target_wide_table=target_wide_table,
            conflict_fields=conflict_fields,
            arbitration_strategy=arbitration_strategy,
            priority_source=priority_source,
            time_baseline=time_baseline or _now_iso(),
            conflict_count=0, rule_status="草稿",
        )
        self._conflict_rules[sid] = rule
        return rule

    def create_quality_gate(self, target_wide_table: str, check_type: str,
                            threshold_condition: str, trigger_action: str = "block",
                            block_level: str = "warning") -> QualityGateRule:
        sid = self._next_seq()
        gate = QualityGateRule(
            rule_id=sid, rule_code=_generate_code("QUAL", sid),
            target_wide_table=target_wide_table,
            check_type=check_type,
            threshold_condition=threshold_condition,
            trigger_action=trigger_action,
            block_level=block_level,
            checked_rows=0, last_triggered="",
        )
        self._quality_gates[sid] = gate
        return gate

    def create_snapshot(self, target_wide_table: str, partition_key_value: str,
                        dependency_partitions: list[str] | None = None,
                        schema_digest: str = "") -> SnapshotPartition:
        sid = self._next_seq()
        snap = SnapshotPartition(
            partition_id=sid,
            partition_code=_generate_code("SNAP", sid),
            target_wide_table=target_wide_table,
            partition_key_value=partition_key_value,
            record_count=0,
            schema_digest=schema_digest or f"SHA256-{uuid.uuid4().hex[:8]}",
            status_mark="创建中",
            dependency_partitions=dependency_partitions or [],
            created_time=_now_iso(),
        )
        self._snapshots[sid] = snap
        return snap

    # ---- 3. fusion_job -> watermark_track -> delta_merge ----

    def create_watermark(self, data_source: str, current_watermark: str,
                         data_origin: str = "fusion_job") -> WatermarkRecord:
        sid = self._next_seq()
        wm = WatermarkRecord(
            record_id=sid, record_code=_generate_code("WATE", sid),
            data_source=data_source,
            current_watermark=current_watermark,
            previous_watermark="",
            advance_interval="5m",
            delay_minutes=0,
            data_origin=data_origin,
            last_advance=_now_iso(),
        )
        self._watermarks[sid] = wm
        return wm

    def advance_watermark(self, record_id: int, new_watermark: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(record_id)
        if wm is None:
            return None
        wm.previous_watermark = wm.current_watermark
        wm.current_watermark = new_watermark
        wm.last_advance = _now_iso()
        return wm

    # ---- 4. snapshot_mgr -> backfill_task -> cold_store ----

    def create_backfill_task(self, target_wide_table: str, backfill_range: str,
                             parallel_partitions: int = 4, dependency_check: str = "",
                             checkpoint_position: str = "") -> BackfillTask:
        sid = self._next_seq()
        task = BackfillTask(
            task_id=sid, task_code=_generate_code("BACK", sid),
            target_wide_table=target_wide_table,
            backfill_range=backfill_range,
            parallel_partitions=parallel_partitions,
            dependency_check=dependency_check,
            checkpoint_position=checkpoint_position,
            processed_rows=0, remaining_rows=0,
        )
        self._backfills[sid] = task
        return task

    def create_cold_store_strategy(self, target_wide_table: str, hot_data_days: int,
                                   cold_storage_type: str = "parquet",
                                   compression_method: str = "zstd",
                                   migration_window: str = "02:00-06:00") -> ColdStoreStrategy:
        sid = self._next_seq()
        cs = ColdStoreStrategy(
            strategy_id=sid, strategy_code=_generate_code("COLD", sid),
            target_wide_table=target_wide_table,
            hot_data_days=hot_data_days,
            cold_storage_type=cold_storage_type,
            migration_window=migration_window,
            compression_method=compression_method,
            migrated_rows=0, last_migration="",
        )
        self._cold_stores[sid] = cs
        return cs

    # ---- 5. wide_table_def -> lineage_edge -> metric_aggr ----

    def create_lineage_edge(self, upstream_node: str, downstream_node: str,
                            related_fields: list[str], transform_function: str = "",
                            edge_type: str = "direct", dependency_level: int = 1) -> LineageEdge:
        sid = self._next_seq()
        edge = LineageEdge(
            edge_id=sid, edge_code=_generate_code("LINE", sid),
            upstream_node=upstream_node, downstream_node=downstream_node,
            related_fields=related_fields,
            transform_function=transform_function,
            edge_type=edge_type,
            dependency_level=dependency_level,
            last_traced=_now_iso(),
        )
        self._lineages[sid] = edge
        return edge

    def create_metric_aggr_rule(self, target_wide_table: str, agg_function: str,
                                group_dimensions: list[str], time_granularity: str = "1d",
                                precompute_window: str = "7d",
                                storage_table: str = "") -> MetricAggrRule:
        sid = self._next_seq()
        rule = MetricAggrRule(
            rule_id=sid, rule_code=_generate_code("METR", sid),
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
            time_granularity=time_granularity,
            precompute_window=precompute_window,
            storage_table=storage_table or f"aggr_{target_wide_table}",
            last_computed="",
        )
        self._metrics[sid] = rule
        return rule

    # ---- 6. partition_rule -> cold_store -> archive ----

    def create_partition_rule(self, target_wide_table: str, partition_type: str,
                              partition_field: str, retention_days: int = 30,
                              archive_policy: str = "move_to_cold") -> PartitionRule:
        sid = self._next_seq()
        rule = PartitionRule(
            rule_id=sid, strategy_code=_generate_code("PART", sid),
            target_wide_table=target_wide_table,
            partition_type=partition_type,
            partition_field=partition_field,
            retention_days=retention_days,
            archive_policy=archive_policy,
            partition_count=0,
            last_adjusted=_now_iso(),
        )
        self._partitions[sid] = rule
        return rule

    # ---- 7. snapshot_mgr -> export_snapshot ----

    def create_export_plan(self, target_wide_table: str, export_format: str = "parquet",
                           partition_filter: str = "", field_filter: list[str] | None = None,
                           export_path: str = "/exports", row_limit: int = 0) -> ExportPlan:
        sid = self._next_seq()
        plan = ExportPlan(
            plan_id=sid, plan_code=_generate_code("EXPO", sid),
            target_wide_table=target_wide_table,
            export_format=export_format,
            partition_filter=partition_filter,
            field_filter=field_filter or [],
            export_path=export_path,
            row_limit=row_limit,
            export_status="待导出",
        )
        self._exports[sid] = plan
        return plan

    # ---- 8. cross_validate -> data_drift -> fusion_monitor ----

    def create_cross_validate_plan(self, primary_wide_table: str,
                                   reference_wide_table: str, join_key: str,
                                   validate_fields: list[str],
                                   deviation_threshold: str = "0.05") -> CrossValidatePlan:
        sid = self._next_seq()
        plan = CrossValidatePlan(
            plan_id=sid, plan_code=_generate_code("CROS", sid),
            primary_wide_table=primary_wide_table,
            reference_wide_table=reference_wide_table,
            join_key=join_key,
            validate_fields=validate_fields,
            deviation_threshold=deviation_threshold,
            last_result="",
            validate_status="待校验",
        )
        self._cross_validates[sid] = plan
        return plan

    def create_drift_record(self, wide_table_name: str, drift_type: str,
                            historical_baseline: str, current_distribution: str,
                            kl_divergence: str = "0.0",
                            drift_direction: str = "none") -> DataDriftRecord:
        sid = self._next_seq()
        record = DataDriftRecord(
            record_id=sid, record_code=_generate_code("DATA", sid),
            wide_table_name=wide_table_name,
            drift_type=drift_type,
            historical_baseline=historical_baseline,
            current_distribution=current_distribution,
            kl_divergence=kl_divergence,
            drift_direction=drift_direction,
            detected_at=_now_iso(),
        )
        self._drifts[sid] = record
        return record

    def create_fusion_monitor(self, fusion_job: str, monitor_dimension: str,
                              collection_interval: str = "60s",
                              alert_threshold: str = "",
                              alert_channel: str = "webhook",
                              silence_window: str = "") -> FusionMonitorMetric:
        sid = self._next_seq()
        metric = FusionMonitorMetric(
            metric_id=sid, metric_code=_generate_code("FUSI", sid),
            fusion_job=fusion_job,
            monitor_dimension=monitor_dimension,
            collection_interval=collection_interval,
            alert_threshold=alert_threshold,
            alert_channel=alert_channel,
            silence_window=silence_window,
            last_sample=_now_iso(),
        )
        self._monitors[sid] = metric
        return metric

    # ---- 9. 版本化交叉校验对账：版本凭据、比较报告与漂移处置 ----

    def submit_wide_table_version(self, wide_table: str, order_day: str,
                                  amount: float, detail: dict[str, Any] | None = None,
                                  confirmed: bool = False) -> WideTableVersion:
        """提交同一订单日的一批宽表明细，生成版本凭据（默认提交中）。"""
        sid = self._next_seq()
        version_no = max(
            (v.version_no for v in self._versions.values()
             if v.wide_table == wide_table and v.order_day == order_day),
            default=0,
        ) + 1
        version = WideTableVersion(
            version_id=sid,
            wide_table=wide_table,
            order_day=order_day,
            version_no=version_no,
            amount=float(amount),
            detail=detail or {},
            submit_status="已确认" if confirmed else "提交中",
            submitted_at=_now_iso(),
        )
        self._versions[sid] = version
        return version

    def confirm_wide_table_version(self, version_id: int) -> WideTableVersion | None:
        """确认宽表版本：提交中的批次变为可参与对账的已确认凭据。"""
        version = self._versions.get(version_id)
        if version is None:
            return None
        version.submit_status = "已确认"
        return version

    def request_comparison(self, plan_id: int, order_day: str) -> dict[str, Any]:
        """值班请求比较：按订单日配对两侧同批次版本，生成或刷新对账报告。

        一轮对账只比较同版本号且均已确认的两张宽表明细；任一侧新版本
        仍在提交即版本冲突，报告保持待配对并注明原因。刷新与重试只作用
        于同一轮报告，不会把不同批次的订单当成同一轮对账。
        """
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        primary_latest = self._latest_version_no(plan.primary_wide_table, order_day)
        reference_latest = self._latest_version_no(plan.reference_wide_table, order_day)
        round_no = min(primary_latest, reference_latest)
        if round_no == 0:
            return {"ok": False,
                    "message": f"订单日 {order_day} 两侧尚未提交可配对的批次"}

        report = self._find_report(plan.plan_id, order_day, round_no)
        if report is not None and report.report_status in ("已确认", "已办结"):
            # 同一轮的刷新/重试：凭据已持久保存，直接复用，不重算不覆盖
            result: dict[str, Any] = {
                "ok": True,
                "paired": True,
                "refreshed": True,
                "report": self._report_view(report),
                "message": f"第 {round_no} 轮对账报告 {report.report_code} "
                           f"已{report.report_status}，刷新不重算",
            }
            if max(primary_latest, reference_latest) > round_no:
                result["message"] += "；存在更新的批次等待两侧到齐后开启新一轮"
            return result

        primary_version = self._version_of(plan.primary_wide_table, order_day, round_no)
        reference_version = self._version_of(plan.reference_wide_table, order_day, round_no)
        blocking = self._pair_blocking(plan, round_no, primary_version, reference_version)

        if report is None:
            sid = self._next_seq()
            report = ComparisonReport(
                report_id=sid,
                report_code=_generate_code("CMPR", sid),
                plan_id=plan.plan_id,
                plan_code=plan.plan_code,
                order_day=order_day,
                round_no=round_no,
                primary_table=plan.primary_wide_table,
                reference_table=plan.reference_wide_table,
            )
            self._reports[sid] = report

        if blocking:
            report.report_status = "待配对"
            report.pair_reason = "；".join(blocking)
            plan.validate_status = "校验中"
            return {
                "ok": True,
                "paired": False,
                "refreshed": False,
                "report": self._report_view(report),
                "reason": f"第 {round_no} 轮比较待配对：{report.pair_reason}",
            }

        # 两侧同批次版本均已确认：配对比较并把版本凭据持久保存到报告
        if primary_version is None or reference_version is None:
            return {"ok": False, "message": f"第 {round_no} 轮版本凭据缺失，无法比较"}
        difference = round(primary_version.amount - reference_version.amount, 6)
        report.primary_version = primary_version.version_no
        report.reference_version = reference_version.version_no
        report.primary_amount = primary_version.amount
        report.reference_amount = reference_version.amount
        report.difference = difference
        report.detail_snapshot = {
            "primary": dict(primary_version.detail),
            "reference": dict(reference_version.detail),
        }
        report.pair_reason = ""
        report.report_status = "已确认"
        report.compared_at = _now_iso()

        plan.validate_status = "偏差告警" if difference != 0 else "一致"
        plan.last_result = json.dumps({
            "report_code": report.report_code,
            "order_day": order_day,
            "round_no": round_no,
            "primary_version": report.primary_version,
            "reference_version": report.reference_version,
            "difference": difference,
            "drift_detected": difference != 0,
        }, ensure_ascii=False)

        drift_code = ""
        if difference != 0:
            drift = self._drift_for_report(report.report_code)
            if drift is None:
                drift = self.create_drift_record(
                    wide_table_name=plan.primary_wide_table,
                    drift_type="cross_validate_mismatch",
                    historical_baseline=(
                        f"参照宽表 {plan.reference_wide_table} "
                        f"v{reference_version.version_no} 金额 {reference_version.amount}"),
                    current_distribution=(
                        f"主体宽表 {plan.primary_wide_table} "
                        f"v{primary_version.version_no} 金额 {primary_version.amount}"),
                    drift_direction="up" if difference > 0 else "down",
                )
                drift.report_code = report.report_code
                drift.order_day = order_day
                drift.confirmable_difference = difference
            drift_code = drift.record_code

        return {
            "ok": True,
            "paired": True,
            "refreshed": False,
            "report": self._report_view(report),
            "drift_record_code": drift_code,
        }

    def claim_drift(self, plan_id: int, order_day: str) -> dict[str, Any]:
        """值班认领本次请求可用的漂移。

        优先领取同一订单日尚未办结的旧报告，按报告持久保存的凭据差额
        处理并留存其报告号；当前轮比较仍待配对时，在结果里说明本次
        领不到差异的原因。
        """
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        current = self._latest_report(plan.plan_id, order_day)
        current_round: dict[str, Any] = {}
        if current is not None:
            current_round = {
                "report_code": current.report_code,
                "round_no": current.round_no,
                "report_status": current.report_status,
                "reason": current.pair_reason,
            }

        # 同一订单日尚未办结、已确认且有差异的旧报告，按轮次先到先得
        for report in self._reports_for(plan.plan_id, order_day):
            if report.report_status != "已确认" or not report.difference:
                continue
            drift = self._drift_for_report(report.report_code)
            if drift is None:
                continue
            if drift.handle_status == "已检测":
                drift.handle_status = "评估中"
                message = f"已领取报告 {report.report_code} 的漂移，差额 {report.difference}"
            else:
                message = f"报告 {report.report_code} 的漂移复核中，继续按差额 {report.difference} 处理"
            return {
                "ok": True,
                "report_code": report.report_code,
                "difference": report.difference,
                "drift_record_id": drift.record_id,
                "drift_record_code": drift.record_code,
                "handle_status": drift.handle_status,
                "current_round": current_round,
                "message": message,
            }

        if current is not None and current.report_status == "待配对":
            reason = (f"第 {current.round_no} 轮比较仍待配对"
                      + (f"（{current.pair_reason}）" if current.pair_reason else ""))
        elif current is not None:
            reason = f"第 {current.round_no} 轮对账无差异或已办结"
        else:
            reason = "尚未发起比较"
        return {
            "ok": False,
            "message": f"本次请求领不到差异：{reason}",
            "current_round": current_round,
        }

    def confirm_drift(self, record_id: int) -> dict[str, Any]:
        """确认漂移（原有动作）：读比较版本凭据，把实际核过的版本对与差值留在处置结果里。

        处置结果只取自漂移记录指向的比较报告；报告凭据已持久保存，
        新比较不会覆盖旧处置，重复确认结果不变。
        """
        record = self._drifts.get(record_id)
        if record is None:
            return {"ok": False, "message": f"漂移记录 {record_id} 不存在"}
        if record.handle_status == "已确认" and record.handling_result:
            return {
                "ok": True,
                "drift_record_id": record.record_id,
                "drift_record_code": record.record_code,
                "handle_status": record.handle_status,
                "handling_result": json.loads(record.handling_result),
                "message": "漂移已确认，处置结果以原报告凭据为准，不被新比较覆盖",
            }
        report = self._find_report_by_code(record.report_code) if record.report_code else None
        if report is None or report.difference is None:
            return {"ok": False,
                    "message": "漂移记录缺少比较版本凭据，无法确认差异"}

        difference = round((report.primary_amount or 0.0) - (report.reference_amount or 0.0), 6)
        handling = {
            "report_code": report.report_code,
            "order_day": report.order_day,
            "round_no": report.round_no,
            "primary_table": report.primary_table,
            "reference_table": report.reference_table,
            "primary_version": report.primary_version,
            "reference_version": report.reference_version,
            "primary_amount": report.primary_amount,
            "reference_amount": report.reference_amount,
            "difference": difference,
            "confirmed_at": _now_iso(),
        }
        record.handling_result = json.dumps(handling, ensure_ascii=False)
        record.confirmable_difference = difference
        record.handle_status = "已确认"
        record.drift_direction = "up" if difference > 0 else "down" if difference < 0 else "none"
        report.report_status = "已办结"
        return {
            "ok": True,
            "drift_record_id": record.record_id,
            "drift_record_code": record.record_code,
            "handle_status": record.handle_status,
            "handling_result": handling,
            "message": f"漂移已确认：报告 {report.report_code} "
                       f"v{report.primary_version} 对 v{report.reference_version}，差值 {difference}",
        }

    def get_comparison_report(self, report_code: str) -> dict[str, Any] | None:
        """回查比较报告：读取持久保存的版本凭据与差异明细。"""
        report = self._find_report_by_code(report_code)
        return self._report_view(report) if report is not None else None

    def list_comparison_reports(self, plan_id: int, order_day: str) -> list[dict[str, Any]]:
        """列出同一订单日的全部对账报告（按轮次排序）。"""
        return [self._report_view(r) for r in self._reports_for(plan_id, order_day)]

    def get_drift_record(self, record_id: int) -> DataDriftRecord | None:
        return self._drifts.get(record_id)

    def recompute_report_difference(self, report_code: str) -> float | None:
        """复算原来的漂移差异：只读报告持久保存的版本凭据，不看新批次。"""
        report = self._find_report_by_code(report_code)
        if report is None or report.primary_amount is None or report.reference_amount is None:
            return None
        return round(report.primary_amount - report.reference_amount, 6)

    def _version_of(self, wide_table: str, order_day: str,
                    version_no: int) -> WideTableVersion | None:
        for version in self._versions.values():
            if (version.wide_table == wide_table and version.order_day == order_day
                    and version.version_no == version_no):
                return version
        return None

    def _latest_version_no(self, wide_table: str, order_day: str) -> int:
        return max(
            (v.version_no for v in self._versions.values()
             if v.wide_table == wide_table and v.order_day == order_day),
            default=0,
        )

    @staticmethod
    def _pair_blocking(plan: CrossValidatePlan, round_no: int,
                       primary_version: WideTableVersion | None,
                       reference_version: WideTableVersion | None) -> list[str]:
        """列出第 round_no 轮还不能配对的原因（版本冲突明细）。"""
        blocking: list[str] = []
        for label, table, version in (
            ("主体", plan.primary_wide_table, primary_version),
            ("参照", plan.reference_wide_table, reference_version),
        ):
            if version is None:
                blocking.append(f"{label}宽表 {table} v{round_no} 尚未提交")
            elif version.submit_status != "已确认":
                blocking.append(f"{label}宽表 {table} v{round_no} 仍在提交（版本冲突）")
        return blocking

    def _find_report(self, plan_id: int, order_day: str,
                     round_no: int) -> ComparisonReport | None:
        for report in self._reports.values():
            if (report.plan_id == plan_id and report.order_day == order_day
                    and report.round_no == round_no):
                return report
        return None

    def _find_report_by_code(self, report_code: str) -> ComparisonReport | None:
        for report in self._reports.values():
            if report.report_code == report_code:
                return report
        return None

    def _reports_for(self, plan_id: int, order_day: str) -> list[ComparisonReport]:
        return sorted(
            (r for r in self._reports.values()
             if r.plan_id == plan_id and r.order_day == order_day),
            key=lambda r: r.round_no,
        )

    def _latest_report(self, plan_id: int, order_day: str) -> ComparisonReport | None:
        reports = self._reports_for(plan_id, order_day)
        return reports[-1] if reports else None

    def _drift_for_report(self, report_code: str) -> DataDriftRecord | None:
        for drift in self._drifts.values():
            if drift.report_code == report_code:
                return drift
        return None

    @staticmethod
    def _report_view(report: ComparisonReport) -> dict[str, Any]:
        """报告视图：注明实际比较过的两端与持久保存的版本凭据。"""
        return {
            "report_code": report.report_code,
            "plan_code": report.plan_code,
            "order_day": report.order_day,
            "round_no": report.round_no,
            "primary_table": report.primary_table,
            "reference_table": report.reference_table,
            "primary_version": report.primary_version,
            "reference_version": report.reference_version,
            "primary_amount": report.primary_amount,
            "reference_amount": report.reference_amount,
            "difference": report.difference,
            "detail_snapshot": report.detail_snapshot,
            "pair_reason": report.pair_reason,
            "report_status": report.report_status,
            "compared_at": report.compared_at,
        }

    # ---- 复合融合操作 ----

    def execute_fusion_pipeline(self, job_id: int) -> dict[str, Any]:
        """执行完整融合流水线：数据源读取 -> 映射转换 -> 冲突仲裁 -> 质量检查 -> 产出快照。

        返回值包含融合记录、冲突日志、质量报告和快照信息。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        raw_records: list[dict[str, Any]] = []
        for source_code in job.participant_sources:
            source = self._find_source_by_code(source_code)
            if source is None:
                continue
            source_records = self._simulate_source_read(source)
            for rec in source_records:
                rec["_source"] = source_code
                rec["_fusion_key"] = f"{source_code}#{rec.get(job.join_key, '')}"
                rec["_timestamp"] = _now_iso()
                rec["_version"] = 1
            raw_records.extend(source_records)

        mapped_records = self._apply_schema_mappings(raw_records, job.target_wide_table)

        job.output_rows = len(mapped_records)
        job.skipped_rows = len(raw_records) - len(mapped_records)
        job.merged_records = mapped_records

        result: dict[str, Any] = {
            "ok": True,
            "job_code": job.job_code,
            "total_sources": len(job.participant_sources),
            "raw_records": len(raw_records),
            "mapped_records": len(mapped_records),
            "skipped_records": job.skipped_rows,
            "records": mapped_records,
        }

        conflict_result = self._apply_conflict_resolution(mapped_records, job.target_wide_table)
        result["conflict_resolution"] = conflict_result

        quality_result = self._apply_quality_gates(
            conflict_result.get("resolved", mapped_records),
            job.target_wide_table,
        )
        result["quality_gate"] = quality_result

        if quality_result.get("passed", True):
            snap = self.create_snapshot(
                job.target_wide_table,
                partition_key_value=datetime.now(timezone.utc).strftime("%Y%m%d"),
            )
            snap.record_count = len(conflict_result.get("resolved", mapped_records))
            result["snapshot"] = {"partition_code": snap.partition_code, "record_count": snap.record_count}

        return result

    def execute_delta_merge(self, job_id: int, delta_records: list[dict[str, Any]],
                            watermark_record_id: int = 0) -> dict[str, Any]:
        """执行增量归并：将增量数据合并到融合作业的输出中。"""
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        existing = {self._make_merge_key(r, job.join_key): r for r in job.merged_records}
        inserted = 0
        updated = 0
        deleted = 0

        for delta in delta_records:
            key = self._make_merge_key(delta, job.join_key)
            op = delta.get("_op", "upsert")
            if op == "delete":
                if key in existing:
                    del existing[key]
                    deleted += 1
            elif key in existing:
                existing[key].update(delta)
                updated += 1
            else:
                existing[key] = dict(delta)
                inserted += 1

        job.merged_records = list(existing.values())

        if watermark_record_id > 0:
            wm = self._watermarks.get(watermark_record_id)
            if wm is not None:
                wm.previous_watermark = wm.current_watermark
                wm.current_watermark = _now_iso()

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "total_after_merge": len(job.merged_records),
        }

    def execute_backfill_with_checkpoint(self, backfill_id: int,
                                         source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """执行带断点续传的数据回填。

        从 checkpoint_position 处继续处理，失败时保留断点可恢复。
        """
        task = self._backfills.get(backfill_id)
        if task is None:
            return {"ok": False, "message": f"回填任务 {backfill_id} 不存在"}

        start_idx = 0
        if task.checkpoint_position:
            try:
                start_idx = int(task.checkpoint_position)
            except (ValueError, TypeError):
                start_idx = 0

        batch_size = max(task.parallel_partitions * 100, 100)
        end_idx = min(start_idx + batch_size, len(source_records))
        batch = source_records[start_idx:end_idx]

        processed = len(batch)
        task.processed_rows += processed
        task.remaining_rows = max(0, len(source_records) - end_idx)

        if end_idx < len(source_records):
            task.checkpoint_position = str(end_idx)
            complete = False
        else:
            task.checkpoint_position = ""
            complete = True

        return {
            "ok": True,
            "batch_processed": processed,
            "total_processed": task.processed_rows,
            "remaining": task.remaining_rows,
            "checkpoint": task.checkpoint_position,
            "complete": complete,
        }

    def migrate_to_cold_store(self, strategy_id: int,
                              hot_partition_cutoff: str) -> dict[str, Any]:
        """执行冷热分层迁移：将超过热数据保留期的分区迁移到冷存储。"""
        strategy = self._cold_stores.get(strategy_id)
        if strategy is None:
            return {"ok": False, "message": f"冷热分层策略 {strategy_id} 不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == strategy.target_wide_table
            and snap.partition_key_value < hot_partition_cutoff
        ]
        migrated = 0
        for snap in eligible_snaps:
            migrated += snap.record_count
            snap.status_mark = "已迁移至冷存储"

        strategy.migrated_rows += migrated
        strategy.last_migration = _now_iso()

        return {
            "ok": True,
            "migrated_partitions": len(eligible_snaps),
            "migrated_rows": migrated,
            "strategy_code": strategy.strategy_code,
        }

    def execute_export_from_snapshot(self, export_id: int,
                                     snapshot_id: int) -> dict[str, Any]:
        """从分区快照执行数据导出。"""
        plan = self._exports.get(export_id)
        snap = self._snapshots.get(snapshot_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}

        fusion_job = self._find_fusion_job_by_table(snap.target_wide_table)
        records = fusion_job.merged_records if fusion_job else []

        if plan.field_filter:
            records = [
                {k: v for k, v in rec.items() if k in plan.field_filter}
                for rec in records
            ]

        if plan.row_limit > 0 and len(records) > plan.row_limit:
            records = records[:plan.row_limit]

        plan.export_status = "已完成"

        return {
            "ok": True,
            "plan_code": plan.plan_code,
            "snapshot_code": snap.partition_code,
            "exported_rows": len(records),
            "format": plan.export_format,
            "path": plan.export_path,
            "records": records,
        }

    def execute_cross_validation(self, plan_id: int) -> dict[str, Any]:
        """执行跨宽表交叉校验，检测数据一致性偏差。"""
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        primary_job = self._find_fusion_job_by_table(plan.primary_wide_table)
        ref_job = self._find_fusion_job_by_table(plan.reference_wide_table)

        primary_records = primary_job.merged_records if primary_job else []
        ref_records = ref_job.merged_records if ref_job else []

        primary_by_key = {str(r.get(plan.join_key, "")): r for r in primary_records}
        ref_by_key = {str(r.get(plan.join_key, "")): r for r in ref_records}

        mismatches: list[dict[str, Any]] = []
        drift_detected = False

        for key in set(primary_by_key.keys()) & set(ref_by_key.keys()):
            p = primary_by_key[key]
            r = ref_by_key[key]
            for field in plan.validate_fields:
                pv = str(p.get(field, ""))
                rv = str(r.get(field, ""))
                if pv != rv:
                    mismatches.append({
                        "key": key, "field": field,
                        "primary_value": pv, "reference_value": rv,
                    })
                    drift_detected = True

        plan.validate_status = "偏差告警" if mismatches else "一致"
        plan.last_result = json.dumps({
            "matched_keys": len(set(primary_by_key.keys()) & set(ref_by_key.keys())),
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
        }, ensure_ascii=False)

        result: dict[str, Any] = {
            "ok": True,
            "plan_code": plan.plan_code,
            "validate_status": plan.validate_status,
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
            "mismatches": mismatches[:20],
        }

        if drift_detected:
            drift_record = self.create_drift_record(
                wide_table_name=plan.primary_wide_table,
                drift_type="schema_mismatch",
                historical_baseline="baseline_" + _now_iso(),
                current_distribution="current_" + _now_iso(),
            )
            result["drift_record_code"] = drift_record.record_code

        return result

    def reconcile_partition_archive(self, partition_rule_id: int,
                                    cold_strategy_id: int) -> dict[str, Any]:
        """根据分区策略和冷热分层策略执行归档。

        将超出保留期的分区数据从冷存储转移到归档。
        """
        part_rule = self._partitions.get(partition_rule_id)
        cold_strat = self._cold_stores.get(cold_strategy_id)

        if part_rule is None or cold_strat is None:
            return {"ok": False, "message": "分区策略或冷热分层策略不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == part_rule.target_wide_table
            and snap.status_mark == "已迁移至冷存储"
        ]

        archived = 0
        for snap in eligible_snaps:
            snap.status_mark = "已归档"
            archived += snap.record_count

        return {
            "ok": True,
            "archived_partitions": len(eligible_snaps),
            "archived_rows": archived,
            "archive_policy": part_rule.archive_policy,
        }

    def reconcile_watermark_with_fusion(self, job_id: int) -> dict[str, Any]:
        """协调水位线与融合作业的推进状态。

        水位延迟时推进受限，融合作业未完成时增量归并暂停。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        related_watermarks = [
            wm for wm in self._watermarks.values()
            if wm.data_source in job.participant_sources
        ]

        stalled = [wm for wm in related_watermarks if wm.delay_minutes > 30]
        advancing = [wm for wm in related_watermarks if wm.delay_minutes <= 30]

        return {
            "ok": True,
            "job_code": job.job_code,
            "total_watermarks": len(related_watermarks),
            "advancing": len(advancing),
            "stalled": len(stalled),
            "can_proceed": len(stalled) == 0,
        }

    def build_wide_table_from_fusion(self, wide_table_id: int, fusion_job_id: int) -> dict[str, Any]:
        """从融合作业结果构建宽表定义，产生数据血缘边。"""
        wt = self._wide_tables.get(wide_table_id)
        job = self._fusion_jobs.get(fusion_job_id)

        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在"}
        if job is None:
            return {"ok": False, "message": f"融合作业 {fusion_job_id} 不存在"}

        wt.source_count = len(job.participant_sources)
        wt.last_updated = _now_iso()

        lineages: list[str] = []
        for source_code in job.participant_sources:
            edge = self.create_lineage_edge(
                upstream_node=source_code,
                downstream_node=wt.table_name,
                related_fields=wt.field_list,
                edge_type="fusion_map",
                dependency_level=1,
            )
            lineages.append(edge.edge_code)

        return {
            "ok": True,
            "table_code": wt.table_code,
            "source_count": wt.source_count,
            "lineage_edges": lineages,
            "last_updated": wt.last_updated,
        }

    def compute_metric_from_lineage(self, lineage_edge_id: int,
                                    target_wide_table: str,
                                    agg_function: str,
                                    group_dimensions: list[str]) -> dict[str, Any]:
        """基于数据血缘边计算指标聚合规则。"""
        edge = self._lineages.get(lineage_edge_id)
        if edge is None:
            return {"ok": False, "message": f"数据血缘边 {lineage_edge_id} 不存在"}

        rule = self.create_metric_aggr_rule(
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
        )
        rule.last_computed = _now_iso()

        return {
            "ok": True,
            "metric_rule_code": rule.rule_code,
            "from_lineage": edge.edge_code,
            "agg_function": agg_function,
            "group_dimensions": group_dimensions,
        }

    # ---- 辅助方法 ----

    def _find_source_by_code(self, source_code: str) -> SourceDef | None:
        for source in self._sources.values():
            if source.source_code == source_code:
                return source
        return None

    def _find_fusion_job_by_table(self, wide_table_name: str) -> FusionJob | None:
        for job in self._fusion_jobs.values():
            if job.target_wide_table == wide_table_name:
                return job
        return None

    def _simulate_source_read(self, source: SourceDef) -> list[dict[str, Any]]:
        """模拟从数据源读取样本记录。"""
        return [
            {"id": f"{source.source_code}-rec-1", "name": f"{source.source_name}-rec1",
             "value": 100.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-2", "name": f"{source.source_name}-rec2",
             "value": 200.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-3", "name": f"{source.source_name}-rec3",
             "value": 300.0, "update_time": _now_iso()},
        ]

    def _apply_schema_mappings(self, records: list[dict[str, Any]],
                               target_wide_table: str) -> list[dict[str, Any]]:
        """应用 Schema 映射：对关联到目标宽表的映射方案进行字段转换。"""
        relevant = [
            m for m in self._schemas.values()
            if m.target_wide_table == target_wide_table
        ]
        if not relevant:
            return list(records)

        mapped: list[dict[str, Any]] = []
        for rec in records:
            source_table = rec.get("_source", "")
            applicable = [m for m in relevant if m.source_table == source_table]
            if applicable:
                new_rec = dict(rec)
                for mapping in applicable:
                    try:
                        field_map = json.loads(mapping.field_mapping_json)
                        for src_field, tgt_field in field_map.items():
                            if src_field in new_rec:
                                new_rec[tgt_field] = new_rec.pop(src_field)
                    except (json.JSONDecodeError, KeyError):
                        pass
                mapped.append(new_rec)
            else:
                mapped.append(rec)
        return mapped

    def _apply_conflict_resolution(self, records: list[dict[str, Any]],
                                   target_wide_table: str) -> dict[str, Any]:
        """应用冲突仲裁规则到融合记录。"""
        rules = [
            r for r in self._conflict_rules.values()
            if r.target_wide_table == target_wide_table and r.rule_status == "生效中"
        ]
        if not rules:
            return {"applied": False, "reason": "无生效的冲突规则", "resolved": records}

        resolved = list(records)
        all_conflicts: list[dict[str, Any]] = []
        for rule in rules:
            resolved, conflicts = rule.resolve(resolved)
            rule.conflict_count = len(conflicts)
            all_conflicts.extend(conflicts)

        return {
            "applied": True,
            "rules_used": len(rules),
            "conflicts_found": len(all_conflicts),
            "conflicts": all_conflicts[:50],
            "resolved": resolved,
        }

    def _apply_quality_gates(self, records: list[dict[str, Any]],
                             target_wide_table: str) -> dict[str, Any]:
        """应用质量门禁检查。"""
        gates = [
            g for g in self._quality_gates.values()
            if g.target_wide_table == target_wide_table
        ]
        if not gates:
            return {"passed": True, "gates_checked": 0, "violations": []}

        violations: list[dict[str, Any]] = []
        for gate in gates:
            gate.checked_rows = len(records)
            if gate.check_type == "row_count_min" and len(records) < int(gate.threshold_condition):
                violations.append({"rule_code": gate.rule_code, "check": "row_count_min",
                                   "actual": len(records), "threshold": gate.threshold_condition})
            elif gate.check_type == "null_check":
                null_count = sum(
                    1 for rec in records
                    if rec.get(gate.threshold_condition) is None
                )
                if null_count > 0:
                    violations.append({"rule_code": gate.rule_code, "check": "null_check",
                                       "null_count": null_count})

        passed = len(violations) == 0
        return {"passed": passed, "gates_checked": len(gates), "violations": violations}

    @staticmethod
    def _make_merge_key(record: dict[str, Any], join_key: str) -> str:
        return f"{record.get('_source', '_')}#{record.get(join_key, '_')}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出引擎全局数据质量快照。"""
        return {
            "sources": len(self._sources),
            "schemas": len(self._schemas),
            "wide_tables": len(self._wide_tables),
            "fusion_jobs": len(self._fusion_jobs),
            "conflict_rules": len(self._conflict_rules),
            "quality_gates": len(self._quality_gates),
            "snapshots": len(self._snapshots),
            "backfills": len(self._backfills),
            "cold_stores": len(self._cold_stores),
            "lineages": len(self._lineages),
            "metrics": len(self._metrics),
            "partitions": len(self._partitions),
            "watermarks": len(self._watermarks),
            "exports": len(self._exports),
            "cross_validates": len(self._cross_validates),
            "drifts": len(self._drifts),
            "monitors": len(self._monitors),
            "wide_table_versions": len(self._versions),
            "comparison_reports": len(self._reports),
        }


# 全局单例供测试和跨模块消费
fusion_engine = FusionEngine()