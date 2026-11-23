"""交叉校验版本一致性与漂移处置测试。

验证比较版本凭据的持久保存：一轮对账只使用配对的两张宽表明细，
版本冲突时报告待配对，漂移处置始终读凭据、不被新比较覆盖。
"""
from __future__ import annotations

import json

import pytest

from app.fusion_engine import FusionEngine

ORDER_DAY = "2026-11-23"
PRIMARY_TABLE = "WT_ORDER_SUBJECT"
REFERENCE_TABLE = "WT_ORDER_REFER"


@pytest.fixture
def engine() -> FusionEngine:
    """每个测试获得独立引擎实例。"""
    return FusionEngine()


def _make_plan(engine: FusionEngine):
    return engine.create_cross_validate_plan(
        primary_wide_table=PRIMARY_TABLE,
        reference_wide_table=REFERENCE_TABLE,
        join_key="order_id",
        validate_fields=["amount"],
    )


def _submit_confirmed_pair_v1(engine: FusionEngine) -> None:
    """两侧 v1 均已确认：主体金额 10、参照金额 8。"""
    engine.submit_wide_table_version(
        PRIMARY_TABLE, ORDER_DAY, 10.0,
        detail={"order_id": "ORD-1", "amount": 10.0}, confirmed=True)
    engine.submit_wide_table_version(
        REFERENCE_TABLE, ORDER_DAY, 8.0,
        detail={"order_id": "ORD-1", "amount": 8.0}, confirmed=True)


class TestVersionedComparisonReport:
    """比较版本凭据的持久保存与配对规则。"""

    def test_confirmed_report_persists_version_credentials(self, engine: FusionEngine) -> None:
        """先比主体金额 10 与参照金额 8，留下已确认报告，注明实际比较过的两端。"""
        plan = _make_plan(engine)
        _submit_confirmed_pair_v1(engine)

        result = engine.request_comparison(plan.plan_id, ORDER_DAY)
        assert result["ok"] is True
        assert result["paired"] is True

        report = result["report"]
        assert report["report_status"] == "已确认"
        # 报告注明实际比较过的两端：两侧宽表 + 版本凭据 + 金额
        assert report["primary_table"] == PRIMARY_TABLE
        assert report["reference_table"] == REFERENCE_TABLE
        assert report["primary_version"] == 1
        assert report["reference_version"] == 1
        assert report["primary_amount"] == 10.0
        assert report["reference_amount"] == 8.0
        assert report["difference"] == 2.0
        # 差异产生漂移记录，可确认差额随凭据持久保存
        assert result["drift_record_code"]

    def test_refresh_and_retry_stay_on_same_round(self, engine: FusionEngine) -> None:
        """刷新及重试只作用同一轮报告，不把不同批次的订单当同一轮对账。"""
        plan = _make_plan(engine)
        _submit_confirmed_pair_v1(engine)
        first = engine.request_comparison(plan.plan_id, ORDER_DAY)
        report_code = first["report"]["report_code"]

        # 同一轮刷新/重试：报告号不变、凭据不重算
        for _ in range(3):
            retry = engine.request_comparison(plan.plan_id, ORDER_DAY)
            assert retry["refreshed"] is True
            assert retry["report"]["report_code"] == report_code
            assert retry["report"]["primary_amount"] == 10.0
            assert retry["report"]["difference"] == 2.0

        # 新批次到达后开启新一轮，而不是改写旧报告
        engine.submit_wide_table_version(PRIMARY_TABLE, ORDER_DAY, 20.0, confirmed=True)
        engine.submit_wide_table_version(REFERENCE_TABLE, ORDER_DAY, 20.0, confirmed=True)
        second = engine.request_comparison(plan.plan_id, ORDER_DAY)
        assert second["report"]["report_code"] != report_code
        assert second["report"]["round_no"] == 2

        reports = engine.list_comparison_reports(plan.plan_id, ORDER_DAY)
        assert len(reports) == 2
        # 旧报告仍是 10 对 8，未被新批次覆盖
        assert reports[0]["primary_amount"] == 10.0
        assert reports[0]["reference_amount"] == 8.0

    def test_version_conflict_keeps_report_awaiting_pairing(self, engine: FusionEngine) -> None:
        """主体 v2 已确认、参照 v2 仍在提交：两侧版本冲突，新一轮待配对。"""
        plan = _make_plan(engine)
        _submit_confirmed_pair_v1(engine)
        engine.request_comparison(plan.plan_id, ORDER_DAY)

        engine.submit_wide_table_version(PRIMARY_TABLE, ORDER_DAY, 20.0, confirmed=True)
        engine.submit_wide_table_version(REFERENCE_TABLE, ORDER_DAY, 20.0, confirmed=False)

        result = engine.request_comparison(plan.plan_id, ORDER_DAY)
        assert result["ok"] is True
        assert result["paired"] is False
        report = result["report"]
        assert report["report_status"] == "待配对"
        assert report["difference"] is None
        # 待配对原因注明卡在参照一侧
        assert "参照宽表" in result["reason"]
        assert "提交" in result["reason"]

        # 重试仍是同一轮待配对报告，不会拿主体 v2 去对参照 v1
        retry = engine.request_comparison(plan.plan_id, ORDER_DAY)
        assert retry["report"]["report_code"] == report["report_code"]
        assert retry["report"]["report_status"] == "待配对"


class TestDriftClaimAndConfirm:
    """漂移认领与确认：处置只读比较版本凭据。"""

    def test_claim_falls_back_to_unfinished_old_report(self, engine: FusionEngine) -> None:
        """值班可用同一订单日尚未办结的旧报告继续复核，领取后按实际差额处理。"""
        plan = _make_plan(engine)
        _submit_confirmed_pair_v1(engine)
        first = engine.request_comparison(plan.plan_id, ORDER_DAY)
        old_report_code = first["report"]["report_code"]

        # 主体改为 20（v2 已确认），参照 v2 仍在提交：当前轮待配对
        engine.submit_wide_table_version(PRIMARY_TABLE, ORDER_DAY, 20.0, confirmed=True)
        engine.submit_wide_table_version(REFERENCE_TABLE, ORDER_DAY, 20.0, confirmed=False)
        engine.request_comparison(plan.plan_id, ORDER_DAY)

        claim = engine.claim_drift(plan.plan_id, ORDER_DAY)
        assert claim["ok"] is True
        # 领取落到旧报告：留存其报告号，按实际拿到的差额 2 处理
        assert claim["report_code"] == old_report_code
        assert claim["difference"] == 2.0
        assert claim["handle_status"] == "评估中"
        # 同时能看到本次请求当前轮为何没有可领差异
        assert claim["current_round"]["report_status"] == "待配对"
        assert "提交" in claim["current_round"]["reason"]

        # 重复认领是继续复核，不会生成新一轮漂移
        again = engine.claim_drift(plan.plan_id, ORDER_DAY)
        assert again["ok"] is True
        assert again["report_code"] == old_report_code
        assert again["drift_record_id"] == claim["drift_record_id"]

    def test_claim_blocked_while_awaiting_pairing(self, engine: FusionEngine) -> None:
        """比较仍待配对且没有旧报告时，说明此次为何领不到差异。"""
        plan = _make_plan(engine)
        engine.submit_wide_table_version(PRIMARY_TABLE, ORDER_DAY, 20.0, confirmed=True)
        engine.submit_wide_table_version(REFERENCE_TABLE, ORDER_DAY, 20.0, confirmed=False)
        engine.request_comparison(plan.plan_id, ORDER_DAY)

        claim = engine.claim_drift(plan.plan_id, ORDER_DAY)
        assert claim["ok"] is False
        assert "待配对" in claim["message"]
        assert "提交" in claim["message"]
        assert claim["current_round"]["report_status"] == "待配对"

    def test_never_treats_20_vs_8_as_one_round(self, engine: FusionEngine) -> None:
        """两种时点都不能出现把 20 对 8 当成一轮的漂移。"""
        plan = _make_plan(engine)
        _submit_confirmed_pair_v1(engine)
        engine.request_comparison(plan.plan_id, ORDER_DAY)

        # 时点一：主体已改 20、参照未确认，反复刷新/重试
        engine.submit_wide_table_version(PRIMARY_TABLE, ORDER_DAY, 20.0, confirmed=True)
        ref_v2 = engine.submit_wide_table_version(REFERENCE_TABLE, ORDER_DAY, 20.0)
        for _ in range(3):
            engine.request_comparison(plan.plan_id, ORDER_DAY)
            engine.claim_drift(plan.plan_id, ORDER_DAY)

        # 时点二：参照也变成 20，新报告确认之后
        engine.confirm_wide_table_version(ref_v2.version_id)
        engine.request_comparison(plan.plan_id, ORDER_DAY)

        reports = engine.list_comparison_reports(plan.plan_id, ORDER_DAY)
        assert len(reports) == 2
        for report in reports:
            assert not (report["primary_amount"] == 20.0
                        and report["reference_amount"] == 8.0)
            if report["report_status"] != "待配对":
                # 每一轮已比较的报告，两侧版本号必须一致
                assert report["primary_version"] == report["reference_version"]

    def test_confirm_drift_keeps_original_report_credentials(self, engine: FusionEngine) -> None:
        """确认旧差异时读持久凭据；新比较不覆盖旧处置，回查仍读到 10 与 8。"""
        plan = _make_plan(engine)
        _submit_confirmed_pair_v1(engine)
        first = engine.request_comparison(plan.plan_id, ORDER_DAY)
        old_report_code = first["report"]["report_code"]

        # 主体改 20、参照未确认：重新比较 + 办理一次漂移复核
        engine.submit_wide_table_version(PRIMARY_TABLE, ORDER_DAY, 20.0, confirmed=True)
        ref_v2 = engine.submit_wide_table_version(REFERENCE_TABLE, ORDER_DAY, 20.0)
        engine.request_comparison(plan.plan_id, ORDER_DAY)
        claim = engine.claim_drift(plan.plan_id, ORDER_DAY)
        drift_id = claim["drift_record_id"]

        # 参照也变成 20：确认新报告（20 对 20，差 0，无新漂移）
        engine.confirm_wide_table_version(ref_v2.version_id)
        second = engine.request_comparison(plan.plan_id, ORDER_DAY)
        assert second["paired"] is True
        assert second["report"]["report_status"] == "已确认"
        assert second["report"]["primary_amount"] == 20.0
        assert second["report"]["reference_amount"] == 20.0
        assert second["report"]["difference"] == 0.0
        assert second["drift_record_code"] == ""

        # 从漂移处置确认旧差异：处置结果留下实际核过的版本对和差值
        confirmed = engine.confirm_drift(drift_id)
        assert confirmed["ok"] is True
        handling = confirmed["handling_result"]
        assert handling["report_code"] == old_report_code
        assert handling["primary_version"] == 1
        assert handling["reference_version"] == 1
        assert handling["primary_amount"] == 10.0
        assert handling["reference_amount"] == 8.0
        assert handling["difference"] == 2.0

        # 记录仍指向原报告，旧漂移可确认的差额仍为 2
        drift = engine.get_drift_record(drift_id)
        assert drift is not None
        assert drift.report_code == old_report_code
        assert drift.handle_status == "已确认"
        assert drift.confirmable_difference == 2.0
        stored = json.loads(drift.handling_result)
        assert stored["report_code"] == old_report_code
        assert stored["difference"] == 2.0

        # 新比较不能覆盖旧处置：再次确认结果不变
        again = engine.confirm_drift(drift_id)
        assert again["handling_result"] == handling

        # 回查原差异明细仍应读到 10 与 8，复算原来的漂移差异仍为 2
        old_view = engine.get_comparison_report(old_report_code)
        assert old_view is not None
        assert old_view["report_status"] == "已办结"
        assert old_view["primary_amount"] == 10.0
        assert old_view["reference_amount"] == 8.0
        assert old_view["detail_snapshot"]["primary"]["amount"] == 10.0
        assert old_view["detail_snapshot"]["reference"]["amount"] == 8.0
        assert engine.recompute_report_difference(old_report_code) == 2.0

    def test_confirm_drift_without_credentials_rejected(self, engine: FusionEngine) -> None:
        """没有比较版本凭据的漂移记录不能确认差异。"""
        drift = engine.create_drift_record(
            wide_table_name="WT_ORPHAN",
            drift_type="manual",
            historical_baseline="b",
            current_distribution="c",
        )
        result = engine.confirm_drift(drift.record_id)
        assert result["ok"] is False
        assert "凭据" in result["message"]


class TestFullReconciliationScenario:
    """按值班对账的完整时序走一遍：10 对 8 → 主体改 20 → 参照补 20 → 确认旧差异。"""

    def test_full_reconciliation_flow(self, engine: FusionEngine) -> None:
        plan = _make_plan(engine)

        # 1. 先比主体金额 10 与参照金额 8，留下已确认报告
        _submit_confirmed_pair_v1(engine)
        r1 = engine.request_comparison(plan.plan_id, ORDER_DAY)
        report1 = r1["report"]
        assert report1["report_status"] == "已确认"
        assert report1["difference"] == 2.0

        # 2. 再只把主体金额改为 20：主体 v2 已确认、参照 v2 仍在提交
        engine.submit_wide_table_version(PRIMARY_TABLE, ORDER_DAY, 20.0, confirmed=True)
        ref_v2 = engine.submit_wide_table_version(REFERENCE_TABLE, ORDER_DAY, 20.0)

        # 3. 参照未提交前重新运行比较：第 2 轮版本冲突待配对
        r2 = engine.request_comparison(plan.plan_id, ORDER_DAY)
        assert r2["paired"] is False
        assert r2["report"]["report_status"] == "待配对"
        assert r2["report"]["report_code"] != report1["report_code"]

        # 4. 办理一次漂移复核：认领落到同一订单日尚未办结的旧报告
        claim = engine.claim_drift(plan.plan_id, ORDER_DAY)
        assert claim["ok"] is True
        assert claim["report_code"] == report1["report_code"]
        assert claim["difference"] == 2.0

        # 5. 参照也变成 20 后确认新报告
        engine.confirm_wide_table_version(ref_v2.version_id)
        r2_done = engine.request_comparison(plan.plan_id, ORDER_DAY)
        assert r2_done["report"]["report_code"] == r2["report"]["report_code"]
        assert r2_done["report"]["report_status"] == "已确认"
        assert r2_done["report"]["difference"] == 0.0

        # 6. 再从漂移处置确认旧差异，记录仍指向原报告
        confirmed = engine.confirm_drift(claim["drift_record_id"])
        assert confirmed["handling_result"]["report_code"] == report1["report_code"]
        assert confirmed["handling_result"]["difference"] == 2.0

        # 7. 回查原差异明细仍应读到 10 与 8，旧漂移可确认的差额仍为 2
        old_view = engine.get_comparison_report(report1["report_code"])
        assert old_view["primary_amount"] == 10.0
        assert old_view["reference_amount"] == 8.0
        assert engine.recompute_report_difference(report1["report_code"]) == 2.0
        drift = engine.get_drift_record(claim["drift_record_id"])
        assert drift.confirmable_difference == 2.0
