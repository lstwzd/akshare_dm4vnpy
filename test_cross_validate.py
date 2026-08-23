"""cross_validate 单元测试：多数据源日线交叉验证纯逻辑。"""

from datetime import datetime

import pytest
from vnpy.trader.constant import Exchange
from vnpy.trader.object import BarData

from cross_validate import (
    PRICE_TOLERANCE,
    VOLUME_TOLERANCE,
    BarDiff,
    CompareResult,
    FieldDiff,
    compare_bars,
    format_diff,
)


def _bar(
    day: int,
    close: float = 10.0,
    open_: float = 9.9,
    high: float = 10.1,
    low: float = 9.8,
    volume: float = 1_000_000,
    turnover: float = 10_000_000,
) -> BarData:
    return BarData(
        symbol="000001",
        exchange=Exchange.SZSE,
        interval="d",
        datetime=datetime(2024, 1, day, 15, 0),
        open_price=open_,
        high_price=high,
        low_price=low,
        close_price=close,
        volume=volume,
        turnover=turnover,
        gateway_name="test",
    )


class TestCompareBars:
    def test_matched_when_identical(self):
        """两源完全一致 → matched=3, inconsistent=0。"""
        primary = [_bar(2), _bar(3), _bar(4)]
        verify = [_bar(2), _bar(3), _bar(4)]
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        assert result.matched == 3
        assert result.inconsistent == 0
        assert result.primary_only == 0
        assert result.verify_only == 0
        assert result.is_consistent

    def test_diff_when_close_price_diverges(self):
        """收盘价超出容差 → inconsistent=1，含 FieldDiff。"""
        primary = [_bar(2, close=10.0)]
        verify = [_bar(2, close=10.2)]  # 相对差异 2% > 0.5%
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        assert result.inconsistent == 1
        assert result.matched == 0
        assert not result.is_consistent
        bar_diff: BarDiff = result.diffs[0]
        fields = {d.field: d for d in bar_diff.diffs}
        assert "close" in fields
        assert fields["close"].primary_value == 10.0
        assert fields["close"].verify_value == 10.2
        assert fields["close"].relative_diff == pytest.approx(0.2 / 10.2, rel=1e-6)

    def test_volume_diff_uses_volume_tolerance(self):
        """成交量差异在 1% 内视为一致。"""
        primary = [_bar(2, volume=1_000_000)]
        verify = [_bar(2, volume=1_005_000)]  # 0.5% < 1%
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        assert result.is_consistent

    def test_volume_diff_over_tolerance(self):
        """成交量差异超 1% → 不一致。"""
        primary = [_bar(2, volume=1_000_000)]
        verify = [_bar(2, volume=1_100_000)]  # 10%
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        assert not result.is_consistent
        fields = {d.field for d in result.diffs[0].diffs}
        assert "volume" in fields

    def test_primary_only_days_counted(self):
        """验证源缺失某日 → primary_only 计数，不算不一致。"""
        primary = [_bar(2), _bar(3)]
        verify = [_bar(2)]
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        assert result.primary_only == 1
        assert result.verify_only == 0
        assert result.is_consistent

    def test_verify_only_days_counted(self):
        """主源缺失某日 → verify_only 计数。"""
        primary = [_bar(2)]
        verify = [_bar(2), _bar(3)]
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        assert result.verify_only == 1
        assert result.is_consistent

    def test_empty_inputs(self):
        """两源都空 → 全部为 0。"""
        result = compare_bars([], [], symbol="000001", exchange="SZSE")
        assert result.total_days == 0
        assert result.is_consistent

    def test_price_tolerance_custom(self):
        """自定义价格容差：2% 差异在 5% 容差内 → 一致。"""
        primary = [_bar(2, close=10.0)]
        verify = [_bar(2, close=10.2)]
        result = compare_bars(
            primary, verify, symbol="000001", exchange="SZSE", price_tol=0.05
        )
        assert result.is_consistent

    def test_multiple_field_diffs_recorded(self):
        """同一天多字段超差 → 一条 BarDiff 含多个 FieldDiff。"""
        primary = [_bar(2, close=10.0, open_=9.9)]
        verify = [_bar(2, close=10.5, open_=9.5)]  # close 5%, open ~4.2%
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        assert result.inconsistent == 1
        fields = {d.field for d in result.diffs[0].diffs}
        assert "close" in fields
        assert "open" in fields

    def test_total_days_dedup(self):
        """total_days 为去重后的交易日数。"""
        primary = [_bar(2), _bar(3)]
        verify = [_bar(3), _bar(4)]
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        assert result.matched == 1
        assert result.primary_only == 1
        assert result.verify_only == 1
        assert result.total_days == 3


class TestFormatDiff:
    def test_format_consistent(self):
        result = compare_bars(
            [_bar(2)], [_bar(2)], symbol="000001", exchange="SZSE"
        )
        text = format_diff(result, "akshare", "baostock")
        assert "一致1天" in text
        assert "不一致0天" in text

    def test_format_diff_lists_fields(self):
        primary = [_bar(2, close=10.0)]
        verify = [_bar(2, close=10.2)]
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        text = format_diff(result, "akshare", "baostock")
        assert "close" in text
        assert "1.9608%" in text  # 10.0 vs 10.2 → 0.2/10.2 = 1.9608%

    def test_format_truncates_many_diffs(self):
        primary = [_bar(d, close=10.0) for d in range(2, 17)]
        verify = [_bar(d, close=10.2) for d in range(2, 17)]
        result = compare_bars(primary, verify, symbol="000001", exchange="SZSE")
        text = format_diff(result, "a", "b")
        assert "其余 5 个差异日" in text


class TestConstants:
    def test_tolerances_sane(self):
        assert 0 < PRICE_TOLERANCE < 1
        assert 0 < VOLUME_TOLERANCE < 1
