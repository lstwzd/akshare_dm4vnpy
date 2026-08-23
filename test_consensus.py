"""consensus 单元测试：多源多数决议 / OHLC 合法性 / 覆盖度守卫。"""

from datetime import date, datetime

from vnpy.trader.constant import Exchange
from vnpy.trader.object import BarData

from consensus import (
    ConsensusBar,
    DayConsensus,
    FieldConflict,
    PRICE_TOLERANCE,
    VOLUME_TOLERANCE,
    check_coverage,
    format_consensus,
    is_ohlc_valid,
    ohlc_issues,
    resolve_majority,
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


class TestResolveMajority:
    def test_unanimous_three_sources(self):
        """三源全字段一致 → 全部 unanimous，无冲突。"""
        primary = [_bar(2), _bar(3)]
        verifies = [("bs", [_bar(2), _bar(3)]), ("md", [_bar(2), _bar(3)])]
        bars, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.unanimous_days == 2
        assert result.overridden_days == 0
        assert result.ambiguous_days == 0
        assert not result.has_conflict
        assert len(bars) == 2
        assert bars[0].close_price == 10.0

    def test_majority_overrides_primary(self):
        """主源收盘价异常(与两验证源不一致) → 多数覆盖主源，采用多数值。"""
        primary = [_bar(2, close=10.0)]
        # 两个验证源一致：10.5
        verifies = [("bs", [_bar(2, close=10.5)]), ("md", [_bar(2, close=10.5)])]
        bars, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.overridden_days == 1
        assert result.unanimous_days == 0
        assert result.has_conflict
        assert bars[0].close_price == 10.5
        day: DayConsensus = result.days[0]
        field: FieldConflict = day.conflicts[0]
        assert field.field == "close"
        assert field.primary_value == 10.0
        assert field.resolved_value == 10.5
        assert field.agreeing_count == 2

    def test_tie_falls_back_to_primary(self):
        """三源互不一致(1:1:1) → 无多数，回退主源并标记 ambiguous。"""
        primary = [_bar(2, close=10.0)]
        verifies = [("bs", [_bar(2, close=10.2)]), ("md", [_bar(2, close=10.4)])]
        bars, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.ambiguous_days == 1
        assert result.overridden_days == 0
        assert result.has_conflict
        assert bars[0].close_price == 10.0  # 回退主源

    def test_single_verify_agrees(self):
        """单验证源与主源一致 → unanimous。"""
        primary = [_bar(2)]
        verifies = [("bs", [_bar(2)])]
        bars, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.unanimous_days == 1
        assert not result.has_conflict

    def test_single_verify_disagrees_is_ambiguous(self):
        """单验证源与主源不一致(2源1:1) → 无法形成多数，回退主源。"""
        primary = [_bar(2, close=10.0)]
        verifies = [("bs", [_bar(2, close=10.2)])]
        bars, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.ambiguous_days == 1
        assert bars[0].close_price == 10.0

    def test_primary_only_days(self):
        """验证源缺失某日 → primary_only 计数，该日直接采用主源。"""
        primary = [_bar(2), _bar(3)]
        verifies = [("bs", [_bar(2)])]
        bars, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.primary_only_days == 1
        assert result.unanimous_days == 1
        assert len(bars) == 2

    def test_verify_only_days_not_in_output(self):
        """主源缺失某日(仅验证源有) → 不入库，verify_only 计数。"""
        primary = [_bar(2)]
        verifies = [("bs", [_bar(2), _bar(3)])]
        bars, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.verify_only_days == 1
        assert len(bars) == 1  # 不虚构主源缺失的数据

    def test_no_verifies(self):
        """无验证源 → 全部按主源原样返回。"""
        primary = [_bar(2), _bar(3)]
        bars, result = resolve_majority(
            primary, [], symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.unanimous_days == 0
        assert result.primary_only_days == 2
        assert len(bars) == 2
        assert bars[0].close_price == 10.0

    def test_empty_inputs(self):
        """主源与验证源全空 → 无输出，无冲突。"""
        bars, result = resolve_majority(
            [], [], symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert bars == []
        assert result.total_days == 0
        assert not result.has_conflict

    def test_volume_majority(self):
        """量额字段同样参与多数决议(容差 1%)。"""
        primary = [_bar(2, volume=1_000_000)]
        verifies = [("bs", [_bar(2, volume=2_000_000)]), ("md", [_bar(2, volume=2_000_000)])]
        bars, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.overridden_days == 1
        assert bars[0].volume == 2_000_000

    def test_minority_within_tolerance_counts_as_agree(self):
        """验证源与主源差异在容差内 → 视为一致(多数簇含主源)。"""
        primary = [_bar(2, close=10.0)]
        # 0.2% 差异 < 0.5% 容差
        verifies = [("bs", [_bar(2, close=10.02)]), ("md", [_bar(2, close=10.01)])]
        bars, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert result.unanimous_days == 1
        assert not result.has_conflict
        assert bars[0].close_price == 10.0

    def test_output_is_consensus_bar(self):
        """输出为 ConsensusBar 类型且携带 symbol/exchange。"""
        primary = [_bar(2)]
        verifies = [("bs", [_bar(2)])]
        bars, _ = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        assert isinstance(bars[0], ConsensusBar)
        assert bars[0].symbol == "000001"
        assert bars[0].exchange == "SZSE"


class TestOhlcValid:
    def test_valid_bar(self):
        assert is_ohlc_valid(_bar(2))
        assert ohlc_issues(_bar(2)) == ()

    def test_high_below_max_open_close(self):
        bar = _bar(2, open_=10.5, close=10.4, high=10.1)
        assert not is_ohlc_valid(bar)
        assert "high<max(open,close)" in ohlc_issues(bar)

    def test_low_above_min_open_close(self):
        bar = _bar(2, open_=9.5, close=9.4, low=9.8)
        assert not is_ohlc_valid(bar)
        assert "low>min(open,close)" in ohlc_issues(bar)

    def test_high_below_low(self):
        bar = _bar(2, high=9.0, low=10.0)
        assert not is_ohlc_valid(bar)
        assert "high<low" in ohlc_issues(bar)

    def test_negative_volume(self):
        bar = _bar(2, volume=-100)
        assert not is_ohlc_valid(bar)
        assert "volume<0" in ohlc_issues(bar)

    def test_nonpositive_price(self):
        bar = _bar(2, close=0.0)
        assert not is_ohlc_valid(bar)
        assert "close<=0" in ohlc_issues(bar)

    def test_consensus_bar_ohlc(self):
        """ConsensusBar 同样可用于 OHLC 校验。"""
        bar = ConsensusBar(
            symbol="000001", exchange="SZSE", datetime=datetime(2024, 1, 2, 15, 0),
            open_price=9.9, high_price=10.1, low_price=9.8,
            close_price=10.0, volume=1_000_000, turnover=10_000_000,
        )
        assert is_ohlc_valid(bar)


class TestCoverage:
    def test_full_coverage(self):
        safe, missing, total = check_coverage([date(2024, 1, 2), date(2024, 1, 3)],
                                              [date(2024, 1, 2), date(2024, 1, 3)])
        assert safe
        assert missing == 0
        assert total == 2

    def test_partial_missing_blocks(self):
        """fresh 缺失库内 2/3 交易日 → 不允许覆盖。"""
        stored = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
        fresh = [date(2024, 1, 2)]
        safe, missing, total = check_coverage(stored, fresh)
        assert not safe
        assert missing == 2
        assert total == 3

    def test_small_missing_allowed(self):
        """fresh 缺失库内 1/21 交易日(≈4.8% ≤5%) → 允许覆盖。"""
        stored = [date(2024, 1, d) for d in range(2, 23)]
        fresh = [date(2024, 1, d) for d in range(3, 23)]  # 缺失 1/21
        safe, _, _ = check_coverage(stored, fresh)
        assert safe

    def test_empty_stored(self):
        safe, missing, total = check_coverage([], [date(2024, 1, 2)])
        assert safe
        assert missing == 0
        assert total == 0

    def test_custom_ratio(self):
        """自定义缺失占比上限。"""
        stored = [date(2024, 1, 2), date(2024, 1, 3)]
        fresh = [date(2024, 1, 2)]  # 缺失 50%
        safe, _, _ = check_coverage(stored, fresh, max_missing_ratio=0.6)
        assert safe


class TestFormatConsensus:
    def test_format_consistent(self):
        primary = [_bar(2), _bar(3)]
        verifies = [("bs", [_bar(2), _bar(3)])]
        _, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        text = format_consensus(result, "akshare", ["baostock"])
        assert "一致2天" in text
        assert "多数覆盖0天" in text

    def test_format_shows_conflicts(self):
        primary = [_bar(2, close=10.0)]
        verifies = [("bs", [_bar(2, close=10.5)]), ("md", [_bar(2, close=10.5)])]
        _, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        text = format_consensus(result, "akshare", ["baostock", "mootdx"])
        assert "多数覆盖1天" in text
        assert "close" in text
        assert "2源" in text

    def test_format_truncates_many_conflicts(self):
        primary = [_bar(d, close=10.0) for d in range(2, 17)]
        verifies = [("bs", [_bar(d, close=10.5) for d in range(2, 17)]),
                    ("md", [_bar(d, close=10.5) for d in range(2, 17)])]
        _, result = resolve_majority(
            primary, verifies, symbol="000001", exchange="SZSE", primary_source="akshare"
        )
        text = format_consensus(result, "akshare", ["baostock", "mootdx"])
        assert "其余 5 个冲突日" in text


class TestConstants:
    def test_tolerances_sane(self):
        assert 0 < PRICE_TOLERANCE < 1
        assert 0 < VOLUME_TOLERANCE < 1
