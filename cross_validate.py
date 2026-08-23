"""
多数据源日线交叉验证模块

按交易日对齐两个数据源的 BarData，比较 OHLCV/成交额的一致性，
输出差异明细与质量结论，供下载/更新流程在入库前校验数据质量。
纯逻辑模块，不发起网络请求；依赖最小 Bar 形状（Protocol）。

用法（在 ak_dm.py 中）：
    result = compare_bars(primary_bars, verify_bars, symbol="000001", exchange="SSE")
    if result.is_consistent:
        save_bar_data(primary_bars)
    else:
        log.error(f"数据不一致: {result}")
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

# 价格字段(open/high/low/close)相对容差：0.5%
PRICE_TOLERANCE = 0.005
# 成交量/成交额相对容差：1%
VOLUME_TOLERANCE = 0.01
# 分母为 0 时的防除零极小值
_EPSILON = 1e-12


class BarLike(Protocol):
    """交叉验证所需的最小 Bar 形状（vnpy BarData 满足该形状）。"""

    datetime: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    turnover: float


@dataclass(frozen=True, slots=True)
class FieldDiff:
    """单字段差异记录。"""

    field: str
    primary_value: float
    verify_value: float
    relative_diff: float


@dataclass(frozen=True, slots=True)
class BarDiff:
    """单个交易日的差异集合。"""

    datetime: date
    diffs: tuple[FieldDiff, ...]


@dataclass(frozen=True, slots=True)
class CompareResult:
    """一次交叉验证的统计结果。"""

    symbol: str
    exchange: str
    matched: int
    inconsistent: int
    primary_only: int
    verify_only: int
    diffs: tuple[BarDiff, ...]

    @property
    def total_days(self) -> int:
        """两源合计覆盖的交易日数（去重后）。"""
        return self.matched + self.inconsistent + self.primary_only + self.verify_only

    @property
    def is_consistent(self) -> bool:
        """是否存在超出容差的差异。"""
        return self.inconsistent == 0


def _relative_diff(left: float, right: float) -> float:
    """计算相对差异（以两值绝对值较大者为分母）。"""
    scale = max(abs(left), abs(right), _EPSILON)
    return abs(left - right) / scale


def _is_close(
    left: float,
    right: float,
    price_tol: float,
    volume_tol: float,
    field: str,
) -> bool:
    """按字段类型选用对应容差比较两值是否一致。"""
    tol = price_tol if field in ("open", "high", "low", "close") else volume_tol
    return _relative_diff(left, right) <= tol


def compare_bars(
    primary: list[BarLike],
    verify: list[BarLike],
    *,
    symbol: str,
    exchange: str,
    price_tol: float = PRICE_TOLERANCE,
    volume_tol: float = VOLUME_TOLERANCE,
) -> CompareResult:
    """
    按交易日对齐主源与验证源的日线，比较各字段一致性。

    :param primary: 主数据源 BarData 列表（将入库的数据）
    :param verify: 验证数据源 BarData 列表
    :param symbol: 股票代码（仅用于结果标识）
    :param exchange: 交易所代码（仅用于结果标识）
    :param price_tol: 价格字段相对容差
    :param volume_tol: 成交量/成交额字段相对容差
    :return: CompareResult 统计结果
    """
    primary_by_day: dict[date, BarLike] = {bar.datetime.date(): bar for bar in primary}
    verify_by_day: dict[date, BarLike] = {bar.datetime.date(): bar for bar in verify}

    all_days = sorted(set(primary_by_day) | set(verify_by_day))

    matched = 0
    inconsistent = 0
    primary_only = 0
    verify_only = 0
    bar_diffs: list[BarDiff] = []

    for day in all_days:
        p_bar = primary_by_day.get(day)
        v_bar = verify_by_day.get(day)

        if p_bar is None:
            verify_only += 1
            continue
        if v_bar is None:
            primary_only += 1
            continue

        field_diffs: list[FieldDiff] = []
        for field in ("open", "high", "low", "close", "volume", "turnover"):
            p_value = getattr(p_bar, f"{field}_price" if field != "volume" and field != "turnover" else field)
            v_value = getattr(v_bar, f"{field}_price" if field != "volume" and field != "turnover" else field)
            if not _is_close(p_value, v_value, price_tol, volume_tol, field):
                field_diffs.append(
                    FieldDiff(
                        field=field,
                        primary_value=p_value,
                        verify_value=v_value,
                        relative_diff=_relative_diff(p_value, v_value),
                    )
                )

        if field_diffs:
            inconsistent += 1
            bar_diffs.append(BarDiff(datetime=day, diffs=tuple(field_diffs)))
        else:
            matched += 1

    return CompareResult(
        symbol=symbol,
        exchange=exchange,
        matched=matched,
        inconsistent=inconsistent,
        primary_only=primary_only,
        verify_only=verify_only,
        diffs=tuple(bar_diffs),
    )


def format_diff(result: CompareResult, primary_source: str, verify_source: str) -> str:
    """生成人类可读的差异摘要（供日志输出）。"""
    lines = [
        f"[交叉验证] {result.symbol}.{result.exchange} "
        f"{primary_source} vs {verify_source}: "
        f"一致{result.matched}天/不一致{result.inconsistent}天/"
        f"仅主源{result.primary_only}天/仅验证源{result.verify_only}天"
    ]
    for bar_diff in result.diffs[:10]:
        fields = ", ".join(
            f"{d.field}: 主={d.primary_value:.4f} 验={d.verify_value:.4f} "
            f"Δ={d.relative_diff:.4%}"
            for d in bar_diff.diffs
        )
        lines.append(f"  {bar_diff.datetime}: {fields}")
    if len(result.diffs) > 10:
        lines.append(f"  ... 其余 {len(result.diffs) - 10} 个差异日")
    return "\n".join(lines)
