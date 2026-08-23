"""
多数据源日线多数一致(决议)模块

对主源与多个验证源的 BarData 按交易日对齐，逐字段以"多数为主"规则决议：
在严格多数(>在场源数/2)的源一致时，采用多数值覆盖主源；无法形成多数的字段
回退主源并标记为"无法定夺"。解决单源偶发脏数据(错价、量额单位不一致等)
污染入库的问题。

纯逻辑模块，不发起网络请求；依赖最小 Bar 形状（Protocol）。
决议结果以 ConsensusBar(冻结 dataclass) 输出，由调用方转换为 vnpy BarData 入库。

用法（在 ak_dm.py 中）：
    resolved, result = resolve_majority(
        primary_bars,
        [("baostock", bs_bars), ("mootdx", md_bars)],
        symbol="000001", exchange="SZSE", primary_source="akshare",
    )
    if result.has_conflict:
        log.war(format_consensus(result, "akshare", ["baostock", "mootdx"]))
    save_bar_data(to_bardata_list(resolved))
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Final, Protocol, Sequence

# 价格字段(open/high/low/close)相对容差：0.5%
PRICE_TOLERANCE: Final[float] = 0.005
# 成交量/成交额相对容差：1%
VOLUME_TOLERANCE: Final[float] = 0.01
# 分母为 0 时的防除零极小值
_EPSILON: Final[float] = 1e-12

PRICE_FIELDS: Final[tuple[str, ...]] = ("open", "high", "low", "close")
# 决议参与字段：价格 + 量额
ALL_FIELDS: Final[tuple[str, ...]] = PRICE_FIELDS + ("volume", "turnover")


class BarLike(Protocol):
    """决议所需的最小 Bar 形状（vnpy BarData 满足该形状）。"""

    @property
    def datetime(self) -> datetime: ...

    @property
    def open_price(self) -> float: ...

    @property
    def high_price(self) -> float: ...

    @property
    def low_price(self) -> float: ...

    @property
    def close_price(self) -> float: ...

    @property
    def volume(self) -> float: ...

    @property
    def turnover(self) -> float: ...


@dataclass(frozen=True, slots=True)
class ConsensusBar:
    """多数决议后的K线(纯数据，满足 BarLike 形状，可直接转 vnpy BarData)。"""

    symbol: str
    exchange: str
    datetime: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    turnover: float


@dataclass(frozen=True, slots=True)
class FieldConflict:
    """单个字段的主源被多数覆盖记录。"""

    field: str
    primary_value: float
    resolved_value: float
    agreeing_count: int  # 与决议一致(含决议本身)的源数


@dataclass(frozen=True, slots=True)
class DayConsensus:
    """单个交易日的决议结果。"""

    day: date
    present_sources: int  # 有该日数据的源数(含主源)
    unanimous: bool  # 全部在场源全字段一致
    overridden: bool  # 是否有字段被多数覆盖(主源被否决)
    ambiguous: bool  # 是否有字段无法形成多数(回退主源)
    conflicts: tuple[FieldConflict, ...]


@dataclass(frozen=True, slots=True)
class ConsensusResult:
    """一次多数决议的统计结果。"""

    symbol: str
    exchange: str
    unanimous_days: int  # 全源一致
    overridden_days: int  # 主源被多数覆盖
    ambiguous_days: int  # 无法形成多数，回退主源
    primary_only_days: int  # 仅主源有数据的交易日数
    verify_only_days: int  # 主源缺失、仅验证源有数据的交易日数
    days: tuple[DayConsensus, ...]

    @property
    def total_days(self) -> int:
        """决议覆盖的交易日数(去重后，不含 verify_only 之外的缺失日)。"""
        return self.unanimous_days + self.overridden_days + self.ambiguous_days + self.primary_only_days

    @property
    def has_conflict(self) -> bool:
        """是否存在主源被覆盖或无法定夺的情况。"""
        return self.overridden_days > 0 or self.ambiguous_days > 0


def _relative_diff(left: float, right: float) -> float:
    """计算相对差异（以两值绝对值较大者为分母）。"""
    scale = max(abs(left), abs(right), _EPSILON)
    return abs(left - right) / scale


def _field_value(bar: BarLike, field: str) -> float:
    """按字段名从 BarLike 取字段值。"""
    if field in ("volume", "turnover"):
        return getattr(bar, field)
    return getattr(bar, f"{field}_price")


def _is_close(left: float, right: float, tol: float) -> bool:
    """按相对容差比较两值是否一致。"""
    return _relative_diff(left, right) <= tol


def _majority_value(
    values: Sequence[tuple[str, float]], tol: float
) -> tuple[float | None, set[str]]:
    """
    从(源名, 值)列表中找出严格多数簇。

    :return: (代表值, 属于该簇的源名集合)；无法形成严格多数时返回 (None, 空集)。
    """
    n = len(values)
    if n == 0:
        return None, set()

    best_group: list[tuple[str, float]] = []
    for name, value in values:
        group = [
            (nm, v) for nm, v in values if _is_close(value, v, tol)
        ]
        if len(group) > len(best_group):
            best_group = group

    if len(best_group) * 2 <= n:
        return None, set()

    names = {nm for nm, _ in best_group}
    # 代表值取多数簇中位数，稳定且抗离群
    sorted_values = sorted(v for _, v in best_group)
    representative = sorted_values[len(sorted_values) // 2]
    return representative, names


def resolve_majority(
    primary: Sequence[BarLike],
    verifies: Sequence[tuple[str, Sequence[BarLike]]],
    *,
    symbol: str,
    exchange: str,
    primary_source: str = "primary",
    price_tol: float = PRICE_TOLERANCE,
    volume_tol: float = VOLUME_TOLERANCE,
) -> tuple[list[ConsensusBar], ConsensusResult]:
    """
    按交易日对齐主源与全部验证源，以"多数为主"逐字段决议。

    :param primary: 主数据源 BarData 列表（将入库的数据）
    :param verifies: 验证源列表，每项为(源名, BarData 列表)
    :param symbol: 股票代码（仅用于结果标识与 ConsensusBar）
    :param exchange: 交易所代码（仅用于结果标识与 ConsensusBar）
    :param primary_source: 主源名（用于判断主源是否在多数簇内）
    :param price_tol: 价格字段相对容差
    :param volume_tol: 成交量/成交额字段相对容差
    :return: (决议后 K 线列表, 决议统计结果)
    """
    primary_by_day: dict[date, BarLike] = {bar.datetime.date(): bar for bar in primary}
    verify_by_day: dict[str, dict[date, BarLike]] = {
        name: {bar.datetime.date(): bar for bar in bars} for name, bars in verifies
    }

    all_days_set: set[date] = set(primary_by_day)
    for day_map in verify_by_day.values():
        all_days_set |= set(day_map)
    all_days: list[date] = sorted(all_days_set)

    resolved_bars: list[ConsensusBar] = []
    day_results: list[DayConsensus] = []
    unanimous_days = 0
    overridden_days = 0
    ambiguous_days = 0
    primary_only_days = 0
    verify_only_days = 0

    for day in all_days:
        p_bar = primary_by_day.get(day)

        present: list[tuple[str, BarLike]] = (
            [(primary_source, p_bar)] if p_bar is not None else []
        )
        for name, day_map in verify_by_day.items():
            v_bar = day_map.get(day)
            if v_bar is not None:
                present.append((name, v_bar))

        if p_bar is None:
            # 主源缺失该日：不入库(不虚构数据)，仅统计
            verify_only_days += 1
            day_results.append(
                DayConsensus(
                    day=day,
                    present_sources=len(present),
                    unanimous=False,
                    overridden=False,
                    ambiguous=False,
                    conflicts=(),
                )
            )
            continue

        if len(present) == 1:
            # 仅主源有该日数据
            primary_only_days += 1
            day_results.append(
                DayConsensus(
                    day=day,
                    present_sources=1,
                    unanimous=True,
                    overridden=False,
                    ambiguous=False,
                    conflicts=(),
                )
            )
            resolved_bars.append(
                ConsensusBar(
                    symbol=symbol,
                    exchange=exchange,
                    datetime=p_bar.datetime,
                    open_price=p_bar.open_price,
                    high_price=p_bar.high_price,
                    low_price=p_bar.low_price,
                    close_price=p_bar.close_price,
                    volume=p_bar.volume,
                    turnover=p_bar.turnover,
                )
            )
            continue

        values: dict[str, float] = {}
        day_conflicts: list[FieldConflict] = []
        day_overridden = False
        day_ambiguous = False

        for field in ALL_FIELDS:
            tol = price_tol if field in PRICE_FIELDS else volume_tol
            vals = [(name, _field_value(bar, field)) for name, bar in present]
            representative, agreeing_names = _majority_value(vals, tol)
            primary_value = _field_value(p_bar, field)

            if representative is None:
                # 无法形成严格多数 → 回退主源，标记无法定夺
                day_ambiguous = True
                values[field] = primary_value
            elif primary_source in agreeing_names:
                # 主源在多数簇内 → 主源值即为决议值
                values[field] = primary_value
            else:
                # 主源被多数否决 → 采用多数值
                day_overridden = True
                values[field] = representative
                day_conflicts.append(
                    FieldConflict(
                        field=field,
                        primary_value=primary_value,
                        resolved_value=representative,
                        agreeing_count=len(agreeing_names),
                    )
                )

        if day_ambiguous:
            ambiguous_days += 1
        elif day_overridden:
            overridden_days += 1
        else:
            unanimous_days += 1

        day_results.append(
            DayConsensus(
                day=day,
                present_sources=len(present),
                unanimous=not day_overridden and not day_ambiguous,
                overridden=day_overridden,
                ambiguous=day_ambiguous,
                conflicts=tuple(day_conflicts),
            )
        )
        resolved_bars.append(
            ConsensusBar(
                symbol=symbol,
                exchange=exchange,
                datetime=p_bar.datetime,
                open_price=values["open"],
                high_price=values["high"],
                low_price=values["low"],
                close_price=values["close"],
                volume=values["volume"],
                turnover=values["turnover"],
            )
        )

    result = ConsensusResult(
        symbol=symbol,
        exchange=exchange,
        unanimous_days=unanimous_days,
        overridden_days=overridden_days,
        ambiguous_days=ambiguous_days,
        primary_only_days=primary_only_days,
        verify_only_days=verify_only_days,
        days=tuple(day_results),
    )
    return resolved_bars, result


def ohlc_issues(bar: BarLike) -> tuple[str, ...]:
    """检查 K 线 OHLC 物理一致性，返回违规项列表(空表示合法)。"""
    issues: list[str] = []
    if bar.high_price < bar.low_price:
        issues.append("high<low")
    if bar.high_price < max(bar.open_price, bar.close_price):
        issues.append("high<max(open,close)")
    if bar.low_price > min(bar.open_price, bar.close_price):
        issues.append("low>min(open,close)")
    if bar.volume < 0:
        issues.append("volume<0")
    for field in ("open", "high", "low", "close"):
        if getattr(bar, f"{field}_price") <= 0:
            issues.append(f"{field}<=0")
    return tuple(issues)


def is_ohlc_valid(bar: BarLike) -> bool:
    """K 线 OHLC 是否物理合法。"""
    return not ohlc_issues(bar)


def check_coverage(
    stored_days: Sequence[date],
    fresh_days: Sequence[date],
    *,
    max_missing_ratio: float = 0.05,
) -> tuple[bool, int, int]:
    """
    重写前覆盖度守卫：fresh 是否包含(几乎)全部 stored 交易日。

    :param stored_days: 库内已有数据的交易日集合
    :param fresh_days: 重新抓取的交易日集合
    :param max_missing_ratio: 允许缺失的库内交易日占比上限
    :return: (是否可安全覆盖, 缺失库内天数, 库内总天数)
    """
    stored_set = set(stored_days)
    if not stored_set:
        return True, 0, 0
    missing = stored_set - set(fresh_days)
    missing_ratio = len(missing) / len(stored_set)
    return missing_ratio <= max_missing_ratio, len(missing), len(stored_set)


def format_consensus(
    result: ConsensusResult,
    primary_source: str,
    verify_sources: Sequence[str],
) -> str:
    """生成人类可读的决议摘要（供日志输出）。"""
    lines = [
        f"[多数决议] {result.symbol}.{result.exchange} "
        f"主源={primary_source} 验证源={','.join(verify_sources) or '无'}: "
        f"一致{result.unanimous_days}天/多数覆盖{result.overridden_days}天/"
        f"无法定夺{result.ambiguous_days}天/仅主源{result.primary_only_days}天/"
        f"仅验证源{result.verify_only_days}天"
    ]

    shown = 0
    for day in result.days:
        if not day.overridden and not day.ambiguous:
            continue
        if day.conflicts:
            fields = ", ".join(
                f"{c.field}: 主={c.primary_value:.4f}→决={c.resolved_value:.4f}({c.agreeing_count}源)"
                for c in day.conflicts
            )
        elif day.ambiguous:
            fields = "无法定夺，回退主源"
        else:
            fields = "一致"
        lines.append(f"  {day.day}: {fields}")
        shown += 1
        if shown >= 10:
            break

    remaining = result.overridden_days + result.ambiguous_days - shown
    if remaining > 0:
        lines.append(f"  ... 其余 {remaining} 个冲突日")
    return "\n".join(lines)
