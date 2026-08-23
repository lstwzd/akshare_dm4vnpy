import argparse
import csv
import multiprocessing
import os
import sys
import traceback
from datetime import datetime, timedelta, time
from time import sleep
from typing import List, Optional, Tuple

from tqdm import tqdm
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.database import get_database, BarOverview
from vnpy.trader.object import HistoryRequest, BarData

from vnpy.trader.database import BaseDatabase, BarOverview

from vnpy_akshare import get_datafeed

database_manager: BaseDatabase = get_database()

from utils import log

sys.path.append(os.getcwd())

from AKShare import akshare_client, TS_DATE_FORMATE, to_vnpy_codes
from cross_validate import compare_bars, format_diff
from consensus import (
    ConsensusBar,
    ConsensusResult,
    check_coverage,
    format_consensus,
    is_ohlc_valid,
    resolve_majority,
)


class SourceDataClient:
    """Adapt vnpy_akshare datafeed to the legacy AKShare script API."""

    def __init__(self, source_name: str = "akshare"):
        self.source_name = (source_name or "akshare").lower()
        self.datafeed = get_datafeed(self.source_name) if self.source_name != "akshare" else akshare_client

    def init(self, retry: int = 3, retry_interval: int = 10) -> bool:
        if self.source_name == "akshare":
            ok = self.datafeed.init(retry=retry, retry_interval=retry_interval)
            self.symbols = self.datafeed.symbols
            self.trade_cal = self.datafeed.trade_cal
            return ok

        if not akshare_client.init(retry=retry, retry_interval=retry_interval):
            return False

        self.symbols = akshare_client.symbols
        self.trade_cal = akshare_client.trade_cal
        return True

    def query_history(self, req: HistoryRequest):
        if self.source_name == "akshare":
            return self.datafeed.query_history(req=req)

        bars = self.datafeed.query_bar_history(req, output=lambda *args, **kwargs: None)
        return bars

# 东方财富接口被封禁时无法查询个股上市时间，下载起始日期退化为固定早日期
FALLBACK_START_DATE: str = '19900101'


class AShareDailyDataManager:

    def __init__(self, source_name: str = "akshare", verify_source: str = ""):
        """"""
        self.source_name = (source_name or "akshare").lower()
        self.akshare_client = SourceDataClient(self.source_name)
        # 支持逗号分隔多个验证源，如 "baostock,mootdx"
        self.verify_sources: List[str] = []
        self.verify_clients: List[SourceDataClient] = []
        if verify_source:
            candidates = [s.strip().lower() for s in verify_source.split(",") if s.strip()]
            seen: set[str] = set()
            for name in candidates:
                if name == self.source_name or name in seen:
                    continue
                seen.add(name)
                self.verify_sources.append(name)
            self.verify_clients = [SourceDataClient(s) for s in self.verify_sources]
        self.symbols = None
        self.trade_cal = None
        self.bar_overviews: List[BarOverview] = None
        self.init()

    def init(self):
        """"""
        if not self.akshare_client.init():
            raise RuntimeError(
                "AKShare数据源初始化失败(股票列表获取失败)，请检查网络或东方财富接口是否被封禁，"
                "详见日志 log.txt，脚本终止"
            )
        self.symbols = self.akshare_client.symbols
        self.trade_cal = self.akshare_client.trade_cal
        self.bar_overviews = database_manager.get_bar_overview()

        # 验证源仅用于交叉对比，无需拉取股票列表；初始化依赖导入即可
        active_clients: List[SourceDataClient] = []
        active_sources: List[str] = []
        for client in self.verify_clients:
            if client.source_name == "akshare":
                ok = client.datafeed.init()
            else:
                ok = client.datafeed.init(output=lambda *args, **kwargs: None)
            if ok:
                active_clients.append(client)
                active_sources.append(client.source_name)
            else:
                log.war(f"验证数据源 {client.source_name} 初始化失败，本次运行将跳过该验证源")
        self.verify_clients = active_clients
        self.verify_sources = active_sources

    @staticmethod
    def _to_bardata(cb: ConsensusBar) -> BarData:
        return BarData(
            symbol=cb.symbol,
            exchange=cb.exchange,
            interval=Interval.DAILY,
            datetime=cb.datetime,
            open_price=cb.open_price,
            high_price=cb.high_price,
            low_price=cb.low_price,
            close_price=cb.close_price,
            volume=cb.volume,
            turnover=cb.turnover,
            gateway_name="akshare",
        )

    def _query_with_consensus(self, req: HistoryRequest) -> Tuple[List[BarData], Optional[ConsensusResult]]:
        """
        主源查询；若配置验证源则多数决议，返回(决议后bars, 决议结果或None)。
        """
        bars = self.akshare_client.query_history(req=req)
        if bars is None:
            bars = []

        if not self.verify_clients or not bars:
            return bars, None

        try:
            verify_series = []
            for client in self.verify_clients:
                verify_bars = client.query_history(req=req) or []
                verify_series.append((client.source_name, verify_bars))
            resolved, result = resolve_majority(
                bars,
                verify_series,
                symbol=req.symbol,
                exchange=req.exchange,
                primary_source=self.source_name,
            )
            # 入库前 OHLC 门禁：过滤物理不合法的决议K线
            filtered = [b for b in resolved if is_ohlc_valid(b)]
            dropped = len(resolved) - len(filtered)
            if dropped:
                log.war(f"{req.symbol}.{req.exchange} 决议后 {dropped} 根K线OHLC不合法，已跳过")
            return [self._to_bardata(b) for b in filtered], result
        except Exception as ex:
            log.war(f"多数决议失败({req.symbol}.{req.exchange})：{ex!r}")
            return bars, None

    def download_all(self):
        """
        使用tushare下载A股股票全市场日线数据
        :return:
        """
        log.info("开始下载A股股票全市场日线数据")
        # stared = False
        if self.symbols is not None:
            with tqdm(total=len(self.symbols)) as pbar:
                for tscode in self.symbols['symbol']:
                    symbol, exchange = to_vnpy_codes(tscode)
                    # 不查上市时间(东财接口被封时不可用)，固定早日期，上市前数据源自然返回空
                    list_date = FALLBACK_START_DATE

                    pbar.set_description_str("下载A股日线数据股票代码:" + tscode)
                    start_date = datetime.strptime(list_date, TS_DATE_FORMATE)
                    req = HistoryRequest(symbol=symbol,
                                         exchange=exchange,
                                         start=start_date,
                                         end=datetime.now(),
                                         interval=Interval.DAILY)
                    bardata, consensus_result = self._query_with_consensus(req=req)

                    if consensus_result is not None and consensus_result.has_conflict:
                        log.war(tscode + " 多源决议存在冲突，明细如下：\n" +
                                format_consensus(consensus_result, self.source_name, self.verify_sources))

                    if bardata:
                        try:
                            database_manager.save_bar_data(bardata)
                        except Exception as ex:
                            log.error(tscode + "数据存入数据库异常")
                            log.error(ex)
                            traceback.print_exc()

                    pbar.update(1)
                    log.info(pbar.desc)

        log.info("A股股票全市场日线数据下载完毕")

    def get_newest_bar_data(self, symbol: str, exchange: Exchange, interval: Interval) -> BarData or None:
        """"""
        for overview in self.bar_overviews:
            if exchange == overview.exchange and interval == overview.interval and symbol == overview.symbol:
                bars = database_manager.load_bar_data(symbol=symbol, exchange=exchange, interval=interval,
                                                      start=overview.end, end=overview.end)
                return bars[0] if bars is not None else None
        return None

    def update_newest(self, ss_symbol=''):
        """
        使用tushare更新本地数据库中的最新数据，默认本地数据库中原最新的数据之前的数据都是完备的
        :return:
        """
        stared = False
        log.info("开始更新最新的A股股票全市场日线数据")
        if self.symbols is not None:
            with tqdm(total=len(self.symbols)) as pbar:
                for tscode in self.symbols['symbol']:
                   
                    symbol, exchange = to_vnpy_codes(tscode)

                    if ss_symbol:
                        if (not stared and ss_symbol != symbol):
                            log.info(symbol + ' ingore.')
                            pbar.update(1)
                            continue
                        else:
                            stared = True
                    
                    newest_local_bar = self.get_newest_bar_data(symbol=symbol,
                                                                exchange=exchange,
                                                                interval=Interval.DAILY)
                    if newest_local_bar is not None:
                        pbar.set_description_str("正在处理股票代码：" + tscode + " 本地最新数据：" +
                                                 newest_local_bar.datetime.strftime(TS_DATE_FORMATE))
                        start_date = newest_local_bar.datetime + timedelta(days=1)
                    else:
                        pbar.set_description_str("正在处理股票代码：" + tscode + " 无本地数据")

                        # 不查上市时间(东财接口被封时不可用)，固定早日期，上市前数据源自然返回空
                        start_date = datetime.strptime(FALLBACK_START_DATE, TS_DATE_FORMATE)
    
                    if start_date.date() < datetime.now().date():
                        req = HistoryRequest(symbol=symbol,
                                            exchange=exchange,
                                            start=start_date,
                                            end=datetime.now(),
                                            interval=Interval.DAILY)
                        bardata, consensus_result = self._query_with_consensus(req=req)
                        if consensus_result is not None and consensus_result.has_conflict:
                            log.war(tscode + " 多源决议存在冲突，明细如下：\n" +
                                    format_consensus(consensus_result, self.source_name, self.verify_sources))
                        if bardata:
                            try:
                                database_manager.save_bar_data(bardata)
                            except Exception as ex:
                                log.error(tscode + "数据存入数据库异常")
                                log.error(ex)
                                traceback.print_exc()

                    pbar.update(1)
                    log.info(pbar.desc)

        log.info("A股股票全市场日线数据更新完毕")

    def check_update_all(self):
        """
        这个方法太慢了，不建议调用。
        这个方法用于本地数据库已经建立，但可能有部分数据缺失时使用
        使用tushare检查更新所有的A股股票全市场日线数据
        检查哪一个交易日的数据是缺失的，补全它
        检查上市后是否每个交易日都有数据，若存在某一交易日无数据，尝试从tushare查询该日数据，若仍无，则说明当天停盘
        :return:
        """
        log.info("开始检查更新所有的A股股票全市场日线数据")

        if self.symbols is not None:
            with tqdm(total=len(self.symbols)) as pbar:
                # for tscode, list_date in zip(self.symbols['symbol'], self.symbols['list_date']):

                for symbol in self.symbols['symbol']:
                    pbar.set_description_str("正在检查A股日线数据，股票代码:" + symbol)

                    symbol, exchange = to_vnpy_codes(symbol)

                    # 不查上市时间(东财接口被封时不可用)，以交易日历起始日为检查起点
                    list_date = self.trade_cal[exchange.value]['trade_date'].min().strftime(TS_DATE_FORMATE)

                    local_bar = database_manager.load_bar_data(symbol=symbol,
                                                               exchange=exchange,
                                                               interval=Interval.DAILY,
                                                               start=datetime.strptime(list_date, TS_DATE_FORMATE),
                                                               end=datetime.now())
                    local_bar_dates = [bar.datetime.strftime(TS_DATE_FORMATE) for bar in local_bar]

                    index = self.trade_cal[exchange.value][(self.trade_cal[exchange.value]['trade_date'] == datetime.date(datetime.strptime(list_date, TS_DATE_FORMATE)))]
                    if index.size == 0:  #当日发行股票，无行情数据
                        continue
                    trade_cal = self.trade_cal[exchange.value].iloc[index.index[0]:]
                    for trade_date in trade_cal['trade_date']:
                        if trade_date not in local_bar_dates:
                            req = HistoryRequest(symbol=symbol,
                                                 exchange=exchange,
                                                 start=trade_date,
                                                 end=trade_date,
                                                 interval=Interval.DAILY)
                            bardata = self.akshare_client.query_history(req=req)
                            if bardata:
                                log.info(symbol + "本地数据库缺失：" + trade_date.strftime(TS_DATE_FORMATE))
                                try:
                                    database_manager.save_bar_data(bardata)
                                except Exception as ex:
                                    log.error(symbol + "数据存入数据库异常")
                                    log.error(ex)
                                    traceback.print_exc()
                    pbar.update(1)
                    log.info(pbar.desc)

        log.info("A股股票全市场日线数据检查更新完毕")

    def _all_daily_overviews(self) -> List[BarOverview]:
        """
        返回全部日线清洗目标(overview)；对仅有K线而无overview的股票，
        从 bar_collection 按 (symbol, exchange) 聚合构造兜底 overview。
        """
        overviews = [o for o in (self.bar_overviews or []) if o.interval == Interval.DAILY]
        known = {(o.symbol, o.exchange) for o in overviews}
        col = getattr(database_manager, "bar_collection", None)
        if col is None:
            return overviews
        try:
            pipeline = [
                {"$match": {"interval": "d"}},
                {"$group": {
                    "_id": {"symbol": "$symbol", "exchange": "$exchange"},
                    "start": {"$min": "$datetime"},
                    "end": {"$max": "$datetime"},
                    "count": {"$sum": 1},
                }},
            ]
            extras: List[BarOverview] = []
            for doc in col.aggregate(pipeline):
                key = (doc["_id"]["symbol"], Exchange(doc["_id"]["exchange"]))
                if key in known:
                    continue
                known.add(key)
                extras.append(BarOverview(
                    symbol=doc["_id"]["symbol"],
                    exchange=Exchange(doc["_id"]["exchange"]),
                    interval=Interval.DAILY,
                    count=doc["count"],
                    start=doc["start"],
                    end=doc["end"],
                ))
            if extras:
                log.info(f"发现 {len(extras)} 只无 overview 但有日线数据的股票，一并纳入清洗")
            return overviews + extras
        except Exception as ex:
            log.war(f"聚合无 overview 股票失败，仅清洗有 overview 的股票：{ex!r}")
            return overviews

    def clean(self, ss_symbol: str = "", force: bool = False):
        """
        清洗已入库日线数据：逐只重新抓取主源+验证源，多数决议后与库内数据对比，
        覆盖度校验通过则删除异常数据并重存决议后数据。
        :param ss_symbol: 仅清洗指定股票(如 000001)；为空则清洗全部已入库日线股票
        :param force: 清洗全部时跳过交互确认(与 --purge 行为一致)
        """
        targets = self._all_daily_overviews()
        if not targets:
            log.info("数据库为空，无需清洗")
            return

        if ss_symbol:
            targets = [o for o in targets if o.symbol == ss_symbol]
        if not targets:
            log.info("没有匹配的日线数据可清洗")
            return

        if not ss_symbol:
            log.war(f"即将清洗数据库全部日线数据，共 {len(targets)} 只，"
                    f"将逐只重新拉取主源+验证源对比，耗时长且会重写数据库")
            if not force:
                answer = input("确认清洗全部，输入 yes 继续: ").strip().lower()
                if answer != "yes":
                    log.info("已取消清洗")
                    return

        log.info(f"开始清洗已入库日线数据，共 {len(targets)} 只")
        repaired = 0
        with tqdm(total=len(targets)) as pbar:
            for overview in targets:
                symbol = overview.symbol
                exchange = overview.exchange
                pbar.set_description_str("正在清洗股票代码:" + symbol)

                stored = database_manager.load_bar_data(
                    symbol=symbol, exchange=exchange, interval=Interval.DAILY,
                    start=overview.start, end=overview.end)
                if not stored:
                    pbar.update(1)
                    continue

                req = HistoryRequest(symbol=symbol, exchange=exchange,
                                     start=overview.start, end=overview.end,
                                     interval=Interval.DAILY)
                fresh, consensus_result = self._query_with_consensus(req)
                if not fresh:
                    pbar.update(1)
                    continue

                # 覆盖度守卫：fresh 必须覆盖(几乎)全部库内交易日，防止以残缺数据覆盖完整历史
                safe, missing, total = check_coverage(
                    [b.datetime.date() for b in stored],
                    [b.datetime.date() for b in fresh])
                if not safe:
                    log.war(symbol + f" 重新抓取数据缺少 {missing}/{total} 个库内交易日，跳过清洗避免覆盖丢失")
                    pbar.update(1)
                    continue

                result = compare_bars(stored, fresh, symbol=symbol, exchange=exchange)
                if result.is_consistent:
                    pbar.update(1)
                    continue

                deleted = database_manager.delete_bar_data(symbol, exchange, Interval.DAILY)
                database_manager.save_bar_data(fresh)
                repaired += 1
                detail = format_diff(result, "库内", self.source_name)
                if consensus_result is not None and consensus_result.has_conflict:
                    detail += "\n" + format_consensus(consensus_result, self.source_name, self.verify_sources)
                log.war(symbol + " 库内数据与多源不一致，已删除" + str(deleted) +
                        "条并重存\n" + detail)
                pbar.update(1)

        log.info(f"A股股票全市场日线数据清洗完毕，修复 {repaired} 只")

    def purge(self, force: bool = False):
        """
        一键清除本地数据库全部K线/overview数据（含日线与分钟线）。
        默认交互确认，--force 跳过确认。
        """
        overviews = database_manager.get_bar_overview()
        tick_overviews = database_manager.get_tick_overview()

        if not overviews and not tick_overviews:
            log.info("数据库已为空，无需清除")
            return

        log.war(f"即将清除数据库全部数据：{len(overviews)} 组K线overview、{len(tick_overviews)} 组Tick overview")
        if not force:
            answer = input("此操作不可恢复，输入 yes 确认清除: ").strip().lower()
            if answer != "yes":
                log.info("已取消清除")
                return

        bar_count = 0
        for overview in overviews:
            bar_count += database_manager.delete_bar_data(
                overview.symbol, overview.exchange, overview.interval)
        tick_count = 0
        for tick_overview in tick_overviews:
            tick_count += database_manager.delete_tick_data(
                tick_overview.symbol, tick_overview.exchange)

        log.info(f"数据库清除完毕：删除K线 {bar_count} 条、Tick {tick_count} 条")


def auto_update(source_name: str = "akshare", verify_source: str = "",
                start_time: time = time(18, 0)):
    """
    每日盘后自动更新最新日线数据到本地数据库
    """
    log.info("启动A股股票全市场日线数据定时更新")
    run_parent(source_name=source_name, verify_source=verify_source, start_time=start_time)


def run_parent(source_name: str = "akshare", verify_source: str = "",
               start_time: time = time(18, 0)):
    """
    运行父进程，定时启动子进程下载任务
    :param source_name: 主数据源
    :param verify_source: 验证数据源(可空)
    :param start_time: 每日启动更新时间
    """
    log.info("启动A股股票全市场日线数据定时更新父进程")

    # 每天晚上18：30从tushare更新当时K线数据
    UPDATE_TIME = start_time

    child_process = None

    while True:
        current_time = datetime.now().time()

        if current_time.hour == UPDATE_TIME.hour and current_time.minute == UPDATE_TIME.minute and child_process is None:
            log.info("启动日线数据更新子进程")
            child_process = multiprocessing.Process(
                target=run_child,
                kwargs={"source_name": source_name, "verify_source": verify_source})
            child_process.start()
            log.info("日线数据更新子进程启动成功")

        if (not (current_time.hour == UPDATE_TIME.hour and current_time.minute == UPDATE_TIME.minute)) \
                and child_process is not None:
            child_process.join()
            child_process = None
            log.info("数据更新子进程关闭成功")
            log.info("进入A股股票全市场日线数据定时更新父进程")

        sleep(10)


def run_child(source_name: str = "akshare", verify_source: str = "", ss_symbol: str = ""):
    """
    子进程更新数据：独立创建数据管理器，避免依赖模块级单例。
    """
    log.info("启动A股股票全市场日线数据定时更新子进程")

    try:
        manager = AShareDailyDataManager(source_name=source_name, verify_source=verify_source)
        manager.update_newest(ss_symbol)
    except Exception:
        log.info("子进程异常")
        traceback.print_exc()


if __name__ == '__main__':

    # 默认验证源候选(优先级从高到低)，自动选取第一个与主源不同的数据源
    DEFAULT_VERIFY_SOURCES: Tuple[str, ...] = ("baostock", "mootdx", "efinance", "akshare")

    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--all", help="download_all",
                        action="store_true")
    parser.add_argument("-u", "--update", help="update_newest",
                        action="store_true")
    parser.add_argument("-c", "--check", help="check_update_all",
                        action="store_true")
    parser.add_argument("-n", "--clean", help="清洗已入库日线数据(多源多数决议修复)",
                        action="store_true")
    parser.add_argument("-p", "--purge", help="一键清除本地数据库全部K线数据",
                        action="store_true")
    parser.add_argument("-f", "--force", help="跳过清除/清洗前的交互确认",
                        action="store_true")
    parser.add_argument("-s", "--symbol", type=str, help="从指定的股票代码开始更新/仅清洗该股票")
    parser.add_argument("--source", type=str, default="akshare",
                        choices=["akshare", "baostock", "mootdx", "efinance"],
                        help="选择数据源：akshare/baostock/mootdx/efinance")
    parser.add_argument("--verify-source", type=str, default="",
                        help="交叉验证数据源，支持逗号分隔多个(如 baostock,mootdx)；空则自动选与主源不同的单个默认源")

    args = parser.parse_args()

    verify_source = args.verify_source
    if not verify_source:
        verify_source = next((s for s in DEFAULT_VERIFY_SOURCES if s != args.source), "")

    a_share_daily_data_manager = AShareDailyDataManager(
        source_name=args.source, verify_source=verify_source)
    active_verify = ",".join(a_share_daily_data_manager.verify_sources) or "无"

    if args.purge:
        log.info(f"一键清除本地数据库全部K线数据")
        a_share_daily_data_manager.purge(force=args.force)
    elif args.clean:
        log.info(f"清洗已入库日线数据，数据源={args.source}，验证源={active_verify}")
        a_share_daily_data_manager.clean(ss_symbol=args.symbol, force=args.force)
    elif args.all:
        log.info(f"下载所有A股股票全市场日线数据，数据源={args.source}，验证源={active_verify}")
        a_share_daily_data_manager.download_all()
    elif args.update:
        log.info(f"自动更新A股股票全市场日线数据，数据源={args.source}，验证源={active_verify}")
        a_share_daily_data_manager.update_newest(args.symbol)
    elif args.check:
        log.info(f"检测并自动更新A股股票全市场日线数据(速度极慢)，数据源={args.source}，验证源={active_verify}")
        a_share_daily_data_manager.check_update_all()
    else:
        log.info(f"自动更新A股股票全市场日线数据，数据源={args.source}，验证源={active_verify}")
        a_share_daily_data_manager.update_newest(args.symbol)

    #a_share_daily_data_manager.download_all()
    #a_share_daily_data_manager.update_newest()
    #a_share_daily_data_manager.check_update_all()
    #auto_update(start_time=time(21, 47))
