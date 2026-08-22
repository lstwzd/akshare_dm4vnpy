import akshare as ak
from pytz import timezone
from typing import List, Optional, Dict
import pandas as pd
from datetime import datetime, timedelta, date
import time
import traceback

from vnpy.trader.object import HistoryRequest, BarData
from vnpy.trader.constant import Exchange, Interval

from utils import log

CHINA_TZ = timezone("Asia/Shanghai")

#akshare_token: str = ""

MAX_QUERY_SIZE: int = 5000
TS_DATE_FORMATE: str = '%Y%m%d'
MAX_QUERY_TIMES: int = 500

EXCHANGE_TS2VT: Dict[str, Exchange] = {
    'sh': Exchange.SSE,
    'SH': Exchange.SSE,
    'SZ': Exchange.SZSE,
    'sz': Exchange.SZSE,
}

EXCHANGE_VT2TS: Dict[Exchange, str] = {v: k for k, v in EXCHANGE_TS2VT.items()}


def get_stock_type(stock_code):
    """判断股票ID对应的证券市场
    匹配规则
    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6', '9'] 开头的为 sh， 其余为 sz
    :param stock_code:股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    :return 'sh' or 'sz'"""
    assert type(stock_code) is str, "stock code need str type"
    if stock_code.startswith(("sh", "sz")):
        return stock_code[:2]
    if stock_code.startswith(
        ("50", "51", "60", "90", "110", "113", "132", "204")
    ):
        return "sh"
    if stock_code.startswith(
        ("00", "13", "18", "15", "16", "18", "20", "30", "39", "115", "1318")
    ):
        return "sz"
    if stock_code.startswith(("5", "6", "9", "7")):
        return "sh"
    return "sz"

def to_vnpy_codes(symbol: str):
    exchange = EXCHANGE_TS2VT[get_stock_type(symbol)]
    return symbol, exchange

class AKShareClient:
    """
    从akshare中查询历史数据的Client
    akshare日线数据说明：交易日每天15点~16点之间更新数据，daily接口是未复权行情，停牌期间不提供数据。
    akshare调取说明：基础积分每分钟内最多调取500次，每次5000条数据
    """

    def __init__(self):
        """"""

        self.pro: object = None

        self.inited: bool = False

        # 获得所有股票代码
        self.symbols: pd.DataFrame = None

        # 获得交易日历
        self.trade_cal: Dict[str, pd.DataFrame] = None

        # 东财失败熔断：网络异常后跳过该源，按探测间隔尝试恢复
        self.em_blocked: bool = False
        self.em_fail_at: float = 0.0
        self.em_probe_interval: float = 1800.0

    def init(self, retry: int = 3, retry_interval: int = 10) -> bool:
        """
        初始化数据源(股票列表+交易日历)
        :param retry: 失败重试次数
        :param retry_interval: 重试基础间隔(秒), 每次递增
        :return: 是否初始化成功
        """
        if self.inited:
            return True

        for attempt in range(1, retry + 1):
            try:
                self.pro = ak
                self.stock_list()
                self.trade_day_list()
                self.inited = True
                return True
            except (BaseException) as ex:
                log.error("AKShareClient初始化失败(第{}/{}次): {}".format(attempt, retry, repr(ex)))
                if attempt < retry:
                    sleep_time = retry_interval * attempt
                    log.info("{}秒后重试初始化".format(sleep_time))
                    time.sleep(sleep_time)

        return False

    def _fetch_kline_em(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """
        东方财富日K线(不复权，与实时行情一致)，成交量单位为手，此处统一归一化为股
        :param symbol: 不带交易所前缀的6位股票代码
        :return: 标准化df(含trade_date/open/high/low/close/volumn/turnover)，失败抛出异常
        """
        df = self.pro.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start, end_date=end, adjust="")
        df['成交量'] = df['成交量'] * 100
        return df.rename(columns={'日期': 'trade_date', '开盘': 'open', '最高': 'high',
                                  '最低': 'low', '收盘': 'close', '成交量': 'volumn', '成交额': 'turnover'})

    def _fetch_kline_sina(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """
        新浪日K线(不复权，与实时行情一致)，成交量单位为股，无需转换
        :param symbol: 带交易所前缀的代码(如sz000001)
        :return: 标准化df(含trade_date/open/high/low/close/volumn/turnover)，失败抛出异常
        """
        df = self.pro.stock_zh_a_daily(symbol=symbol, start_date=start, end_date=end, adjust="")
        # 新浪返回同时含amount(成交额)与turnover(换手率)，必须先选列再重命名，否则产生重复列名
        df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
        return df.rename(columns={'date': 'trade_date', 'volume': 'volumn', 'amount': 'turnover'})

    def _fetch_kline_tx(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """
        腾讯日K线(不复权，与实时行情一致)，akshare已统一为股，仅sz000前缀仍返回手需补转
        :param symbol: 带交易所前缀的代码(如sz000001)
        :return: 标准化df(含trade_date/open/high/low/close/volumn/turnover)，失败抛出异常
        """
        df = self.pro.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end, adjust="")
        if symbol.startswith("sz000"):
            df['volume'] = df['volume'] * 100
        # 腾讯返回同时含volume(成交量)与turnover(换手率)，必须先选列再重命名，否则产生重复列名
        df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
        return df.rename(columns={'date': 'trade_date', 'volume': 'volumn', 'amount': 'turnover'})

    def _fetch_kline(self, symbol: str, prefixed_symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """
        按优先级(东方财富→新浪→腾讯)获取日K线并返回标准化df
        东财网络异常后被熔断跳过，仅按探测间隔尝试一次；某源异常时记录警告并尝试下一源，全部失败返回None
        :param symbol: 不带交易所前缀的6位股票代码
        :param prefixed_symbol: 带交易所前缀的代码(如sz000001)
        """
        sources = [("新浪", self._fetch_kline_sina, prefixed_symbol),
                   ("腾讯", self._fetch_kline_tx, prefixed_symbol)]
        # 东财熔断期间按探测间隔尝试一次，避免反复请求被封接口
        if not self.em_blocked or time.time() - self.em_fail_at >= self.em_probe_interval:
            sources.insert(0, ("东方财富", self._fetch_kline_em, symbol))

        for source_name, fetch_func, sym in sources:
            try:
                kline_df = fetch_func(sym, start, end)
                if source_name == "东方财富" and self.em_blocked:
                    log.info("东方财富接口已恢复，恢复使用东方财富数据源")
                    self.em_blocked = False
                log.info(symbol + " 日K线来源：" + source_name)
                return kline_df
            except OSError as ex:
                log.war("{}获取{}日K线网络异常，尝试下一数据源：{}".format(source_name, symbol, repr(ex)))
                if source_name == "东方财富":
                    if not self.em_blocked:
                        log.war("东方财富接口异常，后续K线查询直接使用新浪，定期探测恢复")
                    self.em_blocked = True
                    self.em_fail_at = time.time()
            except Exception as ex:
                log.war("{}获取{}日K线失败，尝试下一数据源：{}".format(source_name, symbol, repr(ex)))
        return None

    def query_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """
        从akshare里查询历史数据
        :param req:查询请求
        :return: Optional[List[BarData]]
        """
        if self.symbols is None:
            return None

        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start.strftime(TS_DATE_FORMATE)
        end = req.end.strftime(TS_DATE_FORMATE)

        if interval is not Interval.DAILY:
            return None
        if exchange not in [Exchange.SSE, Exchange.SZSE]:
            return None

        # 东财K线用裸代码，新浪K线需带交易所前缀
        prefixed_symbol = get_stock_type(symbol) + symbol

        # tscode = to_ts_symbol(symbol, exchange)

        # 修改查询数据逻辑，在每次5000条数据的限制下，很可能一次无法读取完
        cnt = 0
        df: pd.DataFrame = None
        while datetime.strptime(start, TS_DATE_FORMATE) <= datetime.strptime(end, TS_DATE_FORMATE):
            # 保证每次查询最多5000天数据
            start_date = datetime.strptime(start, TS_DATE_FORMATE)
            simulate_end_date = min(datetime.strptime(end, TS_DATE_FORMATE),
                                    start_date + timedelta(days=MAX_QUERY_SIZE))
            simulate_end = simulate_end_date.strftime(TS_DATE_FORMATE)

            akshare_df = None
            query_retry_cnt = 0
            while True:
                akshare_df = self._fetch_kline(symbol, prefixed_symbol, start, simulate_end)
                if akshare_df is not None:
                    break
                query_retry_cnt += 1
                if query_retry_cnt > 10:
                    log.error(symbol + " 各数据源连续重试失败，跳过该股票")
                    return None
                log.info("获取" + symbol + "失败，15秒后重试")
                time.sleep(15.0)

            if akshare_df is not None and not akshare_df.empty:
                if df is None:
                    df = akshare_df
                else:
                    df = pd.concat([df, akshare_df], ignore_index=True)
            # end_time = time.time()
            # delta = round(end_time - begin_time, 3)
            # if delta < 60 / MAX_QUERY_TIMES:

            sleep_time = 0.10
            log.info("sleep：" + str(sleep_time) + "s")
            time.sleep(sleep_time)

            cnt += 1
            start = (simulate_end_date + timedelta(days=1)).strftime(TS_DATE_FORMATE)

        data: List[BarData] = []

        
        if df is not None:
            for ix, row in df.iterrows():
                date = datetime.strptime(str(row.trade_date), '%Y-%m-%d')
                date = CHINA_TZ.localize(date)

                if pd.isnull(row['open']):
                    log.info(symbol + '.' + EXCHANGE_VT2TS[exchange] + row['trade_date'] + "open_price为None")
                elif pd.isnull(row['high']):
                    log.info(symbol + '.' + EXCHANGE_VT2TS[exchange] + row['trade_date'] + "high_price为None")
                elif pd.isnull(row['low']):
                    log.info(symbol + '.' + EXCHANGE_VT2TS[exchange] + row['trade_date'] + "low_price为None")
                elif pd.isnull(row['close']):
                    log.info(symbol + '.' + EXCHANGE_VT2TS[exchange] + row['trade_date'] + "close_price为None")
                elif pd.isnull(row['volumn']):
                    log.info(symbol + '.' + EXCHANGE_VT2TS[exchange] + row['trade_date'] + "volume为None")

                row.fillna(0)
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=date,
                    open_price=row['open'],
                    high_price=row['high'],
                    low_price=row['low'],
                    close_price=row['close'],
                    volume=row['volumn'],
                    turnover=row['turnover'],
                    gateway_name='akshare'
                )

                data.append(bar)
        return data

    def stock_list(self):
        """
        调用akshare获取沪深A股所有股票代码和名称
        优先东方财富(熔断期间跳过)，接口失败或被封禁时自动切换新浪
        :return:
        """
        if self.symbols is None:
            if not self.em_blocked:
                try:
                    df = self.pro.stock_zh_a_spot_em()
                    self.symbols = df[['代码', '名称']].rename(
                        columns={'代码': 'symbol', '名称': 'name'})
                    self.em_blocked = False
                    log.info("股票列表来源：东方财富")
                    return
                except Exception as ex:
                    log.war("东方财富股票列表获取失败，切换新浪：{}".format(repr(ex)))
                    self.em_blocked = True
                    self.em_fail_at = time.time()
            else:
                log.war("东方财富接口处于熔断状态，股票列表直接使用新浪")

            df = self.pro.stock_info_a_code_name()
            # 过滤北交所(92开头)等非沪深A股，避免to_vnpy_codes误判交易所
            df = df[df['code'].str.startswith(('00', '30', '60', '68'))]
            self.symbols = df[['code', 'name']].rename(
                columns={'code': 'symbol', 'name': 'name'})
            log.info("股票列表来源：新浪")

    def trade_day_list(self):
        """
        查询交易日历
        :return:
        """
        if self.trade_cal is None:
            self.trade_cal = dict()
            list_trade = self.pro.tool_trade_date_hist_sina()
            index = list_trade[list_trade['trade_date'] == date.today()]
            list_trade = list_trade.iloc[:index.index[0]]
            self.trade_cal[Exchange.SZSE.value] = self.trade_cal[Exchange.SSE.value] = list_trade
    
    def stock_individual_info(self, symbol):
        """
        查询个股信息（包括上市时间）
        """
        df = self.pro.stock_individual_info_em(symbol=symbol)
        list_day = df.loc[df['item']=='上市时间','value'].iloc[0]
        return str(list_day)


akshare_client = AKShareClient()

if __name__ == "__main__":
    print("测试akshare数据接口")
    # akshare_client = akshareClient()
    akshare_client.init()
    # print(akshare_client.symbols)
    # print(akshare_client.trade_cal)

    req = HistoryRequest(symbol='600600', exchange=Exchange.SSE,
                         start=datetime(year=1999, month=11, day=10), end=datetime.now(), interval=Interval.DAILY)

    ts_data = akshare_client.query_history(req)
    print(len(ts_data))
