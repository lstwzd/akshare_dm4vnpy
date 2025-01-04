import requests
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

    def init(self) -> bool:
        """"""
        if self.inited:
            return True

        try:
            self.pro = ak
            self.stock_list()
            self.trade_day_list()
        except (BaseException) as ex:
            return False

        self.inited = True
        return True

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

            # 保证每次调用时间在60/500=0.12秒内，以保证每分钟调用次数少于500次
            # begin_time = time.time()
            akshare_df = None
            while True:
                try:
                    # akshare_df = self.pro.query('daily', ts_code=tscode, start_date=start, end_date=simulate_end)
                    akshare_df = self.pro.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start, end_date=simulate_end, adjust="")
                except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
                    log.error(e)
                    # traceback.print_exc()
                    # ('Connection aborted.', ConnectionResetError(10054, '远程主机强迫关闭了一个现有的连接。', None, 10054, None))
                    if '10054' in str(e) or 'Read timed out.' in str(e):
                        sleep_time = 60.0
                        log.info("请求过于频繁，sleep：" + str(sleep_time) + "s")
                        time.sleep(sleep_time)
                        log.info("继续发送请求：" + symbol)
                        continue  # 继续发请求
                    else:
                        raise Exception(e)  # 其他异常，抛出来
                break
            if akshare_df is not None:
                akshare_df.rename(columns={'开盘':'open', '最高':'high', '最低':'low', '收盘':'close', '成交量':'volumn', '成交额':'turnover', '日期':'trade_date'}, inplace=True)
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
        调用akshare stock_basic 接口
        获得上海证券交易所和深圳证券交易所所有股票代码
        获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
        :return:
        """
        if self.symbols is None:
            # symbols_sse = self.pro.query('stock_basic', exchange=Exchange.SSE.value, fields='ts_code,symbol,name,'
            #                                                                                 'fullname,enname,market,'
            #                                                                                 'list_status,list_date,'
            #                                                                                 'delist_date,is_hs')
            # symbols_szse = self.pro.query('stock_basic', exchange=Exchange.SZSE.value, fields='ts_code,symbol,name,'
            #                                                                                   'fullname,enname,market,'
            #                                                                                   'list_status,list_date,'
            #                                                                                   'delist_date,is_hs')
            # self.symbols = pd.concat([symbols_sse, symbols_szse], axis=0, ignore_index=True)

            self.symbols = self.pro.stock_zh_a_spot_em()[['代码','名称']]
            self.symbols.rename(columns = {'代码':'symbol', '名称':'name'}, inplace = True)

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
