# dd2vnpy

#### 介绍
多数据源同步A股历史数据到本地数据库(akshare / baostock / mootdx / efinance / astock)。

#### 软件架构
* vnpy + 多数据源(akshare/baostock/mootdx/efinance/astock)

#### 安装教程
1. 确保vnpy环境已正常安装
2. 安装依赖:
   ```
   pip install -r requirements.txt
   ```
3. 安装本地数据源模块(源码目录, 非 PyPI):
   ```
   cd data/vnpy_akshare && pip install -e .     # akshare/baostock/mootdx/efinance 源
   cd data/vnpy_astock && pip install -e .      # astock 源(通达信+腾讯备胎)
   ```

#### 使用说明

1. 配置好vnpy本地数据库
   * 默认使用 vnpy 本地数据库(SQLite),无需额外配置即可入库
   * 如需使用 mongodb,在 vnpy 全局配置中设置:
   ```
   database.name  mongodb
   database.host  localhost
   database.port  27017
   ```
   * 并确保对应 mongodb 数据库服务已启动


2.  下载所有A股股票全市场日线数据(linux/macos)
```
chmod +x ./akd.sh
./akd.sh
```
更新完成后自动退出

3.  逐日检测并自动更新A股股票全市场日线数据(注意！速度极慢)
```
python ak_dm.py -c
```

#### 多数据源与多数决议

支持 5 个数据源: `akshare`(默认) / `baostock` / `mootdx` / `efinance` / `astock`。

* `-o, --source` 指定主数据源
* `-v, --verify-source` 指定一个或多个验证数据源，支持逗号分隔(如 `baostock,mootdx`)。
  **默认自动选择**：不指定时，从 `baostock > mootdx > astock > efinance > akshare` 中自动
  选取第一个与主源不同的数据源(主源为 akshare 时默认验证源为 baostock)。
* `astock` 源基于通达信(mootdx)+腾讯备胎，日线/分钟线均与多源可比对口径一致(不复权、量按股)。
* 下载/更新/清洗时，主源与全部在场验证源按交易日对齐逐字段做**多数决议**：
  严格多数(>在场源数/2)达成一致的字段取该多数值；主源在多数簇内则用主源值，
  主源被多数否决时用多数簇中位数覆盖并记录冲突；无法形成多数时回退主源并标记。
  价格容差 0.5%，量额容差 1%。决议后 K 线经 OHLC 物理一致性校验后方可入库，
  不合法的决议K线直接跳过。
* 仅主源有数据的交易日按主源入库；主源缺失、仅验证源有的交易日不入库(不虚构)。

示例：
```
python ak_dm.py -u -o akshare                    # 默认自动选验证源(baostock)；-o 为 --source 短别名
python ak_dm.py -u -v baostock                   # 显式指定单个验证源；-v 为 --verify-source 短别名
python ak_dm.py -u -v "baostock,mootdx"          # 指定多个验证源做多数决议
python ak_dm.py -u -v ""                         # 关闭验证(仅主源)
```

#### 入库数据质量清洗

对已入库的日线数据重新拉取主源+验证源做多数决议，与库内数据逐根对比，发现
不一致的交易日自动删除并覆盖为决议后数据(防止停牌/复权/除权导致的历史脏数据)。

* **覆盖度守卫**：重新抓取的数据须覆盖(几乎)全部库内交易日，缺少交易日超过
  5% 时跳过该股票，避免以残缺数据覆盖完整历史。
* **无总览兜底**：仅库内有K线但没有 overview 记录的股票也会被纳入清洗。

```
python ak_dm.py -n                 # 清洗全部已入库日线数据(默认需输入 yes 确认)
python ak_dm.py -n -f              # 跳过确认直接清洗全部
python ak_dm.py -n -s 000001       # 仅清洗指定股票(无需确认)
```

#### 数据库一键清除

删除本地数据库中全部 K 线/ Tick 数据与总览记录(默认需输入 `yes` 确认，
`-f` 跳过确认)。

```
python ak_dm.py -p                 # 交互确认后清库
python ak_dm.py -p -f              # 跳过确认直接清库
```

#### 注意事项

1. 若出现下载无响应则是频发查询导致网站禁用了，重新运行下载命令即可
2. `-p/--purge` 与 `-n/--clean` 为破坏性操作，清库前请确认备份