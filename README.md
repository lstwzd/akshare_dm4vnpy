# akshare_dm4vnpy

#### 介绍
基于akshare同步A股历史数据到本地数据库

#### 软件架构
* vnpy + akshare

#### 安装教程
1. 确保vnpy环境已正常安装
2.  pip install -r requirements.txt

#### 使用说明

1. 配置好vnpy本地数据库
   在vnpy交易软件，配置->全局配置，例如mongodb数据库
   ```
   database.name  mongodb
   ...
   ```

2.  下载所有A股股票全市场日线数据
```
python ak_dm.py -a 
```

3.  自动更新A股股票全市场日线数据
```
python ak_dm.py -u
```

4.  逐日检测并自动更新A股股票全市场日线数据(注意！速度极慢)
```
python ak_dm.py -c
```

#### 注意事项

1. 若出现下载无响应则是频发查询导致网站禁用了，重新运行下载命令即可