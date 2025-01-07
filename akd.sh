#!/bin/bash

# 定义检查间隔时间（秒）
CHECK_INTERVAL=10

# 定义连续无输出的最大次数
MAX_NO_OUTPUT_COUNT=3

# 定义一个临时文件来存储进程的输出
OUTPUT_FILE=$(mktemp)

# 定义日志文件
LOG_FILE="akd.log"

# 函数：从日志文件的结尾部分解析上次处理的股票
get_second_last_stock_code() {
    local log_file="$1"
    # 使用 tail 命令获取日志文件的最后 100 行（可以根据需要调整行数）
    local last_lines=$(tail -n 100 "$log_file")
    # 使用 awk 提取所有“正在处理股票代码”行
    local stock_codes=($(echo "$last_lines" | awk -F'正在处理股票代码：' '/正在处理股票代码：.* /{print $2}' | awk '{print $1}' | grep -E '^\d{6}$'))
   
    local second_last_stock_code="${stock_codes[${#stock_codes[@]}-3]}"
    echo "$second_last_stock_code"
}

# 函数：清理操作
cleanup() {
    echo "脚本被终止，正在清理..." | tee -a "$LOG_FILE"
    if [ -n "$PID" ]; then
        echo "杀死进程 $PID" | tee -a "$LOG_FILE"
        kill -9 $PID
        wait $PID
    fi

    rm -f "$OUTPUT_FILE"
    exit 1
}

# 捕获 SIGINT 信号
trap cleanup SIGINT

echo "首次执行，清理日志和进程..." 
rm -f "$LOG_FILE" | killall -9 python ak_dm.py

# 启动 python 脚本并将输出重定向到临时文件、标准输出和日志文件
python ak_dm.py > >(tee "$OUTPUT_FILE" | tee -a "$LOG_FILE") 2>&1 &
# 获取进程ID
PID=$!

# 初始化无输出计数器
NO_OUTPUT_COUNT=0

while true; do
    # 检查临时文件是否有新输出
    if [ -s "$OUTPUT_FILE" ]; then
        # 有新输出，清空临时文件并重置计数器
        > "$OUTPUT_FILE"
        NO_OUTPUT_COUNT=0
    else
        # 检查日志文件中是否包含“A股股票全市场日线数据更新完毕”
        if grep -q "A股股票全市场日线数据更新完毕" "$LOG_FILE"; then
            echo "检测到“A股股票全市场日线数据更新完毕”，退出脚本..."
            cleanup
        fi
        
        # 无输出，增加计数器
        NO_OUTPUT_COUNT=$((NO_OUTPUT_COUNT + 1))
        
        # 如果连续无输出次数达到最大值，认为进程卡死
        if [ "$NO_OUTPUT_COUNT" -ge "$MAX_NO_OUTPUT_COUNT" ]; then
            echo "进程卡死，杀死进程并重新启动..." | tee -a "$LOG_FILE"
            kill -9 $PID
            wait $PID
   
            # 获取上次处理的股票代码，如果没有找到，使用默认值”
            second_last_stock_code=$(get_second_last_stock_code "$LOG_FILE")
            if [ -n "$second_last_stock_code" ]; then
                echo "重新启动脚本，使用股票代码: $second_last_stock_code" | tee -a "$LOG_FILE"                
                python ak_dm.py -s "$second_last_stock_code" > >(tee "$OUTPUT_FILE" | tee -a "$LOG_FILE") 2>&1 &
                PID=$!
            else
                echo "未找到上次股票代码，重新启动脚本..." | tee -a "$LOG_FILE"
                python ak_dm.py > >(tee "$OUTPUT_FILE" | tee -a "$LOG_FILE") 2>&1 &
                PID=$!
            fi
        fi
    fi
    
    # 等待一段时间后再次检查
    sleep $CHECK_INTERVAL
done

# 删除临时文件
rm "$OUTPUT_FILE"
