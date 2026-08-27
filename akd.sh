#!/bin/bash

# Python 解释器：必须使用含 vnpy 的 niffler venv，裸 python 会解析到系统解释器导致 ModuleNotFoundError。
# 可通过环境变量 NIFTLER_PYTHON 覆盖。
PYTHON="${NIFTLER_PYTHON:-/Users/leo/opt/anaconda3/envs/niffler/bin/python}"

SOURCE="akshare"
PY_ARGS=()

while [ "$#" -gt 0 ]; do
    case "$1" in
        --source)
            SOURCE="$2"
            shift 2
            ;;
        --source=*)
            SOURCE="${1#*=}"
            shift
            ;;
        *)
            PY_ARGS+=("$1")
            shift
            ;;
    esac
done

# 定义检查间隔时间（秒）
CHECK_INTERVAL=10

# 定义连续无输出的最大次数(10次=100秒)。并发清洗下验证源(baostock)经全局锁串行，
# 多个 worker 会同时阻塞在锁上，短暂无新输出属正常，故放宽阈值避免误杀。
MAX_NO_OUTPUT_COUNT=10

# 定义一个临时文件来存储进程的输出
OUTPUT_FILE=$(mktemp)

# 定义日志文件
LOG_FILE="akd.log"

# 函数：从日志尾部解析最近完成的股票代码(兼容清洗/处理两种进度格式)
get_second_last_stock_code() {
    local log_file="$1"
    local last_lines=$(tail -n 200 "$log_file")
    # 提取全部进度标记中的 6 位代码，取倒数第 3 个(跳过可能仍在跑的最后两个)
    local stock_codes=($(echo "$last_lines" | grep -oE "(正在清洗股票代码:|正在处理股票代码：)[0-9]{6}" | grep -oE "[0-9]{6}" | tail -n 30))
    local n=${#stock_codes[@]}
    if [ "$n" -ge 3 ]; then
        echo "${stock_codes[$n-3]}"
    fi
}

# 函数：清理操作
cleanup() {
    echo "脚本被终止，正在清理..." | tee -a "$LOG_FILE"
    if [ -n "$PID" ]; then
        echo "杀死进程 $PID" | tee -a "$LOG_FILE"
        kill -9 $PID 2>/dev/null
        wait $PID 2>/dev/null
    fi

    rm -f "$OUTPUT_FILE"
    exit 1
}

# 捕获 SIGINT 信号
trap cleanup SIGINT

echo "首次执行，清理日志和进程..."
rm -f "$LOG_FILE"
pkill -9 -f "ak_dm\.py" 2>/dev/null

# 启动 python 脚本并将输出重定向到临时文件、标准输出和日志文件
# -u 无缓冲输出，确保看门狗能及时看到 tqdm/日志进度，避免误判卡死
"$PYTHON" -u ak_dm.py --source "$SOURCE" "${PY_ARGS[@]}" > >(tee "$OUTPUT_FILE" | tee -a "$LOG_FILE") 2>&1 &
# 获取进程ID
PID=$!

# 初始化无输出计数器
NO_OUTPUT_COUNT=0

while true; do
    # 先检查是否已完成(更新/清洗完成即退出，避免完成后被误杀重启)
    if grep -qE "A股股票全市场日线数据(更新|清洗)完毕" "$LOG_FILE"; then
        echo "检测到数据更新/清洗完毕，退出脚本..." | tee -a "$LOG_FILE"
        cleanup
    fi

    # 检查临时文件是否有新输出
    if [ -s "$OUTPUT_FILE" ]; then
        # 有新输出，清空临时文件并重置计数器
        > "$OUTPUT_FILE"
        NO_OUTPUT_COUNT=0
    else
        # 无输出，增加计数器
        NO_OUTPUT_COUNT=$((NO_OUTPUT_COUNT + 1))

        # 如果连续无输出次数达到最大值，认为进程卡死
        if [ "$NO_OUTPUT_COUNT" -ge "$MAX_NO_OUTPUT_COUNT" ]; then
            echo "进程卡死，杀死进程并重新启动..." | tee -a "$LOG_FILE"
            kill -9 $PID 2>/dev/null
            wait $PID 2>/dev/null

            # clean 模式断点续跑(读 clean_resume.txt 跳过已清洗完成的股票)
            if [[ " ${PY_ARGS[*]} " == *" -n "* || " ${PY_ARGS[*]} " == *" --clean "* ]]; then
                echo "重新启动脚本(断点续跑)..." | tee -a "$LOG_FILE"
                "$PYTHON" -u ak_dm.py --source "$SOURCE" --resume "${PY_ARGS[@]}" > >(tee "$OUTPUT_FILE" | tee -a "$LOG_FILE") 2>&1 &
            else
                # 其他模式(update 等)从上次处理的股票代码继续
                second_last_stock_code=$(get_second_last_stock_code "$LOG_FILE")
                if [ -n "$second_last_stock_code" ]; then
                    echo "重新启动脚本，使用股票代码: $second_last_stock_code" | tee -a "$LOG_FILE"
                    "$PYTHON" -u ak_dm.py --source "$SOURCE" -s "$second_last_stock_code" "${PY_ARGS[@]}" > >(tee "$OUTPUT_FILE" | tee -a "$LOG_FILE") 2>&1 &
                else
                    echo "未找到上次股票代码，重新启动脚本..." | tee -a "$LOG_FILE"
                    "$PYTHON" -u ak_dm.py --source "$SOURCE" "${PY_ARGS[@]}" > >(tee "$OUTPUT_FILE" | tee -a "$LOG_FILE") 2>&1 &
                fi
            fi
            PID=$!
            NO_OUTPUT_COUNT=0
        fi
    fi

    # 等待一段时间后再次检查
    sleep $CHECK_INTERVAL
done

# 删除临时文件
rm "$OUTPUT_FILE"
