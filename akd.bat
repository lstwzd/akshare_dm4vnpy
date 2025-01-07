@echo off
setlocal enabledelayedexpansion

:: 定义检查间隔时间（秒）
set CHECK_INTERVAL=10

:: 定义连续无输出的最大次数
set MAX_NO_OUTPUT_COUNT=3

:: 定义一个临时文件来存储进程的输出
set OUTPUT_FILE=%TEMP%\akd_output.tmp

:: 定义日志文件
set LOG_FILE=akd.log

:: 函数：从日志文件的结尾部分解析上次处理的股票
:GET_SECOND_LAST_STOCK_CODE
set log_file=%1
setlocal enabledelayedexpansion
set "last_lines="
for /f "delims=" %%i in ('type "%log_file%" ^| findstr /r /c:"正在处理股票代码："') do (
    set "line=%%i"
    set "line=!line:*正在处理股票代码：=!"
    set "line=!line:~0,6!"
    if "!line!" neq "" (
        set "stock_codes=!stock_codes! !line!"
    )
)
set "stock_codes=%stock_codes: =;%
set "stock_codes=%stock_codes:;;=;%
set "stock_codes=%stock_codes:;= !%
set "stock_codes=%stock_codes:~1%"
endlocal & set "second_last_stock_code=%stock_codes:~-10,6%"
goto :eof

:: 函数：清理操作
:CLEANUP
echo 脚本被终止，正在清理... >> "%LOG_FILE%"
if defined PID (
    echo 杀死进程 %PID% >> "%LOG_FILE%"
    taskkill /PID %PID% /F
    timeout /t %CHECK_INTERVAL% /nobreak >nul
)
del "%OUTPUT_FILE%" 2>nul
exit /b 1

:: 捕获 Ctrl+C 信号
if "%~1"=="cleanup" goto CLEANUP

:: 首次执行，清理日志和进程...
del "%LOG_FILE%" 2>nul
taskkill /IM python.exe /F 2>nul

:: 启动 python 脚本并将输出重定向到临时文件、标准输出和日志文件
start /B python ak_dm.py > "%OUTPUT_FILE%" 2>&1
set PID=%errorlevel%

:: 初始化无输出计数器
set NO_OUTPUT_COUNT=0

:LOOP
:: 检查临时文件是否有新输出
if exist "%OUTPUT_FILE%" (
    for /f "delims=" %%i in (%OUTPUT_FILE%) do (
        set "line=%%i"
        if "!line!" neq "" (
            echo !line! >> "%LOG_FILE%"
            echo !line!
        )
    )
    > "%OUTPUT_FILE%"
    set NO_OUTPUT_COUNT=0
) else (
    :: 检查日志文件中是否包含“A股股票全市场日线数据更新完毕”
    findstr /r /c:"A股股票全市场日线数据更新完毕" "%LOG_FILE%" >nul
    if !errorlevel! equ 0 (
        echo 检测到“A股股票全市场日线数据更新完毕”，退出脚本... >> "%LOG_FILE%"
        call :CLEANUP
    )
    
    :: 无输出，增加计数器
    set /a NO_OUTPUT_COUNT+=1
    
    :: 如果连续无输出次数达到最大值，认为进程卡死
    if %NO_OUTPUT_COUNT% geq %MAX_NO_OUTPUT_COUNT% (
        echo 进程卡死，杀死进程并重新启动... >> "%LOG_FILE%"
        if defined PID (
            taskkill /PID %PID% /F
            timeout /t %CHECK_INTERVAL% /nobreak >nul
        )
   
        :: 获取上次处理的股票代码，如果没有找到，使用默认值
        call :GET_SECOND_LAST_STOCK_CODE "%LOG_FILE%"
        if defined second_last_stock_code (
            echo 重新启动脚本，使用股票代码: %second_last_stock_code% >> "%LOG_FILE%"                
            start /B python ak_dm.py -s %second_last_stock_code% > "%OUTPUT_FILE%" 2>&1
            set PID=%errorlevel%
        ) else (
            echo 未找到上次股票代码，重新启动脚本... >> "%LOG_FILE%"
            start /B python ak_dm.py > "%OUTPUT_FILE%" 2>&1
            set PID=%errorlevel%
        )
    )
)

:: 等待一段时间后再次检查
timeout /t %CHECK_INTERVAL% /nobreak >nul
goto LOOP

:: 删除临时文件
del "%OUTPUT_FILE%" 2>nul