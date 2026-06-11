@echo off
setlocal enabledelayedexpansion
title MarketEcho - Startup

echo.
echo  ================================
echo   MarketEcho Pipeline - Starting
echo  ================================
echo.

:: --- Check Docker is running ---
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running. Please start Docker Desktop first.
    pause
    exit /b 1
)

:: --- Start infrastructure ---
echo [1/4] Starting infrastructure (Kafka, Flink, ClickHouse, Grafana)...
docker-compose up -d
if errorlevel 1 (
    echo [ERROR] docker-compose failed. Check docker-compose.yml.
    pause
    exit /b 1
)

:: --- Wait for Kafka to be ready ---
echo [2/4] Waiting for Kafka to be ready...
:wait_kafka
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092 >nul 2>&1
if errorlevel 1 (
    timeout /t 3 /nobreak >nul
    goto wait_kafka
)
echo       Kafka is ready.

:: --- Wait for Flink JobManager ---
echo       Waiting for Flink JobManager...
:wait_flink
curl -s http://localhost:8081/overview >nul 2>&1
if errorlevel 1 (
    timeout /t 3 /nobreak >nul
    goto wait_flink
)
echo       Flink is ready.

:: --- Wait for ClickHouse ---
echo       Waiting for ClickHouse...
:wait_clickhouse
docker exec clickhouse clickhouse-client --query "SELECT 1" >nul 2>&1
if errorlevel 1 (
    timeout /t 3 /nobreak >nul
    goto wait_clickhouse
)
echo       ClickHouse is ready.

:: --- Start Producers in separate windows ---
echo [3/4] Starting data producers...
start "MarketEcho - Price Producer" cmd /k "python producers/price_producer.py"
start "MarketEcho - News Producer"  cmd /k "python producers/news_producer.py"
echo       Producers started in separate windows.

:: --- Submit Flink job ---
echo [4/4] Submitting Flink job...
timeout /t 5 /nobreak >nul
docker exec flink-jobmanager flink run -py /jobs/sentiment_join.py
if errorlevel 1 (
    echo [WARN] Flink job submission failed. You can retry manually:
    echo        docker exec flink-jobmanager flink run -py /jobs/sentiment_join.py
) else (
    echo       Flink job submitted successfully.
)

:: --- Open Grafana ---
echo.
echo  ================================
echo   All systems up. Opening Grafana
echo  ================================
echo.
echo   Grafana:      http://localhost:3000  (admin / admin)
echo   Flink UI:     http://localhost:8081
echo   Kafka UI:     http://localhost:8080  (if kafka-ui enabled)
echo.
timeout /t 2 /nobreak >nul
start http://localhost:3000

endlocal
