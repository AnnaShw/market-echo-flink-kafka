@echo off
title MarketEcho - Shutdown

echo.
echo  ================================
echo   MarketEcho Pipeline - Stopping
echo  ================================
echo.

:: --- Kill producer windows ---
echo [1/2] Stopping producers...
taskkill /fi "WindowTitle eq MarketEcho - Price Producer" /f >nul 2>&1
taskkill /fi "WindowTitle eq MarketEcho - News Producer"  /f >nul 2>&1
echo       Producers stopped.

:: --- Stop Docker services ---
echo [2/2] Stopping Docker services...
docker-compose down
echo       All services stopped.

echo.
echo  Done.
echo.
pause
