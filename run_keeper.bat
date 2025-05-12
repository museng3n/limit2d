@echo off
echo ===================================
echo Script Keeper - Launcher
echo ===================================

if "%1"=="" (
  echo ERROR: No script specified.
  echo Usage: keeper.bat your_script.py [arguments]
  goto end
)

echo Starting Script Keeper for: %1
echo.
echo Press Ctrl+C to stop (you may need to confirm)
echo.

python script_keeper.py %*

echo.
echo Script Keeper has stopped.

:end
pause