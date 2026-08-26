@echo off
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "ROOT=%%~fI"

set "SRC=%ROOT%\lambda\gold"
set "MAIN=%SRC%\main.py"
set "ZIP=%SRC%\gold-lambda.zip"

if not exist "%MAIN%" (
  echo [ERROR] main.py not found:
  echo         %MAIN%
  exit /b 1
)

if exist "%ZIP%" del /f /q "%ZIP%"

powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Compress-Archive -LiteralPath $env:MAIN -DestinationPath $env:ZIP -Force"

if errorlevel 1 (
  echo [ERROR] Gold Lambda ZIP build failed.
  exit /b 1
)

if not exist "%ZIP%" (
  echo [ERROR] ZIP file was not created.
  exit /b 1
)

echo [OK] Gold Lambda ZIP created:
echo      %ZIP%

endlocal