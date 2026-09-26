$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot
$python = Join-Path $projectRoot '.venv\Scripts\python.exe'

if (-not (Test-Path $python)) {
    throw 'Backend dependencies are missing. Run 0.首次安装依赖.ps1 first.'
}

$env:PYTHONIOENCODING = 'utf-8'
Set-Location (Join-Path $projectRoot 'backend')
Write-Host 'Backend started: http://127.0.0.1:19090' -ForegroundColor Green
& $python -m uvicorn main:app --host 0.0.0.0 --port 19090
