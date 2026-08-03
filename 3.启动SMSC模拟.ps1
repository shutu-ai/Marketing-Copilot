$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot
$python = Join-Path $projectRoot '.venv\Scripts\python.exe'

if (-not (Test-Path $python)) {
    throw 'Backend dependencies are missing. Run 0.首次安装依赖.ps1 first.'
}

$env:PYTHONIOENCODING = 'utf-8'
Set-Location (Join-Path $projectRoot 'backend')
Write-Host 'SMSC simulator started: 127.0.0.1:2775 (credentials: test/test)' -ForegroundColor Green
& $python -m tools.smsc_simulator --host 127.0.0.1 --port 2775 --system-id test --password test
