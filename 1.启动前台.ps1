$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot

if (-not (Test-Path (Join-Path $projectRoot 'frontend\node_modules'))) {
    throw 'Frontend dependencies are missing. Run 0.首次安装依赖.ps1 first.'
}

Set-Location (Join-Path $projectRoot 'frontend')
Write-Host 'Frontend started: http://127.0.0.1:5173' -ForegroundColor Green
npm run dev -- --host 0.0.0.0 --port 5173
