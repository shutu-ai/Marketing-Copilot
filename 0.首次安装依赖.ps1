$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw 'Python was not found. Install Python 3.10 or later, then reopen PowerShell.'
}
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    throw 'npm was not found. Install Node.js LTS, then reopen PowerShell.'
}

Set-Location $projectRoot
if (-not (Test-Path '.venv\Scripts\python.exe')) {
    Write-Host 'Creating Python virtual environment...' -ForegroundColor Cyan
    python -m venv .venv
}

$python = Join-Path $projectRoot '.venv\Scripts\python.exe'
Write-Host 'Installing backend dependencies...' -ForegroundColor Cyan
& $python -m pip install --upgrade pip
& $python -m pip install -r (Join-Path $projectRoot 'backend\requirements.txt')

Write-Host 'Installing frontend dependencies...' -ForegroundColor Cyan
Push-Location (Join-Path $projectRoot 'frontend')
try {
    npm install
}
finally {
    Pop-Location
}

Write-Host 'Installation complete. Run scripts 1, 2, and 3 in separate PowerShell windows.' -ForegroundColor Green
