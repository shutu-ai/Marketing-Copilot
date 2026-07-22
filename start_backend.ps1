Write-Host "Starting Marketing Copilot Backend..." -ForegroundColor Green
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
