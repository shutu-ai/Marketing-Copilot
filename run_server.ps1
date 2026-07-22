Set-Location "D:\AGTS\Delivery\coding\MarketingCopilot\backend"
$env:PYTHONIOENCODING = "utf-8"
python -u -m uvicorn main:app --host 0.0.0.0 --port 8000 | Out-File "D:\AGTS\Delivery\coding\MarketingCopilot\backend\server.log" -Encoding utf8
