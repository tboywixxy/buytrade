@echo off
cd /d C:\MT5
"C:\Users\Administrator\AppData\Local\Programs\Python\Python310\python.exe" -m uvicorn orchestrator_service:app --host 0.0.0.0 --port 9100
