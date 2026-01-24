C:\MT5\worker_venv\Scripts\python.exe -m uvicorn manager_main:app --host 0.0.0.0 --port 9000 --log-level info


C:\MT5\worker_venv\Scripts\python.exe -m uvicorn orchestrator_service:app --host 0.0.0.0 --port 9100 --log-level info
