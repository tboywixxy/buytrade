import os
import sys

# Add the app directory to path
sys.path.insert(0, os.path.dirname(__file__))

try:
    print("Starting MT5 Manager...")
    
    # Import manager directly to avoid uvicorn file scanning
    from manager_main import app
    import uvicorn
    
    print("Manager imported successfully, starting server...")
    uvicorn.run(app, host="0.0.0.0", port=9000, log_level="info")
    
except Exception as e:
    print(f"Failed to start manager: {e}")
    import traceback
    traceback.print_exc()
    input("Press Enter to close...")
