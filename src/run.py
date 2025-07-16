# run.py
import sys
from pathlib import Path

# Добавляем путь к проекту в PYTHONPATH
sys.path.append(str(Path(__file__).parent))

from Producer_api.app.main import app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("Producer_api.app.main:app", host="0.0.0.0", port=8000, reload=True)