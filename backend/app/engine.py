from pathlib import Path
from search import EngineController

PROJECT_ROOT = Path(__file__).resolve().parents[2]
engine = EngineController(PROJECT_ROOT)