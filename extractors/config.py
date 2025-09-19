from pathlib import Path

# Ajustá estas rutas si cambian en tu PC (no requiere permisos de admin)
POPPLER_BIN = r"C:\Users\FranciscoLópezGarcía\Downloads\Release-25.07.0-0 (1)\poppler-25.07.0\Library\bin"
TESSERACT_EXE = r"C:\Users\FranciscoLópezGarcía\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"

# Carpeta de trabajo (relative a main.py)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR  = PROJECT_ROOT / "input"
OUTPUT_DIR = PROJECT_ROOT / "output"
REPORTS_DIR = PROJECT_ROOT / "reports"
LOG_FILE = REPORTS_DIR / "run.log"
