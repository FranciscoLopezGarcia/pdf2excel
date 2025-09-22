from pathlib import Path


# === Rutas locales (ajustá si hace falta) ===
POPPLER_BIN = r"C:\\Users\\FranciscoLópezGarcía\\Downloads\\Release-25.07.0-0 (2)\\poppler-25.07.0\\Library\\bin"
TESSERACT_EXE = r"C:\\Users\\FranciscoLópezGarcía\\AppData\\Local\\Programs\\Tesseract-OCR\\tesseract.exe"
TESSDATA_DIR = r"C:\\Users\\FranciscoLópezGarcía\\AppData\\Local\\Programs\\Tesseract-OCR\\tessdata"


# === Proyecto ===
PROJECT_ROOT = Path(__file__).resolve().parent
INPUT_DIR = PROJECT_ROOT / "input"
OUTPUT_DIR = PROJECT_ROOT / "output"
REPORTS_DIR = PROJECT_ROOT / "reports"
LOG_FILE = REPORTS_DIR / "run.log"


# === OCR ===
OCR_LANG_PRIMARY = "spa" # español
OCR_LANG_FALLBACK = "eng" # fallback


# === Batch ===
PARALLEL_WORKERS = 0 # 0 = secuencial; podés subirlo más adelante
MIN_ROWS_TABLE_METHOD = 5