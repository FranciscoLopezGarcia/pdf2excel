"""Backend configuration helpers."""
import os
from pathlib import Path

# Default locations for external tools inside the container.
DEFAULT_TESSERACT_PATH = os.getenv("DEFAULT_TESSERACT_PATH", "tesseract")
DEFAULT_POPPLER_PATH = os.getenv("DEFAULT_POPPLER_PATH", "/usr/bin")

# Public settings consumed around the codebase.
TESSERACT_PATH = os.getenv("TESSERACT_PATH", DEFAULT_TESSERACT_PATH)
POPPLER_PATH = os.getenv("POPPLER_PATH", DEFAULT_POPPLER_PATH)
INPUT_DIR = Path(os.getenv("INPUT_DIR", "input"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))
MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE", str(50 * 1024 * 1024)))

# Security.
SECRET_KEY = os.getenv("SECRET_KEY", "super_secret_key")

# CORS origin (wildcard by default to keep backwards compatibility).
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*")
