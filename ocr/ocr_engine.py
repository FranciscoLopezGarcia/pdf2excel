import os
import logging
import pytesseract
import cv2
import numpy as np
from pdf2image import convert_from_path
from typing import List
from ..config import POPPLER_BIN, TESSERACT_EXE, TESSDATA_DIR, OCR_LANG_PRIMARY, OCR_LANG_FALLBACK

log = logging.getLogger("ocr")

# Configuración base de Tesseract
pytesseract.pytesseract.tesseract_cmd = TESSERACT_EXE
os.environ["TESSDATA_PREFIX"] = TESSDATA_DIR  # apuntamos a carpeta tessdata

# Parche para errores de acentos en Windows
from pytesseract import pytesseract as pytess_mod
def _patched_get_errors(error_string: bytes):
    try:
        return error_string.decode("utf-8", errors="ignore").splitlines()
    except Exception:
        return error_string.decode("latin-1", errors="ignore").splitlines()
pytess_mod.get_errors = _patched_get_errors


class OCRProcessor:
    """OCR genérico con soporte para fallback de idioma."""

    def __init__(self, dpi: int = 300):
        self.dpi = dpi

    def pdf_to_images(self, pdf_path: str) -> List:
        """Convierte PDF en lista de imágenes PIL."""
        return convert_from_path(str(pdf_path), dpi=self.dpi, poppler_path=POPPLER_BIN)

    def image_to_text(self, img, lang: str) -> str:
        """Aplica OCR a una imagen PIL."""
        open_cv_image = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
        gray = cv2.cvtColor(open_cv_image, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
        return pytesseract.image_to_string(thresh, lang=lang)

    def extract_text_from_pdf(self, pdf_path: str) -> List[str]:
        """OCR completo de un PDF → lista de textos por página."""
        pages = self.pdf_to_images(pdf_path)
        out = []
        for i, img in enumerate(pages, start=1):
            txt = self.image_to_text(img, OCR_LANG_PRIMARY)
            if not txt.strip() and OCR_LANG_FALLBACK:
                txt = self.image_to_text(img, OCR_LANG_FALLBACK)
            log.info(f"OCR page {i}: {len(txt)} chars")
            out.append(txt.strip())
        return out
