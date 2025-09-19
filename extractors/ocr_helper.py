import pytesseract
from pdf2image import convert_from_path
from .config import POPPLER_BIN, TESSERACT_EXE
import logging

log = logging.getLogger("ocr_helper")
pytesseract.pytesseract.tesseract_cmd = TESSERACT_EXE

def pdf_to_text(pdf_path: str, lang: str = "spa") -> str:
    """Convierte PDF imagen a texto (OCR) en memoria."""
    pages = convert_from_path(pdf_path, dpi=300, poppler_path=POPPLER_BIN)
    text_pages = []

    for img in pages:
        txt = pytesseract.image_to_string(img, lang=lang)
        if isinstance(txt, bytes):
            txt = txt.decode("utf-8", errors="ignore")
        text_pages.append(txt)

    text = "\n".join(text_pages)
    if not text.strip():
        log.warning("⚠️ OCR no devolvió texto")

    return text
