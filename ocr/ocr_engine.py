import os, logging
import pytesseract
from pdf2image import convert_from_path
from .preprocess import pil_to_cv, binarize
from .postprocess import sanitize_text
from ..config import POPPLER_BIN, TESSERACT_EXE, TESSDATA_DIR, OCR_LANG_PRIMARY, OCR_LANG_FALLBACK

log = logging.getLogger("ocr")

# Rutas ejecutable y tessdata
pytesseract.pytesseract.tesseract_cmd = TESSERACT_EXE
os.environ["TESSDATA_PREFIX"] = TESSDATA_DIR # 👉 apuntamos directo a la carpeta "tessdata"


# Parche para errores con acentos en Windows
from pytesseract import pytesseract as pytess_mod


def _patched_get_errors(error_string: bytes):
    try:
        return error_string.decode("utf-8", errors="ignore").splitlines()
    except Exception:
        return error_string.decode("latin-1", errors="ignore").splitlines()

pytess_mod.get_errors = _patched_get_errors

class OCREngine:
    def __init__(self, dpi: int = 300):
        self.dpi = dpi


    def pdf_to_text_pages(self, pdf_path: str):
        pages = convert_from_path(pdf_path, dpi=self.dpi, poppler_path=POPPLER_BIN)
        out = []
        for i, p in enumerate(pages, start=1):
            cv = pil_to_cv(p)
            thr = binarize(cv)
# Intento con idioma primario; si falla, intento fallback
            txt = self._ocr(thr, OCR_LANG_PRIMARY)
            if not txt.strip() and OCR_LANG_FALLBACK:
                txt = self._ocr(thr, OCR_LANG_FALLBACK)
            out.append(sanitize_text(txt))
            log.info(f"OCR page {i}: {len(txt)} chars")
        return out


    def _ocr(self, image_cv, lang: str) -> str:
        return pytesseract.image_to_string(image_cv, lang=lang)