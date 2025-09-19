import logging
import pandas as pd
import pdfplumber
import re
from typing import Tuple

from .universal_extractor import UniversalBankExtractor
from .ocr_helper import pdf_to_text

log = logging.getLogger("bank_router")

class BankRouter:
    """Nativo primero. OCR solo si es imprescindible. Señala OCR_USED en metadata."""
    def __init__(self):
        self.universal = UniversalBankExtractor()

    def extract(self, pdf_path: str) -> Tuple[pd.DataFrame, bool]:
        ocr_used = False

        # 1) Intento nativo (tablas → texto)
        df = self.universal.extract_from_pdf(pdf_path)
        if not df.empty:
            return df, ocr_used

        # 2) Detección rápida: ¿tiene texto?
        has_text = False
        try:
            with pdfplumber.open(pdf_path) as d:
                for p in d.pages:
                    if (p.extract_text() or "").strip():
                        has_text = True
                        break
        except Exception as e:
            log.warning(f"No se pudo inspeccionar PDF: {e}")

        # 3) OCR solo si no hay texto
        if not has_text:
            log.info("PDF parece imagen, aplicando OCR…")
            try:
                txt = pdf_to_text(pdf_path)
                if txt.strip():
                    rows = self.universal._parse_text_content_improved(txt)
                    df = self.universal._normalize_output(rows) if rows else pd.DataFrame()
                    ocr_used = not df.empty
            except Exception as e:
                log.error(f"OCR fallback falló: {e}")

        return df, ocr_used
