import logging
import pandas as pd
import pdfplumber
from typing import Tuple
from ..extractor.table_extractor import TableExtractor
from ..extractor.text_extractor import TextExtractor
from ..ocr.ocr_engine import OCREngine

log = logging.getLogger("router")

class BankRouter:
#"""Orquesta la estrategia: Tablas → Texto → OCR. Devuelve DF normalizado + flag de OCR usado."""


    def __init__(self):
        self.table = TableExtractor()
        self.text = TextExtractor()
        self.ocr = OCREngine()


    def extract(self, pdf_path: str) -> Tuple[pd.DataFrame, bool]:
        ocr_used = False

# 1) Tablas (Camelot)
        try:
            df = self.table.extract_tables(pdf_path)
            if not df.empty:
                log.info(f"[OK/TABLE] {pdf_path} → {len(df)} rows")
                return df, ocr_used
            log.info("Table method yielded no rows; trying text…")
        except Exception as e:
            log.warning(f"Table extraction failed: {e}")


# 2) Texto nativo (pdfplumber)
        try:
            if self.text.has_any_text(pdf_path):
                df = self.text.extract_text_pdf(pdf_path)
                if not df.empty:
                    log.info(f"[OK/TEXT] {pdf_path} → {len(df)} rows")
                    return df, ocr_used
                log.info("Text method yielded no rows; considering OCR…")
        except Exception as e:
            log.warning(f"Text extraction failed: {e}")


# 3) OCR (Tesseract) — último recurso
        try:
            pages_txt = self.ocr.pdf_to_text_pages(pdf_path)
            txt = "\n".join(pages_txt)
            if txt.strip():
                df = self.text.parse_text_to_df(txt)
                ocr_used = not df.empty
                log.info(f"[OK/OCR] {pdf_path} → {len(df)} rows")
            return df, ocr_used
        except Exception as e:
            log.error(f"OCR failed for {pdf_path}: {e}")


        return pd.DataFrame(), ocr_used