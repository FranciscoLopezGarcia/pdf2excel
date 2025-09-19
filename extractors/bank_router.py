import re
import pandas as pd
import pdfplumber
import logging
from pathlib import Path

from universal_extractor import UniversalBankExtractor
from specific.comafi import ComafiParser  # el parser viejo que pasaste
from specific.icbc import ICBCParser      # idem
import pytesseract
from pdf2image import convert_from_path

log = logging.getLogger("bank_router")

class BankRouter:
    def __init__(self):
        self.universal = UniversalBankExtractor()
        self.comafi = ComafiParser()
        self.icbc = ICBCParser()

    def detect_bank(self, pdf_path: str) -> str:
        """Lee el texto de la primera página para detectar banco"""
        try:
            with pdfplumber.open(pdf_path) as doc:
                first_page = doc.pages[0].extract_text() or ""
        except Exception as e:
            log.warning(f"No se pudo leer texto para detectar banco: {e}")
            return "UNKNOWN"

        if re.search(r'comafi', first_page, re.IGNORECASE):
            return "COMAFI"
        elif re.search(r'icbc', first_page, re.IGNORECASE):
            return "ICBC"
        elif re.search(r'patagonia|macro|banco naci[oó]n|san juan', first_page, re.IGNORECASE):
            return "UNIVERSAL"
        else:
            return "UNKNOWN"

    def extract(self, pdf_path: str) -> pd.DataFrame:
        bank = self.detect_bank(pdf_path)
        log.info(f"Banco detectado: {bank}")

        if bank == "COMAFI":
            return self._extract_comafi(pdf_path)
        elif bank == "ICBC":
            return self._extract_icbc(pdf_path)
        else:
            # Universal extractor (Patagonia, Nación, San Juan, Macro, default)
            df = self.universal.extract_from_pdf(pdf_path)
            if df.empty:
                log.info("Extractor universal no encontró nada, probando OCR fallback")
                return self._extract_with_ocr(pdf_path)
            return df

    def _extract_comafi(self, pdf_path: str) -> pd.DataFrame:
        try:
            with pdfplumber.open(pdf_path) as doc:
                pages = [p.extract_text() or "" for p in doc.pages]
            parsed = self.comafi.parse(pages)
            rows = [row for account in parsed for row in account]
            return pd.DataFrame(rows)
        except Exception as e:
            log.error(f"Fallo parser Comafi: {e}")
            return self._extract_with_ocr(pdf_path)

    def _extract_icbc(self, pdf_path: str) -> pd.DataFrame:
        try:
            with pdfplumber.open(pdf_path) as doc:
                pages = [p.extract_text() or "" for p in doc.pages]
            parsed = self.icbc.parse(pages)
            rows = [row for account in parsed for row in account]
            return pd.DataFrame(rows)
        except Exception as e:
            log.error(f"Fallo parser ICBC: {e}")
            return self._extract_with_ocr(pdf_path)

    def _extract_with_ocr(self, pdf_path: str) -> pd.DataFrame:
        """Convierte PDF imagen a texto con OCR y usa UniversalExtractor sobre ese texto"""
        try:
            images = convert_from_path(pdf_path, dpi=300)
            text_pages = [pytesseract.image_to_string(img, lang="spa") for img in images]
            text = "\n".join(text_pages)

            # Pasamos el texto crudo al parser universal de texto
            rows = self.universal._parse_text_content_improved(text)
            if rows:
                return pd.DataFrame(rows)
            else:
                return pd.DataFrame()
        except Exception as e:
            log.error(f"OCR fallback falló: {e}")
            return pd.DataFrame()
