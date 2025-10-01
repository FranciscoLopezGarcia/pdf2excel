"""Parser placeholder for Banco HSBC."""

import logging
import os
from datetime import datetime
from typing import Sequence

import pandas as pd

from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)

BANK_NAME = "HSBC"
DETECTION_KEYWORDS: Sequence[str] = (
    "HSBC",
    "HSBC ARGENTINA",
    "HSBC BANK",
)


class HSBCParser(BaseParser):
    """Banco HSBC parser scaffold."""

    BANK_NAME = BANK_NAME
    DETECTION_KEYWORDS = DETECTION_KEYWORDS
    PREFER_TABLES = True
    OUTPUT_COLUMNS = [
        "fecha",
        "mes",
        "anio",
        "detalle",
        "referencia",
        "debito",
        "credito",
        "saldo",
    ]

    def detect(self, text: str, filename: str = "") -> bool:
        haystack = f"{text} {filename}".upper()
        return any(keyword in haystack for keyword in self.DETECTION_KEYWORDS)

    def parse(self, pdf_path: str, text: str = "") -> pd.DataFrame:
        from parsers.generic_parser import GenericParser

        if not text and hasattr(self, "reader"):
            try:
                text = self.reader.extract_text(pdf_path)  # type: ignore[attr-defined]
            except Exception:
                text = ""

        logger.warning("Using GenericParser fallback for %s", self.BANK_NAME)
        generic = GenericParser()
        if hasattr(self, "reader"):
            generic.reader = self.reader  # type: ignore[attr-defined]
        raw_data = text.splitlines() if text else []
        return generic.parse(raw_data, filename=os.path.basename(pdf_path))

    @staticmethod
    def _to_amount(raw: str) -> float:
        if not raw:
            return 0.0
        clean = raw.strip()
        if not clean:
            return 0.0
        negative = False
        if clean.startswith("(") and clean.endswith(")"):
            negative = True
            clean = clean[1:-1]
        if clean.startswith("-"):
            negative = True
            clean = clean[1:]
        clean = clean.replace(".", "").replace(" ", "")
        clean = clean.replace(",", ".")
        try:
            value = float(clean)
        except ValueError:
            return 0.0
        return -value if negative else value

    @staticmethod
    def _norm_date(raw: str) -> str:
        if not raw:
            return ""
        raw = raw.strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y"):
            try:
                return datetime.strptime(raw, fmt).strftime("%d/%m/%Y")
            except ValueError:
                continue
        return raw
