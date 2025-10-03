# -*- coding: utf-8 -*-
import logging
import re
import os
from datetime import datetime
from typing import Sequence

import pandas as pd

from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)

BANK_NAME = "GALICIA"
DETECTION_KEYWORDS: Sequence[str] = (
    "BANCO GALICIA",
    "GALICIA",
    "OFFICE BANKING GALICIA",
)


class GaliciaParser(BaseParser):
    """Parser específico para Banco Galicia."""

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

    DATE_PATTERN = re.compile(r"(\d{1,2}[/-]\d{1,2})(?:[/-](\d{2,4}))?")
    AMOUNT_PATTERN = re.compile(r"-?\$?\s*\d{1,3}(?:\.\d{3})*,\d{2}")
    EXCLUDE_KEYWORDS = [
        "detalle de operaciones", "saldo anterior", "saldo al",
        "total movimientos", "página", "concepto", "débito", "crédito"
    ]

    def detect(self, text: str, filename: str = "") -> bool:
        haystack = f"{text} {filename}".upper()
        return any(keyword in haystack for keyword in self.DETECTION_KEYWORDS)

    def parse(self, pdf_path: str, text: str = "") -> pd.DataFrame:
        if not text and hasattr(self, "reader"):
            try:
                text = self.reader.extract_text(pdf_path)  # type: ignore[attr-defined]
            except Exception:
                text = ""

        lines = text.splitlines() if text else []
        rows = []
        year_hint = self._infer_year(text, pdf_path)

        current_tx = None
        buffer_concept = []

        for line in lines:
            clean = line.strip()
            if not clean:
                continue
            if any(kw in clean.lower() for kw in self.EXCLUDE_KEYWORDS):
                continue

            date_match = self.DATE_PATTERN.match(clean)
            if date_match:
                if current_tx:
                    if buffer_concept:
                        current_tx["detalle"] = " ".join(buffer_concept).strip()
                        buffer_concept = []
                    rows.append(current_tx)
                    current_tx = None

                day_month, year = date_match.group(1), date_match.group(2)
                if year:
                    if len(year) == 2:
                        year = 2000 + int(year)
                    else:
                        year = int(year)
                else:
                    year = year_hint or datetime.now().year

                try:
                    fecha = datetime.strptime(f"{day_month}/{year}", "%d/%m/%Y").strftime("%d/%m/%Y")
                except Exception:
                    fecha = f"{day_month}/{year}"

                current_tx = {
                    "fecha": fecha,
                    "mes": str(datetime.strptime(fecha, "%d/%m/%Y").month).zfill(2) if "/" in fecha else "",
                    "anio": str(datetime.strptime(fecha, "%d/%m/%Y").year) if "/" in fecha else "",
                    "detalle": "",
                    "referencia": "",
                    "debito": 0.0,
                    "credito": 0.0,
                    "saldo": 0.0,
                }
                continue

            if self.AMOUNT_PATTERN.match(clean):
                if current_tx:
                    amount = self._to_amount(clean)
                    if current_tx["debito"] == 0.0 and clean.startswith("-"):
                        current_tx["debito"] = abs(amount)
                    elif current_tx["credito"] == 0.0 and not clean.startswith("-"):
                        current_tx["credito"] = amount
                    else:
                        current_tx["saldo"] = amount
                continue

            if current_tx:
                buffer_concept.append(clean)

        if current_tx:
            if buffer_concept:
                current_tx["detalle"] = " ".join(buffer_concept).strip()
            rows.append(current_tx)

        df = pd.DataFrame(rows, columns=self.OUTPUT_COLUMNS)
        return df

    @staticmethod
    def _to_amount(raw: str) -> float:
        if not raw or raw.strip() in ["", "-"]:
            return 0.0
        clean = raw.strip().replace("$", "").replace(" ", "")
        negative = clean.startswith("-") or clean.endswith("-")
        clean = clean.replace("-", "").replace(".", "").replace(",", ".")
        try:
            value = float(clean)
            return -value if negative else value
        except ValueError:
            logger.warning(f"[{BANK_NAME}] No se pudo parsear monto: {raw}")
            return 0.0

    @staticmethod
    def _infer_year(text: str, filename: str = "") -> int:
        match = re.search(r"(20\d{2}|19\d{2})", text)
        if match:
            return int(match.group(1))
        match = re.search(r"(20\d{2}|19\d{2})", filename)
        if match:
            return int(match.group(1))
        return datetime.now().year
