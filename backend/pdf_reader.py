"""Helpers to extract raw data and text from PDF statements."""

import logging
import os
import re
from typing import List, Optional, Tuple, Union

import camelot
import pdfplumber
import pytesseract
from pdf2image import convert_from_path

logger = logging.getLogger(__name__)

RawData = Union[List["DataFrame"], List[str]]  # pandas imported dynamically by camelot


class PDFReader:
    """Centralized PDF reader with Camelot, pdfplumber, and OCR fallbacks."""

    _YEAR_PATTERN = re.compile(r"\b(20\d{2}|19\d{2})\b")
    _YEAR_SHORT_PATTERN = re.compile(r"\b(\d{2})/(\d{2})/(\d{2})\b")

    def __init__(self) -> None:
        self._cache: dict[str, Tuple[RawData, str]] = {}

    def extract_all(self, pdf_path: str, prefer_tables: bool = False) -> Tuple[RawData, str]:
        return self._extract_pdf(pdf_path, prefer_tables=prefer_tables)

    def extract_raw(self, pdf_path: str, prefer_tables: bool = False) -> RawData:
        raw, _ = self._extract_pdf(pdf_path, prefer_tables=prefer_tables)
        return raw

    def extract_text(self, pdf_path: str, prefer_tables: bool = False) -> str:
        _, text = self._extract_pdf(pdf_path, prefer_tables=prefer_tables)
        return text

    def infer_year_from_text(self, text: str, filename: str | None = None) -> Optional[int]:
(self, text: str, filename: str | None = None) -> Optional[int]:
        """Best-effort year inference based on statement text or filename."""
        if text:
            match = self._YEAR_PATTERN.search(text)
            if match:
                return int(match.group(1))

            short_match = self._YEAR_SHORT_PATTERN.search(text)
            if short_match:
                year = int(short_match.group(3))
                return 2000 + year if year < 70 else 1900 + year

        if filename:
            match = self._YEAR_PATTERN.search(filename)
            if match:
                return int(match.group(1))

        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _extract_pdf(self, pdf_path: str, *, prefer_tables: bool = False) -> Tuple[RawData, str]:
        cache_key = f"{os.path.abspath(pdf_path)}::{'tables' if prefer_tables else 'text'}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        strategies = []
        if prefer_tables:
            strategies.extend([self._try_camelot, self._try_pdfplumber])
        else:
            strategies.extend([self._try_pdfplumber, self._try_camelot])
        strategies.append(self._try_ocr)

        for strategy in strategies:
            raw, text = strategy(pdf_path)
            if raw:
                self._cache[cache_key] = (raw, text)
                return raw, text

        self._cache[cache_key] = ([], "")
        return [], ""

    def _try_camelot(self, pdf_path: str) -> Tuple[RawData, str]:
        try:
            tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
            if tables and len(tables) > 0:
                dataframes = [table.df for table in tables]
                text = "\n".join(self._tables_to_lines(dataframes))
                logger.info("Camelot detected %s tables", len(dataframes))
                return dataframes, text
        except Exception as exc:  # pragma: no cover - camelot can fail on some PDFs
            logger.warning("Camelot failed: %s", exc)
        return [], ""

    def _try_pdfplumber(self, pdf_path: str) -> Tuple[RawData, str]:
        lines: List[str] = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    if page_text:
                        for line in page_text.splitlines():
                            clean = line.strip()
                            if clean:
                                lines.append(clean)
            if lines:
                logger.info("pdfplumber extracted text from %s lines", len(lines))
                return lines, "\n".join(lines)
        except Exception as exc:
            logger.warning("pdfplumber failed: %s", exc)
        return [], ""

    def _try_ocr(self, pdf_path: str) -> Tuple[RawData, str]:
        try:
            images = convert_from_path(pdf_path)
            chunks = [pytesseract.image_to_string(image) for image in images]
            lines: List[str] = []
            for chunk in chunks:
                for line in chunk.splitlines():
                    clean = line.strip()
                    if clean:
                        lines.append(clean)
            if lines:
                logger.info("OCR extracted text from %s lines", len(lines))
                return lines, "\n".join(chunks)
        except Exception as exc:
            logger.error("OCR failed: %s", exc)
        return [], ""

    @staticmethod
    def _tables_to_lines(dataframes: List["DataFrame"]) -> List[str]:
        lines: List[str] = []
        for dataframe in dataframes:
            for _, row in dataframe.iterrows():
                values = [str(value).strip() for value in row if str(value).strip()]
                if values:
                    lines.append(" ".join(values))
        return lines
