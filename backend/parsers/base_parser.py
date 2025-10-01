import pandas as pd
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)

class BaseParser:
    """
    Clase base para todos los parsers de bancos.
    Define interfaz común y helpers reutilizables.
    """

    REQUIRED_COLUMNS = ["fecha", "mes", "año", "detalle", "referencia", "debito", "credito", "saldo"]

    def parse(self, raw_data, filename="") -> pd.DataFrame:
        """
        Método principal. Cada parser específico lo implementa.
        raw_data: puede venir como lista de DataFrames (Camelot) o lista de strings (pdfplumber/OCR).
        """
        raise NotImplementedError("El parser específico debe implementar este método")

    # ---------- Helpers comunes ----------

    def normalize_date(self, date_str: str, inferred_year=None) -> str:
        """Normaliza fecha a YYYY-MM-DD (si falla devuelve string original)."""
        if not date_str:
            return ""

        date_str = str(date_str).strip()
        formats = ["%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%d-%m-%Y", "%d-%m-%y"]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                continue

        # caso dd/mm → completar con año inferido o actual
        if "/" in date_str and len(date_str.split("/")) == 2:
            day, month = date_str.split("/")
            year = inferred_year or datetime.now().year
            try:
                dt = datetime(year=int(year), month=int(month), day=int(day))
                return dt.strftime("%Y-%m-%d")
            except Exception:
                pass

        logger.warning(f"No se pudo normalizar fecha: {date_str}")
        return date_str

    def finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza salida: columnas obligatorias y tipos de datos."""
        for col in self.REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = ""

        for col in ["debito", "credito", "saldo"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        if "fecha" in df.columns:
            try:
                parsed_dates = pd.to_datetime(df["fecha"], errors="coerce", dayfirst=True)
                df["año"] = parsed_dates.dt.year.fillna(df["año"])
                df["mes"] = parsed_dates.dt.month.fillna(df["mes"])
            except Exception:
                logger.debug("No se pudo inferir mes/año desde fecha")

        return df[self.REQUIRED_COLUMNS]

    def _infer_year(self, text: str = '', filename: str = '') -> int:
        # Infers statement year using reader (if present) and textual heuristics.
        reader = getattr(self, 'reader', None)
        if reader:
            infer = getattr(reader, 'infer_year_from_text', None)
            if callable(infer):
                try:
                    year = infer(text, filename)
                except TypeError:
                    year = infer(text)
                if year:
                    try:
                        return int(year)
                    except Exception:
                        return year
        text = text or ''
        filename = filename or ''
        match = re.search(r"(20\d{2}|19\d{2})", text)
        if match:
            return int(match.group(1))
        match_date = re.search(r"\d{1,2}[/-]\d{1,2}[/-](\d{2,4})", text)
        if match_date:
            year_str = match_date.group(1)
            if len(year_str) == 2:
                year = int(year_str)
                return 2000 + year if year < 70 else 1900 + year
            try:
                year = int(year_str)
                if 1900 <= year <= 2100:
                    return year
            except Exception:
                pass
        match_filename = re.search(r"(20\d{2}|19\d{2})", filename)
        if match_filename:
            return int(match_filename.group(1))
        return datetime.now().year

    def _finalize_dataframe(self, rows) -> pd.DataFrame:
        # Helper to convert row list into a finalized DataFrame.
        if not rows:
            return pd.DataFrame(columns=self.REQUIRED_COLUMNS)
        df = pd.DataFrame(rows)
        return self.finalize(df)
