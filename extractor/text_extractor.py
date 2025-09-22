import logging, re
import pandas as pd
import pdfplumber
from typing import List, Dict
from .normalizer import Normalizer

log = logging.getLogger("text_extractor")

class TextExtractor:
    def __init__(self):
        self.norm = Normalizer()
   
    def has_any_text(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as doc:
                for p in doc.pages:
                    if (p.extract_text() or "").strip():
                        return True
        except Exception as e:
            log.warning(f"pdfplumber failed: {e}")
        return False
    
    def extract_text_pdf(self, pdf_path: str) -> pd.DataFrame:
        full = ""
        with pdfplumber.open(pdf_path) as doc:
            for p in doc.pages:
                full += (p.extract_text() or "") + "\n"
        return self.parse_text_to_df(full)


    def parse_text_to_df(self, text: str) -> pd.DataFrame:
        rows: List[Dict] = []
        for line in (ln.strip() for ln in text.splitlines() if ln.strip()):
            if self._skip(line):
                continue
            tr = self._parse_tx_line(line)
            if tr:
                rows.append(tr)
        return self.norm.to_dataframe(rows) if rows else pd.DataFrame()
    
# ===== Heurísticas =====
    DATE_PATTERNS = [
    r"\b(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})\b",
    r"\b(\d{2,4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})\b",
    r"(\d{2})-(\d{2})\s+",
    r"(\d{2})-([A-Z]{3})",
    ]
    AMOUNT_PATTERNS = [
    r"\d{1,3}(?:[.,]\d{3})*[.,]\d{2}-",
    r"\$\s*-?\d{1,3}(?:[.,]\d{3})*[.,]\d{2}",
    r"-?\$?\s*\d{1,3}(?:[.,]\d{3})*[.,]\d{2}",
    r"-?\$?\s*\d+[.,]\d{2}",
    ]


    def _skip(self, line: str) -> bool:
        bad = [
r'^\s*p[áa]gina', r'^\s*hoja', r'noticias', r'cbu:', r'situaci[oó]n impositiva', r'transporte',
r'^\s*\d+\s*$', r'estado de cuentas', r'movimientos pendientes'
]
        low = line.lower()
        return any(re.search(p, low) for p in bad)


    def _parse_tx_line(self, line: str):
# fecha
        date_match = None
        for pat in self.DATE_PATTERNS:
            m = re.search(pat, line)
            if m:
                date_match = m
                break
        if not date_match:
            return None
        fecha = self.norm.normalize_date(date_match.group(0))


# montos en orden
        amts = []
        for pat in self.AMOUNT_PATTERNS:
            for m in re.finditer(pat, line):
                v = self.norm.parse_amount(m.group(0))
                if v != 0:
                    amts.append(v)
        amts = self.norm.dedupe_preserve_order(amts)


# detalle
        tmp = line
        tmp = re.sub(re.escape(date_match.group(0)), '', tmp, count=1)
        for pat in self.AMOUNT_PATTERNS:
            tmp = re.sub(pat, '', tmp)
        detalle = self.norm.clean(tmp)


        tr = {"fecha": fecha, "detalle": detalle, "referencia": self.norm.extract_ref(detalle),
    "debito": "", "credito": "", "saldo": ""}


        if not amts:
            return None
        if len(amts) == 1:
            tr['credito'] = self.norm.format_amount(abs(amts[0])) if amts[0] > 0 else ''
            tr['debito'] = self.norm.format_amount(abs(amts[0])) if amts[0] < 0 else ''
        elif len(amts) == 2:
            mov, saldo = amts[0], amts[1]
            tr['credito'] = self.norm.format_amount(abs(mov)) if mov > 0 else ''
            tr['debito'] = self.norm.format_amount(abs(mov)) if mov < 0 else ''
            tr['saldo'] = self.norm.format_amount(saldo)
        else:
            saldo = amts[-1]
            mov = max(amts[:-1], key=abs)
            tr['credito'] = self.norm.format_amount(abs(mov)) if mov > 0 else ''
            tr['debito'] = self.norm.format_amount(abs(mov)) if mov < 0 else ''
            tr['saldo'] = self.norm.format_amount(saldo)
        return tr