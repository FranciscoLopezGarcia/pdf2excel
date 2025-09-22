import re
import pandas as pd
import pdfplumber
import camelot
from datetime import datetime
from typing import List, Dict, Optional
import logging

from ..ocr.ocr_engine import OCRProcessor

log = logging.getLogger("universal_extractor")


class UniversalBankExtractor:
    """Extractor universal: intenta Tablas → Texto → OCR. Normaliza a columnas estándar."""

    DATE_PATTERNS = [
        r'\b(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})\b',
        r'\b(\d{2,4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})\b',
        r'(\d{2})-(\d{2})\s+',
        r'(\d{2})-([A-Z]{3})',
    ]
    AMOUNT_PATTERNS = [
        r'\d{1,3}(?:[.,]\d{3})*[.,]\d{2}-',
        r'\$\s*-?\d{1,3}(?:[.,]\d{3})*[.,]\d{2}',
        r'-?\$?\s*\d{1,3}(?:[.,]\d{3})*[.,]\d{2}',
        r'-?\$?\s*\d+[.,]\d{2}',
    ]

    def __init__(self, lang: str = "spa"):
        self.lang = lang
        self.ocr = OCRProcessor()

    # ============== ORQUESTA ==============
    def extract(self, pdf_path: str) -> tuple[pd.DataFrame, bool]:
        """Devuelve DataFrame normalizado + flag ocr_used"""
        # 1) Tablas
        try:
            rows = self._extract_from_tables(pdf_path)
            if rows and len(rows) >= 5:
                log.info(f"[TABLE] {len(rows)} rows from {pdf_path}")
                return self._normalize_output(rows), False
            log.info("[TABLE] Pocas filas, intentando texto")
        except Exception as e:
            log.warning(f"[TABLE] Falló: {e}")

        # 2) Texto
        try:
            rows = self._extract_from_text(pdf_path)
            if rows and len(rows) >= 5:
                log.info(f"[TEXT] {len(rows)} rows from {pdf_path}")
                return self._normalize_output(rows), False
            log.info("[TEXT] Pocas filas, intentando OCR")
        except Exception as e:
            log.warning(f"[TEXT] Falló: {e}")

        # 3) OCR
        try:
            text = self.ocr.pdf_to_text(pdf_path)
            rows = self._parse_text(text)
            if rows:
                log.info(f"[OCR] {len(rows)} rows from {pdf_path}")
                return self._normalize_output(rows), True
        except Exception as e:
            log.error(f"[OCR] Falló: {e}")

        return pd.DataFrame(), False

    # ============== TABLAS ==============
    def _extract_from_tables(self, pdf_path: str) -> List[Dict]:
        tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        out: List[Dict] = []
        for table in tables:
            df = table.df.fillna('').astype(str)
            if df.empty:
                continue

            header_idx = self._find_header_row(df)
            if header_idx is None:
                continue

            headers = df.iloc[header_idx].tolist()
            colmap = self._map_columns(headers)
            if not colmap:
                continue

            for _, row in df.iloc[header_idx + 1:].iterrows():
                tr = self._parse_table_row(row, colmap)
                if self._is_valid_transaction(tr):
                    out.append(tr)
        return out

    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        indicators = ['fecha', 'concepto', 'detalle', 'referencia', 'débito', 'crédito', 'saldo']
        for i, row in df.iterrows():
            txt = ' '.join([c.lower() for c in row])
            hits = sum(1 for k in indicators if k in txt)
            if hits >= 2:
                return i
        return None

    def _map_columns(self, headers: List[str]) -> Dict[int, str]:
        h = [str(x).lower() for x in headers]
        mapping = {}
        for i, v in enumerate(h):
            if 'fecha' in v:
                mapping[i] = 'fecha'
            elif any(k in v for k in ['concepto', 'detalle', 'descrip']):
                mapping[i] = 'detalle'
            elif 'refer' in v or 'comprob' in v or 'id' in v:
                mapping[i] = 'referencia'
            elif 'déb' in v or 'debi' in v or 'cargo' in v:
                mapping[i] = 'debitos'
            elif 'créd' in v or 'credi' in v or 'abono' in v:
                mapping[i] = 'creditos'
            elif 'saldo' in v or 'saldos' in v:
                mapping[i] = 'saldo'
        return mapping

    def _parse_table_row(self, row: pd.Series, colmap: Dict[int, str]) -> Dict:
        tr = {'fecha': '', 'detalle': '', 'referencia': '', 'debitos': '', 'creditos': '', 'saldo': ''}
        for i, val in enumerate(row):
            key = colmap.get(i)
            if not key:
                continue
            s = str(val).strip()
            if key == 'fecha':
                tr['fecha'] = self._normalize_date(s)
            elif key == 'detalle':
                tr['detalle'] = self._clean(s)
            elif key == 'referencia':
                tr['referencia'] = s
            elif key in ('debitos', 'creditos', 'saldo'):
                amt = self._parse_amount(s)
                if amt != 0:
                    tr[key] = self._format_amount(abs(amt)) if key != 'saldo' else self._format_amount(amt)
        return tr

    # ============== TEXTO ==============
    def _extract_from_text(self, pdf_path: str) -> List[Dict]:
        full = ""
        with pdfplumber.open(pdf_path) as doc:
            for p in doc.pages:
                full += (p.extract_text() or "") + "\n"
        return self._parse_text(full)

    def _parse_text(self, text: str) -> List[Dict]:
        out = []
        for line in (ln.strip() for ln in text.splitlines() if ln.strip()):
            if self._skip(line):
                continue
            tr = self._parse_tx_line(line)
            if tr:
                out.append(tr)
        return out

    def _parse_tx_line(self, line: str) -> Optional[Dict]:
        date_match = None
        for pat in self.DATE_PATTERNS:
            m = re.search(pat, line)
            if m:
                date_match = m
                break
        if not date_match:
            return None
        fecha = self._normalize_date(date_match.group(0))

        amts = []
        for pat in self.AMOUNT_PATTERNS:
            for m in re.finditer(pat, line):
                v = self._parse_amount(m.group(0))
                if v != 0:
                    amts.append(v)
        amts = self._dedupe_preserve_order(amts)

        tmp = line
        tmp = re.sub(re.escape(date_match.group(0)), '', tmp, count=1)
        for pat in self.AMOUNT_PATTERNS:
            tmp = re.sub(pat, '', tmp)
        detalle = self._clean(tmp)

        tr = {'fecha': fecha, 'detalle': detalle, 'referencia': self._extract_ref(detalle),
              'debitos': '', 'creditos': '', 'saldo': ''}
        if not amts:
            return None
        if len(amts) == 1:
            tr['creditos'] = self._format_amount(abs(amts[0])) if amts[0] > 0 else ''
            tr['debitos'] = self._format_amount(abs(amts[0])) if amts[0] < 0 else ''
        elif len(amts) == 2:
            mov, saldo = amts[0], amts[1]
            tr['creditos'] = self._format_amount(abs(mov)) if mov > 0 else ''
            tr['debitos'] = self._format_amount(abs(mov)) if mov < 0 else ''
            tr['saldo'] = self._format_amount(saldo)
        else:
            saldo = amts[-1]
            mov = max(amts[:-1], key=abs)
            tr['creditos'] = self._format_amount(abs(mov)) if mov > 0 else ''
            tr['debitos'] = self._format_amount(abs(mov)) if mov < 0 else ''
            tr['saldo'] = self._format_amount(saldo)
        return tr

    # ============== UTILS ==============
    def _skip(self, line: str) -> bool:
        bad = [
            r'^\s*p[áa]gina', r'^\s*hoja', r'noticias', r'cbu:', r'situaci[oó]n impositiva',
            r'transporte', r'^\s*\d+\s*$', r'estado de cuentas', r'movimientos pendientes'
        ]
        low = line.lower()
        return any(re.search(p, low) for p in bad)

    def _normalize_output(self, rows: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        for col in ['fecha', 'detalle', 'referencia', 'debitos', 'creditos', 'saldo']:
            if col not in df:
                df[col] = ''
        df = df[['fecha', 'detalle', 'referencia', 'debitos', 'creditos', 'saldo']]
        return df

    def _normalize_date(self, s: str) -> str:
        s = re.sub(r'[^\d\/\-\.]', '', s)
        fmts = ['%d/%m/%Y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y',
                '%d.%m.%Y', '%d.%m.%y', '%Y/%m/%d', '%Y-%m-%d']
        for f in fmts:
            try:
                return datetime.strptime(s, f).strftime('%d/%m/%Y')
            except:
                pass
        return s

    def _parse_amount(self, s: str) -> float:
        t = s.strip()
        neg = t.endswith('-') or t.startswith('-')
        t = t.replace('$', '').replace('pesos', '').strip()
        t = re.sub(r'[^\d\.,-]', '', t)
        if t.endswith('-'):
            t = t[:-1]
        if t.startswith('-'):
            t = t[1:]
        if ',' in t and '.' in t:
            if t.rfind(',') > t.rfind('.'):
                t = t.replace('.', '').replace(',', '.')
            else:
                t = t.replace(',', '')
        elif ',' in t:
            parts = t.split(',')
            if len(parts) == 2 and len(parts[1]) == 2:
                t = t.replace(',', '.')
            else:
                t = t.replace(',', '')
        try:
            v = float(t)
            return -v if neg else v
        except:
            return 0.0

    def _format_amount(self, v: float) -> str:
        f = f"{abs(v):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        return f if v >= 0 else f"-{f}"

    def _clean(self, s: str) -> str:
        return re.sub(r'\s+', ' ', str(s)).strip()

    def _extract_ref(self, s: str) -> str:
        m = re.search(r'(?:NRO|REF|REFERENCIA|COMPROBANTE)\s*[:\.]?\s*(\d+)', s, re.I)
        return m.group(1) if m else ''

    def _dedupe_preserve_order(self, seq):
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    def _is_valid_transaction(self, tr: Dict) -> bool:
        return bool(tr.get('fecha') and tr.get('detalle') and
                    (tr.get('debitos') or tr.get('creditos') or tr.get('saldo')))
