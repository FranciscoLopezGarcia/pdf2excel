import re
from datetime import datetime
import pandas as pd
from typing import Dict, List

class Normalizer:
    STD_COLS = ["fecha","detalle","referencia","debito","credito","saldo"]


    def to_dataframe(self, rows: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        for c in self.STD_COLS:
            if c not in df:
                df[c] = ''
        return df[self.STD_COLS]


    def map_headers(self, headers: List[str]) -> Dict[int, str]:
        h = [str(x).lower() for x in headers]
        mapping = {}
        for i, v in enumerate(h):
            if 'fecha' in v: mapping[i] = 'fecha'
            elif any(k in v for k in ['concepto','detalle','descrip']): mapping[i] = 'detalle'
            elif 'refer' in v or 'comprob' in v or 'id' in v: mapping[i] = 'referencia'
            elif 'déb' in v or 'debito' in v or 'debi' in v or 'cargo' in v: mapping[i] = 'debito'
            elif 'créd' in v or 'credito' in v or 'credi' in v or 'abono' in v: mapping[i] = 'credito'
            elif 'saldo' in v or 'saldos' in v: mapping[i] = 'saldo'
        return mapping
    
    def parse_row_from_table(self, row, colmap: Dict[int, str]) -> Dict:
        tr = {c: '' for c in self.STD_COLS}
        for i, val in enumerate(row):
            key = colmap.get(i)
            if not key:
                continue
            s = str(val).strip()
            if key == 'fecha': tr['fecha'] = self.normalize_date(s)
            elif key == 'detalle': tr['detalle'] = self.clean(s)
            elif key == 'referencia': tr['referencia'] = s
            elif key in ('debito','credito','saldo'):
                amt = self.parse_amount(s)
                if amt != 0:
                    tr[key] = self.format_amount(amt)
        return tr


    def is_valid_tx(self, tr: Dict) -> bool:
        return bool(tr.get('fecha') and tr.get('detalle') and (tr.get('debito') or tr.get('credito') or tr.get('saldo')))


# ===== Normalizadores =====
    def normalize_date(self, s: str) -> str:
        s = re.sub(r'[^\d\/\-\.]','', s)
        fmts = ['%d/%m/%Y','%d/%m/%y','%d-%m-%Y','%d-%m-%y','%d.%m.%Y','%d.%m.%y','%Y/%m/%d','%Y-%m-%d']
        for f in fmts:
            try:
                return datetime.strptime(s, f).strftime('%d/%m/%Y')
            except Exception:
                pass
        return s
    

    def parse_amount(self, s: str) -> float:
        t = s.strip()
        neg = t.endswith('-') or t.startswith('-')
        t = t.replace('$','').replace('pesos','').strip()
        t = re.sub(r'[^\d\.,-]','', t)
        if t.endswith('-'): t = t[:-1]
        if t.startswith('-'): t = t[1:]
        if ',' in t and '.' in t:
            if t.rfind(',') > t.rfind('.'):
                t = t.replace('.','').replace(',', '.')
            else:
                t = t.replace(',','')
        elif ',' in t:
            parts = t.split(',')
            if len(parts) == 2 and len(parts[1]) == 2:
                t = t.replace(',', '.')
            else:
                t = t.replace(',', '')
        try:
            v = float(t)
            return -v if neg else v
        except Exception:
            return 0.0


    def format_amount(self, v: float) -> str:
        sign = '-' if v < 0 else ''
        a = f"{abs(v):,.2f}".replace(',', 'X').replace('.', ',').replace('X','.')
        return f"{sign}{a}"


    def clean(self, s: str) -> str:
        return re.sub(r'\s+',' ', str(s)).strip()


    def extract_ref(self, s: str) -> str:
        m = re.search(r'(?:NRO|REF|REFERENCIA|COMPROBANTE)\s*[:\.]?\s*(\d+)', s, re.I)
        return m.group(1) if m else ''


    def dedupe_preserve_order(self, seq):
        seen = set(); out = []
        for x in seq:
            if x not in seen:
                seen.add(x); out.append(x)
        return out