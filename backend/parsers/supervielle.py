"""
Bank-specific parser.
Output columns (strict):
  fecha | mes | año | detalle | referencia | debito | credito | saldo
Notes:
  - Always emits "SALDO ANTERIOR" (first) and "SALDO FINAL" (last) rows if found.
  - Dates accepted: dd/mm[/yy|yyyy], dd-mm-yy, dd-mm-yyyy
  - Amounts: handles dot thousands + comma decimals; also "-" prefix or "( )" as negatives.
"""
from parsers.base import BaseBankParser
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

_AMT = r"[\-\(\)]?\d{1,3}(?:[\.\s]\d{3})*(?:,\d{2})?[\)]?"
_AMT_STRICT = r"-?\d{1,3}(?:\.\d{3})*,\d{2}"
_DATE = r"(?:\d{1,2}[\/-]\d{1,2}(?:[\/-]\d{2,4})?)"


def _to_amount(s: str) -> float:
    if s is None:
        return 0.0
    s = s.strip()
    if not s:
        return 0.0
    neg = False
    if s.startswith('(') and s.endswith(')'):
        neg = True
        s = s[1:-1]
    if s.startswith('-'):
        neg = True
        s = s[1:]
    s = s.replace('.', '').replace(' ', '').replace('\u00a0', '')
    s = s.replace(',', '.')
    try:
        val = float(s)
    except Exception:
        m = re.search(r"\d+(?:\.\d{2})?$", s)
        val = float(m.group(0)) if m else 0.0
    return -val if neg else val


def _norm_date(raw: str, year_hint: Optional[int] = None) -> str:
    if not raw:
        return ""
    raw = raw.strip().replace('.', '/').replace('-', '/')
    parts = raw.split('/')
    if len(parts) == 2:
        d, m = parts
        y = year_hint or datetime.now().year
        try:
            return datetime(int(y), int(m), int(d)).strftime("%d/%m/%Y")
        except Exception:
            pass
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%d/%m/%Y")
        except Exception:
            continue
    return raw


def _emit_row(rows: List[Dict[str, Any]], fecha: str, detalle: str, ref: str,
              deb: float, cre: float, saldo: float):
    mes = año = ""
    try:
        d = datetime.strptime(fecha, "%d/%m/%Y")
        mes = f"{d.month:02d}"
        año = str(d.year)
    except Exception:
        pass
    rows.append({
        "fecha": fecha or "",
        "mes": mes,
        "año": año,
        "detalle": (detalle or "").strip(),
        "referencia": (ref or "").strip(),
        "debito": round(float(deb or 0.0), 2),
        "credito": round(float(cre or 0.0), 2),
        "saldo": round(float(saldo or 0.0), 2),
    })


BANK_NAME = "SUPERVIELLE"
DETECTION_KEYWORDS = ["SUPERVIELLE", "CUENTA CORRIENTE EN U$S", "BANCO SUPERVIELLE"]


class SupervielleParser(BaseBankParser):
    BANK = BANK_NAME
    KEYWORDS = DETECTION_KEYWORDS

    def detect(self, text: str, filename: str = "") -> bool:
        t = (text or "").upper()
        f = (filename or "").upper()
        return any(k in t or k in f for k in self.KEYWORDS)

    def parse(self, pdf_path: str, text: str = ""):
        if not text:
            text = self.reader.extract_text(pdf_path)

        lines = [re.sub(r"\s+", " ", l).strip() for l in text.splitlines()]
        year_hint = self._infer_year(text)
        rows = []
        saldo_anterior = None
        saldo_final = None

        # Detectar saldos
        for ln in lines:
            if re.search(r"SALDO\s+(DEL\s+PER[IÍ]ODO\s+ANTERIOR|ANTERIOR)", ln, re.I):
                m = re.search(rf"({_AMT_STRICT})$", ln)
                if m:
                    saldo_anterior = _to_amount(m.group(1))
            if re.search(r"SALDO\s+(FINAL|ACTUAL|AL)", ln, re.I):
                m = re.search(rf"({_AMT_STRICT})$", ln)
                if m:
                    saldo_final = _to_amount(m.group(1))

        # Transacciones
        patt = re.compile(
            rf"^(?P<fecha>{_DATE})\s+(?P<detalle>.+?)\s+(?P<deb>{_AMT})?\s+(?P<cred>{_AMT})?\s+(?P<saldo>{_AMT})$",
            re.I
        )

        buffer = []
        current = None

        for ln in lines:
            if re.search(r"FECHA\s+CONCEPTO.*SALDO", ln, re.I):
                continue

            m = patt.match(ln)
            if m:
                if current:
                    current["detalle"] += " " + " ".join(buffer).strip()
                    _emit_row(rows, current["fecha"], current["detalle"], "", current["deb"], current["cred"], current["saldo"])
                    buffer = []
                g = m.groupdict()
                fecha = _norm_date(g.get("fecha", ""), year_hint)
                detalle = g.get("detalle", "")
                deb = _to_amount(g.get("deb") or "0")
                cred = _to_amount(g.get("cred") or "0")
                saldo = _to_amount(g.get("saldo") or "0")
                current = {"fecha": fecha, "detalle": detalle, "deb": deb, "cred": cred, "saldo": saldo}
            else:
                if current:
                    buffer.append(ln)

        if current:
            current["detalle"] += " " + " ".join(buffer).strip()
            _emit_row(rows, current["fecha"], current["detalle"], "", current["deb"], current["cred"], current["saldo"])

        # Saldos al inicio y fin
        if saldo_anterior is not None:
            _emit_row(rows, "", "SALDO ANTERIOR", "", 0, 0, saldo_anterior)
        if saldo_final is not None:
            _emit_row(rows, "", "SALDO FINAL", "", 0, 0, saldo_final)

        return self._finalize_dataframe(rows)
