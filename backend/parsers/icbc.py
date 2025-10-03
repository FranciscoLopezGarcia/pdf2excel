"""
Banco ICBC - Parser específico
Output columns (strict):
  fecha | mes | año | detalle | referencia | debito | credito | saldo
Notas:
  - Captura SALDO ANTERIOR y SALDO FINAL si están presentes
  - Maneja multilínea en detalle
  - Regex adaptada al formato de ICBC (fecha + concepto + opcionales + débitos + créditos + saldo)
"""
from parsers.base import BaseBankParser
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

_AMT = r"[\-\(\)]?\d{1,3}(?:[\.\s]\d{3})*(?:,\d{2})?[\)]?"
_AMT_STRICT = r"-?\d{1,3}(?:\.\d{3})*,\d{2}"
_DATE = r"(?:\d{1,2}[\/-]\d{1,2}(?:[\/-]\d{2,4})?)"

def _to_amount(s: str) -> float:
    if s is None:
        return 0.0
    s = s.strip()
    if not s or s in ["", "-", "nan"]:
        return 0.0
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    if s.startswith("-") or s.endswith("-"):
        neg = True
        s = s.replace("-", "")
    # normalize: remove thousands, change comma to dot
    s = s.replace(".", "").replace(" ", "").replace("\u00a0", "")
    s = s.replace(",", ".")
    try:
        val = float(s)
    except Exception:
        m = re.search(r"\d+(?:\.\d{2})?$", s)
        if m:
            val = float(m.group(0))
        else:
            logger.warning(f"[ICBC] No se pudo parsear monto: {s}")
            val = 0.0
    return -val if neg else val

def _norm_date(raw: str, year_hint: Optional[int] = None) -> str:
    if not raw:
        return ""
    raw = raw.strip().replace(".", "/").replace("-", "/")
    parts = raw.split("/")
    if len(parts) == 2:  # dd/mm sin año
        d, m = parts
        y = year_hint or datetime.now().year
        try:
            dt = datetime(int(y), int(m), int(d))
            return dt.strftime("%d/%m/%Y")
        except Exception:
            return raw
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%d/%m/%Y")
        except Exception:
            continue
    m = re.match(r"(\d{1,2})[\/-](\d{1,2})(?:[\/-](\d{2,4}))?$", raw)
    if m:
        d, mo, y = m.groups()
        if not y:
            y = year_hint or datetime.now().year
        else:
            y = int(y)
            if y < 100:
                y = 2000 + y
        try:
            dt = datetime(int(y), int(mo), int(d))
            return dt.strftime("%d/%m/%Y")
        except Exception:
            return raw
    return raw

def _emit_row(rows: List[Dict[str, Any]], fecha: str, detalle: str, ref: str,
              deb: float, cre: float, saldo: float):
    mes = año = ""
    try:
        if fecha:
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

class ICBCParser(BaseBankParser):
    BANK = "ICBC"
    KEYWORDS = ["ICBC", "INDUSTRIAL AND COMMERCIAL BANK OF CHINA (ARGENTINA)"]

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

        # Captura saldos inicial y final
        for ln in lines:
            if re.search(r"SALDO\s+(ULTIMO\s+EXTRACTO|ANTERIOR)", ln, re.I):
                m = re.search(rf"({_AMT_STRICT})$", ln)
                if m:
                    saldo_anterior = _to_amount(m.group(1))
            if re.search(r"\bSALDO\s+(FINAL|AL)\b", ln, re.I):
                m = re.search(rf"({_AMT_STRICT})$", ln)
                if m:
                    saldo_final = _to_amount(m.group(1))

        # Patrón principal para movimientos
        patt = re.compile(
            rf"^(?P<fecha>{_DATE})\s+(?P<detalle>.+?)\s+(?:{_DATE})?\s*(?:\S+)?\s*(?:\S+)?\s+(?P<deb>{_AMT})?\s+(?P<cred>{_AMT})?\s+(?P<saldo>{_AMT})$",
            re.I
        )

        for ln in lines:
            if re.search(r"FECHA\s+CONCEPTO.*CREDITOS?.*SALDO", ln, re.I):
                continue
            m = patt.match(ln)
            if not m:
                continue
            g = m.groupdict()
            fecha = _norm_date(g.get("fecha",""), year_hint)
            detalle = g.get("detalle","")
            deb = _to_amount(g.get("deb") or "0")
            cred = _to_amount(g.get("cred") or "0")
            saldo = _to_amount(g.get("saldo") or "0")
            if deb and deb < 0: deb = abs(deb)
            if cred and cred < 0: cred = abs(cred)
            _emit_row(rows, fecha, detalle, "", deb, cred, saldo)

        if saldo_anterior is not None:
            _emit_row(rows, "", "SALDO ANTERIOR", "", 0, 0, saldo_anterior)
        if saldo_final is not None:
            _emit_row(rows, "", "SALDO FINAL", "", 0, 0, saldo_final)

        return self._finalize_dataframe(rows)
