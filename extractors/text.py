
import logging
import re
from typing import List, Dict, Any
from utils.cleaner import normalize_date, parse_amount, clean_text, format_amount
import pdfplumber

log = logging.getLogger("text_extractor")

class TextExtractor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config or {}
        # amount regex: supports 1.234,56 or 1234.56 or 1.234.567,89 etc
        self.amount_re = r"-?\d{1,3}(?:[.,]\d{3})*[.,]\d{2}"
        self.date_re = r"\b(0?[1-9]|[12]\d|3[01])[\/\-\.](0?[1-9]|1[012])[\/\-\.]\d{2,4}\b"

    def extract_from_pdf(self, pdf_path: str) -> List[Dict]:
        texts = []
        try:
            with pdfplumber.open(pdf_path) as doc:
                for p in doc.pages:
                    t = p.extract_text() or ""
                    texts.append(t)
        except Exception as e:
            log.exception("pdfplumber failed", exc_info=e)
            return []
        full = "\n".join(texts)
        return self.extract_from_text(full)

    def extract_from_text(self, text: str) -> List[Dict]:
        rows = []
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        for line in lines:
            # skip lines that are headings or contain 'pagina' etc
            if len(line) < 4:
                continue
            # find date
            mdate = re.search(self.date_re, line)
            if not mdate:
                # sometimes date is at previous line; skip for now
                continue
            fecha = normalize_date(mdate.group(0))
            rest = line.replace(mdate.group(0), "").strip()
            # find all amounts in rest
            amounts = re.findall(self.amount_re, rest)
            parsed = [parse_amount(a) for a in amounts]
            detalle = re.sub(self.amount_re, "", rest).strip()
            detalle = clean_text(detalle)
            rec = {"fecha": fecha, "detalle": detalle}
            if len(parsed) == 0:
                rows.append(rec)
                continue
            if len(parsed) == 1:
                v = parsed[0]
                if v < 0:
                    rec["debitos"] = format_amount(abs(v))
                else:
                    rec["creditos"] = format_amount(v)
            else:
                # assume first movement, last balance
                mov = parsed[0]
                sal = parsed[-1]
                if mov < 0:
                    rec["debitos"] = format_amount(abs(mov))
                else:
                    rec["creditos"] = format_amount(mov)
                rec["saldo"] = format_amount(sal)
            rows.append(rec)
        return rows
