import re
import pandas as pd
import pdfplumber
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import camelot
import logging
from pathlib import Path
from .ocr_extractor import OCRExtractor
import os
import json
import unicodedata

logger = logging.getLogger(__name__)




class UniversalBankExtractor:
    def __init__(self, config_path: str = None):
        self.ocr_extractor = OCRExtractor(lang="spa")
        self.inferred_year = None


        if config_path is None:
            config_path = Path(__file__).parent / "bank_config.json"
        with open(config_path, 'r', encoding='utf-8') as f:
            self.bank_config = json.load(f)
        
        # # Bank detection patterns
        # self.bank_patterns = {
        #     'CREDICOOP': r'Banco Credicoop|CREDICOOP|bancocredicoop\.coop',
        #     'BBVA': r'BBVA|Banco BBVA Argentina',
        #     'PATAGONIA': r'Banco Patagonia|PATAGONIA|bancopatagonia\.com',
        #     'HSBC': r'HSBC|hsbc\.com\.ar',
        #     'GALICIA': r'Banco Galicia|GALICIA|bancogalicia\.com',
        #     'MACRO': r'Banco Macro|MACRO',
        #     'MERCADOPAGO': r'Mercado Pago|MercadoPago',
        #     'SANJUAN': r'Banco San Juan|SAN JUAN|bancosanjuan\.com',
        #     'RIOJA': r'Banco Rioja|RIOJA',
        # }

    def extract_from_pdf(self, pdf_path: str, filename_hint: str = None) -> pd.DataFrame:

        self.inferred_year = self._extract_year_from_filename(filename_hint or str(pdf_path))
        raw_text = self._extract_raw_text(pdf_path)
        bank_name, bank_config = self._detect_bank(raw_text)
        
        logger.info(f"🏦 Banco detectado: {bank_name or 'GENERICO'}")
        
        # Try bank-specific parser
        if bank_config is not None and isinstance(bank_config, dict):
            try:
                df = self._parse_with_config(pdf_path, bank_config)
                if not df.empty:
                    return self._finalize_output(df, pdf_path)
            except Exception as e:
                logger.error(f"Parser especifico {bank_name} falló: {e}", exc_info=True)

        logger.warning("Usando parser genérico como fallback")
        return self._parse_generic(pdf_path)
    
    def _parse_with_config(self, pdf_path: str, config: Dict) -> pd.DataFrame:
        layout= config["layout"]
        if layout == "separate_all":
            return self._parse_separate_columns(pdf_path, config)
        elif layout == "single_signed":
            return self._parse_single_signed(pdf_path, config)
        elif layout == "signed_separate":
            return self._parse_signed_separate(pdf_path, config)
        else:
            raise ValueError(f"❌ Layout desconocido: {layout}")


    def _parse_separate_columns(self, pdf_path: str, config: Dict) -> pd.DataFrame:
        tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        transtactions =[]
        col_map = config["columns"]

        for table in tables:
            df = table.df
            if df.empty:
                continue

            header_idx = self._find_header_row(df, config["column_names"])
            if header_idx is None:
                continue

            for idx in range(header_idx + 1, len(df)):
                row = [str(cell).strip() for cell in df.iloc[idx]]

                if all (not cell or cell.lower() == 'nan' for cell in row):
                    continue

                fecha_raw = row[col_map["fecha"]] if col_map["fecha"] < len(row) else ""
                if not re.match(r"\d{1,2}[/-]\d{1,2}", fecha_raw):
                    continue

                fecha = self._normalize_date(fecha_raw, config.get("date_format"))
                detalle = row[col_map["descripcion"]] if col_map["descripcion"] < len(row) else ""

                debito = self._parse_amount(row[col_map["debito"]]) if col_map["debito"] < len(row) else 0
                credito = self._parse_amount(row[col_map["credito"]]) if col_map["credito"] < len(row) else 0
                saldo = self._parse_amount(row[col_map["saldo"]]) if col_map["saldo"] < len(row) else 0

                transtactions.append({
                    "fecha": fecha,
                    "detalle": detalle,
                    "referencia": "",
                    "debitos": abs(debito) if debito else 0,
                    "creditos": abs(credito) if credito else 0,
                    "saldo": saldo
                })
        return pd.DataFrame(transtactions)



    def _find_header_row(self, df, expected_headers):
        """
        Devuelve el índice de la fila que actúa como header, o None si no se encuentra.
        - df: DataFrame de Camelot (strings)
        - expected_headers: lista desde bank_config["column_names"]
        """
    

        def norm(s: str) -> str:
            if s is None:
                return ""
            s = str(s)
            # quitar acentos
            s = "".join(c for c in unicodedata.normalize("NFD", s)
                    if unicodedata.category(c) != "Mn")
            return s.strip().lower()

    # alias comunes
        alias = {
            "fecha": {"fecha", "fec", "fech"},
            "descripcion": {"descripcion", "descripción", "detalle", "concepto", "conceptos", "desc"},
            "debito": {"debito", "débitos", "deb", "débito"},
            "credito": {"credito", "créditos", "cred", "crédito"},
            "saldo": {"saldo", "saldos"},
            "importe": {"importe", "imp", "monto"},
            "comprobante": {"comprobante", "combte", "comprob", "comp"},
            "referencia": {"referencia", "ref", "nro", "n°"},
        }

        def canonicalize(h: str) -> str:
            h0 = norm(h)
            for k, vs in alias.items():
                if h0 in {norm(v) for v in vs} or h0 == k:
                 return k
            return h0

        expected = [canonicalize(h) for h in expected_headers]
        expected_set = set(expected)

        def row_keys(cells):
            keys = set()
            for c in cells:
                c0 = norm(c)
                for k, vs in alias.items():
                    if c0 in {norm(v) for v in vs}:
                        keys.add(k)
                keys.add(c0)
            return keys

        for i in range(len(df)):
            cells = [str(x) for x in df.iloc[i].tolist()]
            keys = row_keys(cells)

            strong = sum(1 for k in ("fecha", "saldo", "debito", "credito", "importe", "descripcion") if k in keys)
            coverage = len(expected_set.intersection(keys)) / max(1, len(expected_set))

            if (strong >= 2 and coverage >= 0.5) or (strong >= 3):
                return i

        # header partido en dos filas
            if i + 1 < len(df):
                cells2 = [str(x) for x in df.iloc[i + 1].tolist()]
                keys2 = row_keys(cells + cells2)
                strong2 = sum(1 for k in ("fecha", "saldo", "debito", "credito", "importe", "descripcion") if k in keys2)
                coverage2 = len(expected_set.intersection(keys2)) / max(1, len(expected_set))
                if (strong2 >= 2 and coverage2 >= 0.6) or (strong2 >= 3):
                    return i

        return None

    
    def _parse_single_signed(self, pdf_path: str, config: dict) -> pd.DataFrame:
        """Parse banks with a single signed amount column (e.g. MercadoPago, Macro)."""
        tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        transactions = []
        col_map = config["columns"]

        for table in tables:
            df = table.df
            if df.empty:
                continue

            header_idx = self._find_header_row(df, config["column_names"])
            if header_idx is None:
                continue

            for idx in range(header_idx + 1, len(df)):
                row = [str(cell).strip() for cell in df.iloc[idx]]

                if all(not cell or cell.lower() == "nan" for cell in row):
                    continue

                fecha_raw = row[col_map["fecha"]] if col_map["fecha"] < len(row) else ""
                if not re.match(r"\d{1,2}[/-]\d{1,2}", fecha_raw):
                    continue

                fecha = self._normalize_date(fecha_raw, config.get("date_format"))
                detalle = row[col_map["descripcion"]] if col_map["descripcion"] < len(row) else ""

                valor_raw = row[col_map["valor"]] if col_map["valor"] < len(row) else ""
                valor = self._parse_amount(valor_raw)

                debito, credito = 0.0, 0.0
                if valor is not None:
                    if valor < 0:
                        debito = abs(valor)
                    else:
                        credito = valor

                saldo = 0.0
                if "saldo" in col_map and col_map["saldo"] < len(row):
                    parsed = self._parse_amount(row[col_map["saldo"]])
                    if parsed is not None:
                        saldo = parsed

                transactions.append({
                "fecha": fecha,
                "detalle": detalle,
                "referencia": "",
                "debitos": debito,
                "creditos": credito,
                "saldo": saldo
                })

        return pd.DataFrame(transactions)



    def _parse_signed_separate(self, pdf_path: str, config: dict) -> pd.DataFrame:
        """Parse banks with separate debit/credit columns that may include +/- signs (e.g. Galicia)."""
        tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        transactions = []
        col_map = config["columns"]

        for table in tables:
            df = table.df
            if df.empty:
                continue

            header_idx = self._find_header_row(df, config["column_names"])
            if header_idx is None:
                continue

            for idx in range(header_idx + 1, len(df)):
                row = [str(cell).strip() for cell in df.iloc[idx]]

                if all(not cell or cell.lower() == "nan" for cell in row):
                    continue

                fecha_raw = row[col_map["fecha"]] if col_map["fecha"] < len(row) else ""
                if not re.match(r"\d{1,2}[/-]\d{1,2}", fecha_raw):
                    continue

                fecha = self._normalize_date(fecha_raw, config.get("date_format"))
                detalle = row[col_map["descripcion"]] if col_map["descripcion"] < len(row) else ""

                debito, credito, saldo = 0.0, 0.0, 0.0

                if col_map["debito"] < len(row):
                    parsed = self._parse_amount(row[col_map["debito"]])
                    if parsed is not None:
                        debito = abs(parsed) if parsed < 0 else parsed

                if col_map["credito"] < len(row):
                    parsed = self._parse_amount(row[col_map["credito"]])
                    if parsed is not None:
                        credito = abs(parsed) if parsed > 0 else parsed

                if "saldo" in col_map and col_map["saldo"] < len(row):
                    parsed = self._parse_amount(row[col_map["saldo"]])
                    if parsed is not None:
                        saldo = parsed

                transactions.append({
                "fecha": fecha,
                "detalle": detalle,
                "referencia": "",
                "debitos": debito,
                "creditos": credito,
                "saldo": saldo
            })

        return pd.DataFrame(transactions)



    def _extract_raw_text(self, pdf_path: str) -> str:
        """Extract all text for bank detection"""
        text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages[:3]:  # First 3 pages enough
                    text += page.extract_text() or ""
        except:
            pass
        return text

    def _detect_bank(self, text: str, filename: str = ""):
        text_norm = (text or "").upper()
        filename_norm = (filename or "").upper()

        for bank_name, config in self.bank_config.items():
            for kw in config.get("detection_keywords", []):
                if kw.upper() in text_norm or kw.upper() in filename_norm:
                    return bank_name, config
        return None, None  # o ("GENERICA", None) según tu flujo

    


    # ==================== CREDICOOP PARSER ====================
    def _parse_credicoop(self, pdf_path: str, text: str) -> pd.DataFrame:
        """
        CREDICOOP format:
        FECHA COMBTE DESCRIPCION DEBITO CREDITO SALDO
        SALDO ANTERIOR                              3.923.227,36
        05/05/25 186339 Transf...  492.307,46        3.430.919,90
        """
        tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        transactions = []
        
        for table in tables:
            df = table.df
            if df.empty:
                continue
            
            # Find header row
            header_idx = None
            for i, row in df.iterrows():
                row_text = ' '.join(str(cell).lower() for cell in row)
                if 'fecha' in row_text and 'saldo' in row_text:
                    header_idx = i
                    break
            
            if header_idx is None:
                continue
            
            # Process rows
            for idx in range(header_idx + 1, len(df)):
                row = df.iloc[idx]
                row_list = [str(cell).strip() for cell in row]
                
                # Skip empty rows
                if all(not cell or cell == 'nan' for cell in row_list):
                    continue
                
                # Check if SALDO ANTERIOR
                row_text = ' '.join(row_list).upper()
                if 'SALDO ANTERIOR' in row_text:
                    # Find the balance amount
                    saldo = None
                    for cell in reversed(row_list):
                        parsed = self._parse_amount(cell)
                        if parsed is not None:
                            saldo = parsed
                            break
                    
                    if saldo is not None:
                        transactions.append({
                            'fecha': '',
                            'detalle': 'SALDO ANTERIOR',
                            'referencia': '',
                            'debitos': 0.0,
                            'creditos': 0.0,
                            'saldo': saldo
                        })
                    continue
                
                # Regular transaction row
                fecha = row_list[0] if len(row_list) > 0 else ''
                if not re.match(r'\d{1,2}/\d{1,2}', fecha):
                    continue
                
                # Parse fecha
                fecha_norm = self._normalize_date(fecha)
                
                # Combine middle columns for detail
                detail_parts = []
                amounts = []
                
                for i, cell in enumerate(row_list[1:], 1):
                    parsed = self._parse_amount(cell)
                    if parsed is not None:
                        amounts.append(parsed)
                    elif cell and cell != 'nan':
                        detail_parts.append(cell)
                
                detalle = ' '.join(detail_parts)
                
                # Extract reference from detail
                ref_match = re.search(r'(\d{6,})', detalle)
                referencia = ref_match.group(1) if ref_match else ''
                
                # Categorize amounts: last is saldo, before that debit/credit
                if len(amounts) >= 2:
                    saldo = amounts[-1]
                    # Check if debit or credit
                    movement = amounts[-2]
                    
                    # Detect from keywords
                    detalle_lower = detalle.lower()
                    is_debit = any(kw in detalle_lower for kw in [
                        'transf.inmediata e/ctas.dist tit.o/bco', 'impuesto', 'comision',
                        'pago de servicios', 'debito', 'mantenimiento'
                    ])
                    is_credit = any(kw in detalle_lower for kw in [
                        'credito inmediato', 'transfer. e/cuentas', 'transf. interbanking'
                    ])
                    
                    if is_credit:
                        debitos = 0.0
                        creditos = abs(movement)
                    else:
                        debitos = abs(movement)
                        creditos = 0.0
                    
                    transactions.append({
                        'fecha': fecha_norm,
                        'detalle': detalle,
                        'referencia': referencia,
                        'debitos': debitos,
                        'creditos': creditos,
                        'saldo': saldo
                    })
                elif len(amounts) == 1:
                    # Only saldo (like impuesto line)
                    transactions.append({
                        'fecha': fecha_norm,
                        'detalle': detalle,
                        'referencia': referencia,
                        'debitos': 0.0,
                        'creditos': 0.0,
                        'saldo': amounts[0]
                    })
        
        return pd.DataFrame(transactions)

    # ==================== BBVA PARSER ====================
    def _parse_bbva(self, pdf_path: str, text: str) -> pd.DataFrame:
        """BBVA format similar to CREDICOOP"""
        tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        transactions = []
        
        for table in tables:
            df = table.df
            if df.empty:
                continue
            
            # Find data rows
            for idx, row in df.iterrows():
                row_list = [str(cell).strip() for cell in row]
                row_text = ' '.join(row_list).upper()
                
                # SALDO ANTERIOR
                if 'SALDO ANTERIOR' in row_text:
                    saldo = None
                    for cell in reversed(row_list):
                        parsed = self._parse_amount(cell)
                        if parsed is not None:
                            saldo = parsed
                            break
                    
                    if saldo is not None:
                        transactions.append({
                            'fecha': '',
                            'detalle': 'SALDO ANTERIOR',
                            'referencia': '',
                            'debitos': 0.0,
                            'creditos': 0.0,
                            'saldo': saldo
                        })
                    continue
                
                # Regular transaction
                fecha = row_list[0] if len(row_list) > 0 else ''
                if not re.match(r'\d{1,2}/\d{1,2}', fecha):
                    continue
                
                fecha_norm = self._normalize_date(fecha)
                
                # Parse amounts and detail
                detail_parts = []
                amounts = []
                
                for cell in row_list[1:]:
                    parsed = self._parse_amount(cell)
                    if parsed is not None:
                        amounts.append(parsed)
                    elif cell and cell != 'nan':
                        detail_parts.append(cell)
                
                detalle = ' '.join(detail_parts)
                referencia = re.search(r'(\d{6,})', detalle)
                referencia = referencia.group(1) if referencia else ''
                
                # Categorize
                if len(amounts) >= 2:
                    saldo = amounts[-1]
                    movement = amounts[-2]
                    
                    detalle_lower = detalle.lower()
                    is_credit = any(kw in detalle_lower for kw in ['credito', 'deposito', 'transferencia recibida'])
                    
                    if is_credit or movement > 0:
                        debitos = 0.0
                        creditos = abs(movement)
                    else:
                        debitos = abs(movement)
                        creditos = 0.0
                    
                    transactions.append({
                        'fecha': fecha_norm,
                        'detalle': detalle,
                        'referencia': referencia,
                        'debitos': debitos,
                        'creditos': creditos,
                        'saldo': saldo
                    })
        
        return pd.DataFrame(transactions)

    # ==================== PATAGONIA PARSER ====================
    def _parse_patagonia(self, pdf_path: str, text: str) -> pd.DataFrame:
        """Patagonia parser"""
        return self._parse_generic_running_balance(pdf_path, text)

    # ==================== HSBC PARSER ====================
    def _parse_hsbc(self, pdf_path: str, text: str) -> pd.DataFrame:
        """HSBC parser"""
        return self._parse_generic_running_balance(pdf_path, text)

    # ==================== GALICIA PARSER ====================
    def _parse_galicia(self, pdf_path: str, text: str) -> pd.DataFrame:
        """Galicia parser"""
        return self._parse_generic_running_balance(pdf_path, text)

    # ==================== GENERIC RUNNING BALANCE ====================
    def _parse_generic_running_balance(self, pdf_path: str, text: str) -> pd.DataFrame:
        """Generic parser for running balance statements"""
        tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        transactions = []
        
        for table in tables:
            df = table.df
            if df.empty:
                continue
            
            for idx, row in df.iterrows():
                row_list = [str(cell).strip() for cell in row]
                row_text = ' '.join(row_list).upper()
                
                # SALDO ANTERIOR
                if any(kw in row_text for kw in ['SALDO ANTERIOR', 'SALDO DEL PERIODO ANTERIOR']):
                    saldo = None
                    for cell in reversed(row_list):
                        parsed = self._parse_amount(cell)
                        if parsed is not None:
                            saldo = parsed
                            break
                    
                    if saldo is not None:
                        transactions.append({
                            'fecha': '',
                            'detalle': 'SALDO ANTERIOR',
                            'referencia': '',
                            'debitos': 0.0,
                            'creditos': 0.0,
                            'saldo': saldo
                        })
                    continue
                
                # Transaction row
                fecha = row_list[0] if len(row_list) > 0 else ''
                if not re.match(r'\d{1,2}[/-]\d{1,2}', fecha):
                    continue
                
                fecha_norm = self._normalize_date(fecha)
                
                detail_parts = []
                amounts = []
                
                for cell in row_list[1:]:
                    parsed = self._parse_amount(cell)
                    if parsed is not None:
                        amounts.append(parsed)
                    elif cell and cell != 'nan':
                        detail_parts.append(cell)
                
                detalle = ' '.join(detail_parts)
                referencia = ''
                
                if len(amounts) >= 2:
                    saldo = amounts[-1]
                    movement = amounts[-2]
                    
                    if movement < 0:
                        debitos = abs(movement)
                        creditos = 0.0
                    else:
                        debitos = 0.0
                        creditos = abs(movement)
                    
                    transactions.append({
                        'fecha': fecha_norm,
                        'detalle': detalle,
                        'referencia': referencia,
                        'debitos': debitos,
                        'creditos': creditos,
                        'saldo': saldo
                    })
        
        return pd.DataFrame(transactions)

    # ==================== GENERIC FALLBACK ====================
    def _parse_generic(self, pdf_path: str) -> pd.DataFrame:
        """Ultimate fallback"""
        logger.info("Usando parser genérico")
        
        # Try Camelot
        tables = []
        for flavor in ("stream", "lattice"):
            try:
                t = camelot.read_pdf(pdf_path, flavor=flavor, pages="all", strip_text=" .,$-", edge_tol=50, row_tol=10)
                if t  and len(t) > 0:
                    df = self._parse_generic_running_balance(pdf_path, flavor)
                    if not df.empty:
                        return self._finalize_output(df, pdf_path)
                    
            except Exception as e:
                logger.warning(f"Camelot ({flavor}) falló: {e}")
        # Try PDFPlumber
        try:
            text = self._extract_raw_text(pdf_path)
            transactions = self._parse_text_simple(text)
            if transactions:
                df = pd.DataFrame(transactions)
                return self._finalize_output(df, pdf_path)
        except Exception as e:
            logger.warning(f"PDFPlumber falló: {e}")
        
        # Try OCR
        try:
            pages_data = self.ocr_extractor.extract_text_pages(pdf_path)
            if pages_data:
                ocr_text = "\n\n".join([t for p, t in pages_data])
                transactions = self._parse_text_simple(ocr_text)
                if transactions:
                    df = pd.DataFrame(transactions)
                    return self._finalize_output(df, pdf_path)
        except Exception as e:
            logger.error(f"OCR falló: {e}")
        
        return pd.DataFrame()

    def _parse_text_simple(self, text: str) -> List[Dict]:
        """Simple text parser for fallback"""
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        transactions = []
        
        for line in lines:
            # Skip headers and noise
            if any(kw in line.lower() for kw in ['página', 'banco', 'cbu:', 'estimado']):
                continue
            
            # Find date
            date_match = re.search(r'\b(\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\b', line)
            if not date_match:
                continue
            
            fecha = self._normalize_date(date_match.group(1))
            
            # Extract amounts
            amounts = []
            for match in re.finditer(r'[-]?\$?\s*\d{1,3}(?:[.,]\d{3})*[.,]\d{2}', line):
                parsed = self._parse_amount(match.group(0))
                if parsed is not None:
                    amounts.append(parsed)
            
            if not amounts:
                continue
            
            # Get detail
            detalle = line.replace(date_match.group(0), '')
            for match in re.finditer(r'[-]?\$?\s*\d{1,3}(?:[.,]\d{3})*[.,]\d{2}', detalle):
                detalle = detalle.replace(match.group(0), '')
            detalle = re.sub(r'\s+', ' ', detalle).strip()
            
            # Simple categorization
            if len(amounts) >= 2:
                saldo = amounts[-1]
                movement = amounts[-2]
                
                if movement < 0:
                    debitos = abs(movement)
                    creditos = 0.0
                else:
                    debitos = 0.0
                    creditos = abs(movement)
                
                transactions.append({
                    'fecha': fecha,
                    'detalle': detalle,
                    'referencia': '',
                    'debitos': debitos,
                    'creditos': creditos,
                    'saldo': saldo
                })
        
        return transactions

    # ==================== UTILITIES ====================
    def _parse_amount(self, amount_str: str) -> Optional[float]:
        """Robust amount parser"""
        if not amount_str:
            return None
        
        clean_str = str(amount_str).strip()
        
        # Reject empty or very long
        if not clean_str or len(re.sub(r'[^\d]', '', clean_str)) > 11:
            return None
        
        # Handle parentheses (negative)
        is_parentheses = clean_str.startswith('(') and clean_str.endswith(')')
        if is_parentheses:
            clean_str = clean_str[1:-1].strip()
        
        # Handle trailing minus
        is_negative_suffix = clean_str.endswith('-')
        if is_negative_suffix:
            clean_str = clean_str[:-1].strip()
        
        # Handle leading minus
        is_negative_prefix = clean_str.startswith('-') or clean_str.startswith('−')
        if is_negative_prefix:
            clean_str = clean_str[1:].strip()
        
        # Remove currency symbols
        clean_str = re.sub(r'[$\s]', '', clean_str)
        
        # Validate format
        if not re.match(r'^\d{1,3}(?:[.,]\d{3})*[.,]\d{2}$|^\d+[.,]\d{2}$', clean_str):
            return None
        
        # Determine decimal separator
        if ',' in clean_str and '.' in clean_str:
            last_comma_pos = clean_str.rfind(',')
            last_dot_pos = clean_str.rfind('.')
            
            if last_comma_pos > last_dot_pos:
                clean_str = clean_str.replace('.', '').replace(',', '.')
            else:
                clean_str = clean_str.replace(',', '')
        elif ',' in clean_str:
            comma_pos = clean_str.rfind(',')
            digits_after = len(clean_str) - comma_pos - 1
            if digits_after == 2:
                clean_str = clean_str.replace(',', '.')
            else:
                clean_str = clean_str.replace(',', '')
        
        try:
            result = float(clean_str)
            if is_parentheses or is_negative_suffix or is_negative_prefix:
                result = -result
            return result
        except:
            return None

    def _normalize_date(self, date_str: str, date_format: str = None) -> str:
        """
        Normaliza la fecha a formato YYYY-MM-DD.
        Soporta un date_format explícito (desde el JSON), y si no, prueba varios formatos comunes.
        """
        if not date_str:
            return ""

        raw_date = date_str.strip()
        raw_date_clean = re.sub(r"[^\d\/\-\.]", "", raw_date)

    # Si hay formato explícito en config, probar primero
        if date_format:
            try:
                return datetime.strptime(raw_date, date_format).strftime("%Y-%m-%d")
            except Exception:
                pass  # si falla, sigue a la inferencia

    # Intentos con formatos típicos
        for fmt in ["%d/%m/%y", "%d/%m/%Y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"]:
            try:
                return datetime.strptime(raw_date_clean, fmt).strftime("%Y-%m-%d")
            except Exception:
                continue

    # Intento: día/mes sin año → inferir año actual
        match = re.match(r"(\d{1,2})[/\-\.](\d{1,2})$", raw_date_clean)
        if match:
            day, month = match.groups()
            year = getattr(self, "inferred_year", datetime.now().year)
            try:
                return datetime(year, int(month), int(day)).strftime("%Y-%m-%d")
            except Exception:
                pass

        logger.warning(f"No se pudo normalizar la fecha: {raw_date}")
        return raw_date


    def _extract_year_from_filename(self, filename: str) -> Optional[int]:
        """Extract year from filename"""
        year_match = re.search(r'20\d{2}', filename)
        if year_match:
            return int(year_match.group(0))
        return None

    def _finalize_output(self, df: pd.DataFrame, pdf_path: str = None) -> pd.DataFrame:
        """Finalize output with validation"""
        if df.empty:
            return df
        
        # Ensure columns
        required = ['fecha', 'detalle', 'referencia', 'debitos', 'creditos', 'saldo']
        for col in required:
            if col not in df.columns:
                df[col] = '' if col in ['fecha', 'detalle', 'referencia'] else 0.0
        
        # Convert amounts
        for col in ['debitos', 'creditos', 'saldo']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Extract mes and año
        df['fecha_dt'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y', errors='coerce', dayfirst=True)
        df['mes'] = df['fecha_dt'].dt.month.fillna(0).astype(int)
        df['año'] = df['fecha_dt'].dt.year.fillna(0).astype(int)
        
        # Add observaciones
        df['observaciones'] = ''
        
        # Validate balance
        df = self._validate_balance(df)
        df = self._flag_problem_rows(df)
        
        # Reorder
        ordered = ['fecha', 'mes', 'año', 'detalle', 'referencia', 'debitos', 'creditos', 'saldo', 'observaciones']
        df = df[ordered]
        
        return df

    def _validate_balance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate balance consistency"""
        for i in range(1, len(df)):
            prev_saldo = df.loc[i-1, 'saldo']
            current_credito = df.loc[i, 'creditos']
            current_debito = df.loc[i, 'debitos']
            current_saldo = df.loc[i, 'saldo']
            
            expected_saldo = prev_saldo + current_credito - current_debito
            
            if abs(current_saldo - expected_saldo) > 0.01:
                msg = f"Inconsistencia – revisar (esperado: {expected_saldo:.2f}, actual: {current_saldo:.2f})"
                if df.loc[i, 'observaciones']:
                    df.loc[i, 'observaciones'] += f"; {msg}"
                else:
                    df.loc[i, 'observaciones'] = msg
        
        return df

    def _flag_problem_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flag problem rows"""
        for i in range(len(df)):
            # All zeros
            if (df.loc[i, 'debitos'] == 0 and 
                df.loc[i, 'creditos'] == 0 and 
                df.loc[i, 'saldo'] == 0):
                msg = "Todos los montos en cero"
                if df.loc[i, 'observaciones']:
                    df.loc[i, 'observaciones'] += f"; {msg}"
                else:
                    df.loc[i, 'observaciones'] = msg
            
            # Mostly numeric detail
            detalle = str(df.loc[i, 'detalle'])
            if detalle and len(detalle) > 5:
                digit_ratio = sum(c.isdigit() for c in detalle) / len(detalle)
                if digit_ratio > 0.7:
                    msg = "Detalle parece corrupto"
                    if df.loc[i, 'observaciones']:
                        df.loc[i, 'observaciones'] += f"; {msg}"
                    else:
                        df.loc[i, 'observaciones'] = msg
        
        return df