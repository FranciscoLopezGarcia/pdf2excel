import re
import pandas as pd
import pdfplumber
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import camelot
import logging
from pathlib import Path
from .ocr_extractor import OCRExtractor

log = logging.getLogger("universal_extractor")

class UniversalBankExtractor:
    def __init__(self):
        # Universal patterns
        self.date_patterns = [
            r'\b(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})\b',
            r'\b(\d{2,4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})\b'
        ]
        
        # Universal column indicators
        self.date_indicators = ['fecha', 'date', 'fec', 'dia']
        self.detail_indicators = ['concepto', 'detalle', 'descripcion', 'causal', 'operacion', 'movimiento']
        self.reference_indicators = ['referencia', 'ref', 'nro', 'numero', 'comprobante', 'transaccion']
        self.debit_indicators = ['debito', 'debitos', 'debe', 'egreso', 'salida', 'cargo']
        self.credit_indicators = ['credito', 'creditos', 'haber', 'ingreso', 'entrada', 'abono', 'deposito']
        self.balance_indicators = ['saldo', 'balance', 'total']
        self.amount_indicators = ['importe', 'monto', 'valor']
        self.ocr_extractor = OCRExtractor(lang="spa")
        
        # Inferred year for dates without year
        self.inferred_year = None

    def _parse_amount(self, amount_str: str) -> Optional[float]:
        """
        Robust amount parser that:
        - Accepts comma as decimal, dot as thousands
        - Recognizes negative formats: (1.234,56), -1.234,56, 1.234,56-, −1.234,56
        - Rejects tokens that are too long (12+ digits)
        - Returns None if parsing fails
        - KEEPS the sign (no abs())
        """
        if not amount_str:
            return None
            
        clean_str = str(amount_str).strip()
        
        # Reject empty or very long tokens (likely account numbers)
        if not clean_str or len(re.sub(r'[^\d]', '', clean_str)) > 11:
            return None
        
        # Detect parentheses format (1.234,56) = negative
        is_parentheses = clean_str.startswith('(') and clean_str.endswith(')')
        if is_parentheses:
            clean_str = clean_str[1:-1].strip()
        
        # Detect negative suffix (1.234,56-)
        is_negative_suffix = clean_str.endswith('-')
        if is_negative_suffix:
            clean_str = clean_str[:-1].strip()
        
        # Detect negative prefix (-1.234,56 or unicode minus −)
        is_negative_prefix = clean_str.startswith('-') or clean_str.startswith('−')
        if is_negative_prefix:
            clean_str = clean_str[1:].strip()
        
        # Remove currency symbols and whitespace
        clean_str = re.sub(r'[$\s]', '', clean_str)
        
        # Check if what remains is a valid monetary format
        # Valid: digits with optional dots/commas as separators
        if not re.match(r'^\d{1,3}(?:[.,]\d{3})*[.,]\d{2}$|^\d+[.,]\d{2}$', clean_str):
            return None
        
        # Determine decimal separator (last comma or dot with exactly 2 digits after)
        if ',' in clean_str and '.' in clean_str:
            # Both present - last one with 2 digits is decimal
            last_comma_pos = clean_str.rfind(',')
            last_dot_pos = clean_str.rfind('.')
            
            if last_comma_pos > last_dot_pos:
                # Comma is decimal (Argentine format: 1.234,56)
                clean_str = clean_str.replace('.', '').replace(',', '.')
            else:
                # Dot is decimal (US format: 1,234.56)
                clean_str = clean_str.replace(',', '')
        elif ',' in clean_str:
            # Only comma - check if decimal or thousands
            comma_pos = clean_str.rfind(',')
            digits_after = len(clean_str) - comma_pos - 1
            if digits_after == 2:
                # Decimal separator
                clean_str = clean_str.replace(',', '.')
            else:
                # Thousands separator (unusual but handle it)
                clean_str = clean_str.replace(',', '')
        # else: only dots or no separators - assume US format or no thousands
        
        try:
            result = float(clean_str)
            # Apply negative sign if detected
            if is_parentheses or is_negative_suffix or is_negative_prefix:
                result = -result
            return result
        except (ValueError, OverflowError):
            return None

    def _to_float_strict(self, value) -> Optional[float]:
        """
        Centralized numeric conversion.
        - If string: parse with _parse_amount
        - If float/int: pass through
        - Otherwise: return None
        """
        if isinstance(value, str):
            return self._parse_amount(value)
        elif isinstance(value, (int, float)):
            return float(value)
        else:
            return None

    def extract_from_pdf(self, pdf_path: str, filename_hint: str = None) -> pd.DataFrame:
        """
        Main extraction pipeline: Camelot → PDFPlumber → OCR
        filename_hint: used for year inference (e.g. "Marzo 2025")
        """
        self.inferred_year = self._extract_year_from_filename(filename_hint or pdf_path)
        
        # PASO 1: Camelot (tables)
        log.info(f"🔍 Intentando extracción con Camelot (tablas)...")
        try:
            rows = self._extract_from_tables(pdf_path)
            if rows:
                log.info(f"✅ Camelot exitoso: {len(rows)} transacciones")
                return self._normalize_output(rows, pdf_path)
        except Exception as e:
            log.warning(f"❌ Camelot falló: {e}")

        # PASO 2: PDFPlumber (text)
        log.info(f"🔍 Intentando extracción con PDFPlumber (texto)...")
        try:
            rows = self._extract_from_text(pdf_path)
            if rows:
                log.info(f"✅ PDFPlumber exitoso: {len(rows)} transacciones")
                return self._normalize_output(rows, pdf_path)
        except Exception as e:
            log.warning(f"❌ PDFPlumber falló: {e}")

        # PASO 3: OCR (fallback final)
        log.info(f"🔍 Intentando extracción con OCR (imágenes)...")
        try:
            pages_data = self.ocr_extractor.extract_text_pages(pdf_path)
            if not pages_data:
                log.warning("❌ OCR no detectó páginas relevantes")
                return pd.DataFrame()
            
            log.info(f"📄 OCR detectó {len(pages_data)} páginas relevantes")
            ocr_text = "\n\n".join([f"--- Página {p} ---\n{t}" for p, t in pages_data])
            
            if not ocr_text.strip():
                log.warning("❌ OCR devolvió texto vacío")
                return pd.DataFrame()
            
            rows = self._parse_text_content_improved(ocr_text)
            if rows:
                log.info(f"✅ OCR exitoso: {len(rows)} transacciones extraídas")
                return self._normalize_output(rows, pdf_path)
            else:
                log.warning("❌ OCR no pudo parsear transacciones del texto")
                
        except Exception as e:
            log.error(f"❌ OCR falló completamente: {e}")

        log.error("🚫 Todos los métodos de extracción fallaron")
        return pd.DataFrame()

    def _extract_year_from_filename(self, filename: str) -> Optional[int]:
        """Extract year from filename like 'Marzo 2025' or '2025-03'"""
        year_match = re.search(r'20\d{2}', filename)
        if year_match:
            return int(year_match.group(0))
        return None

    def _extract_from_tables(self, pdf_path: str) -> List[Dict]:
        """Extract using camelot table detection"""
        tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        all_rows = []
        
        for table in tables:
            df = table.df
            if df.empty:
                continue
                
            df = self._clean_dataframe(df)
            header_row = self._find_header_row(df)
            
            if header_row is None:
                continue
                
            headers = df.iloc[header_row].tolist()
            column_map = self._map_columns(headers)
            
            # Process data rows
            for idx, row in df.iloc[header_row + 1:].iterrows():
                parsed_row = self._parse_table_row(row, column_map, original_line=str(row.tolist()))
                if self._is_valid_transaction(parsed_row):
                    all_rows.append(parsed_row)
                    
        return all_rows

    def _extract_from_text(self, pdf_path: str) -> List[Dict]:
        """Extract using PDFPlumber text parsing - IMPLEMENTED"""
        full_text = ""
        try:
            with pdfplumber.open(pdf_path) as doc:
                for page in doc.pages:
                    text = page.extract_text() or ""
                    full_text += text + "\n"
        except Exception as e:
            log.error(f"PDFPlumber reading failed: {e}")
            return []

        if not full_text.strip():
            return []
            
        return self._parse_text_content_improved(full_text)

    def _parse_text_content_improved(self, text: str) -> List[Dict]:
        """Parse transactions from raw text"""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        transactions = []
        
        for line_num, line in enumerate(lines, 1):
            if self._should_skip_line(line):
                continue
            
            transaction = self._parse_transaction_line(line)
            if transaction:
                transaction['_line_num'] = line_num
                transaction['_original_line'] = line
                transactions.append(transaction)
                
        return transactions

    def _parse_transaction_line(self, line: str) -> Optional[Dict]:
        """Parse individual transaction line"""
        # Find date
        date_match = None
        for pattern in self.date_patterns:
            match = re.search(pattern, line)
            if match:
                date_match = match
                break
                
        if not date_match:
            return None
            
        fecha = self._normalize_date(date_match.group(0))
        if not fecha:
            return None

        # Extract all amounts
        amounts = []
        for match in re.finditer(r'-?\$?\s*\d{1,3}(?:[.,]\d{3})*[.,]\d{2}[-]?|\(\d{1,3}(?:[.,]\d{3})*[.,]\d{2}\)', line):
            amount = self._parse_amount(match.group(0))
            if amount is not None:
                amounts.append(amount)

        # Remove date and amounts to get detail
        clean_line = line
        clean_line = re.sub(date_match.re.pattern, '', clean_line, count=1)
        for pattern in [r'-?\$?\s*\d{1,3}(?:[.,]\d{3})*[.,]\d{2}[-]?', r'\(\d{1,3}(?:[.,]\d{3})*[.,]\d{2}\)']:
            clean_line = re.sub(pattern, '', clean_line)
        
        detalle = self._clean_text(clean_line)
        
        return self._categorize_amounts_improved(fecha, detalle, amounts, line)

    def _categorize_amounts_improved(self, fecha: str, detalle: str, amounts: List[float], original_line: str) -> Optional[Dict]:
        """
        Simplified column categorization:
        1. Clean amounts list (remove None)
        2. If detalle contains 'saldo anterior/actual/al cierre' → assign to saldo
        3. Two numbers: first=movement (neg→debit, pos→credit), second=saldo
        4. Three+ numbers: last=saldo, others by sign (neg→debit, pos→credit)
        5. Never move negative saldo into debits/credits
        """
        # Clean amounts list
        amounts = [a for a in amounts if a is not None and a != 0]
        
        if not amounts:
            return None
            
        transaction = {
            'fecha': fecha,
            'detalle': detalle,
            'referencia': self._extract_reference(detalle),
            'debitos': None,
            'creditos': None,
            'saldo': None
        }
        
        detalle_lower = detalle.lower()
        original_lower = original_line.lower()
        
        # Check for saldo keywords
        saldo_keywords = ['saldo anterior', 'saldo actual', 'saldo al cierre', 'saldo del periodo', 'saldo al', 'saldo a ']
        is_saldo_line = any(kw in detalle_lower or kw in original_lower for kw in saldo_keywords)
        
        if is_saldo_line:
            # All amounts go to saldo (preserve sign)
            transaction['saldo'] = amounts[0]
            return transaction
        
        # Detect context
        is_debit_context = any(word in detalle_lower for word in [
            'debito', 'cargo', 'comision', 'impuesto', 'transferencia enviada', 
            'retiro', 'pago', 'automatico', 'imp.', 'iva', 'interes', 'mantenimiento',
            'ret.', 'transf. prop'
        ])
        
        is_credit_context = any(word in detalle_lower for word in [
            'credito', 'deposito', 'transferencia recibida', 'abono', 'ingreso', 
            'transferencia entre', 'credito por transferencia'
        ])

        if len(amounts) == 1:
            # Single amount: classify by sign or context
            amount = amounts[0]
            if amount < 0 or is_debit_context:
                transaction['debitos'] = abs(amount)
            else:
                transaction['creditos'] = abs(amount)
                
        elif len(amounts) == 2:
            # Two amounts: movement + saldo
            movement, saldo = amounts[0], amounts[1]
            
            if is_debit_context:
                transaction['debitos'] = abs(movement)
            elif is_credit_context:
                transaction['creditos'] = abs(movement)
            else:
                # Use sign
                if movement < 0:
                    transaction['debitos'] = abs(movement)
                else:
                    transaction['creditos'] = abs(movement)
            
            # Saldo preserves sign
            transaction['saldo'] = saldo
            
        else:
            # Three or more: last is saldo, others by sign
            saldo = amounts[-1]
            movements = amounts[:-1]
            
            # Classify movements
            debits = [abs(m) for m in movements if m < 0]
            credits = [abs(m) for m in movements if m >= 0]
            
            # Use context if available
            if is_debit_context and debits:
                transaction['debitos'] = sum(debits)
            elif is_credit_context and credits:
                transaction['creditos'] = sum(credits)
            else:
                # Default: sum by sign
                if debits:
                    transaction['debitos'] = sum(debits)
                if credits:
                    transaction['creditos'] = sum(credits)
            
            transaction['saldo'] = saldo
        
        return transaction

    def _extract_reference(self, detalle: str) -> str:
        """Extract reference number from detail text"""
        patterns = [
            r'NRO\.?\s*(\d+)',
            r'REF\.?\s*(\d+)', 
            r'REFERENCIA\s*(\d+)',
            r'COMPROBANTE\s*(\d+)',
            r'(\d{8,})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, detalle, re.IGNORECASE)
            if match:
                return match.group(1)
        return ''

    def _parse_table_row(self, row: pd.Series, column_map: Dict[int, str], original_line: str) -> Dict:
        """Parse a single table row - uses _to_float_strict"""
        transaction = {
            'fecha': '',
            'detalle': '',
            'referencia': '',
            'debitos': None,
            'creditos': None,
            'saldo': None,
            '_original_line': original_line
        }
        
        for i, value in enumerate(row):
            field = column_map.get(i)
            if not field:
                continue
                
            value_str = str(value).strip()
            
            if field == 'fecha':
                transaction['fecha'] = self._normalize_date(value_str)
            elif field == 'detalle':
                transaction['detalle'] = self._clean_text(value_str)
            elif field == 'referencia':
                transaction['referencia'] = value_str
            elif field in ['debitos', 'creditos', 'saldo']:
                amount = self._to_float_strict(value_str)
                if amount is not None:
                    if field == 'saldo':
                        # Preserve sign for saldo
                        transaction[field] = amount
                    else:
                        # Debits/credits are always positive
                        transaction[field] = abs(amount)
            elif field == 'importe':
                amount = self._to_float_strict(value_str)
                if amount is not None:
                    if amount < 0:
                        transaction['debitos'] = abs(amount)
                    else:
                        transaction['creditos'] = amount
        
        return transaction

    def _normalize_date(self, date_str: str) -> str:
        """
        Normalize date to DD/MM/YYYY format.
        When date lacks year, infer from:
        1. Previously parsed fecha column
        2. Filename hint
        3. Current year
        """
        if not date_str:
            return ""
            
        date_clean = re.sub(r'[^\d\/\-\.]', '', date_str)
        
        formats = ['%d/%m/%Y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y', 
                  '%d.%m.%Y', '%d.%m.%y', '%Y/%m/%d', '%Y-%m-%d']
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_clean, fmt)
                return dt.strftime('%d/%m/%Y')
            except:
                continue
        
        # Try parsing DD/MM without year
        match = re.match(r'(\d{1,2})[/\-\.](\d{1,2})$', date_clean)
        if match:
            day, month = match.groups()
            year = self.inferred_year or datetime.now().year
            log.warning(f"Fecha sin año detectada: {date_clean}, usando año {year}")
            try:
                dt = datetime(year, int(month), int(day))
                return dt.strftime('%d/%m/%Y')
            except:
                pass
        
        return date_clean

    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        """Find the header row in a dataframe"""
        for i, row in df.iterrows():
            row_text = ' '.join([str(cell).lower() for cell in row])
            header_score = 0
            for indicator_list in [self.date_indicators, self.detail_indicators, 
                                 self.balance_indicators]:
                if any(ind in row_text for ind in indicator_list):
                    header_score += 1
            
            if header_score >= 2:
                return i
        return None

    def _map_columns(self, headers: List[str]) -> Dict[int, str]:
        """Map column indices to standardized field names"""
        mapping = {}
        
        for i, header in enumerate(headers):
            header_lower = str(header).lower().strip()
            
            if any(ind in header_lower for ind in self.date_indicators):
                mapping[i] = 'fecha'
            elif any(ind in header_lower for ind in self.detail_indicators):
                mapping[i] = 'detalle'
            elif any(ind in header_lower for ind in self.reference_indicators):
                mapping[i] = 'referencia'
            elif any(ind in header_lower for ind in self.debit_indicators):
                mapping[i] = 'debitos'
            elif any(ind in header_lower for ind in self.credit_indicators):
                mapping[i] = 'creditos'
            elif any(ind in header_lower for ind in self.balance_indicators):
                mapping[i] = 'saldo'
            elif any(ind in header_lower for ind in self.amount_indicators):
                mapping[i] = 'importe'
                
        return mapping

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        return text

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean dataframe by removing empty rows/columns"""
        df = df.fillna('').astype(str)
        df = df[~df.apply(lambda row: all(cell.strip() == '' for cell in row), axis=1)]
        df = df.loc[:, ~df.apply(lambda col: all(cell.strip() == '' for cell in col))]
        return df

    def _should_skip_line(self, line: str) -> bool:
        """Check if line should be skipped"""
        skip_patterns = [
            r'^\s*p[áa]gina',
            r'^\s*hoja',
            r'^\s*estimado cliente',
            r'^\s*banco',
            r'^\s*cbu:',
            r'^\s*cuenta corriente en pesos',
            r'movimientos pendientes',
            r'^\s*\d+\s*$',
            r'^\s*subcta\s+suc\s+mda',
            r'^\s*estado de cuentas'
        ]
        
        line_lower = line.lower()
        return any(re.match(pattern, line_lower) for pattern in skip_patterns)

    def _is_valid_transaction(self, transaction: Dict) -> bool:
        """Check if transaction is valid"""
        if not transaction.get('fecha'):
            return False
        if not transaction.get('detalle'):
            return False
        # Must have at least one amount
        return any(transaction.get(field) is not None for field in ['debitos', 'creditos', 'saldo'])

    def _normalize_output(self, transactions: List[Dict], pdf_path: str = None) -> pd.DataFrame:
        """
        Convert transactions to standardized DataFrame with:
        - Proper date parsing with year inference
        - Balance validation and flagging
        - observaciones column for problems
        """
        if not transactions:
            return pd.DataFrame()
            
        df = pd.DataFrame(transactions)
        
        # Ensure all required columns exist
        required_columns = ['fecha', 'detalle', 'referencia', 'debitos', 'creditos', 'saldo']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None if col in ['debitos', 'creditos', 'saldo'] else ''
        
        # Parse dates with year inference
        df['fecha'] = df['fecha'].apply(lambda x: self._parse_date_with_year(x, pdf_path))
        
        # Extract mes and año from parsed fecha
        df['fecha_dt'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y', errors='coerce', dayfirst=True)
        df['mes'] = df['fecha_dt'].dt.month.fillna(0).astype(int)
        df['año'] = df['fecha_dt'].dt.year.fillna(0).astype(int)
        
        # Convert amounts - already parsed as floats
        for col in ['debitos', 'creditos', 'saldo']:
            df[col] = df[col].apply(lambda x: float(x) if x is not None else 0.0)
        
        # Add observaciones column
        df['observaciones'] = ''
        
        # Validate and flag problems
        df = self._validate_balance(df)
        df = self._flag_problem_rows(df)
        
        # Reorder columns
        ordered_columns = ['fecha', 'mes', 'año', 'detalle', 'referencia', 'debitos', 'creditos', 'saldo', 'observaciones']
        df = df[ordered_columns]
        
        return df

    def _parse_date_with_year(self, date_str: str, pdf_path: str = None) -> str:
        """Parse date ensuring year is present"""
        if not date_str:
            return ""
        
        # If already has year, return as-is
        if re.search(r'/\d{4}$', date_str):
            return date_str
        
        # Missing year - infer it
        match = re.match(r'(\d{1,2})/(\d{1,2})$', date_str)
        if match:
            day, month = match.groups()
            year = self.inferred_year or datetime.now().year
            log.warning(f"Año inferido {year} para fecha {date_str}")
            return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
        
        return date_str

    def _validate_balance(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate balance consistency and flag inconsistencies.
        Formula: saldo[i] should equal saldo[i-1] + creditos[i] - debitos[i]
        """
        for i in range(1, len(df)):
            prev_saldo = df.loc[i-1, 'saldo']
            current_credito = df.loc[i, 'creditos']
            current_debito = df.loc[i, 'debitos']
            current_saldo = df.loc[i, 'saldo']
            
            expected_saldo = prev_saldo + current_credito - current_debito
            
            # Allow small rounding errors (0.01)
            if abs(current_saldo - expected_saldo) > 0.01:
                msg = f"Inconsistencia – revisar (esperado: {expected_saldo:.2f}, actual: {current_saldo:.2f})"
                if df.loc[i, 'observaciones']:
                    df.loc[i, 'observaciones'] += f"; {msg}"
                else:
                    df.loc[i, 'observaciones'] = msg
                
                log.warning(f"Fila {i+1}: {msg}")
                if '_original_line' in df.columns:
                    log.warning(f"  Línea original: {df.loc[i, '_original_line']}")
        
        return df

    def _flag_problem_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flag rows with potential problems"""
        for i in range(len(df)):
            # Flag rows with all zeros
            if (df.loc[i, 'debitos'] == 0 and 
                df.loc[i, 'creditos'] == 0 and 
                df.loc[i, 'saldo'] == 0):
                msg = "Todos los montos en cero"
                if df.loc[i, 'observaciones']:
                    df.loc[i, 'observaciones'] += f"; {msg}"
                else:
                    df.loc[i, 'observaciones'] = msg
            
            # Flag rows where detalle is mostly numeric (likely corruption)
            detalle = str(df.loc[i, 'detalle'])
            if detalle and len(detalle) > 5:
                digit_ratio = sum(c.isdigit() for c in detalle) / len(detalle)
                if digit_ratio > 0.7:
                    msg = "Detalle parece corrupto (mayoría dígitos)"
                    if df.loc[i, 'observaciones']:
                        df.loc[i, 'observaciones'] += f"; {msg}"
                    else:
                        df.loc[i, 'observaciones'] = msg
        
        return df