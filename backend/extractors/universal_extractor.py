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
        # Universal patterns - mejorados
        self.date_patterns = [
            r'\b(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})\b',
            r'\b(\d{2,4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})\b'
        ]
        
        # Patrones de montos mejorados para capturar negativos con sufijo
        self.amount_patterns = [
            r'\d{1,3}(?:[.,]\d{3})*[.,]\d{2}-',  # 1.234,56- (negativo con sufijo)
            r'-?\$?\s*\d{1,3}(?:[.,]\d{3})*[.,]\d{2}',
            r'-?\$?\s*\d+[.,]\d{2}',
            r'\d+[.,]\d{2}'
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

    def extract_from_pdf(self, pdf_path: str) -> pd.DataFrame:
        # PASO 1: Camelot (tablas)
        log.info(f"🔍 Intentando extracción con Camelot (tablas)...")
        try:
            rows = self._extract_from_tables(pdf_path)
            if rows:
                log.info(f"✅ Camelot exitoso: {len(rows)} transacciones")
                return self._normalize_output(rows)
            else:
                log.info("❌ Camelot no encontró tablas válidas")
        except Exception as e:
            log.warning(f"❌ Camelot falló: {e}")

        # PASO 2: PDFPlumber (texto)
        log.info(f"🔍 Intentando extracción con PDFPlumber (texto)...")
        try:
            rows = self._extract_from_text(pdf_path)
            if rows:
                log.info(f"✅ PDFPlumber exitoso: {len(rows)} transacciones")
                return self._normalize_output(rows)
            else:
                log.info("❌ PDFPlumber no encontró transacciones válidas")
        except Exception as e:
            log.warning(f"❌ PDFPlumber falló: {e}")

        # PASO 3: OCR (fallback final)
        log.info(f"🔍 Intentando extracción con OCR (imágenes)...")
        try:
            # Usar extract_text_pages en lugar de extract_text_from_pdf
            pages_data = self.ocr_extractor.extract_text_pages(pdf_path)
            if not pages_data:
                log.warning("❌ OCR no detectó páginas relevantes")
                return pd.DataFrame()
            
            log.info(f"📄 OCR detectó {len(pages_data)} páginas relevantes")
            
            # Concatenar texto de todas las páginas
            ocr_text = "\n\n".join([f"--- Página {p} ---\n{t}" for p, t in pages_data])
            
            if not ocr_text.strip():
                log.warning("❌ OCR devolvió texto vacío")
                return pd.DataFrame()
            
            log.info(f"📝 OCR extrajo {len(ocr_text)} caracteres de texto")
            
            # Parse del texto OCR
            rows = self._parse_text_content_improved(ocr_text)
            if rows:
                log.info(f"✅ OCR exitoso: {len(rows)} transacciones extraídas")
                return self._normalize_output(rows)
            else:
                log.warning("❌ OCR no pudo parsear transacciones del texto")
                
        except Exception as e:
            log.error(f"❌ OCR falló completamente: {e}")

        log.error("🚫 Todos los métodos de extracción fallaron")
        return pd.DataFrame()

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
            for _, row in df.iloc[header_row + 1:].iterrows():
                parsed_row = self._parse_table_row(row, column_map)
                if self._is_valid_transaction(parsed_row):
                    all_rows.append(parsed_row)
                    
        return all_rows

    def _extract_from_text(self, pdf_path: str) -> List[Dict]:
        """Extract using text parsing - mejorado para formato tabular"""
        full_text = ""
        try:
            with pdfplumber.open(pdf_path) as doc:
                for page in doc.pages:
                    text = page.extract_text() or ""
                    full_text += text + "\n"
        except Exception as e:
            log.error(f"PDF reading failed: {e}")
            return []

        return self._parse_text_content_improved(full_text)

    def _parse_text_content_improved(self, text: str) -> List[Dict]:
        """Parse transactions from raw text - mejorado para manejar formato tabular"""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        transactions = []
        
        # Buscar secciones de transacciones
        in_transaction_section = False
        header_found = False
        
        for line in lines:
            # Skip headers and irrelevant lines
            if self._should_skip_line(line):
                continue
            
            # Detectar inicio de sección de transacciones
            if self._is_transaction_header(line):
                in_transaction_section = True
                header_found = True
                continue
            
            # Si encontramos otra sección, salir
            if header_found and self._is_other_section(line):
                in_transaction_section = False
                continue
                
            # Si no estamos en sección de transacciones, buscar líneas con fecha
            if not in_transaction_section:
                if not self._line_has_date(line):
                    continue
            
            # Parse línea como transacción
            transaction = self._parse_transaction_line(line)
            if transaction:
                transactions.append(transaction)
                
        return transactions

    def _is_transaction_header(self, line: str) -> bool:
        """Detecta si la línea es un header de transacciones"""
        line_lower = line.lower()
        header_indicators = ['fecha', 'concepto', 'debitos', 'creditos', 'saldo']
        found_indicators = sum(1 for indicator in header_indicators if indicator in line_lower)
        return found_indicators >= 2

    def _is_other_section(self, line: str) -> bool:
        """Detecta si la línea indica el inicio de otra sección"""
        line_lower = line.lower()
        section_indicators = [
            'debitos automaticos', 'transferencias recibidas', 'transferencias enviadas',
            'detalle - comision', 'situacion impositiva', 'retenciones'
        ]
        return any(indicator in line_lower for indicator in section_indicators)

    def _line_has_date(self, line: str) -> bool:
        """Verifica si la línea contiene una fecha"""
        for pattern in self.date_patterns:
            if re.search(pattern, line):
                return True
        return False

    def _parse_transaction_line(self, line: str) -> Optional[Dict]:
        """Parse una línea individual como transacción - mejorado para formato tabular"""
        
        # Buscar fecha
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

        # Extraer todos los montos de la línea - MEJORADO
        amounts = []
        amount_positions = []
        
        # Buscar montos con sufijo negativo primero (prioridad)
        negative_suffix_pattern = r'\d{1,3}(?:[.,]\d{3})*[.,]\d{2}-'
        for match in re.finditer(negative_suffix_pattern, line):
            amount = self._parse_amount(match.group(0))
            if amount != 0:
                amounts.append(amount)
                amount_positions.append((match.start(), match.end()))
        
        # Luego buscar otros patrones, evitando posiciones ya ocupadas
        occupied_ranges = set()
        for start, end in amount_positions:
            occupied_ranges.update(range(start, end))
            
        for pattern in self.amount_patterns[1:]:  # Skip the first one (already processed)
            for match in re.finditer(pattern, line):
                if any(pos in occupied_ranges for pos in range(match.start(), match.end())):
                    continue  # Skip overlapping matches
                    
                amount = self._parse_amount(match.group(0))
                if amount != 0:
                    amounts.append(amount)
                    amount_positions.append((match.start(), match.end()))
                    occupied_ranges.update(range(match.start(), match.end()))

        # Remover fecha y montos para obtener el detalle
        clean_line = line
        clean_line = re.sub(date_match.re.pattern, '', clean_line, count=1)
        
        # Remover montos en orden inverso para no afectar posiciones
        for start, end in sorted(amount_positions, reverse=True):
            clean_line = clean_line[:start-len(line)+len(clean_line)] + clean_line[end-len(line)+len(clean_line):]
        
        detalle = self._clean_text(clean_line)
        
        # Categorizar montos basado en la estructura de la línea
        return self._categorize_amounts_improved(fecha, detalle, amounts, line)

    def _categorize_amounts_improved(self, fecha: str, detalle: str, amounts: List[float], original_line: str) -> Optional[Dict]:
        """Categoriza montos mejorado - análisis de estructura más preciso"""
        if not amounts:
            return None
            
        transaction = {
            'fecha': fecha,
            'detalle': detalle,
            'referencia': self._extract_reference(detalle),
            'debitos': '',
            'creditos': '',
            'saldo': ''
        }
        
        # Análisis contextual
        detalle_lower = detalle.lower()
        original_lower = original_line.lower()
        
        # DETECTAR LÍNEAS DE SALDO - PRIORIDAD MÁXIMA
        is_saldo_line = any(phrase in detalle_lower for phrase in [
            'saldo anterior', 'saldo actual', 'saldo al cierre', 'saldo del periodo'
        ]) or any(phrase in original_lower for phrase in [
            'saldo anterior', 'saldo actual', 'saldo al cierre', 'saldo del periodo'
        ])
        
        # Si es línea de saldo, todo va a saldo
        if is_saldo_line:
            # Tomar el primer/único monto como saldo
            transaction['saldo'] = self._format_amount(amounts[0])
            return transaction
        
        # Detectar tipo de transacción por contexto
        is_debit_context = any(word in detalle_lower for word in [
            'debito', 'cargo', 'comision', 'impuesto', 'transferencia enviada', 
            'retiro', 'pago', 'automatico', 'imp.', 'iva', 'interes', 'mantenimiento',
            'ret.', 'transf. prop'
        ])
        
        is_credit_context = any(word in detalle_lower for word in [
            'credito', 'deposito', 'transferencia recibida', 'abono', 'ingreso', 
            'transferencia entre', 'credito por transferencia'
        ])

        # LÓGICA MEJORADA para múltiples montos (NO saldo)
        if len(amounts) == 1:
            amount = amounts[0]
            # Negativos van a débito SOLO si no es línea de saldo
            if amount < 0 or is_debit_context:
                transaction['debitos'] = self._format_amount(abs(amount))
            else:
                transaction['creditos'] = self._format_amount(abs(amount))
                
        elif len(amounts) == 2:
            # Formato típico: movimiento + saldo
            movement, balance = amounts[0], amounts[1]
            
            # El movimiento va según contexto (no por signo si hay contexto)
            if is_debit_context:
                transaction['debitos'] = self._format_amount(abs(movement))
            elif is_credit_context:
                transaction['creditos'] = self._format_amount(abs(movement))
            else:
                # Solo usar signo si no hay contexto claro
                if movement < 0:
                    transaction['debitos'] = self._format_amount(abs(movement))
                else:
                    transaction['creditos'] = self._format_amount(abs(movement))
            
            # El saldo va tal como viene (puede ser negativo)
            transaction['saldo'] = self._format_amount(balance)
            
        elif len(amounts) >= 3:
            # Múltiples montos - LÓGICA MEJORADA
            # Para Patagonia: típicamente [pequeño_imp, movimiento_principal, saldo]
            # El saldo es generalmente el último o el más grande en valor absoluto
            
            # Buscar el saldo (último monto o el de mayor magnitud)
            balance_candidate = amounts[-1]  # Último por defecto
            
            # Si el último es muy pequeño comparado con otros, buscar el mayor
            max_amount = max(amounts, key=abs)
            if abs(amounts[-1]) < abs(max_amount) / 10:  # Si el último es <10% del mayor
                balance_candidate = max_amount
            
            # El movimiento principal es el que no es saldo y no es muy pequeño
            movement_candidates = [a for a in amounts if a != balance_candidate]
            if movement_candidates:
                # Tomar el mayor de los candidatos restantes
                movement = max(movement_candidates, key=abs)
            else:
                movement = amounts[0]
            
            # Categorizar movimiento según contexto
            if is_debit_context:
                transaction['debitos'] = self._format_amount(abs(movement))
            elif is_credit_context:
                transaction['creditos'] = self._format_amount(abs(movement))
            else:
                # Usar signo para decidir
                if movement < 0:
                    transaction['debitos'] = self._format_amount(abs(movement))
                else:
                    transaction['creditos'] = self._format_amount(abs(movement))
            
            # Saldo respeta signo original (puede ser negativo)
            transaction['saldo'] = self._format_amount(balance_candidate)
        
        return transaction

    def _extract_reference(self, detalle: str) -> str:
        """Extract reference number from detail text"""
        patterns = [
            r'NRO\.?\s*(\d+)',
            r'REF\.?\s*(\d+)', 
            r'REFERENCIA\s*(\d+)',
            r'COMPROBANTE\s*(\d+)',
            r'(\d{8,})'  # Long numbers
        ]
        
        for pattern in patterns:
            match = re.search(pattern, detalle, re.IGNORECASE)
            if match:
                return match.group(1)
        return ''

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

    def _parse_table_row(self, row: pd.Series, column_map: Dict[int, str]) -> Dict:
        """Parse a single table row into standardized format"""
        transaction = {
            'fecha': '',
            'detalle': '',
            'referencia': '',
            'debitos': '',
            'creditos': '',
            'saldo': ''
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
                amount = self._parse_amount(value_str)
                if amount != 0:
                    transaction[field] = self._format_amount(abs(amount))
            elif field == 'importe':
                amount = self._parse_amount(value_str)
                if amount != 0:
                    if amount < 0:
                        transaction['debitos'] = self._format_amount(abs(amount))
                    else:
                        transaction['creditos'] = self._format_amount(amount)
        
        return transaction

    def _normalize_date(self, date_str: str) -> str:
        """Normalize date to DD/MM/YYYY format"""
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
        return date_clean

    def _parse_amount(self, amount_str: str) -> float:
        """Parse amount string to float - MEJORADO para negativos con sufijo"""
        if not amount_str:
            return 0.0
            
        clean_str = str(amount_str).strip()
        
        # Detectar negativo con sufijo ANTES de limpiar (1.234,56-)
        negative_suffix = clean_str.endswith('-')
        
        # Remover caracteres no numéricos excepto separadores y signo
        clean_str = re.sub(r'[^\d\-\.,]', '', clean_str)
        
        if not clean_str or clean_str in ['-', '.', ',']:
            return 0.0
        
        # Remover sufijo negativo si existe
        if negative_suffix and clean_str.endswith('-'):
            clean_str = clean_str[:-1]
            
        # Detectar negativo con prefijo
        negative_prefix = clean_str.startswith('-')
        if negative_prefix:
            clean_str = clean_str[1:]
        
        # Determinar separador decimal
        if ',' in clean_str and '.' in clean_str:
            if clean_str.rfind(',') > clean_str.rfind('.'):
                # Coma es decimal (formato argentino)
                clean_str = clean_str.replace('.', '').replace(',', '.')
            else:
                # Punto es decimal (formato US)
                clean_str = clean_str.replace(',', '')
        elif ',' in clean_str:
            # Solo coma - verificar si es decimal
            comma_parts = clean_str.split(',')
            if len(comma_parts) == 2 and len(comma_parts[1]) == 2:
                # Es decimal: 1234,56
                clean_str = clean_str.replace(',', '.')
            else:
                # Es separador de miles: 1,234
                clean_str = clean_str.replace(',', '')
        
        try:
            result = float(clean_str)
            if negative_suffix or negative_prefix:
                result = -result
            return result
        except:
            return 0.0

    def _format_amount(self, amount: float) -> str:
        """Format amount with Argentine format"""
        try:
            if amount < 0:
                formatted = f"{abs(amount):,.2f}"
                formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
                return f"-{formatted}"
            else:
                formatted = f"{amount:,.2f}"
                return formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
        except:
            return str(amount)

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
            r'^\s*saldo anterior',
            r'^\s*saldo actual', 
            r'movimientos pendientes',
            r'^\s*\d+\s*$',  # Solo números de página
            r'^\s*subcta\s+suc\s+mda',  # Headers de subcuenta
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
        return any(transaction.get(field) for field in ['debitos', 'creditos', 'saldo'])

    def _normalize_output(self, transactions: List[Dict]) -> pd.DataFrame:
        """Convert transactions to standardized DataFrame"""
        if not transactions:
            return pd.DataFrame()
            
        df = pd.DataFrame(transactions)
        
        # Ensure all required columns exist
        required_columns = ['fecha', 'detalle', 'referencia', 'debitos', 'creditos', 'saldo']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
        
        # Reorder columns
        df = df[required_columns]
        
        # Clean up empty strings in amount columns
        for col in ['debitos', 'creditos', 'saldo']:
            df[col] = df[col].replace('', '0,00')
            df[col] = df[col].str.replace('.', '', regex=False).str.replace(',', '.',regex=False)
            df[col] = pd.to_numeric(df[col],errors='coerce').fillna(0.0)
            
        return df