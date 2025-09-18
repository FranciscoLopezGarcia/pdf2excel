import re
import pandas as pd
import pdfplumber
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import camelot
import logging

log = logging.getLogger("universal_extractor")

class UniversalBankExtractor:
    def __init__(self):
        # Universal patterns
        self.date_patterns = [
            r'\b(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})\b',
            r'\b(\d{2,4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})\b'
        ]
        self.amount_patterns = [
            r'-?\$?\s*\d{1,3}(?:[.,]\d{3})*[.,]\d{2}',
            r'-?\$?\s*\d+[.,]\d{2}',
            r'-?\d+[.,]\d{2}-?'  # negative suffix
        ]
        
        # Universal column indicators
        self.date_indicators = ['fecha', 'date', 'fec', 'dia']
        self.detail_indicators = ['concepto', 'detalle', 'descripcion', 'causal', 'operacion', 'movimiento']
        self.reference_indicators = ['referencia', 'ref', 'nro', 'numero', 'comprobante', 'transaccion']
        self.debit_indicators = ['debito', 'debitos', 'debe', 'egreso', 'salida', 'cargo']
        self.credit_indicators = ['credito', 'creditos', 'haber', 'ingreso', 'entrada', 'abono', 'deposito']
        self.balance_indicators = ['saldo', 'balance', 'total']
        self.amount_indicators = ['importe', 'monto', 'valor']

    def extract_from_pdf(self, pdf_path: str) -> pd.DataFrame:
        """Main extraction method - tries tables first, then text"""
        try:
            # Try table extraction first
            rows = self._extract_from_tables(pdf_path)
            if rows:
                log.info(f"Extracted {len(rows)} rows using table method")
                return self._normalize_output(rows)
        except Exception as e:
            log.warning(f"Table extraction failed: {e}")

        # Fallback to text extraction
        try:
            rows = self._extract_from_text(pdf_path)
            log.info(f"Extracted {len(rows)} rows using text method")
            return self._normalize_output(rows)
        except Exception as e:
            log.error(f"Text extraction failed: {e}")
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
        """Extract using text parsing"""
        full_text = ""
        try:
            with pdfplumber.open(pdf_path) as doc:
                for page in doc.pages:
                    text = page.extract_text() or ""
                    full_text += text + "\n"
        except Exception as e:
            log.error(f"PDF reading failed: {e}")
            return []

        return self._parse_text_content(full_text)

    def _parse_text_content(self, text: str) -> List[Dict]:
        """Parse transactions from raw text"""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        transactions = []
        
        for line in lines:
            # Skip headers and irrelevant lines
            if self._should_skip_line(line):
                continue
                
            # Look for date pattern
            date_match = None
            for pattern in self.date_patterns:
                match = re.search(pattern, line)
                if match:
                    date_match = match
                    break
                    
            if not date_match:
                continue
                
            fecha = self._normalize_date(date_match.group(0))
            if not fecha:
                continue
                
            # Extract amounts from the line
            amounts = []
            for pattern in self.amount_patterns:
                matches = re.findall(pattern, line)
                for match in matches:
                    amount = self._parse_amount(match)
                    if amount != 0:
                        amounts.append(amount)
            
            # Remove amounts and date from line to get detail
            clean_line = line
            clean_line = re.sub(date_match.re.pattern, '', clean_line)
            for pattern in self.amount_patterns:
                clean_line = re.sub(pattern, '', clean_line)
            
            detalle = self._clean_text(clean_line)
            
            # Parse transaction
            transaction = self._categorize_amounts(fecha, detalle, amounts)
            if transaction:
                transactions.append(transaction)
                
        return transactions

    def _categorize_amounts(self, fecha: str, detalle: str, amounts: List[float]) -> Optional[Dict]:
        """Categorize amounts into debits/credits based on context and values"""
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
        
        # Context-based categorization
        detalle_lower = detalle.lower()
        is_debit_context = any(word in detalle_lower for word in [
            'debito', 'cargo', 'comision', 'impuesto', 'transferencia enviada', 
            'retiro', 'pago', 'automatico'
        ])
        is_credit_context = any(word in detalle_lower for word in [
            'credito', 'deposito', 'transferencia recibida', 'abono', 'ingreso'
        ])
        
        if len(amounts) == 1:
            amount = amounts[0]
            if amount < 0 or is_debit_context:
                transaction['debitos'] = self._format_amount(abs(amount))
            else:
                transaction['creditos'] = self._format_amount(abs(amount))
                
        elif len(amounts) == 2:
            # Usually movement + balance
            movement = amounts[0]
            balance = amounts[1]
            
            if movement < 0 or is_debit_context:
                transaction['debitos'] = self._format_amount(abs(movement))
            else:
                transaction['creditos'] = self._format_amount(abs(movement))
            transaction['saldo'] = self._format_amount(balance)
            
        elif len(amounts) >= 3:
            # Multiple amounts - take first as movement, last as balance
            movement = amounts[0]
            balance = amounts[-1]
            
            if movement < 0 or is_debit_context:
                transaction['debitos'] = self._format_amount(abs(movement))
            else:
                transaction['creditos'] = self._format_amount(abs(movement))
            transaction['saldo'] = self._format_amount(balance)
            
        return transaction

    def _extract_reference(self, detalle: str) -> str:
        """Extract reference number from detail text"""
        # Look for common reference patterns
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
            # Check if row contains common header indicators
            header_score = 0
            for indicator_list in [self.date_indicators, self.detail_indicators, 
                                 self.balance_indicators]:
                if any(ind in row_text for ind in indicator_list):
                    header_score += 1
            
            if header_score >= 2:  # At least 2 different types of columns
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
                # Single amount column - categorize by sign or context
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
            
        # Clean the date string
        date_clean = re.sub(r'[^\d\/\-\.]', '', date_str)
        
        # Try different formats
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
        """Parse amount string to float"""
        if not amount_str:
            return 0.0
            
        # Clean string
        clean_str = str(amount_str).strip()
        clean_str = re.sub(r'[^\d\-\.,]', '', clean_str)
        
        if not clean_str or clean_str in ['-', '.', ',']:
            return 0.0
        
        # Handle negative suffix (123,45-)
        negative_suffix = clean_str.endswith('-')
        if negative_suffix:
            clean_str = clean_str[:-1]
            
        # Determine decimal separator
        if ',' in clean_str and '.' in clean_str:
            # Both separators present
            if clean_str.rfind(',') > clean_str.rfind('.'):
                # Comma is last = decimal separator
                clean_str = clean_str.replace('.', '').replace(',', '.')
            else:
                # Dot is last = decimal separator
                clean_str = clean_str.replace(',', '')
        elif ',' in clean_str:
            # Only comma - check if it's decimal or thousands
            if len(clean_str.split(',')[-1]) == 2:
                clean_str = clean_str.replace(',', '.')
            else:
                clean_str = clean_str.replace(',', '')
        
        try:
            result = float(clean_str)
            return -result if negative_suffix else result
        except:
            return 0.0

    def _format_amount(self, amount: float) -> str:
        """Format amount with thousands separator and 2 decimals"""
        try:
            formatted = f"{amount:,.2f}"
            # Convert to Argentine format: 1.234,56
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
        # Remove completely empty rows
        df = df[~df.apply(lambda row: all(cell.strip() == '' for cell in row), axis=1)]
        # Remove completely empty columns
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
            r'^\s*cuenta',
            r'^\s*saldo anterior',
            r'^\s*saldo actual',
            r'movimientos pendientes',
            r'^\s*\d+\s*$'  # Just page numbers
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
            
        return df


# Usage example
if __name__ == "__main__":
    extractor = UniversalBankExtractor()
    
    # Process a PDF
    df = extractor.extract_from_pdf("statement.pdf")
    
    if not df.empty:
        # Save to Excel
        df.to_excel("extracted_transactions.xlsx", index=False)
        print(f"Extracted {len(df)} transactions")
    else:
        print("No transactions found")