import pandas as pd
import re
import logging
from .base_parser import BaseParser
from utils.date_utils import normalize_date, extract_year_month

logger = logging.getLogger(__name__)

class NacionParser(BaseParser):
    """
    Parser específico para Banco de la Nación Argentina.
    Formato estándar con columna COMPROB. para referencias.
    """
    
    def parse(self, raw_data, filename=""):
        logger.info(f"Procesando extracto Banco Nación: {filename}")
        
        rows = []
        
        if isinstance(raw_data, list) and len(raw_data) > 0:
            if isinstance(raw_data[0], pd.DataFrame):
                for df in raw_data:
                    rows.extend(self._parse_dataframe(df))
            else:
                rows.extend(self._parse_text_lines(raw_data))
        
        df = pd.DataFrame(rows, columns=[
            "fecha", "mes", "año", "detalle", "referencia", "debito", "credito", "saldo"
        ])
        
        return self.finalize(df)
    
    def _parse_dataframe(self, df):
        """Parsea DataFrame de Camelot."""
        rows = []
        
        for idx, row in df.iterrows():
            try:
                if len(row) >= 5:
                    fecha_str = str(row[0]).strip()
                    movimiento = str(row[1]).strip()
                    comprob = str(row[2]).strip() if len(row) > 2 else ""
                    debito_str = str(row[3]).strip() if len(row) > 3 else ""
                    credito_str = str(row[4]).strip() if len(row) > 4 else ""
                    saldo_str = str(row[5]).strip() if len(row) > 5 else ""
                    
                    if self._is_valid_row(fecha_str, movimiento):
                        parsed_row = self._build_row(
                            fecha_str, movimiento, comprob, debito_str, credito_str, saldo_str
                        )
                        if parsed_row:
                            rows.append(parsed_row)
            except Exception as e:
                logger.warning(f"Error procesando fila {idx}: {e}")
                continue
        
        return rows
    
    def _parse_text_lines(self, lines):
        """Parsea líneas de texto plano."""
        rows = []
        
        pattern = re.compile(
            r'(\d{2}/\d{2}/\d{2,4})\s+'  # Fecha
            r'(.+?)'  # Movimiento
            r'\s+(\d+)?'  # Comprobante (opcional)
            r'\s+([\d\.,]+)?'  # Débito
            r'\s+([\d\.,]+)?'  # Crédito
            r'\s+([\d\.,]+)'  # Saldo
        )
        
        for line in lines:
            line = line.strip()
            
            if not line or self._is_skip_line(line):
                continue
            
            match = pattern.search(line)
            if match:
                fecha_str, movimiento, comprob, debito_str, credito_str, saldo_str = match.groups()
                
                parsed_row = self._build_row(
                    fecha_str, movimiento.strip(), comprob or "", 
                    debito_str, credito_str, saldo_str
                )
                if parsed_row:
                    rows.append(parsed_row)
        
        return rows
    
    def _build_row(self, fecha_str, movimiento, comprob, debito_str, credito_str, saldo_str):
        """Construye fila normalizada."""
        try:
            fecha_norm = normalize_date(fecha_str)
            if not fecha_norm:
                return None
            
            año, mes = extract_year_month(fecha_norm)
            
            return {
                "fecha": fecha_norm,
                "mes": mes,
                "año": año,
                "detalle": movimiento[:200],
                "referencia": comprob[:50],
                "debito": _to_float(debito_str),
                "credito": _to_float(credito_str),
                "saldo": _to_float(saldo_str)
            }
        except Exception as e:
            logger.warning(f"Error construyendo fila: {e}")
            return None
    
    def _is_valid_row(self, fecha_str, movimiento):
        """Valida si es fila de transacción."""
        if not re.match(r'\d{2}/\d{2}/\d{2,4}', fecha_str):
            return False
        
        movimiento_lower = movimiento.lower()
        skip = ['fecha', 'movimientos', 'comprob', 'debitos', 'creditos', 'saldo']
        return not any(kw in movimiento_lower for kw in skip)
    
    def _is_skip_line(self, line):
        """Detecta líneas a ignorar."""
        line_lower = line.lower()
        skip = [
            'banco de la nacion', 'resumen de cuenta', 'cuenta corriente',
            'periodo:', 'hoja:', 'total grav', 'saldo final', 'saldo anterior'
        ]
        return any(kw in line_lower for kw in skip)


def _to_float(value):
    """Convierte string a float (formato argentino)."""
    if not value or value == '-' or value == 'nan':
        return 0.0
    
    try:
        clean = str(value).strip().replace('$', '').replace(' ', '')
        is_negative = clean.startswith('-')
        clean = clean.replace('-', '')
        clean = clean.replace('.', '').replace(',', '.')
        result = float(clean)
        return -result if is_negative else result
    except (ValueError, AttributeError):
        return 0.0