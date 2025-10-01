import pandas as pd
import re
import logging
from .base_parser import BaseParser
from utils.date_utils import normalize_date, extract_year_month

logger = logging.getLogger(__name__)

class SanJuanParser(BaseParser):
    """
    Parser específico para Banco San Juan.
    Formato: Cuenta Corriente con conceptos multilínea.
    """
    
    def parse(self, raw_data, filename=""):
        logger.info(f"Procesando extracto Banco San Juan: {filename}")
        
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
                if len(row) >= 4:
                    fecha_str = str(row[0]).strip()
                    concepto = str(row[1]).strip() if len(row) > 1 else ""
                    debito_str = str(row[2]).strip() if len(row) > 2 else ""
                    credito_str = str(row[3]).strip() if len(row) > 3 else ""
                    saldo_str = str(row[4]).strip() if len(row) > 4 else ""
                    
                    if self._is_valid_row(fecha_str, concepto):
                        parsed_row = self._build_row(
                            fecha_str, concepto, debito_str, credito_str, saldo_str
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
        
        # Regex adaptado al formato San Juan
        pattern = re.compile(
            r'(\d{2}/\d{2}/\d{2,4})\s+'  # Fecha
            r'(.+?)'  # Concepto
            r'\s+([\d\.,]+)?'  # Débito
            r'\s+([\d\.,]+)?'  # Crédito  
            r'\s+([\d\.,]+)'  # Saldo
            r'\s*$'
        )
        
        for line in lines:
            line = line.strip()
            
            if not line or self._is_skip_line(line):
                continue
            
            match = pattern.search(line)
            if match:
                fecha_str, concepto, debito_str, credito_str, saldo_str = match.groups()
                
                parsed_row = self._build_row(
                    fecha_str, concepto.strip(), debito_str, credito_str, saldo_str
                )
                if parsed_row:
                    rows.append(parsed_row)
        
        return rows
    
    def _build_row(self, fecha_str, concepto, debito_str, credito_str, saldo_str):
        """Construye fila normalizada."""
        try:
            fecha_norm = normalize_date(fecha_str)
            if not fecha_norm:
                return None
            
            año, mes = extract_year_month(fecha_norm)
            
            # Extraer referencia (número de operación si existe)
            referencia = self._extract_reference(concepto)
            
            # Limpiar concepto
            concepto_limpio = self._clean_concept(concepto)
            
            return {
                "fecha": fecha_norm,
                "mes": mes,
                "año": año,
                "detalle": concepto_limpio,
                "referencia": referencia,
                "debito": _to_float(debito_str),
                "credito": _to_float(credito_str),
                "saldo": _to_float(saldo_str)
            }
        except Exception as e:
            logger.warning(f"Error construyendo fila: {e}")
            return None
    
    def _extract_reference(self, concepto):
        """Extrae número de referencia del concepto."""
        # Buscar patrones como "NRO.0217468673"
        match = re.search(r'NRO\.(\d+)', concepto, re.IGNORECASE)
        if match:
            return match.group(1)
        return ""
    
    def _clean_concept(self, concepto):
        """Limpia el concepto removiendo CUITs y números de referencia."""
        # Remover CUIT
        concepto = re.sub(r'\d{11}', '', concepto)
        # Remover NRO.
        concepto = re.sub(r'NRO\.\d+', '', concepto, flags=re.IGNORECASE)
        # Limpiar espacios múltiples
        concepto = re.sub(r'\s+', ' ', concepto).strip()
        return concepto[:200]
    
    def _is_valid_row(self, fecha_str, concepto):
        """Valida si es fila de transacción."""
        if not re.match(r'\d{2}/\d{2}/\d{2,4}', fecha_str):
            return False
        
        skip = ['fecha', 'concepto', 'debito', 'credito', 'saldo', 'subtotal', 'transporte']
        concepto_lower = concepto.lower()
        return not any(kw in concepto_lower for kw in skip)
    
    def _is_skip_line(self, line):
        """Detecta líneas a ignorar."""
        line_lower = line.lower()
        skip = [
            'movimientos de cuenta', 'banco san juan', 'home banking',
            'cuenta corriente', 'periodo', 'hoja:', 'subtotal', 'transporte'
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