import pandas as pd
import re
import logging
from .base_parser import BaseParser
from utils.date_utils import normalize_date, extract_year_month

logger = logging.getLogger(__name__)

class ProvinciaParser(BaseParser):
    """
    Parser específico para Banco Provincia.
    Característica: columna "Importe" única (negativos = débitos, positivos = créditos).
    """
    
    def parse(self, raw_data, filename=""):
        logger.info(f"Procesando extracto Banco Provincia: {filename}")
        
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
                    concepto = str(row[1]).strip()
                    importe_str = str(row[2]).strip()
                    # Fecha valor está en row[3], saldo en row[4]
                    saldo_str = str(row[4]).strip() if len(row) > 4 else ""
                    
                    if self._is_valid_row(fecha_str, concepto):
                        parsed_row = self._build_row(fecha_str, concepto, importe_str, saldo_str)
                        if parsed_row:
                            rows.append(parsed_row)
            except Exception as e:
                logger.warning(f"Error procesando fila {idx}: {e}")
                continue
        
        return rows
    
    def _parse_text_lines(self, lines):
        """Parsea líneas de texto plano."""
        rows = []
        
        # Formato: DD-MM-YY Concepto Importe FechaValor Saldo
        pattern = re.compile(
            r'(\d{2}-\d{2}-\d{2,4})\s+'  # Fecha
            r'(.+?)'  # Concepto
            r'\s+([\-\d\.,]+)'  # Importe (puede ser negativo)
            r'\s+\d{2}-\d{2}'  # Fecha valor (ignorar)
            r'\s+([\-\d\.,]+)'  # Saldo
        )
        
        for line in lines:
            line = line.strip()
            
            if not line or self._is_skip_line(line):
                continue
            
            match = pattern.search(line)
            if match:
                fecha_str, concepto, importe_str, saldo_str = match.groups()
                
                parsed_row = self._build_row(
                    fecha_str, concepto.strip(), importe_str, saldo_str
                )
                if parsed_row:
                    rows.append(parsed_row)
        
        return rows
    
    def _build_row(self, fecha_str, concepto, importe_str, saldo_str):
        """
        Construye fila normalizada.
        Nota: Banco Provincia usa importe único con signo.
        Negativo = débito, Positivo = crédito.
        """
        try:
            # Normalizar fecha (DD-MM-YY con guiones)
            fecha_str_normalized = fecha_str.replace('-', '/')
            fecha_norm = normalize_date(fecha_str_normalized)
            if not fecha_norm:
                return None
            
            año, mes = extract_year_month(fecha_norm)
            
            # Convertir importe
            importe = _to_float(importe_str)
            
            # Determinar si es débito o crédito
            if importe < 0:
                debito = abs(importe)
                credito = 0.0
            else:
                debito = 0.0
                credito = importe
            
            # Limpiar concepto
            concepto_limpio = self._clean_concept(concepto)
            
            return {
                "fecha": fecha_norm,
                "mes": mes,
                "año": año,
                "detalle": concepto_limpio,
                "referencia": "",
                "debito": debito,
                "credito": credito,
                "saldo": _to_float(saldo_str)
            }
        except Exception as e:
            logger.warning(f"Error construyendo fila: {e}")
            return None
    
    def _clean_concept(self, concepto):
        """Limpia el concepto."""
        # Remover múltiples espacios
        concepto = re.sub(r'\s+', ' ', concepto).strip()
        # Remover "PERIODO DESDE ... HASTA ..."
        concepto = re.sub(r'PERIODO\s+DESDE.+?HASTA.+?\d{4}', '', concepto, flags=re.IGNORECASE)
        return concepto[:200]
    
    def _is_valid_row(self, fecha_str, concepto):
        """Valida si es fila de transacción."""
        if not re.match(r'\d{2}-\d{2}-\d{2,4}', fecha_str):
            return False
        
        concepto_lower = concepto.lower()
        skip = ['fecha', 'concepto', 'importe', 'saldo', 'subtotal', 'total']
        return not any(kw in concepto_lower for kw in skip)
    
    def _is_skip_line(self, line):
        """Detecta líneas a ignorar."""
        line_lower = line.lower()
        skip = [
            'extracto de cuenta', 'banco provincia', 'cuenta corriente',
            'emitido el', 'frecuencia', 'hoja', 'cbu:', 'total retención'
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