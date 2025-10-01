import pandas as pd
import re
import logging
from .base_parser import BaseParser
from utils.date_utils import normalize_date, extract_year_month

logger = logging.getLogger(__name__)

class SantanderParser(BaseParser):
    """
    Parser específico para extractos de Banco Santander Río.
    Soporta formato estándar de Cuenta Corriente en pesos.
    """
    
    def parse(self, raw_data, filename=""):
        logger.info(f"Procesando extracto Santander: {filename}")
        
        rows = []
        
        # Detectar si raw_data es lista de DataFrames (Camelot) o lista de strings (pdfplumber)
        if isinstance(raw_data, list) and len(raw_data) > 0:
            if isinstance(raw_data[0], pd.DataFrame):
                # Camelot devolvió tablas
                for df in raw_data:
                    rows.extend(self._parse_dataframe(df))
            else:
                # pdfplumber devolvió texto plano
                rows.extend(self._parse_text_lines(raw_data))
        
        # Crear DataFrame final
        df = pd.DataFrame(rows, columns=[
            "fecha", "mes", "año", "detalle", "referencia", "debito", "credito", "saldo"
        ])
        
        return self.finalize(df)
    
    def _parse_dataframe(self, df):
        """Parsea un DataFrame de Camelot."""
        rows = []
        
        for idx, row in df.iterrows():
            try:
                # Intentar extraer columnas del DataFrame
                if len(row) >= 5:
                    fecha_str = str(row[0]).strip()
                    detalle = str(row[2]).strip() if len(row) > 2 else ""
                    debito_str = str(row[3]).strip() if len(row) > 3 else ""
                    credito_str = str(row[4]).strip() if len(row) > 4 else ""
                    saldo_str = str(row[5]).strip() if len(row) > 5 else ""
                    
                    # Validar que sea una fila de transacción válida
                    if self._is_valid_transaction_row(fecha_str, detalle):
                        parsed_row = self._build_row(
                            fecha_str, detalle, debito_str, credito_str, saldo_str
                        )
                        if parsed_row:
                            rows.append(parsed_row)
            except Exception as e:
                logger.warning(f"Error procesando fila {idx}: {e}")
                continue
        
        return rows
    
    def _parse_text_lines(self, lines):
        """Parsea líneas de texto plano (pdfplumber/OCR)."""
        rows = []
        
        # Regex para capturar línea de movimiento Santander
        # Formato: DD/MM/YY Concepto... $ X,XXX.XX $ Y,YYY.YY $ Z,ZZZ.ZZ
        pattern = re.compile(
            r'(\d{2}/\d{2}/\d{2})\s+'  # Fecha
            r'(.+?)'  # Detalle (captura no greedy)
            r'\s+\$?\s*([\d\.,\-]+)?'  # Débito (opcional)
            r'\s+\$?\s*([\d\.,\-]+)?'  # Crédito (opcional)
            r'\s+\$?\s*([\d\.,\-]+)'  # Saldo (siempre presente)
        )
        
        for line in lines:
            line = line.strip()
            
            # Ignorar líneas vacías o encabezados
            if not line or self._is_header_or_footer(line):
                continue
            
            match = pattern.search(line)
            if match:
                fecha_str, detalle, debito_str, credito_str, saldo_str = match.groups()
                
                parsed_row = self._build_row(
                    fecha_str, detalle.strip(), debito_str, credito_str, saldo_str
                )
                if parsed_row:
                    rows.append(parsed_row)
        
        return rows
    
    def _build_row(self, fecha_str, detalle, debito_str, credito_str, saldo_str):
        """Construye una fila normalizada."""
        try:
            # Normalizar fecha (DD/MM/YY → YYYY-MM-DD)
            fecha_norm = normalize_date(fecha_str)
            if not fecha_norm:
                return None
            
            año, mes = extract_year_month(fecha_norm)
            
            # Convertir montos
            debito = _to_float(debito_str)
            credito = _to_float(credito_str)
            saldo = _to_float(saldo_str)
            
            # Limpiar detalle
            detalle_limpio = self._clean_detail(detalle)
            
            return {
                "fecha": fecha_norm,
                "mes": mes,
                "año": año,
                "detalle": detalle_limpio,
                "referencia": "",  # Santander no tiene columna de referencia explícita
                "debito": debito,
                "credito": credito,
                "saldo": saldo
            }
        except Exception as e:
            logger.warning(f"Error construyendo fila: {e}")
            return None
    
    def _is_valid_transaction_row(self, fecha_str, detalle):
        """Valida si es una fila de transacción válida."""
        # Debe tener formato de fecha
        if not re.match(r'\d{2}/\d{2}/\d{2,4}', fecha_str):
            return False
        
        # Ignorar líneas de encabezado o totales
        detalle_lower = detalle.lower()
        skip_keywords = [
            'fecha', 'comprobante', 'movimiento', 'débito', 'crédito', 'saldo',
            'total', 'subtotal', 'página', 'hoja', 'período'
        ]
        
        return not any(kw in detalle_lower for kw in skip_keywords)
    
    def _is_header_or_footer(self, line):
        """Detecta encabezados, pies de página y líneas irrelevantes."""
        line_lower = line.lower()
        
        skip_patterns = [
            'fecha', 'comprobante', 'movimiento', 'débito', 'crédito', 'saldo',
            'cuenta corriente', 'banco santander', 'resumen de cuenta',
            'página', 'hoja', 'cuit', 'cbu', 'salvo error', 'período',
            'totales', 'impuesto', 'garantía', 'seguro'
        ]
        
        return any(pattern in line_lower for pattern in skip_patterns)
    
    def _clean_detail(self, detalle):
        """Limpia el texto del detalle."""
        # Remover múltiples espacios
        detalle = re.sub(r'\s+', ' ', detalle)
        
        # Remover caracteres especiales no deseados
        detalle = detalle.replace('$', '').strip()
        
        return detalle[:200]  # Limitar longitud


def _to_float(value):
    """
    Convierte string de monto a float.
    Formato Santander: 1.234,56 (punto miles, coma decimal)
    También maneja negativos: -$ 42.450,99
    """
    if not value or value == '-' or value == 'nan':
        return 0.0
    
    try:
        # Limpiar
        clean = str(value).strip()
        clean = clean.replace('$', '').replace(' ', '')
        
        # Manejar negativos
        is_negative = clean.startswith('-')
        clean = clean.replace('-', '')
        
        # Remover puntos (separador de miles) y reemplazar coma por punto (decimal)
        clean = clean.replace('.', '').replace(',', '.')
        
        result = float(clean)
        return -result if is_negative else result
    
    except (ValueError, AttributeError) as e:
        logger.warning(f"No se pudo convertir '{value}' a float: {e}")
        return 0.0