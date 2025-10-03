"""
Base Parser - Clase abstracta para todos los parsers bancarios.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    """
    Clase base abstracta para parsers bancarios.
    
    Todos los parsers específicos deben heredar de esta clase
    e implementar los métodos abstractos.
    """
    
    # Atributos de clase que deben ser definidos en cada parser
    BANK_NAME = "BASE"
    DETECTION_KEYWORDS: List[str] = []
    PREFER_TABLES = True  # Si preferir tablas sobre texto cuando ambos están disponibles
    
    # Columnas requeridas en el output
    REQUIRED_COLUMNS = ["fecha", "detalle", "debito", "credito", "saldo", "referencia"]
    OUTPUT_COLUMNS = ["fecha", "mes", "año", "detalle", "referencia", "debito", "credito", "saldo"]
    
    @abstractmethod
    def detect(self, text: str, filename: str = "") -> bool:
        """
        Detecta si este parser puede procesar el documento.
        
        Args:
            text: Texto extraído del PDF
            filename: Nombre del archivo (opcional)
        
        Returns:
            True si este parser puede procesar el documento
        """
        pass
    
    @abstractmethod
    def parse(self, raw_data: Dict, filename: str = "") -> pd.DataFrame:
        """
        Parsea datos crudos y retorna DataFrame estandarizado.
        
        Args:
            raw_data: Dict con:
                - 'text': str - Texto completo extraído
                - 'tables': List[pd.DataFrame] - Tablas detectadas
                - 'method': str - Método de extracción usado
                - 'pages_count': int - Número de páginas
            filename: Nombre del archivo (opcional)
        
        Returns:
            DataFrame con columnas: fecha, mes, año, detalle, referencia, 
                                   debito, credito, saldo
        """
        pass
    
    def finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Estandariza columnas y formatos del DataFrame final.
        
        Args:
            df: DataFrame con al menos las columnas requeridas
        
        Returns:
            DataFrame estandarizado con todas las columnas en orden correcto
        """
        if df.empty:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)
        
        # Asegurar columnas requeridas existen
        for col in self.REQUIRED_COLUMNS:
            if col not in df.columns:
                if col in ["fecha", "detalle", "referencia"]:
                    df[col] = ""
                else:  # debito, credito, saldo
                    df[col] = 0.0
        
        # Convertir columnas de montos a float si no lo son
        for col in ["debito", "credito", "saldo"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Agregar mes y año desde fecha
        if 'mes' not in df.columns or 'año' not in df.columns:
            df = self._add_date_columns(df)
        
        # Limpiar datos
        df = self._clean_dataframe(df)
        
        # Reordenar columnas
        return df[self.OUTPUT_COLUMNS]
    
    def _add_date_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agrega columnas de mes y año desde la fecha."""
        try:
            dates = pd.to_datetime(df['fecha'], format='%d/%m/%Y', errors='coerce')
            df['mes'] = dates.dt.month.fillna(0).astype(int)
            df['año'] = dates.dt.year.fillna(0).astype(int)
        except Exception as e:
            logger.warning(f"Error al parsear fechas para {self.BANK_NAME}: {e}")
            df['mes'] = 0
            df['año'] = 0
        return df
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia el DataFrame removiendo filas inválidas."""
        # Remover filas sin fecha válida
        df = df[df['fecha'].notna() & (df['fecha'] != '')]
        
        # Remover filas sin detalle
        df = df[df['detalle'].notna() & (df['detalle'] != '')]
        
        # Remover filas sin montos (todas las columnas en 0)
        df = df[
            (df['debito'] != 0) | 
            (df['credito'] != 0) | 
            (df['saldo'] != 0)
        ]
        
        # Reset index
        df = df.reset_index(drop=True)
        
        return df
    
    # ============================================================
    # MÉTODOS AUXILIARES COMPARTIDOS
    # ============================================================
    
    @staticmethod
    def normalize_date(date_str: str) -> str:
        """
        Normaliza fecha a formato DD/MM/YYYY.
        
        Args:
            date_str: String con fecha en formato variable
        
        Returns:
            Fecha en formato DD/MM/YYYY o string original si falla
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        
        # Limpiar caracteres no deseados
        date_clean = re.sub(r'[^\d\/\-\.]', '', date_str.strip())
        
        if not date_clean:
            return ""
        
        # Formatos comunes de fecha
        formats = [
            '%d/%m/%Y', '%d/%m/%y',
            '%d-%m-%Y', '%d-%m-%y',
            '%d.%m.%Y', '%d.%m.%y',
            '%Y/%m/%d', '%Y-%m-%d',
            '%Y%m%d'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_clean, fmt)
                return dt.strftime('%d/%m/%Y')
            except ValueError:
                continue
        
        # Si ningún formato funciona, retornar limpio
        return date_clean
    
    @staticmethod
    def parse_amount(amount_str: str) -> float:
        """
        Convierte string de monto a float.
        
        Maneja:
        - Formato argentino: 1.234,56
        - Formato US: 1,234.56
        - Negativos con prefijo: -1.234,56
        - Negativos con sufijo: 1.234,56-
        
        Args:
            amount_str: String con el monto
        
        Returns:
            Float con el valor numérico
        """
        if not amount_str:
            return 0.0
        
        clean_str = str(amount_str).strip()
        
        if not clean_str or clean_str in ['', '-', '.', ',', 'nan', 'None']:
            return 0.0
        
        # Detectar negativo con sufijo (1.234,56-)
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
            # Ambos presentes: el último es el decimal
            if clean_str.rfind(',') > clean_str.rfind('.'):
                # Coma es decimal (formato argentino: 1.234,56)
                clean_str = clean_str.replace('.', '').replace(',', '.')
            else:
                # Punto es decimal (formato US: 1,234.56)
                clean_str = clean_str.replace(',', '')
        elif ',' in clean_str:
            # Solo coma: verificar si es decimal o separador de miles
            comma_parts = clean_str.split(',')
            if len(comma_parts) == 2 and len(comma_parts[1]) == 2:
                # Es decimal: 1234,56
                clean_str = clean_str.replace(',', '.')
            else:
                # Es separador de miles: 1,234
                clean_str = clean_str.replace(',', '')
        # Si solo tiene punto, asumir que es decimal (formato US sin miles)
        
        try:
            result = float(clean_str)
            if negative_suffix or negative_prefix:
                result = -result
            return result
        except ValueError:
            logger.debug(f"No se pudo parsear monto: {amount_str}")
            return 0.0
    
    @staticmethod
    def format_amount(amount: float) -> str:
        """
        Formatea monto con formato argentino (1.234,56).
        
        Args:
            amount: Valor numérico
        
        Returns:
            String formateado
        """
        try:
            if pd.isna(amount):
                return "0,00"
            
            if amount < 0:
                formatted = f"{abs(amount):,.2f}"
                formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
                return f"-{formatted}"
            else:
                formatted = f"{amount:,.2f}"
                return formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
        except:
            return str(amount)
    
    @staticmethod
    def clean_text(text: str) -> str:
        """
        Limpia y normaliza texto.
        
        Args:
            text: Texto a limpiar
        
        Returns:
            Texto limpio
        """
        if not text or pd.isna(text):
            return ""
        
        text = str(text).strip()
        
        # Reemplazar múltiples espacios por uno solo
        text = re.sub(r'\s+', ' ', text)
        
        # Remover caracteres de control
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        return text
    
    def _extract_reference(self, detalle: str) -> str:
        """
        Extrae número de referencia del texto de detalle.
        
        Args:
            detalle: Texto del detalle de la transacción
        
        Returns:
            Número de referencia o string vacío
        """
        patterns = [
            r'NRO\.?\s*(\d+)',
            r'REF\.?\s*(\d+)',
            r'REFERENCIA\s*(\d+)',
            r'COMPROBANTE\s*(\d+)',
            r'TRANSACCION\s*(\d+)',
            r'(\d{8,})'  # Números largos (8+ dígitos)
        ]
        
        for pattern in patterns:
            match = re.search(pattern, detalle, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return ''
    
    def _infer_year(self, text: str, filename: str = "") -> int:
        """
        Infiere el año desde el texto o nombre de archivo.
        
        Args:
            text: Texto del documento
            filename: Nombre del archivo
        
        Returns:
            Año inferido o año actual
        """
        # Buscar año de 4 dígitos en el texto
        match = re.search(r'(20\d{2}|19\d{2})', text)
        if match:
            return int(match.group(1))
        
        # Buscar en nombre de archivo
        if filename:
            match = re.search(r'(20\d{2}|19\d{2})', filename)
            if match:
                return int(match.group(1))
        
        # Fallback: año actual
        return datetime.now().year
    
    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        """
        Encuentra la fila de encabezados en un DataFrame.
        
        Args:
            df: DataFrame donde buscar
        
        Returns:
            Índice de la fila de encabezados o None
        """
        header_indicators = [
            'fecha', 'date', 'fec', 'dia',
            'concepto', 'detalle', 'descripcion', 'operacion',
            'debito', 'credito', 'saldo', 'debe', 'haber',
            'importe', 'monto', 'valor'
        ]
        
        for i, row in df.iterrows():
            row_text = ' '.join([str(cell).lower() for cell in row])
            
            # Contar cuántos indicadores aparecen
            score = sum(1 for indicator in header_indicators if indicator in row_text)
            
            # Si encuentra 2 o más indicadores, es probable que sea el header
            if score >= 2:
                return i
        
        return None
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} bank={self.BANK_NAME}>"