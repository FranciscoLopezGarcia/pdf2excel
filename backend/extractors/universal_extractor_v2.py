"""
Universal Bank Extractor - Extracción pura de datos de PDFs bancarios.
No parsea transacciones, solo extrae datos crudos.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import camelot
import pandas as pd
import pdfplumber

from .ocr_extractor import OCRExtractor

logger = logging.getLogger("universal_extractor")


class UniversalBankExtractor:
    """
    Extractor universal de PDFs bancarios.
    
    Responsabilidades:
    - Extraer texto crudo de PDFs (pdfplumber, OCR)
    - Detectar y extraer tablas (camelot)
    - Limpiar y normalizar datos extraídos
    
    NO parsea transacciones ni interpreta contenido bancario.
    """

    def __init__(self):
        self.ocr_extractor = OCRExtractor(lang="spa")

    def extract_from_pdf(self, pdf_path: str) -> Dict:
        """
        Extrae datos crudos de un PDF usando múltiples métodos.
        
        Returns:
            dict con:
                - 'text': str - Texto completo extraído
                - 'tables': List[pd.DataFrame] - Tablas detectadas
                - 'method': str - Método exitoso ('camelot', 'pdfplumber', 'ocr', 'failed')
                - 'pages_count': int - Número de páginas procesadas
        """
        result = {
            'text': '',
            'tables': [],
            'method': 'failed',
            'pages_count': 0
        }

        # PASO 1: Intentar extraer tablas con Camelot
        logger.info("📊 Intentando extracción con Camelot (tablas)...")
        tables = self._extract_tables_camelot(pdf_path)
        if tables:
            logger.info(f"✅ Camelot exitoso: {len(tables)} tablas detectadas")
            result['tables'] = tables
            result['method'] = 'camelot'
            # Extraer texto también para detección de banco
            result['text'] = self._extract_text_pdfplumber(pdf_path)
            return result

        # PASO 2: Extraer texto con PDFPlumber
        logger.info("📄 Intentando extracción con PDFPlumber (texto)...")
        text, pages = self._extract_text_pdfplumber(pdf_path, return_pages=True)
        if text and len(text.strip()) > 100:  # Mínimo de contenido
            logger.info(f"✅ PDFPlumber exitoso: {len(text)} caracteres, {pages} páginas")
            result['text'] = text
            result['pages_count'] = pages
            result['method'] = 'pdfplumber'
            return result

        # PASO 3: OCR como último recurso
        logger.info("🔍 Intentando extracción con OCR (imágenes)...")
        ocr_text, pages_data = self._extract_text_ocr(pdf_path)
        if ocr_text and len(ocr_text.strip()) > 50:
            logger.info(f"✅ OCR exitoso: {len(ocr_text)} caracteres, {len(pages_data)} páginas")
            result['text'] = ocr_text
            result['pages_count'] = len(pages_data)
            result['method'] = 'ocr'
            return result

        logger.error("🚫 Todos los métodos de extracción fallaron")
        return result

    def _extract_tables_camelot(self, pdf_path: str) -> List[pd.DataFrame]:
        """Extrae tablas usando Camelot."""
        try:
            tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
            clean_tables = []
            
            for table in tables:
                df = table.df
                if df.empty:
                    continue
                
                # Limpiar dataframe
                df = self._clean_dataframe(df)
                
                # Solo agregar si tiene contenido significativo
                if len(df) > 0 and len(df.columns) > 0:
                    clean_tables.append(df)
            
            return clean_tables
            
        except Exception as e:
            logger.warning(f"❌ Camelot falló: {e}")
            return []

    def _extract_text_pdfplumber(
        self, 
        pdf_path: str, 
        return_pages: bool = False
    ) -> str | tuple:
        """Extrae texto usando PDFPlumber."""
        try:
            full_text = ""
            pages_count = 0
            
            with pdfplumber.open(pdf_path) as doc:
                pages_count = len(doc.pages)
                for page in doc.pages:
                    text = page.extract_text() or ""
                    full_text += text + "\n"
            
            if return_pages:
                return full_text, pages_count
            return full_text
            
        except Exception as e:
            logger.error(f"❌ PDFPlumber falló: {e}")
            if return_pages:
                return "", 0
            return ""

    def _extract_text_ocr(self, pdf_path: str) -> tuple[str, List]:
        """Extrae texto usando OCR."""
        try:
            pages_data = self.ocr_extractor.extract_text_pages(pdf_path)
            if not pages_data:
                logger.warning("❌ OCR no detectó páginas relevantes")
                return "", []
            
            logger.info(f"📄 OCR detectó {len(pages_data)} páginas relevantes")
            
            # Concatenar texto de todas las páginas
            ocr_text = "\n\n".join([
                f"--- Página {p} ---\n{t}" 
                for p, t in pages_data
            ])
            
            return ocr_text, pages_data
            
        except Exception as e:
            logger.error(f"❌ OCR falló: {e}")
            return "", []

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia un DataFrame removiendo filas y columnas vacías.
        """
        # Rellenar NaN con string vacío
        df = df.fillna('').astype(str)
        
        # Remover filas completamente vacías
        df = df[~df.apply(
            lambda row: all(cell.strip() == '' for cell in row), 
            axis=1
        )]
        
        # Remover columnas completamente vacías
        df = df.loc[:, ~df.apply(
            lambda col: all(cell.strip() == '' for cell in col)
        )]
        
        return df

    @staticmethod
    def normalize_date(date_str: str) -> str:
        """
        Normaliza fechas a formato DD/MM/YYYY.
        Método auxiliar que pueden usar los parsers.
        """
        from datetime import datetime
        
        if not date_str:
            return ""
        
        # Limpiar caracteres no deseados
        date_clean = re.sub(r'[^\d\/\-\.]', '', date_str)
        
        # Formatos comunes
        formats = [
            '%d/%m/%Y', '%d/%m/%y', 
            '%d-%m-%Y', '%d-%m-%y',
            '%d.%m.%Y', '%d.%m.%y',
            '%Y/%m/%d', '%Y-%m-%d'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_clean, fmt)
                return dt.strftime('%d/%m/%Y')
            except:
                continue
        
        return date_clean

    @staticmethod
    def parse_amount(amount_str: str) -> float:
        """
        Convierte strings de montos a float.
        Maneja formato argentino (1.234,56) y negativos con sufijo (1.234,56-)
        """
        if not amount_str:
            return 0.0
        
        clean_str = str(amount_str).strip()
        
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
        
        try:
            result = float(clean_str)
            if negative_suffix or negative_prefix:
                result = -result
            return result
        except:
            return 0.0

    @staticmethod
    def format_amount(amount: float) -> str:
        """
        Formatea monto con formato argentino (1.234,56).
        """
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

    @staticmethod
    def clean_text(text: str) -> str:
        """Limpia y normaliza texto."""
        if not text:
            return ""
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        return text