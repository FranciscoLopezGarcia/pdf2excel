"""
Generic Parser - Parser genérico con heurísticas para bancos sin implementación específica.
"""

import logging
import re
from typing import Dict, List, Optional

import pandas as pd

from .base_parser import BaseParser

logger = logging.getLogger(__name__)


class GenericParser(BaseParser):
    """
    Parser genérico que usa heurísticas para extraer transacciones
    de PDFs bancarios sin formato específico conocido.
    
    Estrategia:
    1. Prioriza tablas si están disponibles
    2. Si no hay tablas, parsea texto con regex y detección de patrones
    3. Usa contexto para categorizar débitos/créditos/saldos
    """
    
    BANK_NAME = "GENERICO"
    DETECTION_KEYWORDS = []
    PREFER_TABLES = True
    
    def __init__(self):
        super().__init__()
        
        # Patrones de fecha
        self.date_patterns = [
            r'\b(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})\b',
            r'\b(\d{2,4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})\b'
        ]
        
        # Patrones de montos (orden importa)
        self.amount_patterns = [
            r'\d{1,3}(?:[.,]\d{3})*[.,]\d{2}-',  # Negativo con sufijo: 1.234,56-
            r'-?\$?\s*\d{1,3}(?:[.,]\d{3})*[.,]\d{2}',  # Con separadores
            r'-?\$?\s*\d+[.,]\d{2}',  # Simple con decimales
            r'\d+[.,]\d{2}'  # Básico
        ]
        
        # Indicadores de columnas (para tablas)
        self.date_indicators = ['fecha', 'date', 'fec', 'dia']
        self.detail_indicators = ['concepto', 'detalle', 'descripcion', 'causal', 
                                   'operacion', 'movimiento']
        self.reference_indicators = ['referencia', 'ref', 'nro', 'numero', 
                                      'comprobante', 'transaccion']
        self.debit_indicators = ['debito', 'debitos', 'debe', 'egreso', 
                                 'salida', 'cargo']
        self.credit_indicators = ['credito', 'creditos', 'haber', 'ingreso', 
                                  'entrada', 'abono', 'deposito']
        self.balance_indicators = ['saldo', 'balance', 'total']
        self.amount_indicators = ['importe', 'monto', 'valor']
    
    def detect(self, text: str, filename: str = "") -> bool:
        """El parser genérico siempre acepta (es el fallback)."""
        return True
    
    def parse(self, raw_data: Dict, filename: str = "") -> pd.DataFrame:
        """
        Parsea datos crudos usando heurísticas genéricas.
        
        Args:
            raw_data: Dict con 'text', 'tables', 'method', 'pages_count'
            filename: Nombre del archivo
        
        Returns:
            DataFrame con transacciones parseadas
        """
        logger.info(f"Procesando con {self.BANK_NAME}")
        
        # Estrategia 1: Intentar con tablas si existen
        if self.PREFER_TABLES and raw_data.get('tables'):
            logger.info(f"Intentando parsear {len(raw_data['tables'])} tablas")
            rows = self._parse_from_tables(raw_data['tables'])
            if rows:
                logger.info(f"Parseadas {len(rows)} transacciones desde tablas")
                return self.finalize(pd.DataFrame(rows))
        
        # Estrategia 2: Parsear desde texto
        if raw_data.get('text'):
            logger.info("Parseando desde texto")
            rows = self._parse_from_text(raw_data['text'])
            if rows:
                logger.info(f"Parseadas {len(rows)} transacciones desde texto")
                return self.finalize(pd.DataFrame(rows))
        
        logger.warning("No se pudieron extraer transacciones")
        return pd.DataFrame(columns=self.OUTPUT_COLUMNS)
    
    # ================================================================
    # PARSING DESDE TABLAS
    # ================================================================
    
    def _parse_from_tables(self, tables: List[pd.DataFrame]) -> List[Dict]:
        """Extrae transacciones desde tablas detectadas."""
        all_rows = []
        
        for idx, table in enumerate(tables):
            if table.empty:
                continue
            
            logger.debug(f"Procesando tabla {idx+1}/{len(tables)}")
            
            # Encontrar fila de encabezados
            header_row = self._find_header_row(table)
            
            if header_row is None:
                logger.debug(f"No se encontró header en tabla {idx+1}, saltando")
                continue
            
            # Mapear columnas
            headers = table.iloc[header_row].tolist()
            column_map = self._map_columns(headers)
            
            if not column_map:
                logger.debug(f"No se pudo mapear columnas en tabla {idx+1}")
                continue
            
            # Procesar filas de datos
            for row_idx, row in table.iloc[header_row + 1:].iterrows():
                parsed_row = self._parse_table_row(row, column_map)
                if self._is_valid_transaction(parsed_row):
                    all_rows.append(parsed_row)
        
        return all_rows
    
    def _map_columns(self, headers: List[str]) -> Dict[int, str]:
        """Mapea índices de columnas a nombres estándar."""
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
                mapping[i] = 'debito'
            elif any(ind in header_lower for ind in self.credit_indicators):
                mapping[i] = 'credito'
            elif any(ind in header_lower for ind in self.balance_indicators):
                mapping[i] = 'saldo'
            elif any(ind in header_lower for ind in self.amount_indicators):
                mapping[i] = 'importe'
        
        return mapping
    
    def _parse_table_row(self, row: pd.Series, column_map: Dict[int, str]) -> Dict:
        """Parsea una fila de tabla a formato estándar."""
        transaction = {
            'fecha': '',
            'detalle': '',
            'referencia': '',
            'debito': 0.0,
            'credito': 0.0,
            'saldo': 0.0
        }
        
        for i, value in enumerate(row):
            field = column_map.get(i)
            if not field:
                continue
            
            value_str = str(value).strip()
            
            if field == 'fecha':
                transaction['fecha'] = self.normalize_date(value_str)
            elif field == 'detalle':
                transaction['detalle'] = self.clean_text(value_str)
            elif field == 'referencia':
                transaction['referencia'] = value_str
            elif field in ['debito', 'credito', 'saldo']:
                amount = self.parse_amount(value_str)
                if amount != 0:
                    transaction[field] = abs(amount)
            elif field == 'importe':
                # Columna única de importe: clasificar por signo
                amount = self.parse_amount(value_str)
                if amount != 0:
                    if amount < 0:
                        transaction['debito'] = abs(amount)
                    else:
                        transaction['credito'] = amount
        
        return transaction
    
    # ================================================================
    # PARSING DESDE TEXTO
    # ================================================================
    
    def _parse_from_text(self, text: str) -> List[Dict]:
        """Parsea transacciones desde texto crudo."""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        transactions = []
        
        in_transaction_section = False
        header_found = False
        
        for line in lines:
            # Saltar líneas irrelevantes
            if self._should_skip_line(line):
                continue
            
            # Detectar inicio de sección de transacciones
            if self._is_transaction_header(line):
                in_transaction_section = True
                header_found = True
                continue
            
            # Detectar fin de sección
            if header_found and self._is_other_section(line):
                in_transaction_section = False
                continue
            
            # Si no estamos en sección, solo procesar líneas con fecha
            if not in_transaction_section:
                if not self._line_has_date(line):
                    continue
            
            # Intentar parsear como transacción
            transaction = self._parse_transaction_line(line)
            if transaction:
                transactions.append(transaction)
        
        return transactions
    
    def _is_transaction_header(self, line: str) -> bool:
        """Detecta si es un header de transacciones."""
        line_lower = line.lower()
        header_indicators = ['fecha', 'concepto', 'debito', 'credito', 'saldo']
        found = sum(1 for ind in header_indicators if ind in line_lower)
        return found >= 2
    
    def _is_other_section(self, line: str) -> bool:
        """Detecta si es inicio de otra sección."""
        line_lower = line.lower()
        section_indicators = [
            'debitos automaticos', 'transferencias recibidas', 
            'transferencias enviadas', 'detalle - comision',
            'situacion impositiva', 'retenciones', 'resumen'
        ]
        return any(ind in line_lower for ind in section_indicators)
    
    def _line_has_date(self, line: str) -> bool:
        """Verifica si la línea contiene una fecha."""
        for pattern in self.date_patterns:
            if re.search(pattern, line):
                return True
        return False
    
    def _should_skip_line(self, line: str) -> bool:
        """Verifica si la línea debe ser ignorada."""
        skip_patterns = [
            r'^\s*p[aá]gina',
            r'^\s*hoja',
            r'^\s*estimado cliente',
            r'^\s*banco\s+',
            r'^\s*cbu:',
            r'^\s*cuenta corriente',
            r'^\s*caja de ahorro',
            r'movimientos pendientes',
            r'^\s*\d+\s*$',  # Solo números de página
            r'^\s*estado de cuenta',
            r'^\s*resumen de cuenta'
        ]
        
        line_lower = line.lower()
        return any(re.match(pattern, line_lower) for pattern in skip_patterns)
    
    def _parse_transaction_line(self, line: str) -> Optional[Dict]:
        """
        Parsea una línea individual como transacción.
        
        Estrategia:
        1. Buscar fecha
        2. Extraer todos los montos
        3. Limpiar línea para obtener detalle
        4. Categorizar montos según contexto
        """
        # Buscar fecha
        date_match = None
        for pattern in self.date_patterns:
            match = re.search(pattern, line)
            if match:
                date_match = match
                break
        
        if not date_match:
            return None
        
        fecha = self.normalize_date(date_match.group(0))
        if not fecha:
            return None
        
        # Extraer montos
        amounts = []
        amount_positions = []
        
        # Buscar negativos con sufijo primero (prioridad)
        negative_suffix_pattern = r'\d{1,3}(?:[.,]\d{3})*[.,]\d{2}-'
        for match in re.finditer(negative_suffix_pattern, line):
            amount = self.parse_amount(match.group(0))
            if amount != 0:
                amounts.append(amount)
                amount_positions.append((match.start(), match.end()))
        
        # Buscar otros patrones evitando overlaps
        occupied_ranges = set()
        for start, end in amount_positions:
            occupied_ranges.update(range(start, end))
        
        for pattern in self.amount_patterns[1:]:
            for match in re.finditer(pattern, line):
                if any(pos in occupied_ranges for pos in range(match.start(), match.end())):
                    continue
                
                amount = self.parse_amount(match.group(0))
                if amount != 0:
                    amounts.append(amount)
                    amount_positions.append((match.start(), match.end()))
                    occupied_ranges.update(range(match.start(), match.end()))
        
        # Extraer detalle (remover fecha y montos)
        clean_line = line
        clean_line = re.sub(date_match.re.pattern, '', clean_line, count=1)
        
        for start, end in sorted(amount_positions, reverse=True):
            offset = len(line) - len(clean_line)
            adjusted_start = max(0, start - offset)
            adjusted_end = max(0, end - offset)
            clean_line = clean_line[:adjusted_start] + clean_line[adjusted_end:]
        
        detalle = self.clean_text(clean_line)
        
        # Categorizar montos
        return self._categorize_amounts(fecha, detalle, amounts, line)
    
    def _categorize_amounts(
        self, 
        fecha: str, 
        detalle: str, 
        amounts: List[float], 
        original_line: str
    ) -> Optional[Dict]:
        """
        Categoriza montos en débito/crédito/saldo según contexto.
        
        Lógica:
        - Líneas de saldo: todo va a saldo
        - 1 monto: clasificar por signo o contexto
        - 2 montos: movimiento + saldo
        - 3+ montos: movimiento principal + saldo
        """
        if not amounts:
            return None
        
        transaction = {
            'fecha': fecha,
            'detalle': detalle,
            'referencia': self._extract_reference(detalle),
            'debito': 0.0,
            'credito': 0.0,
            'saldo': 0.0
        }
        
        detalle_lower = detalle.lower()
        original_lower = original_line.lower()
        
        # Detectar líneas de saldo
        is_saldo_line = any(phrase in detalle_lower for phrase in [
            'saldo anterior', 'saldo actual', 'saldo al cierre', 
            'saldo del periodo', 'saldo final'
        ]) or any(phrase in original_lower for phrase in [
            'saldo anterior', 'saldo actual', 'saldo al cierre'
        ])
        
        if is_saldo_line:
            transaction['saldo'] = amounts[0]
            return transaction
        
        # Detectar contexto de débito/crédito
        is_debit_context = any(word in detalle_lower for word in [
            'debito', 'cargo', 'comision', 'impuesto', 
            'transferencia enviada', 'retiro', 'pago', 
            'automatico', 'imp.', 'iva', 'interes', 
            'mantenimiento', 'ret.'
        ])
        
        is_credit_context = any(word in detalle_lower for word in [
            'credito', 'deposito', 'transferencia recibida', 
            'abono', 'ingreso', 'acreditacion'
        ])
        
        # Categorizar según cantidad de montos
        if len(amounts) == 1:
            amount = amounts[0]
            if amount < 0 or is_debit_context:
                transaction['debito'] = abs(amount)
            else:
                transaction['credito'] = abs(amount)
        
        elif len(amounts) == 2:
            # Formato típico: movimiento + saldo
            movement, balance = amounts[0], amounts[1]
            
            if is_debit_context:
                transaction['debito'] = abs(movement)
            elif is_credit_context:
                transaction['credito'] = abs(movement)
            else:
                # Usar signo si no hay contexto claro
                if movement < 0:
                    transaction['debito'] = abs(movement)
                else:
                    transaction['credito'] = abs(movement)
            
            transaction['saldo'] = balance
        
        elif len(amounts) >= 3:
            # Múltiples montos: buscar saldo (último o mayor)
            balance_candidate = amounts[-1]
            max_amount = max(amounts, key=abs)
            
            # Si el último es muy pequeño, usar el mayor
            if abs(amounts[-1]) < abs(max_amount) / 10:
                balance_candidate = max_amount
            
            # Movimiento principal
            movement_candidates = [a for a in amounts if a != balance_candidate]
            movement = max(movement_candidates, key=abs) if movement_candidates else amounts[0]
            
            # Categorizar movimiento
            if is_debit_context:
                transaction['debito'] = abs(movement)
            elif is_credit_context:
                transaction['credito'] = abs(movement)
            else:
                if movement < 0:
                    transaction['debito'] = abs(movement)
                else:
                    transaction['credito'] = abs(movement)
            
            transaction['saldo'] = balance_candidate
        
        return transaction
    
    def _is_valid_transaction(self, transaction: Dict) -> bool:
        """Valida que una transacción tenga los datos mínimos."""
        if not transaction.get('fecha'):
            return False
        if not transaction.get('detalle'):
            return False
        # Debe tener al menos un monto
        return any(
            transaction.get(field, 0) != 0 
            for field in ['debito', 'credito', 'saldo']
        )