import re
from typing import List, Optional
from core.config import BankConfig
from core.models import TransactionRow
from utils.logger import get_logger

logger = get_logger("validator")

class TransactionValidator:
    def __init__(self):
        pass

    def validate_transaction(self, trx: TransactionRow, config: BankConfig) -> Optional[TransactionRow]:
        """Validate a single transaction based on config rules"""
        if not trx:
            return None
            
        rules = getattr(config, 'validation_rules', {})
        
        # Check required fields
        required = rules.get('required_fields', ['fecha', 'detalle'])
        for field in required:
            if not getattr(trx, field, '').strip():
                logger.debug(f"Transaction missing required field: {field}")
                return None
        
        # Validate date format
        if not self._is_valid_date(trx.fecha, rules):
            logger.debug(f"Invalid date format: {trx.fecha}")
            return None
            
        # Validate amounts
        if not self._has_valid_amounts(trx, rules):
            logger.debug(f"Transaction has no valid amounts")
            return None
            
        return trx

    def validate_batch(self, transactions: List[TransactionRow], config: BankConfig) -> List[TransactionRow]:
        """Validate a batch of transactions"""
        valid_transactions = []
        
        for trx in transactions:
            validated = self.validate_transaction(trx, config)
            if validated:
                valid_transactions.append(validated)
                
        logger.info(f"Validated {len(valid_transactions)}/{len(transactions)} transactions")
        return valid_transactions

    def _is_valid_date(self, date_str: str, rules: dict) -> bool:
        """Check if date string is valid"""
        if not date_str:
            return False
            
        min_length = rules.get('min_date_length', 8)
        if len(date_str) < min_length:
            return False
            
        # Check basic date pattern
        date_pattern = r'\d{1,2}/\d{1,2}/\d{2,4}'
        if not re.match(date_pattern, date_str):
            return False
            
        return True

    def _has_valid_amounts(self, trx: TransactionRow, rules: dict) -> bool:
        """Check if transaction has at least one valid amount"""
        amount_fields = ['debitos', 'creditos', 'saldo']
        
        for field in amount_fields:
            value = getattr(trx, field, '').strip()
            if value and value != '0' and value != '0,00':
                return True
                
        return False