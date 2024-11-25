# processors/transaction_processor.py
from datetime import datetime
import pandas as pd
import re
from typing import Dict, List, Tuple

class TransactionProcessor:
    def __init__(self):
        # Enhanced patterns for banking statements
        self.amount_patterns = {
            'amount': r'(?i)(amount|debit.*amount|credit.*amount|total|balance)',
            'currency': r'(?i)(USD|EUR|IDR|SGD|\$|€|Rp|[0-9]+,[0-9]+\.[0-9]+)',
            'numeric': r'^-?[\d,]+\.?\d*$'
        }
        
        self.date_patterns = {
            'date': r'(?i)(date|time|day)',
            'format': r'(?i)(\d{1,2}/\d{1,2}/\d{2,4}|\d{1,2}-\d{1,2}-\d{2,4})'
        }
        
        self.source_patterns = {
            'source': r'(?i)(category|type|description|transaction.*type)',
            'destination': r'(?i)(recipient|to|payee)'
        }

    def detect_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """Detect relevant transaction columns with improved accuracy"""
        column_mapping = {}
        
        # Handle special case for banking statement format
        for col in df.columns:
            col_lower = str(col).lower()
            
            # Date column detection
            if 'date' in col_lower:
                column_mapping['date'] = col
            
            # Amount columns detection - handle debit and credit separately
            if 'debit' in col_lower and 'amount' in col_lower:
                column_mapping['debit_amount'] = col
            elif 'credit' in col_lower and 'amount' in col_lower:
                column_mapping['credit_amount'] = col
            
            # Category/Type detection for transaction description
            if any(word in col_lower for word in ['category', 'type']):
                column_mapping['source'] = col

            # Payee column detection
            if 'payee' in col_lower:
                column_mapping['payee'] = col

        return column_mapping

    def process_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ensure all required columns exist
        required_cols = ['date', 'amount', 'description', 'payee']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''
                
        # Process existing columns
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
            
        if 'description' not in df.columns:
            df['description'] = ''
            
        if 'payee' not in df.columns:
            df['payee'] = ''
            
        return df[required_cols]  # Return only required columns in correct order

    def normalize_date(self, value: str) -> str:
        """Convert various date formats to ISO format"""
        if pd.isna(value):
            return None
        
        try:
            # Handle common date formats including DD/MM/YY
            date_str = str(value).strip()
            if '/' in date_str:
                day, month, year = date_str.split('/')
                # Handle 2-digit year
                if len(year) == 2:
                    year = '20' + year
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            return pd.to_datetime(value).strftime('%Y-%m-%d')
        except:
            return None

    def normalize_amount(self, value: str) -> float:
        """Convert amount string to float"""
        if pd.isna(value):
            return 0.0
        try:
            # Remove currency symbols and other non-numeric chars except . and -
            amount_str = re.sub(r'[^\d.-]', '', str(value))
            return float(amount_str)
        except ValueError:
            return 0.0