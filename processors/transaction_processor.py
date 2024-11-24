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
        
        # Add bank statement patterns
        self.bank_patterns = {
            'transaction': r'(?i)(ID#[\d-]+)',
            'transfer': r'(?i)(incoming|outgoing)\s+transfer',
            'pos': r'(?i)pos\s+transaction'
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

        return column_mapping

    def process_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process dataframe into standardized transaction format"""
        # Check if data is already in bank statement format
        if all(col in df.columns for col in ['Date', 'Payee', 'Memo', 'Amount']):
            return df.rename(columns={
                'Date': 'date',
                'Payee': 'description',
                'Amount': 'amount'
            })
            
        # Detect columns
        columns = self.detect_columns(df)
        
        # Create standardized transaction dataframe
        transactions = pd.DataFrame()
        
        # Process date
        if 'date' in columns:
            transactions['date'] = df[columns['date']].apply(self.normalize_date)
        
        # Process amounts - combine debit and credit
        if 'debit_amount' in columns and 'credit_amount' in columns:
            transactions['amount'] = df.apply(
                lambda row: -float(row[columns['debit_amount']]) if pd.notna(row[columns['debit_amount']]) and float(row[columns['debit_amount']]) != 0 
                else float(row[columns['credit_amount']]) if pd.notna(row[columns['credit_amount']]) 
                else 0.0,
                axis=1
            )
        
        # Process description/category
        if 'source' in columns:
            transactions['description'] = df[columns['source']]
        
        return transactions.dropna(how='all')

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