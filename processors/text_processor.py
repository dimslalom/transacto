# processors/text_processor.py
import pandas as pd
import chardet
from pathlib import Path
import re
from datetime import datetime
from .base_processor import BaseProcessor

class TextProcessor(BaseProcessor):
    def __init__(self):
        # Common date patterns at start of line
        self.date_pattern = r'^(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{1,2}[-/]\d{1,2})'
        
    def normalize_date(self, date_str: str) -> str:
        """Convert various date formats to ISO format"""
        try:
            # Try different date formats
            for fmt in ['%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d', '%Y-%m-%d', 
                       '%d/%m/%y', '%d-%m-%y']:
                try:
                    return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return None
        except Exception:
            return None

    def process(self, file_path: Path) -> pd.DataFrame:
        # Detect encoding
        with open(file_path, 'rb') as file:
            raw_data = file.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding'] or 'utf-8'
        
        # Process file content
        entries = []  # List to store entries
        current_date = None
        current_entry = None
        
        with open(file_path, 'r', encoding=encoding) as file:
            lines = file.readlines()
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                
                # Check for date at start of line
                date_match = re.match(self.date_pattern, line)
                if date_match:
                    # Save previous entry if exists
                    if current_entry:
                        entries.append(current_entry)
                    
                    # Extract and normalize date
                    date_str = date_match.group(0)
                    normalized_date = self.normalize_date(date_str)
                    if normalized_date:
                        current_date = normalized_date
                        # Get remainder after date
                        remainder = line[date_match.end():].strip()
                        
                        # Extract description, amount, and payee
                        # Look for amount pattern like "-2108.0"
                        amount_match = re.search(r'\s(-?\d+\.?\d*)\s', remainder)
                        if amount_match:
                            amount = float(amount_match.group(1))
                            # Split remainder into parts before and after amount
                            parts = remainder.split(amount_match.group(0))
                            description = parts[0].strip()
                            
                            # Extract payee if present
                            payee_match = re.search(r'Payee:\s*(\w+)', parts[1]) if len(parts) > 1 else None
                            payee = payee_match.group(1) if payee_match else ''
                            
                            current_entry = {
                                'date': current_date,
                                'amount': amount,
                                'description': description,
                                'payee': payee
                            }
                            
                            # Check if next line is additional description
                            if i + 1 < len(lines):
                                next_line = lines[i + 1].strip()
                                if next_line and not re.match(self.date_pattern, next_line):
                                    current_entry['description'] = f"{current_entry['description']} - {next_line}"
    
        # Add last entry if exists
        if current_entry:
            entries.append(current_entry)
        
        # Convert to DataFrame
        if entries:
            return pd.DataFrame(entries)
        
        return pd.DataFrame(columns=['date', 'amount', 'description', 'payee'])