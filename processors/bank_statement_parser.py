import pdfplumber
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import logging

# Configure logging levels for PDF processing libraries
logging.getLogger('pdfminer').setLevel(logging.WARNING)
logging.getLogger('pdfplumber').setLevel(logging.WARNING)

# Setup application logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class BankStatementParser:
    def __init__(self):
        # Parse individual transactions line by line
        # Example: "08 Nov 2024 PUTRI SALSABILLA Outgoing Transfer -16.000 1.086.542"
        self.transaction_pattern = re.compile(
            r'(\d{2}\s+\w+\s+\d{4})\s+'  # Date
            r'(\d{2}:\d{2})?\s*'          # Optional time
            r'([A-Za-z\s]+?)\s+'          # Payee name
            r'(?:(?:Outgoing|Incoming)\s+Transfer|'  # Transaction types
            r'Payment\s+with\s+Jago\s+Pay|'
            r'POS\s+Transaction|'
            r'Movement\s+between\s+Pockets|'
            r'Cash\s+Withdrawal)\s*'
            r'(?:ID#\s*[^\s]+)?\s*'       # Optional ID
            r'([+-]?\d+(?:\.\d{3})?)\s+'  # Transaction amount
            r'(\d+(?:\.\d{3})?)',         # Balance
            re.IGNORECASE
        )

    def preprocess_text(self, text: str) -> str:
        # Remove page headers and footers
        cleaned = re.sub(r'Pockets Transactions History Page \d+ of \d+.*?Balance', '', text)
        cleaned = re.sub(r'Date & Time Source/Destination Transaction Details Notes Amount Balance', '', cleaned)
        
        # Clean up whitespace and newlines
        cleaned = cleaned.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned.strip()

    def parse_pdf(self, file_path: Path) -> pd.DataFrame:
        try:
            logger.info(f"Processing PDF file: {file_path}")
            transactions = []
            
            with pdfplumber.open(file_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    text = page.extract_text()
                    if not text or 'Transaction Details' not in text:
                        continue
                        
                    text = self.preprocess_text(text)
                    matches = self.transaction_pattern.finditer(text)
                    
                    for match in matches:
                        try:
                            date_str = match.group(1)
                            time_str = match.group(2) or ""
                            payee = match.group(3)
                            amount_str = match.group(4)
                            
                            # Parse date
                            date = datetime.strptime(date_str.strip(), '%d %b %Y')
                            
                            # Parse amount (already has decimal point)
                            amount = float(amount_str)
                            
                            transaction = {
                                'Date': date.strftime('%Y-%m-%d'),
                                'Payee': payee.strip(),
                                'Memo': f"Transaction at {time_str}" if time_str else "Transaction",
                                'Amount': amount
                            }
                            
                            transactions.append(transaction)
                            
                        except Exception as e:
                            logger.warning(f"Failed to parse match: {e}")
                            continue
                
                if not transactions:
                    logger.warning("No transactions found - showing sample text for debugging")
                    logger.warning(text[:500])
                    return pd.DataFrame(columns=['Date', 'Payee', 'Memo', 'Amount'])
                
                df = pd.DataFrame(transactions)
                logger.info(f"Successfully parsed {len(df)} transactions")
                return df
                
        except Exception as e:
            logger.error(f"PDF processing error: {e}")
            raise

# Test the parser with a sample PDF
if __name__ == "__main__":
    parser = BankStatementParser()
    parser.logger.setLevel(logging.DEBUG)  # Enable debug logging
    df = parser.parse_pdf("statement.pdf")
    print(df)