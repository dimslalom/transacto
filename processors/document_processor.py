# processors/document_processor.py
import pandas as pd
from pathlib import Path
import PyPDF2
import docx
import os
import shutil
from .base_processor import BaseProcessor
from .transaction_processor import TransactionProcessor
import tabula

class DocumentProcessor(BaseProcessor):
    def __init__(self):
        self.transaction_processor = TransactionProcessor()
        self._check_java_installation()

    def _check_java_installation(self):
        """Check if Java is installed and set JAVA_HOME if needed"""
        try:
            # First check if JAVA_HOME is already set correctly
            java_home = os.environ.get('JAVA_HOME')
            if (java_home and os.path.exists(os.path.join(java_home, 'bin', 'java.exe'))):
                return True

            # Try to find Java installation
            common_java_paths = [
                r'C:\Program Files\Java',
                r'C:\Program Files (x86)\Java',
                r'C:\Program Files\Common Files\Oracle\Java',
                r'C:\ProgramData\Oracle\Java'
            ]

            # Find all possible Java installations
            java_installations = []
            for base_path in common_java_paths:
                if os.path.exists(base_path):
                    for root, dirs, files in os.walk(base_path):
                        if 'bin' in dirs and 'java.exe' in os.listdir(os.path.join(root, 'bin')):
                            java_installations.append(root)

            if java_installations:
                # Use the most recent version (assuming directory names are version-related)
                java_path = sorted(java_installations)[-1]
                os.environ['JAVA_HOME'] = java_path
                
                # Add Java bin to PATH
                bin_path = os.path.join(java_path, 'bin')
                if bin_path not in os.environ['PATH']:
                    os.environ['PATH'] = bin_path + os.pathsep + os.environ['PATH']
                
                print(f"Set JAVA_HOME to: {java_path}")
                return True
            else:
                print("Java installation found but unable to locate java.exe")
                return False
                
        except Exception as e:
            print(f"Error checking Java installation: {e}")
            return False

    def process(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix.lower() == '.pdf':
            return self._process_pdf(file_path)
        elif file_path.suffix.lower() == '.docx':
            return self._process_docx(file_path)
        else:
            raise ValueError(f"Unsupported document type: {file_path.suffix}")
        
    def _process_pdf(self, file_path: Path) -> pd.DataFrame:
        """Process PDF file using tabula with PyPDF2 fallback"""
        try:
            if not self._check_java_installation():
                print("Java not properly configured, falling back to PyPDF2...")
                return self._process_pdf_fallback(file_path)

            # Try tabula first
            tables = tabula.read_pdf(
                str(file_path), 
                multiple_tables=True,
                pages='all',
                guess=True,
                lattice=True,
                stream=True
            )
            
            if tables:
                for df in tables:
                    # Ensure required columns exist
                    if 'payee' not in df.columns:
                        df['payee'] = ''
                    transactions = self.transaction_processor.process_transactions(df)
                    if not transactions.empty:
                        return transactions
            
            # Fallback to PyPDF2 if no tables found
            print("No valid tables found, falling back to PyPDF2...")
            return self._process_pdf_fallback(file_path)
            
        except Exception as e:
            print(f"Error processing PDF with tabula: {e}")
            return self._process_pdf_fallback(file_path)

    def _process_pdf_fallback(self, file_path: Path) -> pd.DataFrame:
        """Fallback method using PyPDF2 for text extraction"""
        try:
            with open(file_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                text_content = []
                for page in reader.pages:
                    text_content.append(page.extract_text())
                
                # Create DataFrame with basic structure
                df = pd.DataFrame(columns=['date', 'amount', 'description', 'payee'])
                
                # Process extracted text through transaction processor
                if text_content:
                    text_df = pd.DataFrame({'text': text_content})
                    return self.transaction_processor.process_transactions(text_df)
                return df
                
        except Exception as e:
            print(f"Error in PyPDF2 fallback: {e}")
            return pd.DataFrame(columns=['date', 'amount', 'description', 'payee'])
        
    def _process_docx(self, file_path: Path) -> pd.DataFrame:
        doc = docx.Document(file_path)
        tables = doc.tables
        if tables:
            # Iterate over tables to find the one matching expected columns
            for table in tables:
                data = []
                keys = None
                for i, row in enumerate(table.rows):
                    text = [cell.text.strip() for cell in row.cells]
                    if i == 0:
                        # First row is assumed to be the header
                        keys = text
                    else:
                        if keys and len(text) == len(keys):
                            data.append(dict(zip(keys, text)))
                if data:
                    df = pd.DataFrame(data)
                    transactions = self.transaction_processor.process_transactions(df)
                    if not transactions.empty:
                        return transactions
            print(f"No valid transaction tables found in DOCX: {file_path}")
            return pd.DataFrame()
        else:
            print(f"No tables found in DOCX: {file_path}")
            return pd.DataFrame()