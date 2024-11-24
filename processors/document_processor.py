# processors/document_processor.py
import pandas as pd
from pathlib import Path
import PyPDF2
import docx
from .base_processor import BaseProcessor
from .bank_statement_parser import BankStatementParser

class DocumentProcessor(BaseProcessor):
    def __init__(self):
        self.bank_parser = BankStatementParser()
    
    def process(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix.lower() == '.pdf':
            # Try parsing as bank statement first
            try:
                return self.bank_parser.parse_pdf(file_path)
            except Exception:
                # Fall back to regular PDF processing if not a bank statement
                return self._process_pdf(file_path)
        elif file_path.suffix.lower() == '.docx':
            return self._process_docx(file_path)
        else:
            raise ValueError(f"Unsupported document type: {file_path.suffix}")
    
    def _process_pdf(self, file_path: Path) -> pd.DataFrame:
        text = []
        with open(file_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text.append(page.extract_text())
        return pd.DataFrame({'content': text})
    
    def _process_docx(self, file_path: Path) -> pd.DataFrame:
        doc = docx.Document(file_path)
        text = [paragraph.text for paragraph in doc.paragraphs]
        return pd.DataFrame({'content': text})