# processors/excel_processor.py
import pandas as pd
import chardet
from pathlib import Path
from .base_processor import BaseProcessor

class ExcelCsvProcessor(BaseProcessor):
    def process(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix == '.csv':
            # Detect file encoding
            with open(file_path, 'rb') as file:
                raw_data = file.read()
                result = chardet.detect(raw_data)
                encoding = result['encoding']
            
            # Try different encodings
            encodings = [encoding, 'utf-8', 'latin1', 'iso-8859-1', 'cp1252']
            for enc in encodings:
                try:
                    return pd.read_csv(file_path, encoding=enc)
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    print(f"Error reading CSV with {enc} encoding: {e}")
            raise ValueError(f"Could not read CSV file with any supported encoding")
        else:
            return pd.read_excel(file_path)