# processors/text_processor.py
import pandas as pd
import chardet
from pathlib import Path
from .base_processor import BaseProcessor

class TextProcessor(BaseProcessor):
    def process(self, file_path: Path) -> pd.DataFrame:
        # Detect encoding
        with open(file_path, 'rb') as file:
            raw_data = file.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding'] or 'utf-8'
        
        with open(file_path, 'r', encoding=encoding) as file:
            content = file.read()
        return pd.DataFrame({'content': [content]})