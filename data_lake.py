import pandas as pd
import os
from pathlib import Path
import shutil
from typing import Union, List
from processors import ExcelCsvProcessor, DocumentProcessor, TextProcessor

class DataLake:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.raw_zone = self.base_path / "raw"
        self.processed_zone = self.base_path / "processed"
        self.staging_area = self.base_path / "staging"
        
        # Initialize processors
        self.excel_processor = ExcelCsvProcessor()
        self.document_processor = DocumentProcessor()
        self.text_processor = TextProcessor()
        
        # Create necessary directories
        for path in [self.raw_zone, self.processed_zone, self.staging_area]:
            path.mkdir(parents=True, exist_ok=True)

    def copy_to_raw(self, source_path: str, folder: str = None) -> bool:
        """Copy files from external sources to raw zone"""
        try:
            source = Path(source_path)
            dest = self.raw_zone / (folder if folder else "")
            dest.mkdir(parents=True, exist_ok=True)
            
            if source.is_file():
                shutil.copy2(source, dest / source.name)
            return True
        except Exception as e:
            print(f"Error copying file: {e}")
            return False

    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Process different file types using appropriate processor"""
        suffix = file_path.suffix.lower()
        
        if suffix in ['.csv', '.xlsx', '.xls']:
            return self.excel_processor.process(file_path)
        elif suffix in ['.pdf', '.docx']:
            return self.document_processor.process(file_path)
        elif suffix == '.txt':
            return self.text_processor.process(file_path)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

    def process_raw_data(self, folder: str = None) -> None:
        """Process all files in raw zone and save as JSON"""
        source_dir = self.raw_zone / (folder if folder else "")
        for file_path in source_dir.glob("**/*"):
            if file_path.is_file():
                try:
                    df = self.process_file(file_path)
                    output_path = self.processed_zone / folder if folder else self.processed_zone
                    output_path.mkdir(parents=True, exist_ok=True)
                    json_path = output_path / f"{file_path.stem}.json"
                    
                    try:
                        # Convert DataFrame to JSON with orient='records' for better readability
                        df.to_json(json_path, orient='records', indent=4)
                    except Exception as e:
                        print(f"Error saving JSON for {file_path.name}: {e}")
                        
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
                    continue

    # API Interface for external access
    def get_processed_data(self, folder: str = None, file_name: str = None) -> pd.DataFrame:
        """Retrieve processed data from JSON files"""
        path = self.processed_zone / (folder if folder else "")
        if file_name:
            json_path = path / f"{file_name}.json"
            if json_path.exists():
                try:
                    return pd.read_json(json_path, orient='records')
                except Exception as e:
                    print(f"Error reading JSON file {file_name}: {e}")
                    return pd.DataFrame()
            raise FileNotFoundError(f"File not found: {file_name}")
        
        # Return all data in folder
        dfs = []
        for file_path in path.glob("*.json"):
            try:
                dfs.append(pd.read_json(file_path, orient='records'))
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue
                    
        return pd.concat(dfs) if dfs else pd.DataFrame()

# Example usage for transaction data
def import_transactions(data_lake: DataLake, source_files: List[str], folder: str = "transactions"):
    """Import transaction files into the data lake"""
    # Copy files to raw zone
    for file_path in source_files:
        data_lake.copy_to_raw(file_path, folder)
    
    # Process the files
    data_lake.process_raw_data(folder)
    
    # Return processed data
    return data_lake.get_processed_data(folder)