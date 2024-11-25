import time
import pandas as pd
import os
from pathlib import Path
import shutil
from typing import Union, List
from processors import ExcelCsvProcessor, DocumentProcessor, TextProcessor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
from contextlib import contextmanager

class DataLake:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.raw_zone = self.base_path / "raw"
        self.staging_zone = self.base_path / "staging"
        self.master_database = self.base_path / "master_database.json"
        
        # Initialize processors
        self.excel_processor = ExcelCsvProcessor()
        self.document_processor = DocumentProcessor()
        self.text_processor = TextProcessor()
        
        # Create necessary directories
        for path in [self.raw_zone, self.staging_zone]:
            path.mkdir(parents=True, exist_ok=True)
        
        # Initialize master database if it doesn't exist
        if not self.master_database.exists():
            self.master_database.write_text('[]', encoding='utf-8')
        
        # Start watching the staging area
        self.watch_staging()
        self._db_lock = threading.Lock()  # Add lock for master database

    @contextmanager
    def _master_db_lock(self):
        """Context manager for thread-safe master database operations"""
        try:
            self._db_lock.acquire()
            yield
        finally:
            self._db_lock.release()

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
        """Process all files in raw zone and save as JSON in staging"""
        source_dir = self.raw_zone / (folder if folder else "")
        for file_path in source_dir.glob("**/*"):
            if file_path.is_file():
                try:
                    df = self.process_file(file_path)
                    output_path = self.staging_zone / folder if folder else self.staging_zone
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

    def update_master_database(self) -> None:
        """Update master database with data from staging"""
        with self._master_db_lock():  # Use lock when updating
            all_data = []
            for file_path in self.staging_zone.glob("**/*.json"):
                try:
                    df = pd.read_json(file_path, orient='records')
                    df['source_file'] = str(file_path)  # Add source file column here
                    # Fill NA/None values
                    df['payee'] = df['payee'].fillna('')
                    df['description'] = df['description'].fillna('')
                    all_data.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
                    continue
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Load existing master database
                if self.master_database.exists():
                    master_df = pd.read_json(self.master_database, orient='records')
                else:
                    master_df = pd.DataFrame(columns=['date', 'amount', 'description', 'payee', 'source_file'])
                
                # Combine all entries (master and new)
                all_entries = pd.concat([master_df, combined_df], ignore_index=True)
                
                # Group by date and amount
                updated_master_df = all_entries.groupby(['date', 'amount'], as_index=False).agg({
                    'description': lambda x: ' - '.join(filter(None, set(x))),  # Combine unique descriptions
                    'payee': lambda x: ' - '.join(filter(None, set(x))),  # Combine unique payees
                    'source_file': lambda x: ', '.join(set(x))  # Combine source files
                })
                
                # Remove entries with deleted source files
                updated_master_df = updated_master_df[
                    updated_master_df['source_file'].apply(
                        lambda x: all(Path(file.strip()).exists() for file in x.split(','))
                    )
                ]
                
                updated_master_df.to_json(self.master_database, orient='records', indent=4)
                print(f"Updated master database with {len(updated_master_df)} records")
            else:
                print("No data found in staging to update master database")

    def watch_staging(self):
        """Continuously watch the staging area for updates"""
        class StagingHandler(FileSystemEventHandler):
            def __init__(self, data_lake):
                self.data_lake = data_lake
                self._last_modified = 0
                self._update_lock = threading.Lock()

            def _debounced_update(self):
                """Debounce updates to prevent multiple rapid-fire updates"""
                current_time = time.time()
                if current_time - self._last_modified > 1.0:  # 1 second debounce
                    with self._update_lock:
                        self._last_modified = current_time
                        self.data_lake.update_master_database()

            def on_modified(self, event):
                if event.is_directory:
                    return
                self._debounced_update()

            def on_created(self, event):
                if event.is_directory:
                    return
                self._debounced_update()

            def on_deleted(self, event):
                if event.is_directory:
                    return
                self._debounced_update()

        event_handler = StagingHandler(self)
        observer = Observer()
        observer.schedule(event_handler, str(self.staging_zone), recursive=True)
        observer.start()
        print("Started watching staging area")  # Debug print

    def refresh_app(self):
        """Refresh the app with a loading screen overlay"""
        pass

    def get_master_data(self) -> pd.DataFrame:
        """Retrieve data from master database"""
        with self._master_db_lock():  # Use lock when reading
            try:
                if self.master_database.exists():
                    return pd.read_json(self.master_database, orient='records')
                return pd.DataFrame()
            except Exception as e:
                print(f"Error reading master database: {e}")
                return pd.DataFrame()

    def search_transactions(self, query: str, fields: list = None) -> pd.DataFrame:
        """
        Search transactions in master database using a query string
        """
        try:
            df = self.get_master_data()
            if df.empty:
                return pd.DataFrame()

            # If no specific fields are provided, search all text fields
            if not fields:
                fields = ['description', 'payee', 'source_file']

            # Convert query to lowercase for case-insensitive search
            query = query.lower()
            
            # Create mask for each searchable field
            masks = []
            for field in fields:
                if field in df.columns:
                    # Convert field values to string and lowercase
                    field_mask = df[field].astype(str).str.lower().str.contains(query, na=False)
                    masks.append(field_mask)
                    
            # For amount field, handle numeric search
            if 'amount' in fields:
                try:
                    amount = float(query)
                    masks.append(df['amount'].round(2) == round(amount, 2))
                except ValueError:
                    pass

            # Combine all masks with OR operation
            final_mask = pd.concat(masks, axis=1).any(axis=1)
            
            # Return filtered results
            return df[final_mask].copy()

        except Exception as e:
            print(f"Search error: {e}")
            return pd.DataFrame()

    def get_entry(self, timestamp: int) -> dict:
        """Get a specific entry by timestamp"""
        try:
            df = self.get_master_data()
            entry = df[df['date'] == timestamp].to_dict('records')
            return entry[0] if entry else None
        except Exception as e:
            print(f"Error getting entry: {e}")
            return None

    def add_entry(self, entry_data: dict) -> bool:
        """Add a new entry to master database"""
        try:
            df = self.get_master_data()
            new_entry = pd.DataFrame([entry_data])
            updated_df = pd.concat([df, new_entry], ignore_index=True)
            updated_df.to_json(self.master_database, orient='records', indent=4)
            return True
        except Exception as e:
            print(f"Error adding entry: {e}")
            return False

    def update_entry(self, timestamp: int, entry_data: dict) -> bool:
        """Update an existing entry in master database"""
        try:
            df = self.get_master_data()
            mask = df['date'] == timestamp
            if not mask.any():
                return False
            
            for key, value in entry_data.items():
                if key in df.columns:
                    df.loc[mask, key] = value
                    
            df.to_json(self.master_database, orient='records', indent=4)
            return True
        except Exception as e:
            print(f"Error updating entry: {e}")
            return False

    def delete_entry(self, timestamp: int) -> bool:
        """Delete an entry from master database"""
        try:
            df = self.get_master_data()
            mask = df['date'] == timestamp
            if not mask.any():
                return False
                
            df = df[~mask]
            df.to_json(self.master_database, orient='records', indent=4)
            return True
        except Exception as e:
            print(f"Error deleting entry: {e}")
            return False

# Example usage for transaction data
def import_transactions(data_lake: DataLake, source_files: List[str], folder: str = "transactions"):
    """Import transaction files into the data lake"""
    # Copy files to raw zone
    for file_path in source_files:
        data_lake.copy_to_raw(file_path, folder)
    
    # Process the files
    data_lake.process_raw_data(folder)
    
    # Return processed data
    return data_lake.get_master_data()