# processors/base_processor.py
from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path

class BaseProcessor(ABC):
    @abstractmethod
    def process(self, file_path: Path) -> pd.DataFrame:
        pass