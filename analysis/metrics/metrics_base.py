from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class Metrics(ABC):
    @abstractmethod
    def calculate_metrics(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        pass
