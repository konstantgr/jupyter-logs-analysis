from abc import ABC, abstractmethod
import pandas as pd


class Metrics(ABC):
    @abstractmethod
    def calculate_metrics(self, df) -> pd.DataFrame:
        pass
