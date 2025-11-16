from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseAlerter(ABC):
    @abstractmethod
    def notify(self, error_log: Dict[str, Any]):
        pass
