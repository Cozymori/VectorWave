from .base import BaseAlerter
from typing import Dict, Any

class NullAlerter(BaseAlerter):

    def notify(self, error_log: Dict[str, Any]):
        pass