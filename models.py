from dataclasses import dataclass
from typing import Optional

@dataclass
class DailyResult:
    name: str
    high: Optional[float]
    high_time: str
    high_pct: Optional[float]
    low: Optional[float]
    low_time: str
    low_pct: Optional[float]
    close: Optional[float]
    close_pct: Optional[float]
    amplitude_pct: Optional[float]
    result_type: str
    date_str: str
    status: str = "ok"