# models.py
from dataclasses import dataclass

@dataclass
class DailyResult:
    name: str
    high: float
    high_time: str
    high_pct: float
    low: float
    low_time: str
    low_pct: float
    close: float
    close_pct: float
    amplitude_pct: float
    result_type: str
    date_str: str
    status: str = "ok"

    # 基于收盘价的波动率（C2C）
    hv7: float = 0.0          # 7日年化波动率（%）
    hv7_change: float = 0.0   # 较前一日变化（百分比点）
    hv7_up: float = 0.0       # 7日上行波动率（%）
    hv7_up_change: float = 0.0
    hv7_down: float = 0.0     # 7日下行波动率（%）
    hv7_down_change: float = 0.0
    hv7_ratio: float = 0.0    # 7日上/下比值
    hv7_ratio_change: float = 0.0

    hv30: float = 0.0         # 30日年化波动率（%）
    hv30_change: float = 0.0  # 较前一日变化（百分比点）
    hv30_up: float = 0.0      # 30日上行波动率（%）
    hv30_up_change: float = 0.0
    hv30_down: float = 0.0    # 30日下行波动率（%）
    hv30_down_change: float = 0.0
    hv30_ratio: float = 0.0   # 30日上/下比值
    hv30_ratio_change: float = 0.0

    # 基于盘中波动的波动率（Parkinson）
    parkinson_hv7: float = 0.0          # 7日 Parkinson 年化波动率（%）
    parkinson_hv7_change: float = 0.0   # 较前一日变化（百分比点）
    parkinson_hv7_up: float = 0.0       # 7日上行 Parkinson 波动率（%）
    parkinson_hv7_up_change: float = 0.0
    parkinson_hv7_down: float = 0.0     # 7日下行 Parkinson 波动率（%）
    parkinson_hv7_down_change: float = 0.0
    parkinson_hv7_ratio: float = 0.0    # 7日上/下比值
    parkinson_hv7_ratio_change: float = 0.0

    parkinson_hv30: float = 0.0         # 30日 Parkinson 年化波动率（%）
    parkinson_hv30_change: float = 0.0  # 较前一日变化（百分比点）
    parkinson_hv30_up: float = 0.0      # 30日上行 Parkinson 波动率（%）
    parkinson_hv30_up_change: float = 0.0
    parkinson_hv30_down: float = 0.0    # 30日下行 Parkinson 波动率（%）
    parkinson_hv30_down_change: float = 0.0
    parkinson_hv30_ratio: float = 0.0   # 30日上/下比值
    parkinson_hv30_ratio_change: float = 0.0