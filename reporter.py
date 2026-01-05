# reporter.py
from datetime import date
from typing import List, Optional

from models import DailyResult

def _val(v: Optional[float], fmt: str = ".1f", default: str = "-") -> str:
    return f"{v:{fmt}}" if v is not None else default

def _pct_change(pct: float) -> str:
    if pct > 0:
        return f"+{pct:.2f}%"
    elif pct < 0:
        return f"{pct:.2f}%"
    else:
        return f"{pct:.2f}%"

def render_report(results: List[DailyResult], crypto_report_date: Optional[date], us_report_date: Optional[date]):
    dates = sorted(
        {r.date_str for r in results if r.date_str},
        key=lambda x: x.split("（")[0],
        reverse=True
    )

    print("\n" + "=" * 80)
    print("                             市场日报")
    print("=" * 80)

    us_market = [r for r in results if r.name in ["QQQ", "SPY", "DIA", "纳指", "SPX", "道指", "罗素2000"]]
    crypto_market = [r for r in results if r.name in ["BTC", "ETH"]]
    asia_market = [r for r in results if r.name in ["恒科", "恒生", "上证", "日经"]]

    for d in dates:
        print(f"【{d}】\n")

        us_results = [res for res in us_market if res.date_str == d]
        if us_results:
            print("【美国市场】")
            for r in us_results:
                unit = "美元" if r.result_type == "stock" else ""
                time_prefix = "美东时间" if r.result_type == "stock" else ""
                status_suffix = {"partial": "（数据部分缺失）", "stale": "（数据陈旧）"}.get(r.status, "")
                line = f"{r.name}{status_suffix}: "
                high_str = f"最高：{r.high}{unit}" + (f"，{time_prefix}{r.high_time}触及" if r.high_time else "") + f"，{_pct_change(r.high_pct)}"
                low_str = f"；最低：{r.low}{unit}" + (f"，{time_prefix}{r.low_time}触及" if r.low_time else "") + f"，{_pct_change(r.low_pct)}"
                close_str = f"；收盘：{r.close}{unit}，{_pct_change(r.close_pct)}"
                amp_str = f"；振幅：{r.amplitude_pct:.2f}%。"
                print(line + high_str + low_str + close_str + amp_str)
            print("")

        crypto_results = [res for res in crypto_market if res.date_str == d]
        if crypto_results:
            print("【加密货币】")
            for r in crypto_results:
                unit = "美元"
                time_prefix = "北京时间"
                status_suffix = {"partial": "（数据部分缺失）", "stale": "（数据陈旧）"}.get(r.status, "")
                line = f"{r.name}{status_suffix}: "
                high_str = f"最高：{r.high}{unit}" + (f"，{time_prefix}{r.high_time}触及" if r.high_time else "") + f"，{_pct_change(r.high_pct)}"
                low_str = f"；最低：{r.low}{unit}" + (f"，{time_prefix}{r.low_time}触及" if r.low_time else "") + f"，{_pct_change(r.low_pct)}"
                close_str = f"；收盘：{r.close}{unit}，{_pct_change(r.close_pct)}"
                amp_str = f"；振幅：{r.amplitude_pct:.2f}%。"
                print(line + high_str + low_str + close_str + amp_str)
            print("")

        asia_results = [res for res in asia_market if res.date_str == d]
        if asia_results:
            print("【亚洲市场】")
            for r in asia_results:
                unit = ""
                time_prefix = ""
                status_suffix = {"partial": "（数据部分缺失）", "stale": "（数据陈旧）"}.get(r.status, "")
                line = f"{r.name}{status_suffix}: "
                high_str = f"最高：{r.high}{unit}" + (f"，{time_prefix}{r.high_time}触及" if r.high_time else "") + f"，{_pct_change(r.high_pct)}"
                low_str = f"；最低：{r.low}{unit}" + (f"，{time_prefix}{r.low_time}触及" if r.low_time else "") + f"，{_pct_change(r.low_pct)}"
                close_str = f"；收盘：{r.close}{unit}，{_pct_change(r.close_pct)}"
                amp_str = f"；振幅：{r.amplitude_pct:.2f}%。"
                print(line + high_str + low_str + close_str + amp_str)
            print("")