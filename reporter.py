# reporter.py
from datetime import datetime, date
from typing import List, Optional

from models import DailyResult
from data_fetchers import fetch_matching_fng, fetch_matching_cnn_fng

def render_report(results: List[DailyResult], crypto_report_date: Optional[date], us_report_date: Optional[date]):
    crypto_fng_value, crypto_fng_class = 0, "未知"
    if crypto_report_date:
        crypto_fng_value, crypto_fng_class = fetch_matching_fng(crypto_report_date)

    stock_fng_value, stock_fng_class = 0, "未知"
    if us_report_date:
        stock_fng_value, stock_fng_class = fetch_matching_cnn_fng(us_report_date)

    dates = sorted(
        {r.date_str for r in results if r.date_str},
        key=lambda x: datetime.strptime(x.split("（")[0], "%Y-%m-%d"),
        reverse=True
    )

    print("\n" + "=" * 80)
    print("                             市场日报")
    print("=" * 80)

    us_market = [r for r in results if r.name in ["QQQ", "SPY", "DIA", "纳指", "SPX", "道指"]]
    crypto_market = [r for r in results if r.name in ["BTC", "ETH"]]
    asia_market = [r for r in results if r.name in ["恒科", "恒生", "上证", "日经"]]

    for d in dates:
        print(f"【{d}】\n")

        # 美国市场
        us_results = [res for res in us_market if res.date_str == d]
        if us_results:
            print("【美国市场】")
            print(f"CNN恐贪指数：{stock_fng_value}（{stock_fng_class}）\n")
            for r in us_results:
                unit = "美元" if r.result_type == "stock" else ""
                time_prefix = "美东时间" if r.result_type == "stock" else ""
                status_suffix = {"partial": "（数据部分缺失）", "stale": "（数据陈旧）"}.get(r.status, "")
                line = f"{r.name}{status_suffix}: "
                high_str = f"最高：{r.high}{unit}" + (f"，{time_prefix}{r.high_time}触及" if r.high_time else "") + f"，涨幅{'+' if r.high_pct >= 0 else ''}{r.high_pct:.2f}%"
                low_str = f"；最低：{r.low}{unit}" + (f"，{time_prefix}{r.low_time}触及" if r.low_time else "") + f"，{'涨幅+' if r.low_pct >= 0 else '跌幅-'}{abs(r.low_pct):.2f}%"
                close_str = f"；收盘：{r.close}{unit}，{'涨幅+' if r.close_pct >= 0 else '跌幅-'}{abs(r.close_pct):.2f}%"
                amp_str = f"；振幅：{r.amplitude_pct:.2f}%。"
                print(line + high_str + low_str + close_str + amp_str)
            print("")

        # 加密货币价格部分
        crypto_results = [res for res in crypto_market if res.date_str == d]
        if crypto_results:
            print("【加密货币】")
            for r in crypto_results:
                unit = "美元"
                time_prefix = "北京时间"
                status_suffix = {"partial": "（数据部分缺失）", "stale": "（数据陈旧）"}.get(r.status, "")

                line = f"{r.name}{status_suffix}: "
                high_str = f"最高：{r.high}{unit}" + (f"，{time_prefix}{r.high_time}触及" if r.high_time else "") + f"，涨幅{'+' if r.high_pct >= 0 else ''}{r.high_pct:.2f}%"
                low_str = f"；最低：{r.low}{unit}" + (f"，{time_prefix}{r.low_time}触及" if r.low_time else "") + f"，{'涨幅+' if r.low_pct >= 0 else '跌幅-'}{abs(r.low_pct):.2f}%"
                close_str = f"；收盘：{r.close}{unit}，{'涨幅+' if r.close_pct >= 0 else '跌幅-'}{abs(r.close_pct):.2f}%"
                amp_str = f"；振幅：{r.amplitude_pct:.2f}%。"
                print(line + high_str + low_str + close_str + amp_str)
            print("")

            # 加密货币单独的贪恐指数和波动率部分
            print("【加密货币情绪与波动率】")
            print(f"Alternative恐贪指数：{crypto_fng_value}（{crypto_fng_class}）\n")

            for r in crypto_results:
                # 基于收盘价 C2C 波动率
                prev_hv7 = round(r.hv7 - r.hv7_change, 1)
                prev_hv30 = round(r.hv30 - r.hv30_change, 1)
                prev_hv7_up = round(r.hv7_up - r.hv7_up_change, 1)
                prev_hv30_up = round(r.hv30_up - r.hv30_up_change, 1)
                prev_hv7_down = round(r.hv7_down - r.hv7_down_change, 1)
                prev_hv30_down = round(r.hv30_down - r.hv30_down_change, 1)
                prev_hv7_ratio = round(r.hv7_ratio - r.hv7_ratio_change, 2)
                prev_hv30_ratio = round(r.hv30_ratio - r.hv30_ratio_change, 2)

                change7_str = f"+{r.hv7_change:.1f}" if r.hv7_change > 0 else f"{r.hv7_change:.1f}"
                change30_str = f"+{r.hv30_change:.1f}" if r.hv30_change > 0 else f"{r.hv30_change:.1f}"
                hv7_up_change_str = f"+{r.hv7_up_change:.1f}" if r.hv7_up_change > 0 else f"{r.hv7_up_change:.1f}"
                hv30_up_change_str = f"+{r.hv30_up_change:.1f}" if r.hv30_up_change > 0 else f"{r.hv30_up_change:.1f}"
                hv7_down_change_str = f"+{r.hv7_down_change:.1f}" if r.hv7_down_change > 0 else f"{r.hv7_down_change:.1f}"
                hv30_down_change_str = f"+{r.hv30_down_change:.1f}" if r.hv30_down_change > 0 else f"{r.hv30_down_change:.1f}"
                hv7_ratio_change_str = f"+{r.hv7_ratio_change:.2f}" if r.hv7_ratio_change > 0 else f"{r.hv7_ratio_change:.2f}"
                hv30_ratio_change_str = f"+{r.hv30_ratio_change:.2f}" if r.hv30_ratio_change > 0 else f"{r.hv30_ratio_change:.2f}"

                print(f"  {r.name}（基于收盘价）")
                print(f"    7日总：{r.hv7:.1f}%（前一交易日{prev_hv7:.1f}%，{change7_str}）")
                print(f"      上行：{r.hv7_up:.1f}%（前一交易日{prev_hv7_up:.1f}%，{hv7_up_change_str}） | 下行：{r.hv7_down:.1f}%（前一交易日{prev_hv7_down:.1f}%，{hv7_down_change_str}） | 比值：{r.hv7_ratio:.2f}（前一交易日{prev_hv7_ratio:.2f}，{hv7_ratio_change_str}）")
                print(f"    30日总：{r.hv30:.1f}%（前一交易日{prev_hv30:.1f}%，{change30_str}）")
                print(f"      上行：{r.hv30_up:.1f}%（前一交易日{prev_hv30_up:.1f}%，{hv30_up_change_str}） | 下行：{r.hv30_down:.1f}%（前一交易日{prev_hv30_down:.1f}%，{hv30_down_change_str}） | 比值：{r.hv30_ratio:.2f}（前一交易日{prev_hv30_ratio:.2f}，{hv30_ratio_change_str}）")

                # 基于盘中波动 Parkinson
                prev_parkinson_hv7 = round(r.parkinson_hv7 - r.parkinson_hv7_change, 1)
                prev_parkinson_hv30 = round(r.parkinson_hv30 - r.parkinson_hv30_change, 1)
                prev_parkinson_hv7_up = round(r.parkinson_hv7_up - r.parkinson_hv7_up_change, 1)
                prev_parkinson_hv30_up = round(r.parkinson_hv30_up - r.parkinson_hv30_up_change, 1)
                prev_parkinson_hv7_down = round(r.parkinson_hv7_down - r.parkinson_hv7_down_change, 1)
                prev_parkinson_hv30_down = round(r.parkinson_hv30_down - r.parkinson_hv30_down_change, 1)
                prev_parkinson_hv7_ratio = round(r.parkinson_hv7_ratio - r.parkinson_hv7_ratio_change, 2)
                prev_parkinson_hv30_ratio = round(r.parkinson_hv30_ratio - r.parkinson_hv30_ratio_change, 2)

                parkinson_change7_str = f"+{r.parkinson_hv7_change:.1f}" if r.parkinson_hv7_change > 0 else f"{r.parkinson_hv7_change:.1f}"
                parkinson_change30_str = f"+{r.parkinson_hv30_change:.1f}" if r.parkinson_hv30_change > 0 else f"{r.parkinson_hv30_change:.1f}"
                parkinson_hv7_up_change_str = f"+{r.parkinson_hv7_up_change:.1f}" if r.parkinson_hv7_up_change > 0 else f"{r.parkinson_hv7_up_change:.1f}"
                parkinson_hv30_up_change_str = f"+{r.parkinson_hv30_up_change:.1f}" if r.parkinson_hv30_up_change > 0 else f"{r.parkinson_hv30_up_change:.1f}"
                parkinson_hv7_down_change_str = f"+{r.parkinson_hv7_down_change:.1f}" if r.parkinson_hv7_down_change > 0 else f"{r.parkinson_hv7_down_change:.1f}"
                parkinson_hv30_down_change_str = f"+{r.parkinson_hv30_down_change:.1f}" if r.parkinson_hv30_down_change > 0 else f"{r.parkinson_hv30_down_change:.1f}"
                parkinson_hv7_ratio_change_str = f"+{r.parkinson_hv7_ratio_change:.2f}" if r.parkinson_hv7_ratio_change > 0 else f"{r.parkinson_hv7_ratio_change:.2f}"
                parkinson_hv30_ratio_change_str = f"+{r.parkinson_hv30_ratio_change:.2f}" if r.parkinson_hv30_ratio_change > 0 else f"{r.parkinson_hv30_ratio_change:.2f}"

                print(f"  {r.name}（基于盘中波动）")
                print(f"    7日总：{r.parkinson_hv7:.1f}%（前一交易日{prev_parkinson_hv7:.1f}%，{parkinson_change7_str}）")
                print(f"      上行：{r.parkinson_hv7_up:.1f}%（前一交易日{prev_parkinson_hv7_up:.1f}%，{parkinson_hv7_up_change_str}） | 下行：{r.parkinson_hv7_down:.1f}%（前一交易日{prev_parkinson_hv7_down:.1f}%，{parkinson_hv7_down_change_str}） | 比值：{r.parkinson_hv7_ratio:.2f}（前一交易日{prev_parkinson_hv7_ratio:.2f}，{parkinson_hv7_ratio_change_str}）")
                print(f"    30日总：{r.parkinson_hv30:.1f}%（前一交易日{prev_parkinson_hv30:.1f}%，{parkinson_change30_str}）")
                print(f"      上行：{r.parkinson_hv30_up:.1f}%（前一交易日{prev_parkinson_hv30_up:.1f}%，{parkinson_hv30_up_change_str}） | 下行：{r.parkinson_hv30_down:.1f}%（前一交易日{prev_parkinson_hv30_down:.1f}%，{parkinson_hv30_down_change_str}） | 比值：{r.parkinson_hv30_ratio:.2f}（前一交易日{prev_parkinson_hv30_ratio:.2f}，{parkinson_hv30_ratio_change_str}）")

                # 盘中 vs 收盘 差值
                diff7 = round(r.parkinson_hv7 - r.hv7, 1)
                diff30 = round(r.parkinson_hv30 - r.hv30, 1)
                diff7_str = f"+{diff7:.1f}" if diff7 > 0 else f"{diff7:.1f}"
                diff30_str = f"+{diff30:.1f}" if diff30 > 0 else f"{diff30:.1f}"
                print(f"  比对（盘中 - 收盘） → 7日差值：{diff7_str} | 30日差值：{diff30_str}")
                print("")

        # 亚洲市场
        asia_results = [res for res in asia_market if res.date_str == d]
        if asia_results:
            print("【亚洲市场】")
            for r in asia_results:
                unit = ""
                time_prefix = ""
                status_suffix = {"partial": "（数据部分缺失）", "stale": "（数据陈旧）"}.get(r.status, "")
                line = f"{r.name}{status_suffix}: "
                high_str = f"最高：{r.high}{unit}" + (f"，{time_prefix}{r.high_time}触及" if r.high_time else "") + f"，涨幅{'+' if r.high_pct >= 0 else ''}{r.high_pct:.2f}%"
                low_str = f"；最低：{r.low}{unit}" + (f"，{time_prefix}{r.low_time}触及" if r.low_time else "") + f"，{'涨幅+' if r.low_pct >= 0 else '跌幅-'}{abs(r.low_pct):.2f}%"
                close_str = f"；收盘：{r.close}{unit}，{'涨幅+' if r.close_pct >= 0 else '跌幅-'}{abs(r.close_pct):.2f}%"
                amp_str = f"；振幅：{r.amplitude_pct:.2f}%。"
                print(line + high_str + low_str + close_str + amp_str)
            print("")