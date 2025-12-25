# main.py
from processor import collect_data, post_process
from reporter import render_report

if __name__ == "__main__":
    raw_results, crypto_date, us_date = collect_data()
    ordered_results = post_process(raw_results)
    render_report(ordered_results, crypto_date, us_date)