NSE STOCK ANALYZER v3 - QUICK START
===================================
1. Install Python 3.9+. In this folder run:   pip install yfinance pandas numpy
2. Start:  python server.py      (Windows: double-click run_windows.bat)
3. Open http://localhost:8765

What's new in v3 (data sourcing only - scoring, weights, dip check and verdict logic unchanged)
Fundamentals are filled metric-by-metric, in this order, until every gap is closed:
  1 Yahoo Finance quote          2 Yahoo Finance annual statements (ROE, ROA, ROCE, D/E,
                                   margins, growth, current ratio computed from reports)
  3 Screener.in                  4 Tickertape
  5 Moneycontrol                 6 NSE India
  7 Trendlyne                    8 StockEdge          9 Google Finance
Sources 3-6 are fetched in parallel; 7-9 only if gaps remain.
Sector P/E benchmark: Moneycontrol -> Tickertape -> NSE.
The Fundamentals table shows a status chip per source (ok / filled N gaps / failed) and
a Source column. A metric shows N/A only after all sources were tried, and lists them.
Results are cached for 15 minutes per stock.

Troubleshooting a stock: open http://localhost:8765/api/sources?symbol=SYMBOL
to see exactly what each source returned.

Offline UI test: python server.py --demo
Personal, low-volume use only; respect each website's terms. Not investment advice.
