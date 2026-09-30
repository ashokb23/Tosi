"""
NSE Stock Analyzer - local data engine  (v3: multi-source fundamentals)
-----------------------------------------------------------------------
Serves index.html and a small JSON API:
  /api/suggest?q=...    live suggestions while typing
  /api/resolve?q=...    validate + correct the stock name against the NSE equity list
  /api/analyze?symbol=  technical, fundamental and sector indicators
  /api/sources?symbol=  diagnostic: what each data source returned for a stock

Fundamentals waterfall (a metric is taken from the FIRST source that has it):
  1 Yahoo Finance (quote)          2 Yahoo Finance (annual statements, computed)
  3 Screener.in                    4 Tickertape
  5 Moneycontrol                   6 NSE India (quote API)
  7 Trendlyne                      8 StockEdge
  9 Google Finance
Sources 7-9 are only contacted if gaps remain. A metric is shown as unavailable only
after every source has been tried; the UI lists the sources that were tried.
Sector P/E benchmark: Moneycontrol industry P/E -> Tickertape industry P/E -> NSE sector P/E.

Run:   pip install yfinance pandas numpy
       python server.py            (live data)
       python server.py --demo     (synthetic data, for offline UI testing only)
Then open http://localhost:8765
"""
import csv, io, json, os, re, sys, math, time, difflib, hashlib, html as htmlmod
import urllib.request, urllib.parse, http.cookiejar
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

import numpy as np
import pandas as pd

DEMO = "--demo" in sys.argv
PORT = 8765
HERE = os.path.dirname(os.path.abspath(__file__))
NSE_CSV_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_CACHE = os.path.join(HERE, "nse_equity_list.csv")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
CACHE_TTL = 900          # seconds; repeated lookups of the same stock reuse data
_CACHE = {}

if not DEMO:
    try:
        import yfinance as yf
    except ImportError:
        print("yfinance not installed. Run: pip install yfinance   (or start with --demo)")
        sys.exit(1)


def http_get(url, referer=None, timeout=12, opener=None, accept="text/html,application/json,*/*"):
    h = {"User-Agent": UA, "Accept": accept, "Accept-Language": "en-IN,en;q=0.9"}
    if referer:
        h["Referer"] = referer
    req = urllib.request.Request(url, headers=h)
    r = (opener.open(req, timeout=timeout) if opener else urllib.request.urlopen(req, timeout=timeout))
    return r.read().decode("utf-8", "ignore")


# ----------------------------------------------------------------------------------
# 1. NSE symbol master
# ----------------------------------------------------------------------------------
POPULAR = [
    ("RELIANCE", "Reliance Industries Limited"), ("TCS", "Tata Consultancy Services Limited"),
    ("HDFCBANK", "HDFC Bank Limited"), ("ICICIBANK", "ICICI Bank Limited"), ("INFY", "Infosys Limited"),
    ("SBIN", "State Bank of India"), ("BHARTIARTL", "Bharti Airtel Limited"), ("ITC", "ITC Limited"),
    ("HINDUNILVR", "Hindustan Unilever Limited"), ("LT", "Larsen & Toubro Limited"),
    ("KOTAKBANK", "Kotak Mahindra Bank Limited"), ("AXISBANK", "Axis Bank Limited"),
    ("BAJFINANCE", "Bajaj Finance Limited"), ("BAJAJFINSV", "Bajaj Finserv Limited"),
    ("BAJAJ-AUTO", "Bajaj Auto Limited"), ("ASIANPAINT", "Asian Paints Limited"),
    ("MARUTI", "Maruti Suzuki India Limited"), ("M&M", "Mahindra & Mahindra Limited"),
    ("SUNPHARMA", "Sun Pharmaceutical Industries Limited"), ("HCLTECH", "HCL Technologies Limited"),
    ("WIPRO", "Wipro Limited"), ("TECHM", "Tech Mahindra Limited"), ("LTIM", "LTIMindtree Limited"),
    ("ULTRACEMCO", "UltraTech Cement Limited"), ("TITAN", "Titan Company Limited"),
    ("NESTLEIND", "Nestle India Limited"), ("POWERGRID", "Power Grid Corporation of India Limited"),
    ("NTPC", "NTPC Limited"), ("ONGC", "Oil & Natural Gas Corporation Limited"),
    ("COALINDIA", "Coal India Limited"), ("TATASTEEL", "Tata Steel Limited"), ("JSWSTEEL", "JSW Steel Limited"),
    ("HINDALCO", "Hindalco Industries Limited"), ("ADANIENT", "Adani Enterprises Limited"),
    ("ADANIPORTS", "Adani Ports and Special Economic Zone Limited"), ("ADANIGREEN", "Adani Green Energy Limited"),
    ("ADANIPOWER", "Adani Power Limited"), ("GRASIM", "Grasim Industries Limited"),
    ("CIPLA", "Cipla Limited"), ("DRREDDY", "Dr. Reddy's Laboratories Limited"),
    ("DIVISLAB", "Divi's Laboratories Limited"), ("APOLLOHOSP", "Apollo Hospitals Enterprise Limited"),
    ("EICHERMOT", "Eicher Motors Limited"), ("HEROMOTOCO", "Hero MotoCorp Limited"),
    ("TATACONSUM", "Tata Consumer Products Limited"), ("BRITANNIA", "Britannia Industries Limited"),
    ("HDFCLIFE", "HDFC Life Insurance Company Limited"), ("SBILIFE", "SBI Life Insurance Company Limited"),
    ("HDFCAMC", "HDFC Asset Management Company Limited"), ("INDUSINDBK", "IndusInd Bank Limited"),
    ("BPCL", "Bharat Petroleum Corporation Limited"), ("IOC", "Indian Oil Corporation Limited"),
    ("SHRIRAMFIN", "Shriram Finance Limited"), ("TRENT", "Trent Limited"), ("ETERNAL", "Eternal Limited"),
    ("BEL", "Bharat Electronics Limited"), ("HAL", "Hindustan Aeronautics Limited"),
    ("DMART", "Avenue Supermarts Limited"), ("PIDILITIND", "Pidilite Industries Limited"),
    ("SIEMENS", "Siemens Limited"), ("DLF", "DLF Limited"), ("GODREJCP", "Godrej Consumer Products Limited"),
    ("GODREJPROP", "Godrej Properties Limited"), ("DABUR", "Dabur India Limited"), ("MARICO", "Marico Limited"),
    ("VEDL", "Vedanta Limited"), ("TATAPOWER", "Tata Power Company Limited"), ("TATAELXSI", "Tata Elxsi Limited"),
    ("TATACOMM", "Tata Communications Limited"), ("TATACHEM", "Tata Chemicals Limited"),
    ("TMPV", "Tata Motors Passenger Vehicles Limited"), ("MAXHEALTH", "Max Healthcare Institute Limited"),
    ("BANKBARODA", "Bank of Baroda"), ("PNB", "Punjab National Bank"), ("CANBK", "Canara Bank"),
    ("UNIONBANK", "Union Bank of India"), ("IDFCFIRSTB", "IDFC First Bank Limited"),
    ("FEDERALBNK", "The Federal Bank Limited"), ("YESBANK", "Yes Bank Limited"),
    ("AUBANK", "AU Small Finance Bank Limited"), ("BANDHANBNK", "Bandhan Bank Limited"),
    ("CHOLAFIN", "Cholamandalam Investment and Finance Company Limited"),
    ("MUTHOOTFIN", "Muthoot Finance Limited"), ("ICICIPRULI", "ICICI Prudential Life Insurance Company Limited"),
    ("ICICIGI", "ICICI Lombard General Insurance Company Limited"), ("LICI", "Life Insurance Corporation of India"),
    ("IRCTC", "Indian Railway Catering And Tourism Corporation Limited"), ("IRFC", "Indian Railway Finance Corporation Limited"),
    ("PFC", "Power Finance Corporation Limited"), ("RECLTD", "REC Limited"), ("GAIL", "GAIL (India) Limited"),
    ("HAVELLS", "Havells India Limited"), ("POLYCAB", "Polycab India Limited"), ("ABB", "ABB India Limited"),
    ("CUMMINSIND", "Cummins India Limited"), ("BHEL", "Bharat Heavy Electricals Limited"),
    ("AMBUJACEM", "Ambuja Cements Limited"), ("SHREECEM", "Shree Cement Limited"),
    ("LUPIN", "Lupin Limited"), ("AUROPHARMA", "Aurobindo Pharma Limited"), ("TORNTPHARM", "Torrent Pharmaceuticals Limited"),
    ("ZYDUSLIFE", "Zydus Lifesciences Limited"), ("MANKIND", "Mankind Pharma Limited"),
    ("PERSISTENT", "Persistent Systems Limited"), ("COFORGE", "Coforge Limited"), ("MPHASIS", "Mphasis Limited"),
    ("OFSS", "Oracle Financial Services Software Limited"), ("KPITTECH", "KPIT Technologies Limited"),
    ("NAUKRI", "Info Edge (India) Limited"), ("PAYTM", "One 97 Communications Limited"),
    ("NYKAA", "FSN E-Commerce Ventures Limited"), ("POLICYBZR", "PB Fintech Limited"),
    ("JIOFIN", "Jio Financial Services Limited"), ("INDIGO", "InterGlobe Aviation Limited"),
    ("TVSMOTOR", "TVS Motor Company Limited"), ("ASHOKLEY", "Ashok Leyland Limited"),
    ("BOSCHLTD", "Bosch Limited"), ("MOTHERSON", "Samvardhana Motherson International Limited"),
    ("MRF", "MRF Limited"), ("PAGEIND", "Page Industries Limited"), ("COLPAL", "Colgate Palmolive (India) Limited"),
    ("UBL", "United Breweries Limited"), ("UNITDSPR", "United Spirits Limited"), ("JUBLFOOD", "Jubilant Foodworks Limited"),
    ("SRF", "SRF Limited"), ("PIIND", "PI Industries Limited"), ("UPL", "UPL Limited"),
    ("DIXON", "Dixon Technologies (India) Limited"), ("VOLTAS", "Voltas Limited"),
    ("IDEA", "Vodafone Idea Limited"), ("INDUSTOWER", "Indus Towers Limited"),
    ("SAIL", "Steel Authority of India Limited"), ("NMDC", "NMDC Limited"), ("HINDZINC", "Hindustan Zinc Limited"),
    ("JINDALSTEL", "Jindal Steel & Power Limited"), ("MAZDOCK", "Mazagon Dock Shipbuilders Limited"),
    ("SUZLON", "Suzlon Energy Limited"), ("RVNL", "Rail Vikas Nigam Limited"),
]
POPULAR_SET = {s for s, _ in POPULAR}

ALIASES = {
    "sbi": "SBIN", "state bank": "SBIN", "l&t": "LT", "l and t": "LT", "larsen": "LT", "lnt": "LT",
    "hul": "HINDUNILVR", "hindustan unilever": "HINDUNILVR", "airtel": "BHARTIARTL", "bharti": "BHARTIARTL",
    "m&m": "M&M", "mahindra": "M&M", "mahindra and mahindra": "M&M", "maruti": "MARUTI", "maruti suzuki": "MARUTI",
    "infosys": "INFY", "ril": "RELIANCE", "reliance": "RELIANCE", "kotak": "KOTAKBANK", "axis": "AXISBANK",
    "sun pharma": "SUNPHARMA", "hcl": "HCLTECH", "tech mahindra": "TECHM",
    "ltimindtree": "LTIM", "ltm": "LTIM", "lti mindtree": "LTIM", "ultratech": "ULTRACEMCO", "nestle": "NESTLEIND",
    "power grid": "POWERGRID", "coal india": "COALINDIA", "tata steel": "TATASTEEL", "jsw steel": "JSWSTEEL",
    "dr reddy": "DRREDDY", "dr reddys": "DRREDDY", "divis": "DIVISLAB", "apollo": "APOLLOHOSP",
    "eicher": "EICHERMOT", "royal enfield": "EICHERMOT", "hero": "HEROMOTOCO", "indusind": "INDUSINDBK",
    "zomato": "ETERNAL", "dmart": "DMART", "avenue supermarts": "DMART", "pidilite": "PIDILITIND",
    "bob": "BANKBARODA", "bank of baroda": "BANKBARODA", "canara": "CANBK", "lic": "LICI",
    "paytm": "PAYTM", "nykaa": "NYKAA", "policybazaar": "POLICYBZR", "indigo": "INDIGO", "tvs": "TVSMOTOR",
    "vodafone": "IDEA", "vi": "IDEA", "jio financial": "JIOFIN", "rec": "RECLTD", "info edge": "NAUKRI",
    "naukri": "NAUKRI", "hdfc bank": "HDFCBANK", "icici bank": "ICICIBANK", "tata motors": "TMPV",
    "asian paints": "ASIANPAINT", "bajaj finance": "BAJFINANCE", "bajaj auto": "BAJAJ-AUTO",
}

MASTER = []
MASTER_SOURCE = "embedded"
STOP = {"limited", "ltd", "the", "co", "company", "corporation", "corp", "inc", "of"}


def norm(s):
    s = s.lower().replace("&", " and ").replace("'", "")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return " ".join(t for t in s.split() if t not in STOP)


def load_master():
    global MASTER, MASTER_SOURCE
    rows, text = [], None
    if not DEMO:
        try:
            if os.path.exists(NSE_CACHE) and time.time() - os.path.getmtime(NSE_CACHE) < 86400 * 3:
                text = open(NSE_CACHE, encoding="utf-8").read()
            else:
                text = http_get(NSE_CSV_URL, referer="https://www.nseindia.com/", timeout=15)
                open(NSE_CACHE, "w", encoding="utf-8").write(text)
        except Exception as e:
            print("  ! Could not download NSE equity list (%s). Using embedded popular list." % e)
            if os.path.exists(NSE_CACHE):
                text = open(NSE_CACHE, encoding="utf-8").read()
    if text:
        for r in csv.DictReader(io.StringIO(text)):
            r = {k.strip(): (v or "").strip() for k, v in r.items() if k}
            sym, name, series = r.get("SYMBOL"), r.get("NAME OF COMPANY"), r.get("SERIES", "EQ")
            if sym and name and series in ("EQ", "BE", "BZ", "SM", "ST"):
                rows.append((sym, name))
        if rows:
            MASTER_SOURCE = "NSE official equity list (%d symbols)" % len(rows)
    if not rows:
        rows = POPULAR[:]
    seen, MASTER = set(), []
    for sym, name in rows + POPULAR:
        if sym in seen:
            continue
        seen.add(sym)
        n = norm(name)
        MASTER.append({"symbol": sym, "name": name, "n": n, "toks": n.split()})
    print("  Symbol master: %s" % MASTER_SOURCE)


def score(q, rec):
    qsym = re.sub(r"\s+", "", q.strip().upper())
    nq = norm(q)
    sym = rec["symbol"]
    if qsym == sym:
        return 100
    if nq and nq == rec["n"]:
        return 98
    s = 0.0
    qt = nq.split()
    if nq and rec["n"].startswith(nq) and len(nq) >= 3:
        s = max(s, 90)
    if len(qsym) >= 3 and sym.startswith(qsym):
        s = max(s, 86)
    if qt and len(nq) >= 3 and all(any(nt.startswith(t) for nt in rec["toks"]) for t in qt):
        s = max(s, 84)
    r_name = difflib.SequenceMatcher(None, nq, rec["n"]).ratio()
    r_sym = difflib.SequenceMatcher(None, qsym, sym).ratio()
    r_tok = 0
    if qt and rec["toks"]:
        per = [max(difflib.SequenceMatcher(None, t, nt).ratio() for nt in rec["toks"]) for t in qt]
        r_tok = sum(per) / len(per) * (0.92 + 0.08 * min(1, len(qt) / max(1, len(rec["toks"]))))
    s = max(s, 90 * max(r_name, r_sym, r_tok))
    if sym in POPULAR_SET:
        s += 5
    return min(s, 99.5)


def search(q, limit=8):
    q = (q or "").strip()
    if not q:
        return []
    alias = ALIASES.get(norm(q)) or ALIASES.get(q.lower())
    out = {}
    for rec in MASTER:
        sc = score(q, rec)
        if alias and rec["symbol"] == alias:
            sc = max(sc, 97)
        if sc >= 55:
            out[rec["symbol"]] = (sc, rec)
    ranked = sorted(out.values(), key=lambda x: -x[0])[:limit]
    return [{"symbol": r["symbol"], "name": r["name"], "score": round(sc, 1)} for sc, r in ranked]


def resolve(q):
    c = search(q, 8)
    if not c:
        if not DEMO:
            sym = re.sub(r"\s+", "", q.upper())
            try:
                if len(yf.Ticker(sym + ".NS").history(period="5d")):
                    return {"status": "exact", "match": {"symbol": sym, "name": sym, "score": 100}, "candidates": []}
            except Exception:
                pass
        return {"status": "notfound", "candidates": []}
    top = c[0]
    second = c[1]["score"] if len(c) > 1 else 0
    alts = [x for x in c[1:5] if x["score"] >= 72]
    if top["score"] >= 99.9 or (top["score"] >= 95 and second < top["score"] - 3):
        return {"status": "exact" if top["score"] >= 99.9 else "corrected", "match": top, "candidates": alts}
    if top["score"] >= 78 and second <= top["score"] - 10:
        return {"status": "corrected", "match": top, "candidates": alts}
    return {"status": "ambiguous", "candidates": c[:8]}


# ----------------------------------------------------------------------------------
# 2. Indicators (unchanged logic)
# ----------------------------------------------------------------------------------
SECTOR_INDEX = {
    "Technology": ("^CNXIT", "NIFTY IT"),
    "Healthcare": ("^CNXPHARMA", "NIFTY PHARMA"),
    "Consumer Defensive": ("^CNXFMCG", "NIFTY FMCG"),
    "Basic Materials": ("^CNXMETAL", "NIFTY METAL"),
    "Energy": ("^CNXENERGY", "NIFTY ENERGY"),
    "Utilities": ("^CNXENERGY", "NIFTY ENERGY"),
    "Real Estate": ("^CNXREALTY", "NIFTY REALTY"),
    "Industrials": ("^CNXINFRA", "NIFTY INFRA"),
    "Communication Services": ("^CNXMEDIA", "NIFTY MEDIA"),
    "Consumer Cyclical": ("^CNXAUTO", "NIFTY AUTO"),
    "Financial Services": ("NIFTY_FIN_SERVICE.NS", "NIFTY FINANCIAL SERVICES"),
}
MC_SECTOR_MAP = [("bank", "Financial Services"), ("financ", "Financial Services"), ("insur", "Financial Services"),
                 ("software", "Technology"), ("it", "Technology"), ("pharma", "Healthcare"), ("health", "Healthcare"),
                 ("fmcg", "Consumer Defensive"), ("food", "Consumer Defensive"), ("metal", "Basic Materials"),
                 ("steel", "Basic Materials"), ("cement", "Basic Materials"), ("chemical", "Basic Materials"),
                 ("oil", "Energy"), ("gas", "Energy"), ("power", "Utilities"), ("realty", "Real Estate"),
                 ("real estate", "Real Estate"), ("auto", "Consumer Cyclical"), ("telecom", "Communication Services"),
                 ("media", "Communication Services"), ("capital goods", "Industrials"), ("infra", "Industrials"),
                 ("construct", "Industrials"), ("retail", "Consumer Cyclical")]


def pick_sector_index(sector, industry):
    ind = (industry or "").lower()
    if sector == "Financial Services" and "bank" in ind:
        return ("^NSEBANK", "NIFTY BANK")
    if sector == "Consumer Cyclical" and "auto" not in ind:
        return ("^CNXCONSUM", "NIFTY INDIA CONSUMPTION")
    if sector == "Communication Services" and "telecom" in ind:
        return ("^CNXINFRA", "NIFTY INFRA")
    return SECTOR_INDEX.get(sector, ("^NSEI", "NIFTY 50"))


def f(x, nd=2):
    try:
        if x is None:
            return None
        x = float(str(x).replace(",", "").replace("%", "").replace("₹", "").strip()) if isinstance(x, str) else float(x)
        if math.isnan(x) or math.isinf(x):
            return None
        return round(x, nd)
    except Exception:
        return None


def ret(series, days):
    s = series.dropna()
    if len(s) <= days:
        return None
    return (s.iloc[-1] / s.iloc[-1 - days] - 1) * 100


def technicals(df, nifty):
    c, h, l, v = df["Close"], df["High"], df["Low"], df["Volume"]
    sma20, sma50, sma200 = c.rolling(20).mean(), c.rolling(50).mean(), c.rolling(200).mean()
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi = 100 - 100 / (1 + up / dn.replace(0, np.nan))
    ema12, ema26 = c.ewm(span=12, adjust=False).mean(), c.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    sig = macd.ewm(span=9, adjust=False).mean()
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    upm, dnm = h.diff(), -l.diff()
    pdm = np.where((upm > dnm) & (upm > 0), upm, 0.0)
    ndm = np.where((dnm > upm) & (dnm > 0), dnm, 0.0)
    atr = tr.ewm(alpha=1 / 14, adjust=False).mean()
    pdi = 100 * pd.Series(pdm, index=c.index).ewm(alpha=1 / 14, adjust=False).mean() / atr
    ndi = 100 * pd.Series(ndm, index=c.index).ewm(alpha=1 / 14, adjust=False).mean() / atr
    dx = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, np.nan)
    adx = dx.ewm(alpha=1 / 14, adjust=False).mean()
    sd20 = c.rolling(20).std()
    pctb = (c - (sma20 - 2 * sd20)) / (4 * sd20)
    ll, hh = l.rolling(14).min(), h.rolling(14).max()
    stoch = 100 * (c - ll) / (hh - ll).replace(0, np.nan)
    hi52, lo52 = h.iloc[-252:].max(), l.iloc[-252:].min()
    vol_ratio = v.iloc[-20:].mean() / max(1, v.iloc[-50:].mean())
    rets = c.pct_change().dropna()
    vol_ann = rets.iloc[-252:].std() * math.sqrt(252) * 100
    beta = None
    if nifty is not None and len(nifty):
        j = pd.concat([rets, nifty["Close"].pct_change()], axis=1, join="inner").dropna().iloc[-252:]
        if len(j) > 60:
            beta = j.iloc[:, 0].cov(j.iloc[:, 1]) / j.iloc[:, 1].var()
    px = c.iloc[-1]
    return {
        "price": f(px), "sma50": f(sma50.iloc[-1]), "sma200": f(sma200.iloc[-1]),
        "px_vs_sma20": f((px / sma20.iloc[-1] - 1) * 100),
        "px_vs_sma50": f((px / sma50.iloc[-1] - 1) * 100), "px_vs_sma200": f((px / sma200.iloc[-1] - 1) * 100),
        "sma50_vs_sma200": f((sma50.iloc[-1] / sma200.iloc[-1] - 1) * 100),
        "rsi14": f(rsi.iloc[-1], 1), "rsi14_5d_ago": f(rsi.iloc[-6], 1),
        "macd": f(macd.iloc[-1]), "macd_signal": f(sig.iloc[-1]), "macd_hist": f(macd.iloc[-1] - sig.iloc[-1]),
        "adx14": f(adx.iloc[-1], 1), "plus_di": f(pdi.iloc[-1], 1), "minus_di": f(ndi.iloc[-1], 1),
        "bb_pctb": f(pctb.iloc[-1], 2), "stoch_k": f(stoch.iloc[-1], 1),
        "hi52": f(hi52), "lo52": f(lo52), "from_52w_high": f((px / hi52 - 1) * 100),
        "from_52w_low": f((px / lo52 - 1) * 100),
        "vol_ratio": f(vol_ratio), "ret_1m": f(ret(c, 21)), "ret_3m": f(ret(c, 63)), "ret_6m": f(ret(c, 126)),
        "ret_1y": f(ret(c, 252)), "volatility": f(vol_ann, 1), "beta": f(beta),
    }


FUND_KEYS = ["pe", "forward_pe", "pb", "peg", "ev_ebitda", "roe", "roce", "roa", "debt_equity", "current_ratio",
             "net_margin", "op_margin", "rev_growth", "eps_growth", "div_yield", "insider_holding", "eps", "book_value"]
SCORED_KEYS = ["pe", "peg", "pb", "ev_ebitda", "roe", "roce", "roa", "debt_equity", "current_ratio",
               "net_margin", "op_margin", "rev_growth", "eps_growth", "div_yield", "insider_holding"]

# Plausibility limits: values outside these are treated as parsing noise and ignored
SANE = {"pe": (-5000, 5000), "forward_pe": (-5000, 5000), "pb": (0, 500), "peg": (-100, 100), "ev_ebitda": (-500, 1000),
        "roe": (-500, 500), "roce": (-500, 500), "roa": (-200, 200), "debt_equity": (0, 100), "current_ratio": (0, 100),
        "net_margin": (-1000, 100), "op_margin": (-1000, 100), "rev_growth": (-100, 10000), "eps_growth": (-10000, 10000),
        "div_yield": (0, 50), "insider_holding": (0, 100), "eps": (-1e6, 1e6), "book_value": (-1e6, 1e7)}


def sane(k, v):
    v = f(v)
    if v is None:
        return None
    lo, hi = SANE.get(k, (-1e12, 1e12))
    return v if lo <= v <= hi else None


# ---------------- Source 1: Yahoo Finance quote ----------------
def yahoo_fundamentals(info, price):
    g = info.get
    de = g("debtToEquity")
    dy = None
    if g("dividendRate") and price:
        dy = g("dividendRate") / price * 100
    elif g("trailingAnnualDividendYield") is not None:
        dy = g("trailingAnnualDividendYield") * 100
    pct = lambda k: f(g(k) * 100) if g(k) is not None else None
    return {
        "pe": f(g("trailingPE")), "forward_pe": f(g("forwardPE")), "pb": f(g("priceToBook")),
        "peg": f(g("trailingPegRatio") or g("pegRatio")), "ev_ebitda": f(g("enterpriseToEbitda")),
        "roe": pct("returnOnEquity"), "roce": None, "roa": pct("returnOnAssets"),
        "debt_equity": f(de / 100) if de is not None else None, "current_ratio": f(g("currentRatio")),
        "net_margin": pct("profitMargins"), "op_margin": pct("operatingMargins"),
        "rev_growth": pct("revenueGrowth"), "eps_growth": pct("earningsGrowth"),
        "div_yield": f(dy), "insider_holding": pct("heldPercentInsiders"),
        "eps": f(g("trailingEps")), "book_value": f(g("bookValue")),
    }


# ---------------- Source 2: Yahoo Finance annual statements (computed ratios) ----------------
def _series(df, *names):
    if df is None or not hasattr(df, "index") or df.empty:
        return []
    for n in names:
        if n in df.index:
            s = df.loc[n]
            s = s[~s.isna()]
            if len(s):
                return [float(x) for x in s.sort_index(ascending=False).values]
    return []


def yahoo_statements(t, price):
    out = {}
    try:
        inc = t.income_stmt
    except Exception:
        inc = None
    try:
        bs = t.balance_sheet
    except Exception:
        bs = None
    rev = _series(inc, "Total Revenue", "Operating Revenue")
    ni = _series(inc, "Net Income Common Stockholders", "Net Income", "Net Income From Continuing Operation Net Minority Interest")
    opi = _series(inc, "Operating Income", "EBIT")
    ebit = _series(inc, "EBIT", "Operating Income")
    eps = _series(inc, "Diluted EPS", "Basic EPS")
    eq = _series(bs, "Stockholders Equity", "Common Stock Equity", "Total Equity Gross Minority Interest")
    ta = _series(bs, "Total Assets")
    debt = _series(bs, "Total Debt")
    ca = _series(bs, "Current Assets")
    cl = _series(bs, "Current Liabilities")
    shares = _series(bs, "Ordinary Shares Number", "Share Issued")
    if ni and eq and eq[0] > 0:
        out["roe"] = ni[0] / eq[0] * 100
    if ni and ta and ta[0] > 0:
        out["roa"] = ni[0] / ta[0] * 100
    if debt and eq and eq[0] > 0:
        out["debt_equity"] = debt[0] / eq[0]
    if ca and cl and cl[0] > 0:
        out["current_ratio"] = ca[0] / cl[0]
    if ni and rev and rev[0] > 0:
        out["net_margin"] = ni[0] / rev[0] * 100
    if opi and rev and rev[0] > 0:
        out["op_margin"] = opi[0] / rev[0] * 100
    if len(rev) > 1 and rev[1] > 0:
        out["rev_growth"] = (rev[0] / rev[1] - 1) * 100
    if len(ni) > 1 and ni[1] > 0:
        out["eps_growth"] = (ni[0] / ni[1] - 1) * 100
    if ebit and ta and cl and (ta[0] - cl[0]) > 0:
        out["roce"] = ebit[0] / (ta[0] - cl[0]) * 100
    if eps:
        out["eps"] = eps[0]
        if price and eps[0] > 0:
            out["pe"] = price / eps[0]
    if eq and shares and shares[0] > 0:
        out["book_value"] = eq[0] / shares[0]
        if price:
            out["pb"] = price / out["book_value"]
    return {k: sane(k, v) for k, v in out.items() if sane(k, v) is not None}


# ---------------- Source 3: Screener.in ----------------
def _clean(s):
    s = re.sub(r"<[^>]+>", " ", s)
    s = htmlmod.unescape(s).replace("\xa0", " ").replace("+", " ")
    return re.sub(r"\s+", " ", s).strip()


def page_text(html_):
    html_ = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html_)
    return _clean(html_)


def _rows(section_html):
    out = {}
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", section_html, re.S):
        cells = [_clean(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
        if len(cells) >= 2 and cells[0]:
            out[cells[0].strip()] = [f(x) for x in cells[1:]]
    return out


def _section(page, sid):
    m = re.search(r'<section[^>]+id="%s".*?</section>' % sid, page, re.S)
    return m.group(0) if m else ""


def _last(vals):
    vals = [v for v in (vals or []) if v is not None]
    return vals[-1] if vals else None


def _parse_screener(page):
    out = {}
    top = re.search(r'id="top-ratios".*?</ul>', page, re.S)
    ratios = {}
    if top:
        for li in re.findall(r"<li[^>]*>(.*?)</li>", top.group(0), re.S):
            nm = re.search(r'class="name"[^>]*>(.*?)</span>', li, re.S)
            num = re.search(r'class="number"[^>]*>(.*?)</span>', li, re.S)
            if nm and num:
                ratios[_clean(nm.group(1))] = f(_clean(num.group(1)))
    out["pe"] = ratios.get("Stock P/E")
    out["book_value"] = ratios.get("Book Value")
    out["div_yield"] = ratios.get("Dividend Yield")
    out["roce"] = ratios.get("ROCE")
    out["roe"] = ratios.get("ROE")
    price = ratios.get("Current Price")
    if price and out.get("book_value"):
        out["pb"] = price / out["book_value"]
    for tbl in re.findall(r'<table class="ranges-table">(.*?)</table>', page, re.S):
        th = re.search(r"<th[^>]*>(.*?)</th>", tbl, re.S)
        title = _clean(th.group(1)) if th else ""
        vals = dict((_clean(a).rstrip(":"), f(_clean(b))) for a, b in
                    re.findall(r"<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>", tbl, re.S))
        if "Sales Growth" in title:
            out["rev_growth"] = vals.get("TTM", vals.get("1 Year"))
        elif "Profit Growth" in title:
            out["eps_growth"] = vals.get("TTM", vals.get("1 Year"))
    pl = _rows(_section(page, "profit-loss"))
    sales = _last(pl.get("Sales") or pl.get("Revenue"))
    np_ = _last(pl.get("Net Profit"))
    out["op_margin"] = _last(pl.get("OPM %"))
    if sales and np_ is not None:
        out["net_margin"] = np_ / sales * 100
    out["eps"] = _last(pl.get("EPS in Rs"))
    bs = _rows(_section(page, "balance-sheet"))
    borrow, eq, res = _last(bs.get("Borrowings")), _last(bs.get("Equity Capital")), _last(bs.get("Reserves"))
    if None not in (borrow, eq, res) and (eq + res) > 0:
        out["debt_equity"] = borrow / (eq + res)
    sh = _rows(_section(page, "shareholding"))
    out["insider_holding"] = _last(sh.get("Promoters"))
    rt = _rows(_section(page, "ratios"))
    if out.get("roce") is None:
        out["roce"] = _last(rt.get("ROCE %"))
    return {k: sane(k, v) for k, v in out.items() if sane(k, v) is not None}


def screener_fundamentals(symbol):
    sym = urllib.parse.quote(symbol)
    best = {}
    for url in ("https://www.screener.in/company/%s/consolidated/" % sym, "https://www.screener.in/company/%s/" % sym):
        try:
            p = http_get(url, referer="https://www.screener.in/")
        except Exception:
            continue
        if 'id="top-ratios"' not in p:
            continue
        got = _parse_screener(p)
        for k, v in got.items():          # consolidated first; standalone fills gaps
            best.setdefault(k, v)
        if len(best) >= 10:
            break
    return best


# ---------------- Source 4: Tickertape (public JSON used by its web pages) ----------------
TT_H = "https://www.tickertape.in/"


def _pick(d, *keys):
    """case-insensitive key lookup on a flat dict"""
    low = {str(k).lower(): v for k, v in (d or {}).items()}
    for k in keys:
        v = low.get(k.lower())
        if v is not None and f(v) is not None:
            return f(v)
    return None


def tickertape_data(symbol):
    raw = http_get("https://api.tickertape.in/search?text=%s&types=stock" % urllib.parse.quote(symbol), referer=TT_H)
    stocks = (json.loads(raw).get("data") or {}).get("stocks") or []
    sid = next((s.get("sid") for s in stocks if str(s.get("ticker", "")).upper() == symbol.upper()), None)
    if not sid:
        return {}
    data = json.loads(http_get("https://api.tickertape.in/stocks/info/%s" % sid, referer=TT_H)).get("data") or {}
    r = data.get("ratios") or {}
    out = {
        "pe": _pick(r, "pe", "ttmPe", "apef"), "pb": _pick(r, "pb", "pbr", "ttmPb"),
        "div_yield": _pick(r, "divYield", "dy", "divyield"), "roe": _pick(r, "roe", "aroe"),
        "eps": _pick(r, "eps", "ttmEps"), "book_value": _pick(r, "bookValue", "bv"),
        "peg": _pick(r, "peg"), "debt_equity": _pick(r, "dbtEqt", "de", "debtToEquity"),
        "rev_growth": _pick(r, "revenueGrowth", "rvng"), "net_margin": _pick(r, "pftMrg", "netProfitMargin"),
        "roce": _pick(r, "roce"),
    }
    out = {k: sane(k, v) for k, v in out.items() if sane(k, v) is not None}
    extra = {"sector_pe": _pick(r, "indpe", "indPe", "sectorPe", "industryPe"),
             "market_cap_cr": _pick(r, "mrktCapf", "marketCap")}
    try:
        hold = json.loads(http_get("https://api.tickertape.in/stocks/holdings/%s" % sid, referer=TT_H)).get("data") or []
        if hold:
            latest = sorted(hold, key=lambda h: h.get("date", ""))[-1].get("data") or {}
            p = sane("insider_holding", latest.get("pmPctT"))
            if p is not None:
                out["insider_holding"] = p
    except Exception:
        pass
    return {"fund": out, "extra": {k: v for k, v in extra.items() if v is not None}}


# ---------------- Source 5: Moneycontrol (price feed incl. industry P/E) ----------------
def moneycontrol_data(symbol):
    q = urllib.parse.quote(symbol)
    raw = http_get("https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php?classic=true&query=%s"
                   "&type=1&format=json" % q, referer="https://www.moneycontrol.com/")
    items = json.loads(raw[raw.find("["): raw.rfind("]") + 1] or "[]")
    sc_id = None
    for it in items:
        label = htmlmod.unescape(str(it.get("pdt_dis_nm", ""))).upper()
        if re.search(r"[ ,>]%s[ ,<]" % re.escape(symbol.upper()), label + " "):
            sc_id = it.get("sc_id")
            break
    if not sc_id and items:
        sc_id = items[0].get("sc_id")
    if not sc_id:
        return {}
    d = json.loads(http_get("https://priceapi.moneycontrol.com/pricefeed/nse/equitycash/%s" % sc_id,
                            referer="https://www.moneycontrol.com/")).get("data") or {}
    if d.get("NSEID") and d["NSEID"].upper() != symbol.upper():
        return {}
    pick = lambda *ks: next((f(d.get(k)) for k in ks if f(d.get(k)) is not None), None)
    fund = {"pe": pick("PECONS", "PE"), "pb": pick("PBCONS", "PB"), "div_yield": pick("DYCONS", "DY"),
            "book_value": pick("BV")}
    fund = {k: sane(k, v) for k, v in fund.items() if sane(k, v) is not None}
    extra = {"sector_pe": pick("IND_PE"), "mc_sector": d.get("main_sector") or d.get("SC_SUBSEC"),
             "market_cap_cr": pick("MKTCAP")}
    return {"fund": fund, "extra": {k: v for k, v in extra.items() if v is not None}}


# ---------------- Source 6: NSE India quote API (needs a cookie from the home page) ----------------
def nse_data(symbol):
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    http_get("https://www.nseindia.com/get-quotes/equity?symbol=%s" % urllib.parse.quote(symbol), opener=op)
    d = json.loads(http_get("https://www.nseindia.com/api/quote-equity?symbol=%s" % urllib.parse.quote(symbol),
                            referer="https://www.nseindia.com/get-quotes/equity?symbol=%s" % symbol,
                            opener=op, accept="application/json"))
    md = d.get("metadata") or {}
    fund = {"pe": sane("pe", md.get("pdSymbolPe"))}
    info = d.get("industryInfo") or {}
    extra = {"sector_pe": f(md.get("pdSectorPe")), "nse_industry": info.get("basicIndustry") or info.get("industry"),
             "nse_sector": info.get("sector")}
    return {"fund": {k: v for k, v in fund.items() if v is not None},
            "extra": {k: v for k, v in extra.items() if v is not None}}


# ---------------- Sources 7-9: page text scanners (Trendlyne, StockEdge, Google Finance) ----------------
N = r"(-?[\d,]+(?:\.\d+)?)"
TEXT_PATTERNS = {
    "pe": [r"(?:PE TTM|P/E TTM|P/E \(TTM\)|P/E ratio|Stock P/E|PE Ratio|Price to Earnings(?: Ratio)?)\s*:?\s*" + N],
    "pb": [r"(?:PBV(?: Adjusted)?|Price to Book(?: Value)?|P/B(?: ratio)?|PB Ratio)\s*:?\s*" + N],
    "peg": [r"PEG(?: TTM| Ratio)?\s*:?\s*" + N],
    "roe": [r"(?:ROE(?: Annual)?(?: %)?|Return on Equity(?: \(ROE\))?)\s*:?\s*" + N + r"\s*%?"],
    "roce": [r"(?:ROCE(?: Annual)?(?: %)?|Return on Capital Employed)\s*:?\s*" + N],
    "roa": [r"(?:RoA|ROA)(?: Annual)?(?: %)?\s*:?\s*" + N],
    "div_yield": [r"Dividend yield(?: %)?\s*:?\s*" + N + r"\s*%"],
    "debt_equity": [r"(?:Total Debt to Total Equity(?: Annual)?|Debt to Equity(?: Ratio)?|Debt/Equity)\s*:?\s*" + N],
    "insider_holding": [r"Promoters?(?: Holding)?(?: current Qtr)?\s*%?\s*:?\s*" + N + r"\s*%"],
    "op_margin": [r"Operating (?:Profit )?Margin(?: TTM)?(?: %)?\s*:?\s*" + N],
    "net_margin": [r"Net Profit Margin(?: TTM)?(?: %)?\s*:?\s*" + N],
    "eps": [r"EPS(?: TTM)?\s*:?\s*₹?\s*" + N],
}


def scan_text(text, keys=None):
    out = {}
    for k, pats in TEXT_PATTERNS.items():
        if keys and k not in keys:
            continue
        for p in pats:
            m = re.search(p, text, re.I)
            if m:
                v = sane(k, m.group(1))
                if v is not None:
                    out[k] = v
                    break
    return out


def trendlyne_data(symbol):
    raw = http_get("https://trendlyne.com/member/api/ac_snames/stock/?term=%s" % urllib.parse.quote(symbol),
                   referer="https://trendlyne.com/", accept="application/json")
    items = json.loads(raw)
    if isinstance(items, dict):
        items = items.get("body") or items.get("data") or items.get("results") or []
    url = None
    for it in items or []:
        txt = json.dumps(it).upper()
        if '"%s"' % symbol.upper() in txt or "/%s/" % symbol.upper() in txt:
            u = it.get("nexturl") or it.get("url") or it.get("link")
            if not u and it.get("pk") and it.get("slug"):
                u = "/equity/%s/%s/%s/" % (it["pk"], symbol.upper(), it["slug"])
            if u:
                url = u if u.startswith("http") else "https://trendlyne.com" + u
                break
    if not url:
        return {}
    return {"fund": scan_text(page_text(http_get(url, referer="https://trendlyne.com/")))}


def stockedge_data(symbol):
    raw = http_get("https://api.stockedge.com/Api/SecurityDashboardApi/GetSecuritiesBySearch?term=%s&page=1&pageSize=10&lang=en"
                   % urllib.parse.quote(symbol), referer="https://web.stockedge.com/", accept="application/json")
    items = json.loads(raw)
    if isinstance(items, dict):
        items = items.get("Data") or items.get("data") or []
    hit = next((it for it in items or [] if symbol.upper() in json.dumps(it).upper()), None)
    if not hit:
        return {}
    sid = hit.get("ID") or hit.get("Id") or hit.get("SecurityID")
    slug = hit.get("Slug") or hit.get("slug") or re.sub(r"[^a-z0-9]+", "-", str(hit.get("Name", symbol)).lower())
    if not sid:
        return {}
    txt = page_text(http_get("https://web.stockedge.com/share/%s/%s" % (slug, sid), referer="https://web.stockedge.com/"))
    return {"fund": scan_text(txt)}


def google_finance_data(symbol):
    txt = page_text(http_get("https://www.google.com/finance/quote/%s:NSE?hl=en" % urllib.parse.quote(symbol)))
    return {"fund": scan_text(txt, keys={"pe", "div_yield"})}


SOURCES = [   # (name, function, stage) - stage 1 runs always (in parallel), stage 2 only if gaps remain
    ("Screener.in", screener_fundamentals, 1),
    ("Tickertape", tickertape_data, 1),
    ("Moneycontrol", moneycontrol_data, 1),
    ("NSE India", nse_data, 1),
    ("Trendlyne", trendlyne_data, 2),
    ("StockEdge", stockedge_data, 2),
    ("Google Finance", google_finance_data, 2),
]


def _run_source(fn, symbol):
    try:
        r = fn(symbol) or {}
        if "fund" not in r and "extra" not in r:
            r = {"fund": r}
        n = len(r.get("fund") or {})
        return r, ("ok: %d metrics" % n) if n or r.get("extra") else "no data returned"
    except Exception as e:
        return {}, "failed: %s" % str(e)[:80]


def build_fundamentals(symbol, info, price, ticker=None):
    fund = {k: None for k in FUND_KEYS}
    src, status, tried, extras = {}, {}, [], {}

    def merge(name, vals):
        n = 0
        for k, v in (vals or {}).items():
            if k in fund and fund[k] is None and v is not None:
                fund[k], src[k] = v, name
                n += 1
        return n

    tried.append("Yahoo Finance")
    n = merge("Yahoo Finance", {k: sane(k, v) for k, v in yahoo_fundamentals(info, price).items()})
    status["Yahoo Finance"] = "ok: %d metrics" % n if n else "no data returned"
    if ticker is not None and any(fund[k] is None for k in SCORED_KEYS):
        tried.append("Yahoo statements")
        n = merge("Yahoo statements", yahoo_statements(ticker, price))
        status["Yahoo statements"] = "ok: filled %d gaps" % n if n else "no new data"

    stage1 = [s for s in SOURCES if s[2] == 1]
    with ThreadPoolExecutor(max_workers=len(stage1)) as ex:
        futs = {name: ex.submit(_run_source, fn, symbol) for name, fn, _ in stage1}
        results = {name: fut.result() for name, fut in futs.items()}
    for name, _, _ in stage1:                     # merge in priority order
        r, st = results[name]
        tried.append(name)
        filled = merge(name, r.get("fund"))
        extras[name] = r.get("extra") or {}
        status[name] = st + ("; filled %d gaps" % filled if filled else "")

    for name, fn, stage in SOURCES:
        if stage != 2:
            continue
        if not any(fund[k] is None for k in SCORED_KEYS):
            status[name] = "not needed (no gaps left)"
            continue
        r, st = _run_source(fn, symbol)
        tried.append(name)
        filled = merge(name, r.get("fund"))
        status[name] = st + ("; filled %d gaps" % filled if filled else "")

    sector_pe, sector_pe_src = None, None
    for name in ("Moneycontrol", "Tickertape", "NSE India"):
        v = sane("pe", extras.get(name, {}).get("sector_pe"))
        if v and v > 0:
            sector_pe, sector_pe_src = v, name
            break
    meta_extra = {
        "mc_sector": extras.get("Moneycontrol", {}).get("mc_sector") or extras.get("NSE India", {}).get("nse_sector"),
        "industry": extras.get("NSE India", {}).get("nse_industry"),
        "market_cap_cr": extras.get("Moneycontrol", {}).get("market_cap_cr") or extras.get("Tickertape", {}).get("market_cap_cr"),
    }
    used = []
    for s in src.values():
        if s not in used:
            used.append(s)
    return fund, src, used, tried, status, sector_pe, sector_pe_src, meta_extra


def sector_block(stock_df, sect_df, nifty_df, idx_name):
    out = {"index_name": idx_name}
    if sect_df is None or len(sect_df) < 60:
        return out
    sc = sect_df["Close"]
    out.update({
        "sec_ret_1m": f(ret(sc, 21)), "sec_ret_3m": f(ret(sc, 63)), "sec_ret_6m": f(ret(sc, 126)),
        "sec_ret_1y": f(ret(sc, 252)),
        "sec_vs_200dma": f((sc.iloc[-1] / sc.rolling(200).mean().iloc[-1] - 1) * 100) if len(sc) >= 200 else None,
    })
    n6 = ret(nifty_df["Close"], 126) if nifty_df is not None else None
    n1 = ret(nifty_df["Close"], 252) if nifty_df is not None else None
    s6, s1, x6 = ret(stock_df["Close"], 126), ret(stock_df["Close"], 252), ret(sc, 126)
    out["nifty_ret_6m"], out["nifty_ret_1y"] = f(n6), f(n1)
    out["sec_vs_nifty_6m"] = f(x6 - n6) if None not in (x6, n6) else None
    out["stock_vs_sec_6m"] = f(s6 - x6) if None not in (s6, x6) else None
    out["stock_vs_nifty_1y"] = f(s1 - n1) if None not in (s1, n1) else None
    return out


def hist(ticker, period="2y"):
    try:
        d = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=True)
        return d if len(d) else None
    except Exception:
        return None


def analyze(symbol):
    hit = _CACHE.get(symbol)
    if hit and time.time() - hit[0] < CACHE_TTL:
        return hit[1]
    res = demo_analyze(symbol) if DEMO else live_analyze(symbol)
    _CACHE[symbol] = (time.time(), res)
    return res


def live_analyze(symbol):
    t = yf.Ticker(symbol + ".NS")
    df = t.history(period="2y", interval="1d", auto_adjust=True)
    if df is None or len(df) < 60:
        raise ValueError("Not enough price history on NSE for %s" % symbol)
    try:
        info = t.info or {}
    except Exception:
        info = {}
    nifty = hist("^NSEI")
    tech = technicals(df, nifty)
    fund, src, used, tried, status, sector_pe, sector_pe_src, mx = build_fundamentals(symbol, info, tech["price"], t)
    sector, industry = info.get("sector"), info.get("industry")
    if not sector and mx.get("mc_sector"):
        low = mx["mc_sector"].lower()
        sector = next((s for k, s in MC_SECTOR_MAP if re.search(r"\b%s" % k, low)), None)
        industry = industry or mx.get("industry") or mx["mc_sector"]
    if not info.get("marketCap") and mx.get("market_cap_cr"):
        info["marketCap"] = mx["market_cap_cr"] * 1e7
    idx_t, idx_n = pick_sector_index(sector, industry)
    sect = hist(idx_t)
    if sect is None and idx_t != "^NSEI":
        idx_n, sect = "NIFTY 50 (sector index unavailable)", nifty
    return package(symbol, info, df, tech, fund, src, used, tried, status, sector_pe, sector_pe_src,
                   sector_block(df, sect, nifty, idx_n), sector, industry, False)


def package(symbol, info, df, tech, fund, src, used, tried, status, sector_pe, sector_pe_src, sect, sector, industry, demo):
    c = df["Close"]
    tail = df.iloc[-250:]
    return {
        "demo": demo,
        "meta": {
            "symbol": symbol, "name": info.get("longName") or info.get("shortName") or symbol,
            "sector": sector or "Unclassified", "industry": industry or "-",
            "price": f(c.iloc[-1]), "change_pct": f((c.iloc[-1] / c.iloc[-2] - 1) * 100),
            "market_cap": info.get("marketCap"), "as_of": str(df.index[-1].date()),
            "is_financial": (sector == "Financial Services"),
        },
        "technical": tech, "fundamental": {k: f(v) for k, v in fund.items()}, "fund_src": src,
        "fund_sources_used": used, "fund_sources_tried": tried, "fund_source_status": status,
        "sector_pe_live": sector_pe, "sector_pe_src": sector_pe_src, "sector": sect,
        "chart": {
            "dates": [str(d.date()) for d in tail.index],
            "close": [f(x) for x in tail["Close"]],
            "sma50": [f(x) for x in c.rolling(50).mean().iloc[-250:]],
            "sma200": [f(x) for x in c.rolling(200).mean().iloc[-250:]],
        },
    }


def sources_report(symbol):
    """Diagnostic: raw output of every source for one symbol."""
    out = {}
    if DEMO:
        return {"demo": True}
    for name, fn, _ in SOURCES:
        r, st = _run_source(fn, symbol)
        out[name] = {"status": st, "fund": r.get("fund"), "extra": r.get("extra")}
    return out


# ---------------------------- demo data (offline testing only) --------------------
DEMO_FALLEN = {"INFY", "HDFCBANK", "RELIANCE"}
DEMO_WEAK = {"IDEA", "YESBANK", "SUZLON"}


def _synthetic(seed, n=500, start=1000, drift=0.0004, vol=0.017, crash=0.0):
    rng = np.random.default_rng(seed)
    r = rng.normal(drift, vol, n)
    if crash:
        r[-126:] += crash / 126
        r[-8:] += 0.004
    close = start * np.exp(np.cumsum(r))
    idx = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=n)
    high = close * (1 + np.abs(rng.normal(0, 0.008, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.008, n)))
    return pd.DataFrame({"Close": close, "High": high, "Low": low,
                         "Volume": rng.integers(2_000_000, 6_000_000, n).astype(float)}, index=idx)


def demo_analyze(symbol):
    seed = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)
    crash = -0.35 if symbol in DEMO_FALLEN | DEMO_WEAK else 0.0
    df = _synthetic(seed, drift=0.0006 if crash else rng.normal(0.0004, 0.0008), crash=crash)
    nifty = _synthetic(7, start=22000, drift=0.0003, vol=0.009)
    sect = _synthetic(seed + 1, start=30000, drift=rng.normal(0.0003, 0.0006), vol=0.012, crash=crash / 2)
    name = next((m["name"] for m in MASTER if m["symbol"] == symbol), symbol)
    sectors = ["Technology", "Financial Services", "Energy", "Consumer Defensive", "Healthcare", "Industrials"]
    sector = sectors[seed % len(sectors)]
    strong, weak = symbol in DEMO_FALLEN, symbol in DEMO_WEAK
    u = lambda a, b: rng.uniform(a, b)
    info = {"longName": name, "sector": sector, "industry": "Banks - Regional" if sector == "Financial Services" else "-",
            "marketCap": float(rng.integers(20_000, 1_800_000)) * 1e7,
            "trailingPE": u(12, 20) if strong else u(8, 60), "forwardPE": u(8, 50), "priceToBook": u(1.2, 2.8) if strong else u(0.8, 12),
            "trailingPegRatio": u(0.6, 0.95) if strong else u(0.5, 3), "enterpriseToEbitda": u(6, 11) if strong else u(6, 35),
            "returnOnEquity": u(0.2, 0.3) if strong else u(-0.1, 0.05) if weak else u(0.05, 0.35),
            "returnOnAssets": u(0.09, 0.15) if strong else u(0.01, 0.15), "debtToEquity": u(5, 40) if strong else u(150, 300) if weak else u(5, 180),
            "currentRatio": u(1.6, 3) if strong else u(0.7, 3), "profitMargins": u(0.16, 0.25) if strong else u(-0.1, 0.02) if weak else u(0.03, 0.28),
            "operatingMargins": u(0.21, 0.3) if strong else u(0.06, 0.35),
            "revenueGrowth": u(0.12, 0.2) if strong else u(-0.1, 0.02) if weak else u(-0.05, 0.30),
            "earningsGrowth": u(0.15, 0.25) if strong else u(-0.4, -0.1) if weak else u(-0.15, 0.40),
            "trailingAnnualDividendYield": u(0, 0.04), "heldPercentInsiders": u(0.1, 0.75),
            "trailingEps": u(10, 150), "bookValue": u(100, 900)}
    tech = technicals(df, nifty)
    full = yahoo_fundamentals(info, tech["price"])
    full["roce"] = f(u(18, 28) if strong else u(4, 22))
    # Simulate the waterfall: Yahoo misses some metrics, later sources fill them
    plan = {"roe": "Yahoo statements", "debt_equity": "Yahoo statements", "roce": "Screener.in",
            "insider_holding": "Tickertape", "div_yield": "Moneycontrol", "peg": "Trendlyne", "ev_ebitda": None}
    fund, src = {}, {}
    for k, v in full.items():
        s = plan.get(k, "Yahoo Finance")
        fund[k] = v if s else None
        if s and v is not None:
            src[k] = s + " (demo)"
    tried = ["Yahoo Finance", "Yahoo statements", "Screener.in", "Tickertape", "Moneycontrol", "NSE India",
             "Trendlyne", "StockEdge", "Google Finance"]
    status = {s: "demo" for s in tried}
    used = []
    for s in src.values():
        if s not in used:
            used.append(s)
    idx_t, idx_n = pick_sector_index(sector, info["industry"])
    return package(symbol, info, df, tech, fund, src, used, tried, status, f(u(15, 30)), "Moneycontrol",
                   sector_block(df, sect, nifty, idx_n), sector, info["industry"], True)


# ----------------------------------------------------------------------------------
# 3. HTTP
# ----------------------------------------------------------------------------------
class H(SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=HERE, **k)

    def log_message(self, fmt, *args):
        pass

    def _json(self, obj, code=200):
        b = json.dumps(obj, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        arg = lambda k: (q.get(k) or [""])[0]
        try:
            if u.path == "/api/health":
                return self._json({"ok": True, "demo": DEMO, "master": MASTER_SOURCE})
            if u.path == "/api/suggest":
                return self._json({"items": search(arg("q"), 8)})
            if u.path == "/api/resolve":
                return self._json(resolve(arg("q")))
            if u.path == "/api/analyze":
                sym = arg("symbol").upper().strip()
                if not sym:
                    return self._json({"error": "symbol required"}, 400)
                return self._json(analyze(sym))
            if u.path == "/api/sources":
                return self._json(sources_report(arg("symbol").upper().strip()))
        except Exception as e:
            return self._json({"error": str(e)}, 500)
        if u.path in ("/", ""):
            self.path = "/index.html"
        return super().do_GET()


if __name__ == "__main__":
    print("NSE Stock Analyzer engine v3%s" % ("  [DEMO MODE - synthetic data]" if DEMO else ""))
    load_master()
    print("  Open http://localhost:%d in your browser  (Ctrl+C to stop)" % PORT)
    print("  Diagnostics: http://localhost:%d/api/sources?symbol=RELIANCE" % PORT)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
