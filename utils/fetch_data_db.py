import sqlite3
import pandas as pd
import dateutil.parser


def fetch_data_db(coin, start="2018-01-01", end="2019-03-01"):

    if coin in ["BTC", "BCC", "ADA", "NEO", "BNB", "QTUM"]:
        conn = sqlite3.connect("../gekko-develop/history/binance_0.1.db")
        base = "USDT"
    if coin in ["XRP", "ETH", "EOS", "XLM", "LTC", "XMR", "DASH", "ETC"]:
        conn = sqlite3.connect("../gekko-develop/history/kraken_0.1.db")
        base = "USD"
    if coin in ["IOT", "TRX", "VEN", "OMG", "ZRX"]:
        conn = sqlite3.connect("../gekko-develop/history/bitfinex_0.1.db")
        base = "USD"
    if coin in ["ZEC"]:
        conn = sqlite3.connect("../gekko-develop/history/poloniex_0.1.db")
        base = "USDT"

    start = dateutil.parser.parse(f"{start}T00:00:00.000Z")
    end = dateutil.parser.parse(f"{end}T00:00:00.000Z")

    cursor = conn.cursor()

    cursor.execute(
        f"select start as date, open, high, low, close, volume from candles_{base}_{coin} where start >= ? and start <= ? order by start asc",
        [start.timestamp(), end.timestamp()],
    )
    rows = cursor.fetchall()
    data = pd.DataFrame(
        rows, columns=["date", "open", "high", "low", "close", "volume"]
    )
    # rename start => date because (not sure?) https://www.backtrader.com/docu/pandas-datafeed/pandas-datafeed.html
    data["date"] = pd.to_datetime(data["date"], unit="s")

    data.set_index("date", inplace=True)

    # print_green(coin, data.shape)

    return data
