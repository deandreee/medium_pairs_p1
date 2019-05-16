from __future__ import absolute_import, division, print_function, unicode_literals

import argparse
import datetime

import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
from backtrader.indicators import PeriodN
import backtrader.analyzers as btanalyzers

from utils.fetch_data_db import fetch_data_db

import matplotlib.pyplot as plt

plt.rcParams["figure.figsize"] = [16, 8]

# spread = a/b let's see what happens ...


class OLS_TransformationN_3(PeriodN):
    _mindatas = 2  # ensure at least 2 data feeds are passed
    lines = ("zscore",)
    params = (("period", 10),)

    def __init__(self):
        spread = self.data0 / self.data1
        spread_mean = bt.ind.SMA(spread, period=self.p.period)
        spread_std = bt.ind.StdDev(spread, period=self.p.period)

        self.l.zscore = (spread - spread_mean) / spread_std


class Spread(bt.Indicator):
    _mindatas = 2  # ensure at least 2 data feeds are passed
    alias = ("SPR", "Spread")
    # lines = ("spread", "spread_60")
    lines = ("spread", "spread_sma")

    def __init__(self):
        self.lines.spread = self.data0 / self.data1
        self.lines.spread_sma = bt.ind.SimpleMovingAverage(
            self.lines.spread, period=50)


class CryptoPairsStrat(bt.Strategy):
    params = dict(
        period=10,
        qty1=1,
        qty2=1,
        printout=False,
        upper=2.5,
        lower=-2.5,
        status=0,
        portfolio_value=1000,
        ols=3,
        order_pct=0.5,
    )

    def log(self, txt, dt=None):
        if self.p.printout:
            dt = dt or self.data.datetime[0]
            dt = bt.num2date(dt)
            print("%s, %s" % (dt.isoformat(), txt))

    def __init__(self):
        # To control operation entries
        self.orderid = None
        self.qty1 = self.p.qty1
        self.qty2 = self.p.qty2
        self.upper_limit = self.p.upper
        self.lower_limit = self.p.lower

        self.status = self.p.status
        self.portfolio_value = self.p.portfolio_value

        if self.p.ols == 2:
            self.transform = btind.OLS_TransformationN_2(period=self.p.period)
        elif self.p.ols == 3:
            self.transform = OLS_TransformationN_3(period=self.p.period)
        else:
            raise Exception("Unknown OLS type: " + self.p.ols)

        self.zscore = self.transform.zscore
        self.spread = Spread(self.data0, self.data1)

    def next(self):

        order_pct = self.p.order_pct

        if self.orderid:
            return  # if an order is active, no new orders are allowed

        if (self.zscore[0] > self.upper_limit) and (self.status != 1):
            self.order_target_percent(data=self.data0, target=-order_pct)
            self.order_target_percent(data=self.data1, target=order_pct)
            self.status = 1  # The current status is "short the spread"
        elif (self.zscore[0] < self.lower_limit) and (self.status != 2):
            self.order_target_percent(data=self.data1, target=-order_pct)
            self.order_target_percent(data=self.data0, target=order_pct)
            self.status = 2  # The current status is "long the spread"

    def stop(self):

        sharpe = self.analyzers.sharpe.get_analysis()
        end_value = self.broker.getvalue()

        print("==================================================")
        print("START VALUE: %.2f" % self.broker.startingcash)
        print("END VALUE: %.2f" % end_value)
        print("SHARPE: ", sharpe)

        print("==================================================")


def get_period(compression, p):
    h = int(60 / compression)

    if p.endswith("d"):
        d = int(h * 24)
        return int(p.replace("d", "")) * d
    if p.endswith("h"):
        return int(p.replace("h", "")) * h

    raise Exception("can't parse period: " + p)


def runstrategy():
    args = parse_args()

    print(args)

    # Create a cerebro
    cerebro = bt.Cerebro()

    # Get the dates from the args
    # fromdate = datetime.datetime.strptime(args.fromdate, "%Y-%m-%d")
    # todate = datetime.datetime.strptime(args.todate, "%Y-%m-%d")
    ols = args.ols
    compression = args.compression
    spread_period = args.spread_period
    period = get_period(compression, spread_period)
    start = args.fromdate or "2018-07-01"
    end = args.todate or "2018-09-01"

    coin0 = args.c0 or "BTC"
    data_raw = fetch_data_db(coin0, start=start, end=end)
    data_btc = bt.feeds.PandasData(dataname=data_raw)

    # cerebro.adddata(data_btc)
    cerebro.resampledata(
        data_btc, timeframe=bt.TimeFrame.Minutes, compression=compression
    )

    coin1 = args.c1 or "XMR"
    data_raw = fetch_data_db(coin1, start=start, end=end)
    data_bcc = bt.feeds.PandasData(dataname=data_raw)

    cerebro.resampledata(
        data_bcc, timeframe=bt.TimeFrame.Minutes, compression=compression
    )

    threshold = float(args.threshold or 2)
    order_pct = float(args.order_pct or 0.5)

    # Add the strategy
    cerebro.addstrategy(
        CryptoPairsStrat,
        period=period,
        upper=threshold,
        lower=-threshold,
        ols=ols,
        order_pct=order_pct,
    )

    # Add the commission - only stocks like a for each operation
    start_value = 1000
    cerebro.broker.setcash(start_value)

    commission = float(args.commission or 0.001)
    cerebro.broker.setcommission(commission=commission)

    cerebro.addanalyzer(btanalyzers.SharpeRatio, _name="sharpe")

    strats = cerebro.run(
        runonce=not args.runnext, preload=not args.nopreload, oldsync=args.oldsync
    )

    sharpe = strats[0].analyzers.sharpe.get_analysis()
    sharperatio = sharpe and sharpe["sharperatio"]
    # sharpe = 0
    end_value = cerebro.broker.getvalue()

    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M")

    filename = args.filename or "tmp"

    figs = cerebro.plot(
        numfigs=args.numfigs,
        # ytight=True,
        # plotdist=50,
        volume=False,
        zdown=False,
        style="candlestick",
        barup="green",
        # scheme={"barup": "green"},
        noplot=args.noplot,
    )


def parse_args():
    parser = argparse.ArgumentParser(description="MultiData Strategy")

    parser.add_argument("--c0")

    parser.add_argument("--c1")

    parser.add_argument("--fromdate", "-f")

    parser.add_argument("--todate", "-t")

    # parser.add_argument("--threshold", "-threshold")
    parser.add_argument("--threshold")

    # parser.add_argument("--spread_period", "-spread_period")
    parser.add_argument("--spread_period", default="7d")

    parser.add_argument("--ols", default=3)

    parser.add_argument("--compression", default=60)

    parser.add_argument("--commission", default=0.002)

    parser.add_argument("--filename", default=None)

    parser.add_argument("--order_pct", default=0.5)

    parser.add_argument(
        "--runnext", action="store_true", help="Use next by next instead of runonce"
    )

    parser.add_argument(
        "--nopreload", action="store_true", help="Do not preload the data"
    )

    parser.add_argument(
        "--oldsync", action="store_true", help="Use old data synchronization method"
    )

    # parser.add_argument("--plot", default=True, type=bool)

    parser.add_argument("--noplot", action="store_true")

    parser.add_argument("--numfigs", "-n", default=1)

    return parser.parse_args()


if __name__ == "__main__":
    runstrategy()
