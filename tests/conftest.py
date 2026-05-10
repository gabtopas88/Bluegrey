import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


if "ib_async" not in sys.modules:
    ib_async = types.ModuleType("ib_async")

    class Contract:
        def __init__(self, symbol="", secType=""):
            self.symbol = symbol
            self.secType = secType
            self.localSymbol = symbol

    class Stock(Contract):
        def __init__(self, symbol, exchange, currency):
            super().__init__(symbol=symbol, secType="STK")
            self.exchange = exchange
            self.currency = currency

    class Forex(Contract):
        def __init__(self, symbol):
            super().__init__(symbol=symbol, secType="CASH")

    class Crypto(Contract):
        def __init__(self, symbol, exchange, currency):
            super().__init__(symbol=symbol, secType="CRYPTO")
            self.exchange = exchange
            self.currency = currency

    class Future(Contract):
        pass

    class MarketOrder:
        def __init__(self, action, totalQuantity):
            self.action = action
            self.totalQuantity = totalQuantity

    class LimitOrder(MarketOrder):
        def __init__(self, action, totalQuantity, lmtPrice):
            super().__init__(action, totalQuantity)
            self.lmtPrice = lmtPrice

    class Trade:
        pass

    class Fill:
        pass

    ib_async.Contract = Contract
    ib_async.Stock = Stock
    ib_async.Forex = Forex
    ib_async.Crypto = Crypto
    ib_async.Future = Future
    ib_async.MarketOrder = MarketOrder
    ib_async.LimitOrder = LimitOrder
    ib_async.Trade = Trade
    ib_async.Fill = Fill
    ib_async.__all__ = [
        "Contract",
        "Stock",
        "Forex",
        "Crypto",
        "Future",
        "MarketOrder",
        "LimitOrder",
        "Trade",
        "Fill",
    ]

    sys.modules["ib_async"] = ib_async
