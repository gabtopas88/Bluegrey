import json
from types import SimpleNamespace

from src.engine.execution import ExecutionHandler


class FakeIB:
    def __init__(self):
        self.place_order_calls = 0
        self.qualify_calls = 0

    def qualifyContracts(self, contract):
        self.qualify_calls += 1

    def placeOrder(self, contract, order):
        self.place_order_calls += 1
        raise AssertionError("placeOrder should not be called in dry-run mode")


class DummySignal:
    def __init__(self, orders, signal_type="TEST"):
        self.orders = orders
        self.signal_type = signal_type


def test_execute_signal_dry_run_does_not_place_order_and_logs_intended_order(monkeypatch, tmp_path):
    from src.engine import execution

    monkeypatch.setattr(execution.config, "ENABLE_ORDER_SUBMISSION", False)
    monkeypatch.setattr(execution.config, "STATE_DIR", tmp_path)

    ib = FakeIB()
    handler = ExecutionHandler(ib)

    contract = SimpleNamespace(localSymbol="SPY", symbol="SPY")
    signal = DummySignal(
        orders=[
            {
                "contract": contract,
                "action": "BUY",
                "qty": 10,
                "type": "MKT",
            }
        ]
    )

    handler.execute_signal(signal)

    assert ib.qualify_calls == 1
    assert ib.place_order_calls == 0

    orders_file = tmp_path / "orders.jsonl"
    assert orders_file.exists()
    rows = [json.loads(line) for line in orders_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["symbol"] == "SPY"
    assert rows[0]["action"] == "BUY"
    assert rows[0]["qty"] == 10
    assert rows[0]["type"] == "MKT"
    assert rows[0]["mode"] == "dry-run"
