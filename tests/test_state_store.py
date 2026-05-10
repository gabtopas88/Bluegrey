import json

from src.infra import state_store


def test_append_jsonl_writes_to_state_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(state_store.config, "STATE_DIR", tmp_path)

    state_store.append_jsonl("orders.jsonl", {"symbol": "SPY", "qty": 1})

    out = tmp_path / "orders.jsonl"
    assert out.exists()
    line = out.read_text(encoding="utf-8").strip()
    payload = json.loads(line)
    assert payload["symbol"] == "SPY"
    assert payload["qty"] == 1
    assert "ts" in payload


def test_write_json_writes_heartbeat_style_json(monkeypatch, tmp_path):
    monkeypatch.setattr(state_store.config, "STATE_DIR", tmp_path)

    state_store.write_json("heartbeat.json", {"status": "ok"})

    out = tmp_path / "heartbeat.json"
    assert out.exists()
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["status"] == "ok"
    assert "ts" in payload
