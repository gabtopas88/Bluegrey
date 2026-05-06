import json
from datetime import datetime
from pathlib import Path

from src.config import config


def _state_path(filename: str) -> Path:
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    return config.STATE_DIR / filename


def _normalize_payload(payload):
    if isinstance(payload, dict):
        data = dict(payload)
    else:
        data = {"value": payload}
    data.setdefault("ts", datetime.utcnow().isoformat())
    return data


def append_jsonl(filename, payload):
    path = _state_path(filename)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(_normalize_payload(payload), default=str) + "\n")


def write_json(filename, payload):
    path = _state_path(filename)
    with path.open("w", encoding="utf-8") as f:
        json.dump(_normalize_payload(payload), f, default=str, indent=2)
