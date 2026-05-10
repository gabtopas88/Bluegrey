import importlib

import pytest



def _reload_config_with_universe(monkeypatch, universe: str):
    monkeypatch.setenv("BLUEGREY_UNIVERSE", universe)
    import src.config.config as config

    return importlib.reload(config)


def test_default_universe_builds_spy_stock(monkeypatch):
    config = _reload_config_with_universe(monkeypatch, "SPY:STK")

    assert "SPY" in config.INSTRUMENTS
    contract = config.INSTRUMENTS["SPY"]
    assert contract.symbol == "SPY"
    assert contract.secType == "STK"


@pytest.mark.parametrize("raw", ["SPY", "SPY- STK", "SPY:", ":STK", "SPY:STK:EXTRA"])
def test_malformed_universe_raises_value_error(raw):
    from src.config.config import parse_universe

    with pytest.raises(ValueError, match="Malformed BLUEGREY_UNIVERSE entry"):
        parse_universe(raw)
