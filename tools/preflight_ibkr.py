"""Manual IBKR preflight check for VM readiness.

Run with:
    python tools/preflight_ibkr.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


logger = logging.getLogger("preflight_ibkr")


def _contract_labels(instruments: dict) -> list[str]:
    labels: list[str] = []
    for symbol, contract in instruments.items():
        sec_type = getattr(contract, "secType", "?")
        labels.append(f"{symbol}:{sec_type}")
    return labels


def _verify_account(ib, account_id: str) -> None:
    if not account_id:
        logger.info("ACCOUNT_ID not configured; skipping managed account verification")
        return

    accounts: Iterable[str] = ib.managedAccounts()
    if account_id not in accounts:
        raise RuntimeError(
            "Configured ACCOUNT_ID is not available in IB Gateway managed accounts. "
            f"configured={account_id!r} available={list(accounts)!r}"
        )
    logger.info("ACCOUNT_ID verified in managed accounts")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    try:
        from src.config import config
        from ib_async import IB
    except ModuleNotFoundError as exc:
        logger.error("PRECHECK FAIL: missing dependency/module: %s", exc)
        return 1

    ib = IB()
    logger.info("Starting IBKR preflight")
    logger.info(
        "Config: host=%s port=%s client_id=%s market_data_type=%s universe=%s",
        config.IB_HOST,
        config.IB_PORT,
        config.IB_CLIENT_ID,
        config.IB_MARKET_DATA_TYPE,
        _contract_labels(config.INSTRUMENTS),
    )

    try:
        ib.connect(config.IB_HOST, config.IB_PORT, clientId=config.IB_CLIENT_ID)
        logger.info("Connected to IB Gateway/TWS")

        ib.reqMarketDataType(config.IB_MARKET_DATA_TYPE)
        logger.info("Market data type set to %s", config.IB_MARKET_DATA_TYPE)

        _verify_account(ib, config.ACCOUNT_ID)

        contracts = list(config.INSTRUMENTS.values())
        qualified_contracts = ib.qualifyContracts(*contracts)
        if len(qualified_contracts) != len(contracts):
            raise RuntimeError(
                f"Instrument qualification mismatch: configured={len(contracts)} qualified={len(qualified_contracts)}"
            )

        logger.info("Qualified %d/%d configured instruments", len(qualified_contracts), len(contracts))
        logger.info("PRECHECK PASS: IBKR connectivity and instrument qualification succeeded")
        return 0

    except Exception as exc:  # noqa: BLE001 - explicit preflight failure reporting
        logger.error("PRECHECK FAIL: %s", exc)
        return 1

    finally:
        if ib.isConnected():
            ib.disconnect()
            logger.info("Disconnected from IB Gateway/TWS")


if __name__ == "__main__":
    sys.exit(main())
