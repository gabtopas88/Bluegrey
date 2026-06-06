"""
src/portfolio.py
Institutional Position Reconciliation & Portfolio State Manager.

Solves the State Ephemerality problem: when the engine crashes or restarts,
the strategy's in-memory state machine forgets which positions it holds at IBKR.
This module queries the broker as the source of truth and injects reality into
the strategy state BEFORE the live event loop starts.

It is the bridge between the (persistent, durable) IBKR state and the
(transient, in-memory) Strategy state. Designed as a persistent service —
the boot-time sync is one of its methods; the same class will later handle
periodic reconciliation and fill-driven updates.
"""
import logging
from dataclasses import dataclass
from typing import Dict, List, TYPE_CHECKING

from ib_async import IB, Contract, Position, MarketOrder

if TYPE_CHECKING:
    from strategies.base import BaseStrategy

logger = logging.getLogger("PortfolioManager")


@dataclass(frozen=True)
class LivePosition:
    """
    Standardized representation of a broker-confirmed position.
    Immutable to prevent accidental state corruption across the strategy boundary.
    """
    symbol: str             # Universe key in config.INSTRUMENTS (e.g., "C:AUDUSD")
    contract: Contract      # IBKR contract object (reusable for liquidation orders)
    quantity: float         # Signed: positive = long, negative = short
    avg_cost: float         # Weighted average entry price (per share/unit)
    account: str            # IBKR account identifier

    @property
    def is_long(self) -> bool:
        return self.quantity > 0

    @property
    def is_short(self) -> bool:
        return self.quantity < 0


class PortfolioManager:
    """
    The Reconciliation Layer.

    Responsibilities:
    1. Query IBKR for the true, broker-side portfolio state.
    2. Filter positions against the configured INSTRUMENTS universe (via conId for safety).
    3. Identify FOREIGN positions (held by other strategies / discretionary trades).
    4. Hand a standardized position dict to the strategy for reconciliation.
    5. Enforce policy when the strategy reports an ANOMALY.
    """

    # --- Anomaly Resolution Policies ---
    POLICY_HALT = 'HALT'              # Default: Stop engine. Require manual intervention.
    POLICY_LIQUIDATE = 'LIQUIDATE'    # Flatten anomalous positions and start clean.
    POLICY_ADOPT = 'ADOPT'            # DANGEROUS: Let strategy guess. Logged but allowed.

    def __init__(self, ib: IB, instruments: Dict[str, Contract]):
        self.ib = ib
        self.instruments = instruments

        # conId -> symbol_key map. We never trust symbol strings; conId is IBKR's
        # unique contract identifier and the only safe way to reconcile positions.
        self._conid_to_symbol: Dict[int, str] = {}
        self._initialized = False

    def initialize(self):
        """
        Qualifies contracts to populate conId attributes and builds the lookup map.
        Must be called AFTER ib.connect() but BEFORE fetch_live_positions().
        Idempotent — safe to call even if contracts were already qualified upstream.
        """
        logger.info("🔧 Initializing Portfolio Manager: qualifying contracts...")
        for symbol, contract in self.instruments.items():
            self.ib.qualifyContracts(contract)

            if not contract.conId:
                logger.error(f"❌ Failed to qualify contract for {symbol}. "
                             f"Cannot reconcile positions for this asset.")
                continue

            self._conid_to_symbol[contract.conId] = symbol

        self._initialized = True
        logger.info(f"   -> Indexed {len(self._conid_to_symbol)} contracts by conId.")

    def fetch_live_positions(self) -> Dict[str, LivePosition]:
        """
        Queries IBKR for current account positions. Returns only positions matching
        this engine's universe; foreign positions are logged and discarded.
        """
        if not self._initialized:
            raise RuntimeError("PortfolioManager.initialize() must be called first.")

        # Subscribe to position updates and wait for the cache to populate.
        # In ib_async, reqPositions() triggers the subscription; the data lands
        # in ib.positions() asynchronously. ib.sleep() processes pending events.
        logger.info("📡 Querying IBKR for live positions...")
        self.ib.reqPositions()
        self.ib.sleep(1.0)
        broker_positions: List[Position] = self.ib.positions()

        relevant: Dict[str, LivePosition] = {}
        foreign: List[Position] = []

        for pos in broker_positions:
            # IBKR sometimes returns zero-quantity records for recently flattened positions.
            if pos.position == 0:
                continue

            symbol_key = self._conid_to_symbol.get(pos.contract.conId)

            if symbol_key is None:
                foreign.append(pos)
                continue

            relevant[symbol_key] = LivePosition(
                symbol=symbol_key,
                contract=pos.contract,
                quantity=float(pos.position),
                avg_cost=float(pos.avgCost),
                account=pos.account
            )

        # Foreign positions exist; we just don't manage them. Visibility is critical.
        if foreign:
            logger.warning(f"⚠️ {len(foreign)} FOREIGN position(s) detected "
                           f"(outside this engine's universe):")
            for p in foreign:
                logger.warning(
                    f"   -> {p.contract.localSymbol} (conId={p.contract.conId}) "
                    f"qty={p.position} @ avgCost={p.avgCost:.4f} [acct: {p.account}]"
                )
            logger.warning("   These positions will NOT be touched by this engine.")

        return relevant

    def sync_strategy_state(self, strategy: 'BaseStrategy',
                            anomaly_policy: str = POLICY_HALT) -> bool:
        """
        Master boot-sync method. Reconciles broker reality into strategy memory.

        :param strategy: The strategy instance to sync.
        :param anomaly_policy: How to handle positions that don't match any valid
                               strategy state. Default HALT requires manual intervention.
        :return: True if sync succeeded and the engine may proceed.
                 False if anomaly was hit under HALT policy.
        """
        live_positions = self.fetch_live_positions()

        # CASE A: Clean slate. Nothing to reconcile.
        if not live_positions:
            logger.info("✅ Boot Sync: No managed positions at broker. Strategy starts FLAT.")
            strategy.sync_positions({})  # Explicitly tell strategy it's flat.
            return True

        # CASE B: Positions exist. Report and delegate to strategy.
        logger.info(f"📊 Boot Sync: {len(live_positions)} managed position(s) detected:")
        for sym, pos in live_positions.items():
            direction = "LONG" if pos.is_long else "SHORT"
            logger.info(
                f"   -> {sym}: {direction} {abs(pos.quantity):,.0f} units "
                f"@ avg ${pos.avg_cost:.5f}"
            )

        try:
            reconciled = strategy.sync_positions(live_positions)
        except NotImplementedError:
            logger.error(
                "🚨 Strategy does not implement sync_positions() but live "
                "positions exist. Cannot safely resume. Halting engine."
            )
            return False
        except Exception as e:
            logger.error(f"🚨 Strategy sync_positions() raised: {e}", exc_info=True)
            return False

        if reconciled:
            logger.info("✅ Boot Sync: Strategy state reconciled with broker reality.")
            return True

        # CASE C: ANOMALY. Strategy could not reconcile.
        logger.critical(
            "🚨 BOOT SYNC ANOMALY: Strategy could not reconcile live positions. "
            "Broker state is inconsistent with any valid strategy configuration."
        )

        if anomaly_policy == self.POLICY_HALT:
            logger.critical(
                "🛑 Anomaly Policy: HALT. Engine will not start. "
                "Manual intervention required: liquidate broker positions, "
                "or restart with LIQUIDATE policy."
            )
            return False

        elif anomaly_policy == self.POLICY_LIQUIDATE:
            logger.critical(
                "💣 Anomaly Policy: LIQUIDATE. Flattening anomalous positions."
            )
            self._liquidate_positions(live_positions)
            strategy.sync_positions({})  # After liquidation, strategy is flat.
            return True

        elif anomaly_policy == self.POLICY_ADOPT:
            logger.critical(
                "⚠️ Anomaly Policy: ADOPT. Strategy will guess at state. "
                "This is NOT safe for production. Use only in controlled testing."
            )
            return True

        else:
            logger.error(f"❌ Unknown anomaly policy '{anomaly_policy}'. Defaulting to HALT.")
            return False

    def _liquidate_positions(self, positions: Dict[str, LivePosition]):
        """
        Emergency flatten. Sends MKT orders to zero out every managed position.
        Bypasses the normal Risk Manager — this is a recovery path that runs
        before the event loop starts, and the alternative is leaving exposure unmanaged.
        """
        for sym, pos in positions.items():
            action = 'SELL' if pos.is_long else 'BUY'
            qty = int(abs(pos.quantity))

            order = MarketOrder(action, qty)
            trade = self.ib.placeOrder(pos.contract, order)

            logger.warning(
                f"💣 LIQUIDATION FIRED: {action} {qty} {sym} "
                f"(orderId={trade.order.orderId})"
            )

        # Give IBKR time to acknowledge and route the orders before we proceed.
        self.ib.sleep(2.0)