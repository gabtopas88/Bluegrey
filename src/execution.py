from ib_async import *
from datetime import datetime, timezone
import logging
from typing import Optional, Dict

from src.telemetry import TelemetryStore

logger = logging.getLogger(__name__)


# Terminal IBKR order statuses. Once an order hits any of these, the broker
# will send no further events for it and we can safely evict it from our cache.
# 'Inactive' is a soft-cancel state used by IBKR when an order is rejected before
# transmission; treat it as terminal too.
_TERMINAL_STATUSES = {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}


def _contract_label(contract) -> str:
    """
    Best-effort human label for a contract, safe on UNQUALIFIED contracts.

    localSymbol is only populated after IBKR qualification. Every boot path
    (DataManager.subscribe, PortfolioManager.initialize, _prime_strategy)
    qualifies the universe, so localSymbol is normally set — but logging must
    never itself raise or emit an empty string when something upstream failed.
    """
    return (
        getattr(contract, 'localSymbol', '')
        or getattr(contract, 'symbol', '')
        or str(contract)
    )


class ExecutionHandler:
    """
    Asset-Agnostic Order Router & Manager.

    Responsibilities:
    1. Translate abstract 'StrategySignal' orders into IBKR Contracts/Orders.
    2. Route them to the exchange.
    3. Monitor Order Status (Filled, Cancelled).
    4. Reconcile Fills.
    5. Persist every send and fill to the telemetry store (if provided).
    """

    def __init__(self, ib_instance, telemetry: Optional[TelemetryStore] = None):
        self.ib = ib_instance
        self.telemetry = telemetry

        # State Tracking
        # Map: IB_OrderID -> { 'symbol', 'con_id', 'action', 'qty', 'status',
        #                      'timestamp', 'estimated_price', 'signal_type' }
        # estimated_price and con_id are persisted so on_exec_details can denormalize them
        # onto the fill row without a lookup against persisted orders.
        # Entries are evicted in on_order_status() once the order reaches a terminal
        # state — bounding memory growth over long sessions.
        self.active_orders: Dict[int, dict] = {}

    def execute_signal(self, signal) -> int:
        """
        Input: A StrategySignal object (defined in base.py).
        Action: Places all orders contained in the signal and records telemetry.

        Returns the number of orders actually handed to IBKR. The engine uses
        this to detect the state-desync case where the strategy has already
        committed a position transition but nothing reached the broker.

        """
        if not signal or not signal.orders:
            return 0

        logger.info(f"🚀 SIGNAL RECEIVED: {signal.signal_type}")

        requested = len(signal.orders)
        placed = 0

        for order_instruction in signal.orders:
            # UNPACKING THE GENERIC INSTRUCTION
            contract = order_instruction['contract']  # <--- The Object, not a string
            action = order_instruction['action']
            qty = order_instruction['qty']
            order_type = order_instruction['type']    # MKT, LMT, etc.
            estimated_price = order_instruction.get('estimated_price', 0.0)
            estimated_volatility = order_instruction.get('volatility')

            label = _contract_label(contract)

            # Create the IB Order Object
            limit_price = None
            if order_type == 'MKT':
                ib_order = MarketOrder(action, qty)
            elif order_type == 'LMT':
                limit_price = order_instruction.get('price')
                if not limit_price:
                    logger.error(f"❌ Limit Order missing price for {contract.symbol}")
                    continue
                ib_order = LimitOrder(action, qty, limit_price)
            else:
                logger.warning(f"⚠️ Order type {order_type} not implemented yet. Defaulting to MKT.")
                ib_order = MarketOrder(action, qty)

            # Diagnostic only — we do NOT qualify here (see docstring). An
            # unqualified contract still routes if symbol/secType/exchange/
            # currency are populated, so we warn loudly and attempt the send
            # rather than dropping a leg and leaving the other one naked.
            if not getattr(contract, 'conId', 0):
                logger.error(
                    f"⚠️ Contract {label} has no conId (not qualified at boot). "
                    f"Attempting the send anyway — investigate the boot sequence."
                )

            # FIRE
            try:
                trade = self.ib.placeOrder(contract, ib_order)
            except Exception as e:
                # NEVER fail silently again. This is the exact path that cost a
                # week of ambiguity: the strategy committed a position while
                # nothing reached the broker.
                logger.critical(
                    f"🚨 ORDER SEND FAILED for {action} {qty} {label} "
                    f"({signal.signal_type}): {e}",
                    exc_info=True,
                )
                continue

            placed += 1

            # Track the Order (denormalize estimated_price and con_id for the fill callback)
            self.active_orders[trade.order.orderId] = {
                'symbol':          label,
                'con_id':          getattr(contract, 'conId', 0),
                'action':          action,
                'qty':             qty,
                'status':          'SUBMITTED',
                'timestamp':       datetime.now(timezone.utc),
                'estimated_price': estimated_price,
                'signal_type':     signal.signal_type,
            }

            logger.info(f"🔫 ORDER SENT: {action} {qty} {label} (ID: {trade.order.orderId})")

            # --- TELEMETRY: persist the order send ---
            if self.telemetry:
                try:
                    self.telemetry.record_order(
                        ib_order_id=trade.order.orderId,
                        signal_type=signal.signal_type,
                        symbol=label,
                        con_id=getattr(contract, 'conId', 0),
                        action=action,
                        qty=qty,
                        order_type=order_type,
                        estimated_price=estimated_price,
                        limit_price=limit_price,
                        estimated_volatility=estimated_volatility,
                    )
                except Exception as e:
                    logger.error(f"❌ Telemetry record_order failed: {e}")

        # --- POST-SEND INTEGRITY REPORT ---
        # A pairs trade is only meaningful as a complete set of legs. Surface
        # both total and partial failure at CRITICAL so they can never again be
        # inferred only from the absence of telemetry rows.
        if placed == 0:
            logger.critical(
                f"🚨 EXECUTION FAILURE: {requested} order(s) for {signal.signal_type} "
                f"were requested and NONE reached IBKR."
            )
        elif placed < requested:
            logger.critical(
                f"🚨 PARTIAL EXECUTION: only {placed}/{requested} order(s) for "
                f"{signal.signal_type} reached IBKR. A spread leg may now be NAKED."
            )

        return placed

    def on_order_status(self, trade: Trade):
        """
        Callback: Triggered when order status changes (Submitted -> Filled, etc.)

        On terminal status, evicts the order from active_orders to bound memory.
        IBKR sends execDetails (the fill callback that reads estimated_price)
        BEFORE the final 'Filled' status event, so eviction here is safe — by
        the time we see 'Filled', on_exec_details has already consumed the cache.
        """
        status = trade.orderStatus.status
        filled = trade.orderStatus.filled
        remaining = trade.orderStatus.remaining
        order_id = trade.order.orderId

        if order_id in self.active_orders:
            self.active_orders[order_id]['status'] = status

        logger.info(f"📡 ORDER STATUS [ID:{order_id}]: {status} | Filled: {filled} | Rem: {remaining}")

        # Bounded-cache eviction: drop terminal orders. Prevents memory growth
        # over long-running sessions (weeks of paper trading produce thousands
        # of orders; without eviction this dict grows monotonically).
        if status in _TERMINAL_STATUSES and order_id in self.active_orders:
            evicted = self.active_orders.pop(order_id)
            logger.debug(
                f"🧹 Evicted terminal order {order_id} ({evicted['symbol']}, {status})"
            )

    def on_exec_details(self, trade: Trade, fill: Fill):
        """
        Callback: Triggered when a trade actually happens.
        SOURCE OF TRUTH for PnL and Positions.
        """
        symbol = trade.contract.localSymbol
        con_id = getattr(trade.contract, 'conId', 0)
        side = fill.execution.side
        shares = fill.execution.shares
        price = fill.execution.price
        exec_id = fill.execution.execId

        comm = fill.commissionReport.commission if fill.commissionReport else 0.0
        realized_pnl = fill.commissionReport.realizedPNL if fill.commissionReport else 0.0

        logger.info(f"💸 EXECUTION CONFIRMED: {side} {shares} {symbol} @ ${price:.5f} (Comm: {comm})")

        # --- TELEMETRY: persist the fill ---
        # Denormalize the estimated_price from active_orders so the parity harness
        # can compute slippage from fills alone without joining back to orders.
        if self.telemetry:
            order_id = trade.order.orderId
            estimated_price = self.active_orders.get(order_id, {}).get('estimated_price', 0.0)

            try:
                self.telemetry.record_fill(
                    ib_order_id=order_id,
                    exec_id=exec_id,
                    symbol=symbol,
                    con_id=con_id,
                    side=side,
                    shares=shares,
                    price=price,
                    commission=comm,
                    realized_pnl=realized_pnl,
                    estimated_price=estimated_price,
                )
            except Exception as e:
                logger.error(f"❌ Telemetry record_fill failed: {e}")

        # FUTURE: Push this to a PortfolioManager class to update 'Live' positions.