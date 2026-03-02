from ib_async import *
import logging

logger = logging.getLogger(__name__)

class ExecutionHandler:
    """
    Asset-Agnostic Order Router & Manager.
    
    Responsibilities:
    1. Translate abstract 'StrategySignal' orders into IBKR Contracts/Orders.
    2. Route them to the exchange.
    3. Monitor Order Status (Filled, Cancelled).
    4. Reconcile Fills.
    """
    def __init__(self, ib_instance):
        self.ib = ib_instance
        
        # State Tracking
        # Map: IB_OrderID -> { 'signal_id': ..., 'symbol': ..., 'status': ... }
        self.active_orders = {} 

    def execute_signal(self, signal):
        """
        Input: A StrategySignal object (defined in base.py).
        Action: Places all orders contained in the signal.
        """
        if not signal or not signal.orders:
            return

        logger.info(f"🚀 SIGNAL RECEIVED: {signal.signal_type}")
        
        for order_instruction in signal.orders:
            # UNPACKING THE GENERIC INSTRUCTION
            contract = order_instruction['contract'] # <--- The Object, not a string
            action = order_instruction['action']
            qty = order_instruction['qty']
            order_type = order_instruction['type'] # MKT, LMT, etc.
            
            # Create the IB Order Object
            if order_type == 'MKT':
                ib_order = MarketOrder(action, qty)
            elif order_type == 'LMT':
                price = order_instruction.get('price')
                if not price:
                    logger.error(f"❌ Limit Order missing price for {contract.symbol}")
                    continue
                ib_order = LimitOrder(action, qty, price)
            else:
                logger.warning(f"⚠️ Order type {order_type} not implemented yet. Defaulting to MKT.")
                ib_order = MarketOrder(action, qty)

            # Safety: Qualify Contract
            self.ib.qualifyContracts(contract) 
            
            # FIRE
            trade = self.ib.placeOrder(contract, ib_order)
            
            # Track the Order
            self.active_orders[trade.order.orderId] = {
                'symbol': contract.localSymbol,
                'action': action,
                'qty': qty,
                'status': 'SUBMITTED',
                'timestamp': datetime.now()
            }
            
            logger.info(f"🔫 ORDER SENT: {action} {qty} {contract.localSymbol} (ID: {trade.order.orderId})")

    def on_order_status(self, trade: Trade):
        """
        Callback: Triggered when order status changes (Submitted -> Filled, etc.)
        """
        status = trade.orderStatus.status
        filled = trade.orderStatus.filled
        remaining = trade.orderStatus.remaining
        order_id = trade.order.orderId
        
        if order_id in self.active_orders:
            self.active_orders[order_id]['status'] = status
            
        logger.info(f"📡 ORDER STATUS [ID:{order_id}]: {status} | Filled: {filled} | Rem: {remaining}")

    def on_exec_details(self, trade: Trade, fill: Fill):
        """
        Callback: Triggered when a trade actually happens. 
        SOURCE OF TRUTH for PnL and Positions.
        """
        symbol = trade.contract.localSymbol
        side = fill.execution.side
        shares = fill.execution.shares
        price = fill.execution.price
        comm = fill.commissionReport.commission if fill.commissionReport else 0.0
        
        logger.info(f"💸 EXECUTION CONFIRMED: {side} {shares} {symbol} @ ${price:.2f} (Comm: {comm})")
        
        # FUTURE: Push this to a PortfolioManager class to update 'Live' positions.