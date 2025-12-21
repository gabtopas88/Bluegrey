from ib_async import *
import logging

logger = logging.getLogger()

class ExecutionHandler:
    """
    Asset-Agnostic Order Router.
    It receives a Contract Object and fires. It does not care about the contract type.
    Responsibilities:
    1. Translate abstract 'StrategySignal' orders into IBKR Contracts/Orders.
    2. Route them to the exchange.
    """
    def __init__(self, ib_instance):
        self.ib = ib_instance

    def execute_signal(self, signal):
        """
        Input: A StrategySignal object (defined in base.py).
        Action: Places all orders contained in the signal.
        """
        if not signal or not signal.orders:
            return

        logger.info(f"🚀 SIGNAL: {signal.signal_type}")
        
        for order_instruction in signal.orders:
            # UNPACKING THE GENERIC INSTRUCTION
            contract = order_instruction['contract'] # <--- The Object, not a string
            action = order_instruction['action']
            qty = order_instruction['qty']
            order_type = order_instruction['type']
            
            # Create the IB Order Object
            if order_type == 'MKT':
                ib_order = MarketOrder(action, qty)
            else:
                # Add support for LMT, STOP, etc. as needed
                logger.warning(f"Order type {order_type} not implemented yet.")
                continue

            # FIRE
            # We don't need to qualify contracts here because they came from 
            # the Config/Data layer where they were likely already qualified.
            # But safety first:
            self.ib.qualifyContracts(contract) 
            
            trade = self.ib.placeOrder(contract, ib_order)
            logger.info(f"🔫 ORDER SENT: {action} {qty} of {contract.localSymbol} (ID: {trade.order.orderId})")