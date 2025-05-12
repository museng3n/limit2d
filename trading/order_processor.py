import MetaTrader5 as mt5
import logging
from config import DEVIATION, MAGIC_NUMBER

logger = logging.getLogger(__name__)

class OrderProcessor:
    def prepare_order_request(self, signal, volume, tp):
        """Prepare order request with proper error handling"""
        try:
            symbol_info = mt5.symbol_info(signal['symbol'])
            if symbol_info is None:
                logger.error(f"❌ Symbol info not found for {signal['symbol']}")
                return None
            
            if signal['is_limit']:
                # Handle limit orders
                order_type = mt5.ORDER_TYPE_BUY_LIMIT if 'BUY' in signal['direction'] else mt5.ORDER_TYPE_SELL_LIMIT
                price = signal['entry']
                action = mt5.TRADE_ACTION_PENDING
                logger.info(f"📋 Preparing LIMIT order at {price}")
            else:
                # Handle immediate orders
                order_type = mt5.ORDER_TYPE_BUY if 'BUY' in signal['direction'] else mt5.ORDER_TYPE_SELL
                price = mt5.symbol_info_tick(signal['symbol']).ask if order_type == mt5.ORDER_TYPE_BUY else mt5.symbol_info_tick(signal['symbol']).bid
                action = mt5.TRADE_ACTION_DEAL
                logger.info(f"📋 Preparing MARKET order at current price")
            
            request = {
                "action": action,
                "symbol": signal['symbol'],
                "volume": volume,
                "type": order_type,
                "price": price,
                "sl": signal['sl'],
                "tp": tp,
                "deviation": DEVIATION,
                "magic": MAGIC_NUMBER,
                "comment": f"Signal Trade TP:{tp}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            logger.info(f"📦 Order details:"
                       f"\n    Symbol: {signal['symbol']}"
                       f"\n    Type: {'LIMIT' if signal['is_limit'] else 'MARKET'}"
                       f"\n    Direction: {signal['direction']}"
                       f"\n    Price: {price}"
                       f"\n    Volume: {volume}"
                       f"\n    SL: {signal['sl']}"
                       f"\n    TP: {tp}")
            
            return request
            
        except Exception as e:
            logger.error(f"❌ Error preparing order request: {e}")
            return None
    
    def execute_order(self, request):
        """Execute order with proper error handling"""
        try:
            result = mt5.order_send(request)
            if result is None:
                error = mt5.last_error()
                logger.error(f"❌ Order failed - Error: {error[0]}, {error[1]}")
                return None
                
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                logger.error(f"❌ Order failed - {result.comment} (code: {result.retcode})")
                return None
                
            logger.info(f"✅ Order executed successfully - Ticket: {result.order}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error executing order: {e}")
            return None
