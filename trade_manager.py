import MetaTrader5 as mt5
import logging
import asyncio
from collections import defaultdict
from utils.validation import validate_modify_sl

logger = logging.getLogger(__name__)

class TradeManager:
    def __init__(self):
        self.active_trades = defaultdict(list)
        
    async def start_monitoring(self):
        while True:
            try:
                await self._check_trades()
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in trade monitoring: {e}")
                await asyncio.sleep(5)
                
    async def _check_trades(self):
        for symbol, trades in list(self.active_trades.items()):
            positions = mt5.positions_get(symbol=symbol)
            if positions is None:
                continue
                
            position_tickets = {pos.ticket for pos in positions}
            positions_dict = {pos.ticket: pos for pos in positions}
            
            # Sort trades by TP level
            sorted_trades = sorted(trades, key=lambda x: x['tp'])
            
            # Find completed trades
            for i, trade in enumerate(sorted_trades[:]):
                if trade['ticket'] not in position_tickets:
                    logger.info(f"TP hit for ticket {trade['ticket']}")
                    
                    # Move SL to entry for remaining trades
                    if i == 0:  # First TP hit
                        entry_price = trade['entry']
                        logger.info(f"Moving SL to entry price {entry_price} for remaining trades")
                        
                        for next_trade in sorted_trades[1:]:
                            if next_trade['ticket'] in position_tickets:
                                position = positions_dict[next_trade['ticket']]
                                await self._modify_sl(next_trade, entry_price, position)
                    
                    trades.remove(trade)
            
            # Clean up empty symbols
            if not trades:
                del self.active_trades[symbol]
    
    async def _modify_sl(self, trade, new_sl, position):
        """Modify stop loss for a trade with better error handling"""
        try:
            if not validate_modify_sl(position, new_sl):
                return
                
            request = {
                "action": mt5.TRADE_ACTION_MODIFY,
                "symbol": position.symbol,
                "position": trade['ticket'],
                "sl": new_sl,
                "tp": position.tp,
                "type_time": mt5.ORDER_TIME_GTC
            }
            
            logger.info(f"Modifying SL for ticket {trade['ticket']} to {new_sl}")
            result = mt5.order_send(request)
            
            if result is None:
                error = mt5.last_error()
                logger.error(f"SL modification failed - Error: {error[0]}, {error[1]}")
                return
                
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                logger.error(f"SL modification failed - {result.comment} (code: {result.retcode})")
                return
                
            logger.info(f"Successfully modified SL to {new_sl} for ticket {trade['ticket']}")
            trade['sl'] = new_sl
                    
        except Exception as e:
            logger.error(f"Error modifying SL: {str(e)}")
    
    def add_trades(self, symbol, trades_info):
        """Add new trades to monitoring"""
        for trade in trades_info:
            position = mt5.positions_get(ticket=trade['ticket'])
            if position:
                trade['entry'] = position[0].price_open
                logger.info(f"Added trade {trade['ticket']} to monitoring. Entry: {trade['entry']}")
        self.active_trades[symbol].extend(trades_info)
