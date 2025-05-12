import re
import logging

logger = logging.getLogger(__name__)

def parse_signal(message_text):
    """Parse trading signal from message text, handling Cash suffix in symbols"""
    try:
        lines = message_text.split('\n')
        
        # Extract symbol - Updated pattern to handle cash indices and potential variations
        symbol_match = re.search(r'([A-Z0-9]+)', lines[0].strip()) #Simplified regex
        if not symbol_match:
            logger.error("❌ Symbol not found in message")
            return None
        symbol = symbol_match.group(1)
        
        #Check for variations and map if necessary
        indices_map = {
            'US30': 'US30Cash',
            'US100': 'US100Cash',
            'DAX': 'GER40Cash',
            'OIL': 'OILCash',
            'NIKKEI': 'JP225Cash',
            'GER40': 'GER40Cash'
        }
        symbol = indices_map.get(symbol, symbol) #Use original symbol if no mapping found

        logger.info(f"📊 Processing signal for {symbol}")
        
        # Find immediate orders (NOW) and limit orders
        now_orders = re.findall(r'(BUY|SELL)\s+NOW', message_text, re.IGNORECASE)
        limit_orders = re.findall(r'(BUY|SELL)\s+limit\s+from\s+(\d+(?:\.\d+)?)', message_text, re.IGNORECASE)
        
        if not now_orders and not limit_orders:
            logger.error(f"❌ No valid orders found for {symbol}")
            return None
            
        # Find TPs - Updated pattern to handle whole numbers
        tp_matches = re.findall(r'(?:TP\d*|Tp\d*)\s*[@]?\s*(\d+(?:\.\d+)?)', message_text)
        if not tp_matches:
            logger.error(f"❌ No take profit levels found for {symbol}")
            return None
        tp_levels = [float(tp) for tp in tp_matches]
        logger.info(f"📈 Take profit levels: {tp_levels}")
        
        # Find SL - Updated pattern to handle whole numbers
        sl_match = re.search(r'(?:SL|SI)\s*[@]?\s*(\d+(?:\.\d+)?)', message_text, re.IGNORECASE)
        if not sl_match:
            logger.error(f"❌ Stop loss not found for {symbol}")
            return None
        sl = float(sl_match.group(1))
        logger.info(f"🛑 Stop loss: {sl}")
        
        signals = []
        
        # Process immediate orders
        for direction in now_orders:
            signal = {
                'symbol': symbol,
                'direction': direction.upper(),
                'is_limit': False,
                'entry': None,  # Will be determined at execution
                'sl': sl,
                'tp': tp_levels,
                'tp_count': len(tp_levels)
            }
            signals.append(signal)
            logger.info(f"✅ Added {direction} NOW order for {symbol}")
        
        # Process limit orders
        for direction, entry in limit_orders:
            signal = {
                'symbol': symbol,
                'direction': f"{direction.upper()} LIMIT",
                'is_limit': True,
                'entry': float(entry),
                'sl': sl,
                'tp': tp_levels,
                'tp_count': len(tp_levels)
            }
            signals.append(signal)
            logger.info(f"✅ Added {direction} LIMIT order at {entry} for {symbol}")
        
        return signals
        
    except Exception as e:
        logger.error(f"❌ Error parsing signal: {str(e)}")
        logger.error(f"📝 Message content: {message_text}")
        return None
