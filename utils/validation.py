import MetaTrader5 as mt5
import logging

logger = logging.getLogger(__name__)

def validate_symbol(symbol):
    """Validate and format symbol name with improved indices handling and error logging"""
    try:
        # Common indices mappings (updated with broker-specific names)
        indices_map = {
            'US30': 'US30Cash',
            'US100': 'US100Cash',
            'DAX': 'GER40Cash',  # Updated mapping for DAX 40
            'OIL': 'OILCash',  # Assuming OILCash is used by your broker
            'NIKKEI': 'JP225Cash',  # Updated mapping for NIKKEI
            'GER40': 'GER40Cash' # Add mapping for GER40
        }

        # Check if symbol needs mapping
        mapped_symbol = indices_map.get(symbol, symbol) #Use original symbol if no mapping found

        # Perform a single check after mapping
        symbol_info = mt5.symbol_info(mapped_symbol)
        if symbol_info is not None:
            logger.info(f"✅ Found symbol: {mapped_symbol}")
            return symbol_info
        else:
            logger.error(f"❌ Symbol '{mapped_symbol}' not found in MT5. Please check your MT5 terminal for the correct symbol name.")
            return None

    except Exception as e:
        logger.error(f"❌ Error validating symbol '{symbol}': {e}")
        return None

def calculate_valid_volume(symbol_info, volume, symbol):
    """Calculate valid volume, handling different minimum lot sizes for indices"""
    try:
        min_volume = symbol_info.volume_min
        max_volume = symbol_info.volume_max
        volume_step = symbol_info.volume_step

        # Handle indices separately (adjust minimum lot size as needed)
        if symbol in ['US30Cash', 'US100Cash', 'GER40Cash', 'JP225Cash', 'OILCash']: #Updated list
            min_volume = 0.1  # Set minimum lot size for indices
            volume = max(volume, min_volume) #Ensure volume is at least minimum lot size

        # Round to nearest valid volume
        valid_volume = round(volume / volume_step) * volume_step
        
        # Ensure volume is within limits
        valid_volume = max(min_volume, min(valid_volume, max_volume))
        
        logger.info(f"📊 Volume calculated: {valid_volume}")
        return valid_volume
        
    except Exception as e:
        logger.error(f"❌ Error calculating valid volume: {e}")
        return None

def validate_modify_sl(position, new_sl):
    """Validate stop loss modification"""
    try:
        symbol_info = mt5.symbol_info(position.symbol)
        if symbol_info is None:
            logger.error(f"❌ Symbol info not found for {position.symbol}")
            return False
            
        # Check if new SL is valid
        if position.type == mt5.ORDER_TYPE_BUY and new_sl >= position.price_current:
            logger.error(f"❌ Invalid SL for buy position: {new_sl} >= {position.price_current}")
            return False
            
        if position.type == mt5.ORDER_TYPE_SELL and new_sl <= position.price_current:
            logger.error(f"❌ Invalid SL for sell position: {new_sl} <= {position.price_current}")
            return False
            
        logger.info(f"✅ SL modification validated: {new_sl}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error validating SL modification: {e}")
        return False

def price_to_pips(price, current_price, point, symbol):
    """Converts price difference to pips, handling JPY pairs"""
    if "JPY" in symbol:
        # JPY pairs have a different point size (usually 1/100)
        return round((price - current_price) / point * 100)  # Adjust as needed for your JPY point size
    else:
        return round((price - current_price) / point)
