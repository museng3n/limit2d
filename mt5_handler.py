import MetaTrader5 as mt5
import logging
import asyncio
from config import MT5_LOGIN, MT5_PASSWORD, MT5_SERVER, MT5_PATH, DEFAULT_VOLUME, DEVIATION, MAGIC_NUMBER
from utils.validation import validate_symbol, calculate_valid_volume, price_to_pips
from decimal import Decimal
import os
import time
import asyncio
from collections import deque, defaultdict
logger = logging.getLogger(__name__)

async def connect_mt5():
    """Initialize MT5 connection asynchronously"""
    try:
        # Terminate any existing MT5 connections
        mt5.shutdown()
        
        # Initialize with specific terminal path
        if not mt5.initialize(path=MT5_PATH):
            logger.error(f"MT5 initialization failed")
            return False
        
        # Connect to specific TNFX server
        authorized = mt5.login(
            login=MT5_LOGIN,
            password=MT5_PASSWORD,
            server=MT5_SERVER
        )
        
        if not authorized:
            logger.error(f"MT5 login failed")
            mt5.shutdown()
            return False
        
        logger.info(f"Connected to MT5")
        return True
        
    except Exception as e:
        logger.error(f"Error connecting to MT5: {str(e)}")
        return False

# Global order queue
order_queue = deque()
queue_processing = False

async def get_pending_orders_count(symbol):
    """Get the number of pending orders for a specific symbol."""
    try:
        orders = mt5.orders_get(symbol=symbol)
        if orders is None:
            logger.error(f"Failed to retrieve orders for {symbol}")
            return 0
        pending_orders = [order for order in orders if order.state == mt5.ORDER_STATE_PLACED]
        return len(pending_orders)
    except Exception as e:
        logger.error(f"Error getting pending orders count: {e}")
        return 0


async def get_symbol_info(symbol):
    """Get symbol information from MT5."""
    try:
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            logger.error(f"Symbol '{symbol}' not found")
            return None
        return symbol_info
    except Exception as e:
        logger.error(f"Error getting symbol info: {e}")
        return None



async def process_order_queue():
    """Process orders from the queue in batches."""
    global queue_processing
    if queue_processing:
        return

    queue_processing = True
    try:
        while order_queue:
            batch = []
            batch_size = 250  # Adjust batch size as needed
            while order_queue and len(batch) < batch_size:
                batch.append(order_queue.popleft())

            logger.info(f"Processing batch of {len(batch)} orders")

            for signal in batch:
                try:
                    await execute_trade_internal(signal)
                except Exception as e:
                    logger.error(f"Error processing order from queue: {e}")

            if order_queue:
                logger.info("Waiting before processing next batch...")
                await asyncio.sleep(10)  # Adjust delay as needed

    finally:
        queue_processing = False

async def execute_trade(signal):
    """Add trade signal to the order queue."""
    order_queue.append(signal)
    logger.info(f"Added {signal['symbol']} {signal['direction']} to order queue")
    
    # Start processing queue if not already running
    if not queue_processing:
        asyncio.create_task(process_order_queue())

async def execute_trade_internal(signal):
    """Execute trade in MT5 with improved error handling and detailed logging"""
    try:
        success = await place_trade(signal)
        if not success:
            logger.error(f"Failed: {signal['symbol']} {signal['direction']}")
    except Exception as e:
        logger.error(f"Error executing trade: {e}")
        return False

async def execute_trade(signal):
    """Execute trade in MT5 with improved error handling and detailed logging"""
    try:
        symbol_info = validate_symbol(signal['symbol'])
        if symbol_info is None:
            logger.error(f"Invalid symbol: {signal['symbol']}")
            return False

        # Calculate volume per trade based on number of TP levels
        base_volume = DEFAULT_VOLUME / signal['tp_count']
        volume_per_trade = calculate_valid_volume(symbol_info, base_volume, signal['symbol']) #Added symbol argument

        if volume_per_trade is None:
            logger.error(f"Invalid volume for {signal['symbol']}")
            return False

        # Store trade information for monitoring
        trades_info = []
        success = False
        
        # Check pending orders count before placing new orders
        pending_orders_count = await get_pending_orders_count(signal['symbol'])
        if pending_orders_count >= 200:  # Adjust the limit as needed
            logger.warning(f"Too many pending orders for {signal['symbol']}. Skipping new order.")
            return False

        # Place one trade for each TP level
        for tp in signal['tp']:
            try:
                request = prepare_order_request(signal, volume_per_trade, tp)
                if request is None:
                    continue

                # Add a small delay before sending the order
                time.sleep(0.5)  # Adjust delay as needed

                max_retries = 3
                for attempt in range(max_retries):
                    result = mt5.order_send(request)

                    if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                        break  # Exit the retry loop if the order is successful
                    else:
                        logger.error(f"Order failed (attempt {attempt + 1}/{max_retries}): Retcode={result.retcode}, Comment={result.comment}")
                        await asyncio.sleep(2)  # Wait before retrying
                else:
                    logger.error(f"Order failed after multiple retries for {signal['symbol']} at TP {tp}")
                    continue
                
                if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                    trades_info.append({
                        'ticket': result.order,
                        'tp': tp,
                        'sl': signal['sl'],
                        'direction': signal['direction'],
                        'entry': signal['entry'] #Added entry price
                    })
                    success = True
                    logger.info(f"Order sent successfully for {signal['symbol']} at TP {tp}, Ticket: {result.order}")
                else:
                    logger.error(f"Order failed for {signal['symbol']} at TP {tp}: Retcode={result.retcode}, Comment={result.comment}")
            except Exception as e:
                logger.error(f"Error executing trade at TP {tp}: {e}")
                continue
        
        return success
        
    except Exception as e:
        logger.error(f"Error executing trade: {e}")
        return False

def prepare_order_request(signal, volume, tp):
    """Prepare order request with price validation for limit orders in pips and detailed logging"""
    try:
        symbol_info = validate_symbol(signal['symbol'])
        if symbol_info is None:
            return None

        if signal['is_limit']:
            # Handle limit orders
            order_type = mt5.ORDER_TYPE_BUY_LIMIT if 'BUY' in signal['direction'] else mt5.ORDER_TYPE_SELL_LIMIT
            price = signal['entry']
            
            # Get current bid/ask price
            tick = mt5.symbol_info_tick(signal['symbol'])
            if tick is None:
                # Retry getting tick data
                tick = mt5.symbol_info_tick(signal['symbol'])
                if tick is None:
                    logger.error(f"❌ Could not retrieve tick data for {signal['symbol']} after retry")
                    return None
                logger.error(f"❌ Could not retrieve tick data for {signal['symbol']}")
                return None
            current_price = tick.ask if order_type == mt5.ORDER_TYPE_BUY_LIMIT else tick.bid
            
            #Convert to pips
            point = symbol_info.point
            limit_price_pips = price_to_pips(price, current_price, point, signal['symbol'])
            current_price_pips = price_to_pips(current_price, current_price, point, signal['symbol'])

            # Check for sufficient deviation in pips
            if abs(limit_price_pips - current_price_pips) < DEVIATION:
                logger.error(f"❌ Limit price {price} is too close to current price {current_price} for {signal['symbol']}")
                return None
                
            action = mt5.TRADE_ACTION_PENDING
            logger.info(f"📋 Preparing LIMIT order at {price}")
        else:
            # Handle immediate orders
            order_type = mt5.ORDER_TYPE_BUY if 'BUY' in signal['direction'] else mt5.ORDER_TYPE_SELL
            tick = mt5.symbol_info_tick(signal['symbol'])
            if tick is None:
                # Retry getting tick data
                tick = mt5.symbol_info_tick(signal['symbol'])
                if tick is None:
                    logger.error(f"❌ Could not retrieve tick data for {signal['symbol']} after retry")
                    return None
                logger.error(f"❌ Could not retrieve tick data for {signal['symbol']}")
                return None
            price = tick.ask if order_type == mt5.ORDER_TYPE_BUY else tick.bid
            action = mt5.TRADE_ACTION_DEAL
            logger.info(f"📋 Preparing MARKET order at current price")
            
        # Validate SL and TP prices
        if not validate_sl_tp_prices(price, signal['sl'], tp, symbol_info, order_type):
            return None

        # Adjust SL and TP to nearest price increment
        price_increment = symbol_info.point
        sl = round_to_increment(signal['sl'], price_increment)
        tp = round_to_increment(tp, price_increment)

        logger.info(f"📈 Adjusted SL: {sl}, TP: {tp} to nearest increment")


        request = {
            "action": action,
            "symbol": signal['symbol'],
            "volume": volume,
            "type": order_type,
            "price": price,
            "sl": sl,
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
                       f"\n    SL: {sl}"
                       f"\n    TP: {tp}")
        
        return request
        
    except Exception as e:
        logger.error(f"❌ Error preparing order request: {e}")
        return None

def validate_sl_tp_prices(price, sl, tp, symbol_info, order_type):
    """Validates SL and TP prices based on order type and minimum distance."""
    min_distance = symbol_info.trade_stops_level * symbol_info.point

    if order_type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_BUY_LIMIT):
        if sl >= price:
            logger.error(f"❌ Invalid SL: {sl} >= price {price}")
            return False
        if tp <= price:
            logger.error(f"❌ Invalid TP: {tp} <= price {price}")
            return False
        if abs(sl - price) < min_distance or abs(tp - price) < min_distance:
            logger.error(f"❌ SL or TP too close to price. Min distance: {min_distance}")
            return False
    elif order_type in (mt5.ORDER_TYPE_SELL, mt5.ORDER_TYPE_SELL_LIMIT):
        if sl <= price:
            logger.error(f"❌ Invalid SL: {sl} <= price {price}")
            return False
        if tp >= price:
            logger.error(f"❌ Invalid TP: {tp} >= price {price}")
            return False
        if abs(price - sl) < min_distance or abs(price - tp) < min_distance:
            logger.error(f"❌ SL or TP too close to price. Min distance: {min_distance}")
            return False
    return True

def round_to_increment(price, increment):
    """Rounds the price to the nearest increment."""
    decimal_increment = Decimal(str(increment))
    decimal_price = Decimal(str(price))
    
    # Use quantize to round to the nearest increment
    rounded_price = decimal_price.quantize(decimal_increment)
    
    # Convert back to float
    return float(rounded_price)
