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

async def check_pending_orders_status():
    """Check pending orders count and return status info."""
    try:
        orders = mt5.orders_get()
        if orders is None:
            logger.error(f"Failed to get orders. Error: {mt5.last_error()}")
            return 0, False, "ERROR"
            
        count = len(orders)
        
        # Check against thresholds
        status = "OK"
        is_warning = False
        
        if count > 190:
            status = "CRITICAL"
            is_warning = True
        elif count > 170:
            status = "WARNING"
            is_warning = True
        elif count > 150:
            status = "ATTENTION"
            is_warning = False
            
        # Count by symbol for detailed reporting
        symbols_count = {}
        for order in orders:
            if order.symbol not in symbols_count:
                symbols_count[order.symbol] = 0
            symbols_count[order.symbol] += 1
            
        top_symbols = sorted(symbols_count.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return count, is_warning, status, top_symbols
    except Exception as e:
        logger.error(f"Error checking pending orders: {e}")
        return 0, False, "ERROR", []
    
# Store original execute_trade function


async def execute_trade(signal):
    """Enhanced execute_trade that preserves group integrity."""
    try:
        # Check pending orders count
        count, is_warning, status, top_symbols = await check_pending_orders_status()
        
        if is_warning:
            # Log warning with details
            symbols_info = ", ".join([f"{s}:{c}" for s, c in top_symbols])
            logger.warning(f"⚠️ High pending order count: {count}/200 - Status: {status}")
            logger.warning(f"Top symbols: {symbols_info}")
            
            # If absolutely critical (>190), take action
            if status == "CRITICAL" and count > 190:
                # Check if we can fit this signal
                positions_needed = len(signal['tp'])
                remaining_capacity = 200 - count
                
                if positions_needed > remaining_capacity:
                    # Critical decision point:
                    # 1. Execute partial signal (risky for groups)
                    # 2. Skip entire signal
                    # 3. Adjust volume
                    
                    # Let's use the safest option - skip entire signal
                    logger.error(f"❌ SKIPPING signal for {signal['symbol']} - Cannot fit all {positions_needed} positions in remaining capacity ({remaining_capacity})")
                    
                    # Record this in a special log file for user review
                    with open('logs/skipped_signals.log', 'a') as f:
                        f.write(f"{datetime.now()} - Skipped {signal['symbol']} {signal['direction']} - Needed {positions_needed} positions, only {remaining_capacity} available\n")
                    
                    return False
                    
                # If we can fit it, proceed with caution
                logger.warning(f"Proceeding with signal execution despite high order count. Can fit all {positions_needed} positions.")
        
        # Generate a unique group identifier
        group_id = f"{signal['symbol']}_{signal['direction']}_{int(time.time())}"
        
        # Add group info to the signal
        signal['group_id'] = group_id
        
        # Call the original execute_trade with enhanced signal
        return await original_execute_trade(signal)
        
    except Exception as e:
        logger.error(f"Error in enhanced execute_trade: {e}")
        # Fall back to original implementation
        return await original_execute_trade(signal)

async def execute_trade_enhanced(signal):
    """Enhanced execute_trade that preserves group integrity."""
    try:
        # Check pending orders count
        count, is_warning, status, top_symbols = await check_pending_orders_status()
        
        if is_warning:
            # Log warning with details
            symbols_info = ", ".join([f"{s}:{c}" for s, c in top_symbols])
            logger.warning(f"⚠️ High pending order count: {count}/200 - Status: {status}")
            logger.warning(f"Top symbols: {symbols_info}")
            
            # If absolutely critical (>190), take action
            if status == "CRITICAL" and count > 190:
                # Check if we can fit this signal
                positions_needed = len(signal['tp'])
                remaining_capacity = 200 - count
                
                if positions_needed > remaining_capacity:
                    # Skip entire signal
                    logger.error(f"❌ SKIPPING signal for {signal['symbol']} - Cannot fit all {positions_needed} positions in remaining capacity ({remaining_capacity})")
                    
                    # Record this in a special log file
                    with open('logs/skipped_signals.log', 'a') as f:
                        f.write(f"{datetime.now()} - Skipped {signal['symbol']} {signal['direction']} - Needed {positions_needed} positions, only {remaining_capacity} available\n")
                    
                    return False
                    
                # If we can fit it, proceed with caution
                logger.warning(f"Proceeding with signal execution despite high order count. Can fit all {positions_needed} positions.")
        
        # Generate a unique group identifier
        group_id = f"{signal['symbol']}_{signal['direction']}_{int(time.time())}"
        
        # Add group info to the signal
        signal['group_id'] = group_id
        
        # Call the original function
        return await execute_trade(signal)
        
    except Exception as e:
        logger.error(f"Error in enhanced execute_trade: {e}")
        # Fall back to original implementation
        return await execute_trade(signal)
    
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
    """Process orders from the queue in batches with better error handling."""
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
                    logger.info(f"Processing {signal['symbol']} {signal['direction']}")
                    success = await execute_trade_internal(signal)
                    if not success:
                        logger.error(f"Failed: {signal['symbol']} {signal['direction']}")
                        # Try to get more specific error info
                        last_error = mt5.last_error()
                        if last_error:
                            logger.error(f"MT5 error: {last_error}")
                except Exception as e:
                    logger.error(f"❌ Error processing order from queue: {e}", exc_info=True)

            if order_queue:
                logger.info("Waiting before processing next batch...")
                await asyncio.sleep(10)  # Adjust delay as needed

    except Exception as e:
        logger.error(f"❌ Critical error in process_order_queue: {e}", exc_info=True)
    finally:
        queue_processing = False

async def execute_trade_original(signal):
    """Add trade signal to the order queue."""
    order_queue.append(signal)
    logger.info(f"Added {signal['symbol']} {signal['direction']} to order queue")
    
    # Start processing queue if not already running
    if not queue_processing:
        asyncio.create_task(process_order_queue())

async def execute_trade(signal):
    """Enhanced version that checks pending order limits"""
    try:
        # Get pending orders count
        orders = mt5.orders_get()
        count = len(orders) if orders is not None else 0
        
        # Simple limit check
        if count > 190:
            logger.warning(f"⚠️ CRITICAL: {count}/200 pending orders. Can't add more orders safely.")
            return False
        elif count > 170:
            logger.warning(f"⚠️ WARNING: High pending order count: {count}/200")
        
        # Add group info to signal
        signal['group_id'] = f"{signal['symbol']}_{int(time.time())}"
        
        # Call the original function
        return await execute_trade_original(signal)
        
    except Exception as e:
        logger.error(f"Error in enhanced execute_trade: {e}")
        # Fall back to original implementation
        return await execute_trade_original(signal)

async def execute_trade_internal(signal):
    """Execute trade in MT5 with improved error handling and detailed logging"""
    try:
        # Validate symbol
        symbol_info = validate_symbol(signal['symbol'])
        if symbol_info is None:
            logger.error(f"❌ Invalid symbol: {signal['symbol']}")
            return False
            
        logger.info(f"Symbol validation passed for {signal['symbol']}")

        # Check account info and margin
        account_info = mt5.account_info()
        if account_info:
            logger.info(f"Account margin: {account_info.margin_free}, Equity: {account_info.equity}")
        else:
            logger.warning(f"Could not retrieve account info: {mt5.last_error()}")

        # Calculate volume per trade based on number of TP levels
        base_volume = DEFAULT_VOLUME / signal['tp_count']
        volume_per_trade = calculate_valid_volume(symbol_info, base_volume, signal['symbol'])

        if volume_per_trade is None:
            logger.error(f"❌ Invalid volume for {signal['symbol']}")
            return False

        # Store trade information for monitoring
        trades_info = []
        success = False
        
        # Check pending orders count before placing new orders
        pending_orders_count = await get_pending_orders_count(signal['symbol'])
        if pending_orders_count >= 200:  # Adjust the limit as needed
            logger.warning(f"⚠️ Too many pending orders for {signal['symbol']}. Skipping new order.")
            return False

        # Place one trade for each TP level
        for tp in signal['tp']:
            try:
                # Log attempt details
                logger.info(f"Preparing order for {signal['symbol']} at TP {tp}")
                
                request = prepare_order_request(signal, volume_per_trade, tp)
                if request is None:
                    logger.error(f"❌ Failed to prepare order request for {signal['symbol']} at TP {tp}")
                    continue

                # Log order details before sending
                order_type_name = "LIMIT" if signal['is_limit'] else "MARKET"
                logger.info(f"Sending {order_type_name} order - Symbol: {signal['symbol']}, Direction: {signal['direction']}, Entry: {signal.get('entry', 'market')}, TP: {tp}, SL: {signal['sl']}")

                # Add a small delay before sending the order
                time.sleep(0.5)  # Adjust delay as needed

                max_retries = 3
                for attempt in range(max_retries):
                    result = mt5.order_send(request)

                    if result is None:
                        error_code = mt5.last_error()
                        logger.error(f"Attempt {attempt + 1}/{max_retries}: Order send returned None - Error: {error_code}")
                        await asyncio.sleep(2)  # Wait before retrying
                        continue
                        
                    if result.retcode == mt5.TRADE_RETCODE_DONE:
                        break  # Exit the retry loop if the order is successful
                    else:
                        logger.error(f"Attempt {attempt + 1}/{max_retries}: Order failed - Code: {result.retcode}, Comment: {result.comment}")
                        
                        # Log specific common errors
                        if result.retcode == mt5.TRADE_RETCODE_INVALID_PRICE:
                            logger.error(f"  - Invalid price: {request['price']}")
                        elif result.retcode == mt5.TRADE_RETCODE_INVALID_STOPS:
                            logger.error(f"  - Invalid stops: SL={request['sl']}, TP={request['tp']}")
                        elif result.retcode == mt5.TRADE_RETCODE_INVALID_VOLUME:
                            logger.error(f"  - Invalid volume: {request['volume']}")
                        
                        await asyncio.sleep(2)  # Wait before retrying
                else:
                    logger.error(f"❌ Order failed after {max_retries} attempts for {signal['symbol']} at TP {tp}")
                    continue
                
                if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                    trades_info.append({
                        'ticket': result.order,
                        'tp': tp,
                        'sl': signal['sl'],
                        'direction': signal['direction'],
                        'entry': signal.get('entry', 'market')
                    })
                    success = True
                    logger.info(f"✅ Order sent successfully for {signal['symbol']} at TP {tp}, Ticket: {result.order}")
                else:
                    logger.error(f"❌ Order failed for {signal['symbol']} at TP {tp}: Retcode={result.retcode if result else 'None'}, Comment={result.comment if result else 'No result'}")
            except Exception as e:
                logger.error(f"❌ Error executing trade at TP {tp}: {e}", exc_info=True)
                continue
        
        # Log summary
        if success:
            logger.info(f"Successfully placed {len(trades_info)}/{len(signal['tp'])} orders for {signal['symbol']}")
        else:
            logger.error(f"Failed to place any orders for {signal['symbol']}")
            
        return success
        
    except Exception as e:
        logger.error(f"❌ Error executing trade: {e}", exc_info=True)
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
    """Prepare order request with price validation and group tracking."""
    try:
        symbol_info = validate_symbol(signal['symbol'])
        if symbol_info is None:
            return None

        # Generate or use group ID
        group_id = signal.get('group_id', f"{signal['symbol']}_{int(time.time())}")

        # Add retry logic for tick data
        tick = None
        max_tick_retries = 3
        for attempt in range(max_tick_retries):
            tick = mt5.symbol_info_tick(signal['symbol'])
            if tick is not None:
                break
            logger.warning(f"Failed to get tick data for {signal['symbol']} (attempt {attempt + 1}/{max_tick_retries})")
            time.sleep(0.5)
        
        if tick is None:
            logger.error(f"❌ Could not retrieve tick data for {signal['symbol']} after {max_tick_retries} retries")
            return None

        if signal['is_limit']:
            # Handle limit orders
            order_type = mt5.ORDER_TYPE_BUY_LIMIT if 'BUY' in signal['direction'] else mt5.ORDER_TYPE_SELL_LIMIT
            price = signal['entry']
            
            # Get current bid/ask price
            current_price = tick.ask if order_type == mt5.ORDER_TYPE_BUY_LIMIT else tick.bid
            
            # Validate current price is reasonable
            if current_price == 0 or current_price is None:
                logger.error(f"❌ Invalid current price {current_price} for {signal['symbol']}")
                return None
            
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
            price = tick.ask if order_type == mt5.ORDER_TYPE_BUY else tick.bid
            
            # Validate market price
            if price == 0 or price is None:
                logger.error(f"❌ Invalid market price {price} for {signal['symbol']}")
                return None
                
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

        # Additional validation to prevent 0.0 values
        if price == 0 or sl == 0 or tp == 0:
            logger.error(f"❌ Invalid price values detected - Price: {price}, SL: {sl}, TP: {tp}")
            return None

        # Create a simple, safe comment with group ID
        tp_index = signal['tp'].index(tp) + 1 if tp in signal['tp'] else 0
        comment = f"G{group_id[:8]}_TP{tp_index}"
        
        # Ensure comment isn't too long
        if len(comment) > 28:  # MT5 has a limit around 32 chars
            comment = comment[:28]

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
            "comment": comment,
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
                       f"\n    TP: {tp}"
                       f"\n    Group: {group_id[:8]}")
        
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
# Keep your original execute_trade function as is
async def execute_trade(signal):
    """Add trade signal to the order queue with better error handling."""
    try:
        # Validate signal basic structure first
        required_fields = ['symbol', 'direction', 'sl', 'tp', 'tp_count']
        for field in required_fields:
            if field not in signal:
                logger.error(f"❌ Signal missing required field '{field}': {signal}")
                return False
        
        # Check if symbol exists in MT5
        symbol_info = mt5.symbol_info(signal['symbol'])
        if symbol_info is None:
            # Try using symbol validation to map it
            symbol_info = validate_symbol(signal['symbol'])
            if symbol_info is None:
                logger.error(f"❌ Symbol {signal['symbol']} not found in MT5 terminal")
                return False
            else:
                logger.info(f"Symbol mapped: {signal['symbol']} → {symbol_info.name}")
                signal['symbol'] = symbol_info.name
        
        # Check if trading is enabled for this symbol
        if not symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_FULL:
            logger.error(f"❌ Trading disabled for {signal['symbol']} (mode: {symbol_info.trade_mode})")
            return False
            
        # Check account state
        account_info = mt5.account_info()
        if account_info:
            logger.info(f"Account: Balance={account_info.balance}, Equity={account_info.equity}, Margin={account_info.margin}, Free Margin={account_info.margin_free}")
            if account_info.margin_free < 100:  # Adjust this threshold as needed
                logger.warning(f"⚠️ Low free margin: {account_info.margin_free}")
        
        # Add to queue and start processing
        order_queue.append(signal)
        logger.info(f"Added {signal['symbol']} {signal['direction']} to order queue")
        
        # Start processing queue if not already running
        if not queue_processing:
            asyncio.create_task(process_order_queue())
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in execute_trade: {e}", exc_info=True)
        return False

