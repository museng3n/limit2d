import asyncio
import logging
import time
import random
from telethon import TelegramClient, events
from config import API_ID, API_HASH, PHONE, CHANNEL_IDS
from mt5_handler import connect_mt5, execute_trade, start_market_monitor
from signal_parser import parse_signal
import os

# Setup logging with minimal output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    filename='trading_log.txt',
    filemode='w'  # Create new log file each time
)
logger = logging.getLogger(__name__)
# Add these lines right after the logger initialization
logger.info("="*50)
logger.info("Starting enhanced order management system")
logger.info("Order limits: WARNING at 170, CRITICAL at 190")
logger.info("="*50)
# Reduce verbosity of other loggers
logging.getLogger('telethon').setLevel(logging.WARNING)
logging.getLogger('mt5').setLevel(logging.WARNING)

# Session file path
SESSION_FILE = 'signal_session.session'

async def connect_mt5_with_retry(max_retries=6, retry_delay=5):
    retries = 0
    while retries < max_retries:
        try:
            if await connect_mt5():
                return True
        except Exception as e:
            logger.error(f"MT5 connection failed (attempt {retries+1}): {e}")
            await asyncio.sleep(retry_delay * (2**retries))
            retries += 1
    logger.critical("Failed to connect to MT5 after multiple retries.")
    return False

async def main():
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)
    
    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    
    try:
        await client.start(phone=PHONE)  # ← Connection happens HERE
        logger.info("✅ Connected to Telegram successfully")  # ← Move logging HERE
        logger.info(f"📡 Monitoring channels: {CHANNEL_IDS}")
        logger.info("🚀 Starting message listener...")
        
        # Connect to MT5
        mt5_connected = await connect_mt5_with_retry()
        if not mt5_connected:
            return
        
        # Rest of your code...
        # Start the market monitor for queued signals
        asyncio.create_task(start_market_monitor())
        
        @client.on(events.NewMessage(chats=CHANNEL_IDS))
        async def handle_new_message(event):
            try:
                signals = parse_signal(event.message.text)
                if signals:
                    if isinstance(signals, list):
                        for signal in signals:
                            logger.info(f"Processing {signal['symbol']} {signal['direction']}")
                            success = await execute_trade(signal)
                            if not success:
                                logger.error(f"Failed: {signal['symbol']} {signal['direction']}")
                    else:
                        logger.info(f"Processing {signals['symbol']} {signals['direction']}")
                        success = await execute_trade(signals)
                        if not success:
                            logger.error(f"Failed: {signals['symbol']} {signals['direction']}")
            except Exception as e:
                logger.exception(f"Error handling message: {e}") #Log full traceback
        
        logger.info("Bot started")
        await client.run_until_disconnected()
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        if client.is_connected():
            await client.disconnect()
            logger.info("Disconnected")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
