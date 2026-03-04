"""
Weather Prediction Bot — Entry Point

Runs the Telegram bot + weather trading engine concurrently.
The trading engine discovers weather markets, fetches forecasts,
runs strategies, and executes trades.
"""

import asyncio
import os
import sys
import signal
import time

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from weather_prediction.config import Config

# ═══════════════════════════════════════════════════════════════════
# PROXY SETUP — must happen BEFORE any HTTP requests
# ═══════════════════════════════════════════════════════════════════
if Config.PROXY_URL:
    os.environ['HTTP_PROXY'] = Config.PROXY_URL
    os.environ['HTTPS_PROXY'] = Config.PROXY_URL
    os.environ['http_proxy'] = Config.PROXY_URL
    os.environ['https_proxy'] = Config.PROXY_URL
    print(f"🌐 Proxy configured: {Config.PROXY_URL[:30]}...", flush=True)
else:
    print("🌐 No proxy configured (PROXY_URL not set)", flush=True)

from weather_prediction.data.weather_client import WeatherClient
from weather_prediction.data.weather_market_client import WeatherMarketClient
from weather_prediction.data.clob_client import ClobClient
from weather_prediction.data.database import Database
from weather_prediction.strategies.dynamic_picker import WeatherDynamicPicker
from weather_prediction.trading.paper_trader import PaperTrader
from weather_prediction.trading.live_trader import LiveTrader
from weather_prediction.bot.telegram_bot import TelegramBot


class WeatherTradingEngine:
    """Core engine — weather prediction trading on Polymarket."""

    def __init__(self):
        # Data layer
        self.weather_client = WeatherClient()
        self.weather_markets = WeatherMarketClient()
        self.clob = ClobClient()
        self.db = Database()

        # Trading
        self.paper_trader = PaperTrader(self.db)
        self.live_trader = LiveTrader(self.db)

        # Active mode
        self.trading_mode = Config.TRADING_MODE

        # Strategy
        self.strategy = WeatherDynamicPicker()

        # Telegram bot
        self.bot = TelegramBot(engine=self)

        # State
        self.is_running = False
        self._scan_task = None

    @property
    def active_trader(self):
        """Returns the currently active trader (paper or live)."""
        if self.trading_mode == 'live' and self.live_trader.is_ready:
            return self.live_trader
        return self.paper_trader

    def switch_mode(self, mode: str) -> tuple:
        """Switch trading mode. Returns (success, message)."""
        mode = mode.lower()
        if mode == 'live':
            if not self.live_trader.is_ready:
                return False, '❌ Live trader not initialized. Check POLY_PRIVATE_KEY.'
            self.trading_mode = 'live'
            return True, f'🔴 LIVE MODE activated — Balance: ${self.live_trader.balance:.2f}'
        elif mode == 'paper':
            self.trading_mode = 'paper'
            return True, f'📋 Paper mode activated — Balance: ${self.paper_trader.balance:.2f}'
        return False, f'Unknown mode: {mode}'

    async def init(self):
        """Initialize all components."""
        Config.print_status()
        await self.db.init()

        # Check Polymarket geoblock
        try:
            import requests
            geo = requests.get('https://polymarket.com/api/geoblock', timeout=5).json()
            ip = geo.get('ip', '?')
            country = geo.get('country', '?')
            blocked = geo.get('blocked', True)
            if blocked:
                print(f"🚫 GEOBLOCKED! IP: {ip} | Country: {country}", flush=True)
                print(f"⚠️ Orders will be REJECTED. Set PROXY_URL to a non-blocked country.", flush=True)
            else:
                print(f"✅ Geoblock OK — IP: {ip} | Country: {country}", flush=True)
        except Exception as e:
            print(f"⚠️ Geoblock check failed: {e}", flush=True)

        # Initialize live trader
        live_ok = await self.live_trader.init()
        if live_ok:
            real_bal = await self.live_trader.fetch_balance()
            if real_bal and real_bal > 0:
                print(f"💰 Live balance: ${real_bal:.2f}", flush=True)
            else:
                print(f"⚠️ Using configured balance: ${Config.STARTING_BALANCE:.2f}", flush=True)
        else:
            print("📋 Paper trading only (no live credentials)", flush=True)
            self.trading_mode = 'paper'

        # Calibrate forecasts with historical data
        print("📊 Calibrating forecast bias...", flush=True)
        for city in Config.WEATHER_CITIES:
            accuracy = self.weather_client.get_historical_accuracy(city)
            if accuracy:
                print(f"  {city.title()}: bias={accuracy['bias']:+.1f}°C, "
                      f"MAE={accuracy['mae']:.1f}°C ({accuracy['days']} days)", flush=True)
            else:
                print(f"  {city.title()}: no historical data yet", flush=True)

        # Setup Telegram bot
        if Config.TELEGRAM_BOT_TOKEN:
            await self.bot.setup()
        else:
            print("⚠️ No TELEGRAM_BOT_TOKEN — running without Telegram", flush=True)

        print(f"✅ All components initialized — Mode: {self.trading_mode.upper()}", flush=True)

    async def start(self):
        """Start the trading loop."""
        if self.is_running:
            return

        self.is_running = True
        print(f"▶️ Weather trading started — Cities: {Config.WEATHER_CITIES}", flush=True)

        self._scan_task = asyncio.create_task(self._scan_loop())

    async def stop(self):
        """Stop trading."""
        self.is_running = False
        if self._scan_task:
            self._scan_task.cancel()
        print("⏹️ Weather trading stopped", flush=True)

    async def _scan_loop(self):
        """
        Main trading loop — scans for weather markets, runs strategies,
        executes trades.
        """
        print("🔄 Weather scan loop started", flush=True)
        scan_count = 0
        _last_pnl_report = time.time()

        while self.is_running:
            try:
                scan_count += 1

                # Discover weather markets
                markets = self.weather_markets.discover_markets()

                if not markets:
                    if scan_count <= 3:
                        print("⚠️ No weather markets found — retrying in 60s", flush=True)
                    await asyncio.sleep(60)
                    continue

                # Get forecasts for each market
                for market in markets:
                    if not self.is_running:
                        break

                    city = market.get('city', '')
                    target_date_str = market.get('date', '')

                    # Parse target date
                    from datetime import date as date_type
                    try:
                        parts = target_date_str.split('-')
                        target_date = date_type(int(parts[0]), int(parts[1]), int(parts[2]))
                    except (ValueError, IndexError):
                        target_date = None

                    # Get ensemble forecast for this city/date
                    forecast = self.weather_client.get_ensemble_forecast(city, target_date)
                    if not forecast:
                        continue

                    # Set fallback prices from market data
                    for outcome in market.get('outcomes', []):
                        yes_token = outcome.get('token_id_yes', '')
                        no_token = outcome.get('token_id_no', '')
                        if yes_token:
                            self.clob.set_fallback_price(yes_token, outcome.get('price_yes', 0.5))
                        if no_token:
                            self.clob.set_fallback_price(no_token, outcome.get('price_no', 0.5))

                    seconds_remaining = self.weather_markets.get_seconds_until_resolution(market)

                    context = {
                        'clob': self.clob,
                        'weather_client': self.weather_client,
                        'forecast': forecast,
                        'seconds_remaining': seconds_remaining,
                    }

                    # Run strategies
                    try:
                        signals = await self.strategy.analyze(market, context)
                    except Exception as e:
                        if scan_count <= 3:
                            print(f"❌ Strategy error: {e}", flush=True)
                        continue

                    # Execute signals
                    for signal in signals:
                        if not self.is_running:
                            break

                        # Check per-event position limit
                        event_positions = sum(
                            1 for p in self.active_trader.get_open_positions()
                            if p.get('city') == city and p.get('target_date') == target_date_str
                        )
                        if event_positions >= Config.WEATHER_MAX_POSITIONS_PER_EVENT:
                            continue

                        mode_tag = '🔴LIVE' if self.trading_mode == 'live' else '📋PAPER'
                        print(
                            f"🎯 [{mode_tag}] Signal: {signal.strategy} → "
                            f"{signal.city.title()} {signal.direction} "
                            f"{signal.outcome_label} @ ${signal.entry_price:.3f} "
                            f"(conf={signal.confidence:.0%})",
                            flush=True
                        )

                        trade = await self.active_trader.execute_signal(signal)
                        if trade:
                            await self.bot.send_trade_alert(trade)
                            print(f"✅ Trade executed: {trade.get('city', '')} "
                                  f"{trade.get('outcome_label', '')}", flush=True)

                # Check open positions
                current_prices = {}
                for market in markets:
                    for outcome in market.get('outcomes', []):
                        yes_token = outcome.get('token_id_yes', '')
                        no_token = outcome.get('token_id_no', '')
                        if yes_token:
                            price = self.clob.get_price(yes_token)
                            if price:
                                current_prices[yes_token] = price
                        if no_token:
                            price = self.clob.get_price(no_token)
                            if price:
                                current_prices[no_token] = price

                closed = await self.active_trader.check_positions(current_prices)
                for trade in closed:
                    await self.bot.send_close_alert(trade)

                # Log status
                if scan_count <= 3 or scan_count % 20 == 0:
                    summary = self.active_trader.get_summary()
                    mode_tag = '🔴LIVE' if self.trading_mode == 'live' else '📋PAPER'
                    print(
                        f"📊 [{mode_tag}] Scan #{scan_count} | "
                        f"Markets: {len(markets)} | "
                        f"Balance: ${summary['balance']:.2f} | "
                        f"Trades: {summary['total_trades']} | "
                        f"Open: {summary['open_positions']}",
                        flush=True
                    )

                # PnL report every 15 minutes
                if time.time() - _last_pnl_report >= 900:
                    _last_pnl_report = time.time()
                    summary = self.active_trader.get_summary()
                    positions = self.active_trader.get_open_positions()
                    await self.bot.send_pnl_report(summary, positions)

                await asyncio.sleep(Config.WEATHER_SCAN_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Scan error: {e}", flush=True)
                import traceback
                traceback.print_exc()
                await asyncio.sleep(10)

        print("🔄 Scan loop stopped", flush=True)


async def main():
    """Entry point."""
    engine = WeatherTradingEngine()
    await engine.init()

    # Run Telegram bot
    if engine.bot.app:
        print("🤖 Starting Telegram bot...", flush=True)
        await engine.bot.app.initialize()

        try:
            from telegram import BotCommand
            await engine.bot.app.bot.set_my_commands([
                BotCommand("start", "Welcome & menu"),
                BotCommand("trade", "Start trading"),
                BotCommand("stop", "Stop trading"),
                BotCommand("status", "Position & P&L status"),
                BotCommand("balance", "Check balance"),
                BotCommand("weather", "Markets & forecasts"),
                BotCommand("forecast", "Detailed forecast"),
                BotCommand("markets", "Scan live markets"),
                BotCommand("history", "Trade history"),
                BotCommand("mode", "Switch paper/live"),
            ])
        except Exception as e:
            print(f"⚠️ Commands setup: {e}", flush=True)

        await engine.bot.app.start()
        await engine.bot.app.updater.start_polling(drop_pending_updates=True)
        print("✅ Telegram bot is polling!", flush=True)

        # Startup notification
        if Config.TELEGRAM_CHAT_ID:
            try:
                mode = "🔴 LIVE" if engine.trading_mode == 'live' else "📋 PAPER"
                msg = (
                    f"🌤️ *Weather Prediction Bot is ONLINE*\n\n"
                    f"Mode: {mode}\n"
                    f"Cities: {', '.join(Config.WEATHER_CITIES).title()}\n"
                    f"Min Edge: {Config.WEATHER_MIN_EDGE*100:.0f}%\n"
                    f"Balance: ${engine.active_trader.balance:.2f}\n\n"
                    f"Type /trade to start!\n"
                    f"Type /weather to see markets."
                )
                await engine.bot.app.bot.send_message(
                    chat_id=Config.TELEGRAM_CHAT_ID,
                    text=msg,
                    parse_mode='Markdown'
                )
            except Exception as e:
                print(f"⚠️ Startup msg: {e}", flush=True)
    else:
        print("⚠️ No Telegram — auto-starting trading...", flush=True)
        await engine.start()

    print("\n💡 Bot is ready! Send /trade in Telegram to start.\n", flush=True)

    # Keep running
    try:
        stop_event = asyncio.Event()

        def handle_signal(*args):
            stop_event.set()

        if sys.platform != 'win32':
            loop = asyncio.get_event_loop()
            loop.add_signal_handler(signal.SIGINT, handle_signal)
            loop.add_signal_handler(signal.SIGTERM, handle_signal)

        await stop_event.wait()

    except (KeyboardInterrupt, SystemExit):
        print("\n⏹️ Shutting down...")

    finally:
        await engine.stop()
        if engine.bot.app:
            try:
                await engine.bot.app.updater.stop()
                await engine.bot.app.stop()
                await engine.bot.app.shutdown()
            except Exception:
                pass
        await engine.db.close()
        print("👋 Goodbye!")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bye!")
