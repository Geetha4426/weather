"""
Weather Prediction Bot — Configuration

All settings with environment variable overrides.
Designed for Polymarket weather prediction markets.

Updated with real Polymarket data:
- Slug format: highest-temperature-in-{city}-on-{month}-{day}-{year}
- US cities use Fahrenheit, non-US use Celsius
- negativeRisk: true on all weather markets
- Thresholds: 15% entry edge, 45% exit edge
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Central configuration for the Weather Prediction bot."""

    # ═══════════════════════════════════════════════════════════════════
    # TELEGRAM
    # ═══════════════════════════════════════════════════════════════════
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

    # ═══════════════════════════════════════════════════════════════════
    # POLYMARKET WALLET
    # ═══════════════════════════════════════════════════════════════════
    POLY_PRIVATE_KEY = os.getenv('POLY_PRIVATE_KEY', '')
    POLY_SAFE_ADDRESS = os.getenv('POLY_SAFE_ADDRESS', '')
    POLY_FUNDER_ADDRESS = os.getenv('POLY_FUNDER_ADDRESS', '')
    POLY_PROXY_WALLET = os.getenv('POLY_PROXY_WALLET', '')
    POLY_API_KEY = os.getenv('POLY_API_KEY', '')
    POLY_API_SECRET = os.getenv('POLY_API_SECRET', '')
    POLY_PASSPHRASE = os.getenv('POLY_PASSPHRASE', '')
    POLY_SIGNATURE_TYPE = int(os.getenv('POLY_SIGNATURE_TYPE', '0'))
    POLY_CHAIN_ID = int(os.getenv('POLY_CHAIN_ID', '137'))

    # ═══════════════════════════════════════════════════════════════════
    # API ENDPOINTS
    # ═══════════════════════════════════════════════════════════════════
    GAMMA_API_URL = 'https://gamma-api.polymarket.com'
    CLOB_API_URL = 'https://clob.polymarket.com'
    DATA_API_URL = 'https://data-api.polymarket.com'

    # ═══════════════════════════════════════════════════════════════════
    # PROXY — NOT REQUIRED for EU (Netherlands) deployment
    # Only set if you are in a geoblocked country
    # ═══════════════════════════════════════════════════════════════════
    PROXY_URL = os.getenv('PROXY_URL', '')

    # ═══════════════════════════════════════════════════════════════════
    # TRADING MODE
    # ═══════════════════════════════════════════════════════════════════
    TRADING_MODE = os.getenv('TRADING_MODE', 'paper')  # 'paper' or 'live'
    STARTING_BALANCE = float(os.getenv('STARTING_BALANCE', '100.0'))
    POLYMARKET_MIN_ORDER_SIZE = 1.0  # $1 minimum (FOK)
    USE_FOK_ORDERS = os.getenv('USE_FOK_ORDERS', 'true').lower() == 'true'

    # ═══════════════════════════════════════════════════════════════════
    # WEATHER-SPECIFIC SETTINGS — Tuned from real market data
    # ═══════════════════════════════════════════════════════════════════

    # Cities to track — uses Polymarket slug names
    # Real slug cities: nyc, london, chicago, munich, lucknow, miami, seattle, atlanta, dallas
    WEATHER_CITIES = [
        c.strip().lower().replace(' ', '-')
        for c in os.getenv('WEATHER_CITIES',
            'nyc,london,chicago,miami,seattle,atlanta,dallas,munich,lucknow').split(',')
    ]

    # Scan intervals: dynamic based on resolution proximity (Improvement 5)
    WEATHER_SCAN_INTERVAL = int(os.getenv('WEATHER_SCAN_INTERVAL', '120'))
    WEATHER_SCAN_INTERVAL_FAST = int(os.getenv('WEATHER_SCAN_INTERVAL_FAST', '30'))   # Resolution day < 6h
    WEATHER_SCAN_INTERVAL_MED = int(os.getenv('WEATHER_SCAN_INTERVAL_MED', '60'))     # Resolution day 6-12h
    WEATHER_SCAN_INTERVAL_SLOW = int(os.getenv('WEATHER_SCAN_INTERVAL_SLOW', '300'))  # 2+ days out

    # Entry edge threshold: 15% (forecast_prob - market_price > 0.15 to BUY)
    WEATHER_MIN_EDGE = float(os.getenv('WEATHER_MIN_EDGE', '0.15'))

    # Exit edge threshold: 45% profit to take profit
    WEATHER_EXIT_EDGE = float(os.getenv('WEATHER_EXIT_EDGE', '0.45'))

    # Max position per trade: $2.00 (conservative)
    WEATHER_MAX_POSITION_USD = float(os.getenv('WEATHER_MAX_POSITION_USD', '2.0'))

    # Minimum market price to buy — skip junk outcomes below this
    # Outcomes at $0.01 = 1% chance → almost always lose
    WEATHER_MIN_MARKET_PRICE = float(os.getenv('WEATHER_MIN_MARKET_PRICE', '0.04'))

    # Minimum forecast probability to trade an outcome
    # Don't bet on outcomes our models say have < 10% chance
    WEATHER_MIN_FORECAST_PROB = float(os.getenv('WEATHER_MIN_FORECAST_PROB', '0.10'))

    # Position sizing as % of balance (used when max_position not set)
    WEATHER_POSITION_SIZE_PCT = float(os.getenv('WEATHER_POSITION_SIZE_PCT', '5.0'))

    # Max trades per scan run
    WEATHER_MAX_TRADES_PER_RUN = int(os.getenv('WEATHER_MAX_TRADES_PER_RUN', '5'))

    # Max positions per single weather event
    WEATHER_MAX_POSITIONS_PER_EVENT = int(os.getenv('WEATHER_MAX_POSITIONS_PER_EVENT', '3'))

    # Max total open positions
    WEATHER_MAX_TOTAL_POSITIONS = int(os.getenv('WEATHER_MAX_TOTAL_POSITIONS', '15'))

    # Days ahead to look for markets (today + N)
    WEATHER_LOOKAHEAD_DAYS = int(os.getenv('WEATHER_LOOKAHEAD_DAYS', '3'))

    # Confidence thresholds
    WEATHER_MIN_CONFIDENCE = float(os.getenv('WEATHER_MIN_CONFIDENCE', '0.40'))
    WEATHER_HIGH_CONFIDENCE = float(os.getenv('WEATHER_HIGH_CONFIDENCE', '0.75'))

    # Trend detection (from insights)
    WEATHER_TREND_DETECTION = os.getenv('WEATHER_TREND_DETECTION', 'true').lower() == 'true'

    # Risk management
    MAX_DAILY_LOSS_PCT = float(os.getenv('MAX_DAILY_LOSS_PCT', '20.0'))
    RISK_MODE = os.getenv('RISK_MODE', 'conservative')  # conservative, moderate, aggressive

    # ═══════════════════════════════════════════════════════════════════
    # WEATHER API KEYS (optional — Open-Meteo is free)
    # ═══════════════════════════════════════════════════════════════════
    TOMORROW_IO_API_KEY = os.getenv('TOMORROW_IO_API_KEY', '')
    WEATHERAPI_KEY = os.getenv('WEATHERAPI_KEY', '')
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY', '')

    # ═══════════════════════════════════════════════════════════════════
    # DATABASE
    # ═══════════════════════════════════════════════════════════════════
    DATABASE_PATH = os.getenv('DATABASE_PATH', 'data/weather_trades.db')

    # ═══════════════════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════════════════
    @classmethod
    def is_paper(cls) -> bool:
        return cls.TRADING_MODE.lower() == 'paper'

    @classmethod
    def is_live_ready(cls) -> bool:
        pk = cls.POLY_PRIVATE_KEY.strip() if cls.POLY_PRIVATE_KEY else ''
        return bool(pk)

    @classmethod
    def derive_wallet_address(cls) -> str:
        pk = cls.POLY_PRIVATE_KEY.strip() if cls.POLY_PRIVATE_KEY else ''
        if not pk:
            return ''
        try:
            from eth_account import Account
            if not pk.startswith('0x'):
                pk = '0x' + pk
            wallet = Account.from_key(pk)
            return wallet.address
        except Exception:
            return ''

    @classmethod
    def get_funder_address(cls) -> str:
        if cls.POLY_FUNDER_ADDRESS and cls.POLY_FUNDER_ADDRESS.strip():
            return cls.POLY_FUNDER_ADDRESS.strip()
        if cls.POLY_SIGNATURE_TYPE == 2:
            if cls.POLY_PROXY_WALLET and cls.POLY_PROXY_WALLET.strip():
                return cls.POLY_PROXY_WALLET.strip()
            return ''
        if cls.POLY_SIGNATURE_TYPE == 0:
            return cls.derive_wallet_address()
        return ''

    @classmethod
    def is_configured(cls) -> bool:
        return bool(cls.TELEGRAM_BOT_TOKEN)

    @classmethod
    def print_status(cls):
        mode = '📋 PAPER' if cls.is_paper() else '🔴 LIVE'
        pk_ok = bool(cls.POLY_PRIVATE_KEY and cls.POLY_PRIVATE_KEY.strip())
        wallet = cls.derive_wallet_address() if pk_ok else ''
        funder = cls.get_funder_address()

        print(f"\n{'='*60}", flush=True)
        print(f"🌤️ Weather Prediction Bot — Polymarket", flush=True)
        print(f"{'='*60}", flush=True)
        print(f"Mode: {mode}", flush=True)
        print(f"Risk: {cls.RISK_MODE.upper()}", flush=True)
        print(f"Cities: {', '.join(cls.WEATHER_CITIES)}", flush=True)
        print(f"Scan: every {cls.WEATHER_SCAN_INTERVAL}s", flush=True)
        print(f"Entry Edge: {cls.WEATHER_MIN_EDGE*100:.0f}% | Exit: {cls.WEATHER_EXIT_EDGE*100:.0f}%", flush=True)
        print(f"Max Position: ${cls.WEATHER_MAX_POSITION_USD:.2f}", flush=True)
        print(f"Max Trades/Run: {cls.WEATHER_MAX_TRADES_PER_RUN}", flush=True)
        print(f"Telegram: {'✅' if cls.TELEGRAM_BOT_TOKEN else '❌'}", flush=True)
        print(f"{'─'*60}", flush=True)
        print(f"🔐 LIVE TRADING:", flush=True)
        print(f"  Private Key: {'✅ set' if pk_ok else '❌ NOT SET'}", flush=True)
        if pk_ok:
            print(f"  Wallet: {wallet[:8]}...{wallet[-4:]}" if wallet else "  Wallet: ❌", flush=True)
            print(f"  Funder: {funder[:8]}...{funder[-4:]}" if funder else "  Funder: ⚠️ not set", flush=True)
            print(f"  Sig Type: {cls.POLY_SIGNATURE_TYPE}", flush=True)
        print(f"Balance: ${cls.STARTING_BALANCE:.2f}", flush=True)
        print(f"{'─'*60}", flush=True)
        apis = ['Open-Meteo (free)']
        if cls.TOMORROW_IO_API_KEY:
            apis.append('Tomorrow.io')
        if cls.WEATHERAPI_KEY:
            apis.append('WeatherAPI')
        if cls.OPENWEATHER_API_KEY:
            apis.append('OpenWeatherMap')
        print(f"Weather APIs: {', '.join(apis)}", flush=True)
        print(f"Trend Detection: {'✅' if cls.WEATHER_TREND_DETECTION else '❌'}", flush=True)
        print(f"{'='*60}\n", flush=True)
