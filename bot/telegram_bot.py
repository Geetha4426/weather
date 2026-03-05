"""
Telegram Bot — Weather Prediction Trading

Commands:
  /start    — Welcome + status
  /trade    — Start auto-trading
  /stop     — Stop trading
  /status   — Positions & P&L
  /balance  — Current balance
  /weather  — Show markets & forecasts
  /forecast — Show forecast for a city
  /markets  — Scan live weather markets
  /history  — Trade history
  /mode     — Switch paper/live
"""

import asyncio
from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.config import Config


def _md_escape(text: str) -> str:
    """Escape Telegram MarkdownV1 special characters in dynamic text."""
    for ch in ('_', '*', '`', '['):
        text = text.replace(ch, f'\\{ch}')
    return text


class TelegramBot:
    """Telegram bot for the weather prediction trader."""

    def __init__(self, engine=None):
        self.engine = engine
        self.app = None

    async def setup(self):
        """Build the Telegram application."""
        if not Config.TELEGRAM_BOT_TOKEN:
            print("⚠️ No TELEGRAM_BOT_TOKEN — bot disabled", flush=True)
            return

        self.app = Application.builder().token(Config.TELEGRAM_BOT_TOKEN).build()

        # Commands
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("trade", self.cmd_trade))
        self.app.add_handler(CommandHandler("stop", self.cmd_stop))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("balance", self.cmd_balance))
        self.app.add_handler(CommandHandler("weather", self.cmd_weather))
        self.app.add_handler(CommandHandler("forecast", self.cmd_forecast))
        self.app.add_handler(CommandHandler("markets", self.cmd_markets))
        self.app.add_handler(CommandHandler("history", self.cmd_history))
        self.app.add_handler(CommandHandler("mode", self.cmd_mode))
        self.app.add_handler(CommandHandler("live", self.cmd_live))
        self.app.add_handler(CommandHandler("paper", self.cmd_paper))
        self.app.add_handler(CommandHandler("risk", self.cmd_risk))
        self.app.add_handler(CommandHandler("ml", self.cmd_ml))
        self.app.add_handler(CommandHandler("calibration", self.cmd_calibration))

        # Callbacks
        self.app.add_handler(CallbackQueryHandler(self.cb_handler))

        async def error_handler(update, context):
            print(f"⚠️ Bot error: {context.error}", flush=True)
        self.app.add_error_handler(error_handler)

        print("🤖 Telegram bot configured", flush=True)

    # ═══════════════════════════════════════════════════════════════════
    # COMMANDS
    # ═══════════════════════════════════════════════════════════════════

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Welcome message."""
        mode = '🔴 LIVE' if self.engine and self.engine.trading_mode == 'live' else '📋 PAPER'
        cities = ', '.join(Config.WEATHER_CITIES).title()

        msg = (
            f"🌤️ *Weather Prediction Bot*\n\n"
            f"Mode: {mode}\n"
            f"Cities: {cities}\n"
            f"Min Edge: {Config.WEATHER_MIN_EDGE*100:.0f}%\n\n"
            f"*Commands:*\n"
            f"/trade — Start auto-trading\n"
            f"/stop — Stop trading\n"
            f"/weather — Markets & forecasts\n"
            f"/forecast — Detailed forecast\n"
            f"/status — Positions & P&L\n"
            f"/balance — Check balance\n"
            f"/markets — Scan live markets\n"
            f"/history — Trade history\n"
            f"/risk — Risk manager status\n"
            f"/ml — ML module stats\n"
            f"/calibration — Confidence calibration\n"
            f"/mode — Switch paper/live\n"
        )
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_trade(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start trading."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        if self.engine.is_running:
            await update.message.reply_text("⚡ Already running!")
            return

        await self.engine.start()
        mode = '🔴 LIVE' if self.engine.trading_mode == 'live' else '📋 PAPER'
        await update.message.reply_text(
            f"▶️ Trading started!\n"
            f"Mode: {mode}\n"
            f"Scanning for weather opportunities..."
        )

    async def cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Stop trading."""
        if self.engine:
            await self.engine.stop()
        await update.message.reply_text("⏹️ Trading stopped")

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show positions and P&L."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        trader = self.engine.active_trader
        summary = trader.get_summary()
        positions = trader.get_open_positions()

        mode = '🔴 LIVE' if self.engine.trading_mode == 'live' else '📋 PAPER'
        msg = (
            f"📊 *Status* [{mode}]\n\n"
            f"Balance: ${summary['balance']:.2f}\n"
            f"Trades: {summary['total_trades']} "
            f"(W:{summary['wins']} L:{summary['losses']})\n"
            f"Win Rate: {summary['win_rate']:.0f}%\n"
            f"Total P&L: ${summary['total_pnl']:+.4f}\n"
            f"Open: {len(positions)}\n"
        )

        # ML stats
        if hasattr(self.engine, 'risk_manager'):
            rm = self.engine.risk_manager
            msg += (
                f"\n*Risk Manager:*\n"
                f"  Daily P&L: ${rm.daily_pnl:+.2f}\n"
                f"  Portfolio Heat: {rm.get_portfolio_heat():.0f}%\n"
                f"  Circuit Breaker: {'🔴 TRIPPED' if rm.is_circuit_broken() else '🟢 OK'}\n"
            )

        if positions:
            msg += "\n*Open Positions:*\n"
            for pos in positions[:5]:
                msg += (
                    f"  🌡️ {pos.get('city', '').title()} "
                    f"{pos.get('outcome_label', '')} "
                    f"@ ${pos.get('entry_price', 0):.3f}\n"
                )

        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show balance."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        trader = self.engine.active_trader
        balance = trader.balance if hasattr(trader, 'balance') else 0

        mode = '🔴 LIVE' if self.engine.trading_mode == 'live' else '📋 PAPER'
        await update.message.reply_text(
            f"💰 *Balance* [{mode}]\n\n"
            f"Available: ${balance:.2f}\n"
            f"Open positions: {len(trader.get_open_positions())}",
            parse_mode='Markdown'
        )

    async def cmd_weather(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show weather markets and forecasts summary."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        msg = "🌤️ *Weather Markets*\n\n"

        markets = self.engine.weather_markets.discover_markets()
        if not markets:
            msg += "No active weather markets found.\n"
        else:
            for market in markets[:5]:
                city = market.get('city', '').title()
                date = market.get('date', '')
                outcomes = market.get('outcomes', [])
                msg += f"📍 *{city}* — {date}\n"
                msg += f"  Outcomes: {len(outcomes)}\n"

                # Show top 3 priced outcomes
                sorted_outcomes = sorted(
                    outcomes,
                    key=lambda x: x.get('price_yes', 0),
                    reverse=True
                )
                for o in sorted_outcomes[:3]:
                    label = o.get('label', '')
                    price = o.get('price_yes', 0)
                    msg += f"  🌡️ {label}: ${price:.2f}\n"
                msg += "\n"

        # Show forecast if available
        for city in Config.WEATHER_CITIES[:2]:
            forecast = self.engine.weather_client.get_forecast(city)
            if forecast:
                msg += (
                    f"📊 *Forecast: {forecast['city_name']}*\n"
                    f"  Max: {forecast['max_temp']}{forecast['unit_symbol']} | "
                    f"Min: {forecast['min_temp']}{forecast['unit_symbol']}\n\n"
                )

        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_forecast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show detailed forecast for a city."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        # Default to first configured city
        city = Config.WEATHER_CITIES[0] if Config.WEATHER_CITIES else 'london'
        if context.args:
            city = context.args[0].lower().replace(' ', '-')

        ensemble = self.engine.weather_client.get_ensemble_forecast(city)
        if not ensemble:
            await update.message.reply_text(f"⚠️ No forecast data for {city}")
            return

        msg = (
            f"🌡️ *Ensemble Forecast: {city.title()}*\n"
            f"Date: {ensemble['date']}\n\n"
            f"Mean Max: {ensemble['mean_max']}{ensemble.get('unit_symbol', '°C')}\n"
            f"Spread: ±{ensemble['std_max']}{ensemble.get('unit_symbol', '°C')}\n"
            f"Range: {ensemble['min_forecast']}{ensemble.get('unit_symbol', '°C')} — {ensemble['max_forecast']}{ensemble.get('unit_symbol', '°C')}\n"
            f"Models: {ensemble['num_models']}\n"
            f"Confidence: {ensemble['confidence']:.0%}\n\n"
            f"*Probability Distribution:*\n"
        )

        for temp, prob in sorted(ensemble['probability_distribution'].items()):
            bar = '█' * int(prob * 30)
            msg += f"  {temp:3d}°C: {prob:.0%} {bar}\n"

        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_markets(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Scan live weather markets."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        await update.message.reply_text("🔍 Scanning Polymarket for weather markets...")

        self.engine.weather_markets._cache_ts = 0  # Force refresh
        markets = self.engine.weather_markets.discover_markets()

        if not markets:
            await update.message.reply_text("⚠️ No weather markets found on Polymarket right now")
            return

        msg = f"🌤️ Found *{len(markets)}* weather markets\n\n"
        for m in markets[:5]:
            msg += (
                f"📍 *{m.get('city', '').title()}* — {m.get('date', '')}\n"
                f"  Outcomes: {m.get('num_outcomes', 0)} | "
                f"Volume: ${m.get('total_volume', 0):,.0f}\n\n"
            )

        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show trade history."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        trades = await self.engine.db.get_trade_history(10)
        if not trades:
            await update.message.reply_text("📜 No trade history yet")
            return

        msg = "📜 *Recent Trades*\n\n"
        for t in trades:
            pnl = t.get('pnl', 0) or 0
            emoji = '✅' if pnl > 0 else '❌' if pnl < 0 else '⏳'
            msg += (
                f"{emoji} {t.get('city', '').title()} "
                f"{t.get('outcome_label', '')} "
                f"P&L: ${pnl:+.4f}\n"
            )

        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show/switch mode."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📋 Paper", callback_data="mode_paper"),
                InlineKeyboardButton("🔴 Live", callback_data="mode_live"),
            ]
        ])
        current = '🔴 LIVE' if self.engine.trading_mode == 'live' else '📋 PAPER'
        await update.message.reply_text(
            f"Current: {current}\nSelect mode:", reply_markup=kb
        )

    async def cmd_live(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Switch to live mode."""
        if not self.engine:
            return
        ok, msg = self.engine.switch_mode('live')
        await update.message.reply_text(msg)

    async def cmd_paper(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Switch to paper mode."""
        if not self.engine:
            return
        ok, msg = self.engine.switch_mode('paper')
        await update.message.reply_text(msg)

    async def cmd_risk(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show risk manager status."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        rm = getattr(self.engine, 'risk_manager', None)
        if not rm:
            await update.message.reply_text("⚠️ Risk manager not initialized")
            return

        heat = rm.get_portfolio_heat()
        max_heat = rm.max_portfolio_heat * 100
        city_exposure = rm.city_exposure

        msg = (
            f"🛡️ *Risk Manager*\n\n"
            f"Daily P&L: ${rm.daily_pnl:+.2f}\n"
            f"Daily Trades: {rm.daily_trade_count}\n"
            f"Circuit Breaker: {'🔴 TRIPPED' if rm.is_circuit_broken() else '🟢 OK'}\n"
            f"Portfolio Heat: {heat:.0f}% / {max_heat:.0f}%\n"
            f"Risk Mode: {rm.risk_mode}\n"
            f"Fractional Kelly: {rm.fractional_kelly:.0%}\n\n"
        )

        if city_exposure:
            msg += "*City Exposure:*\n"
            for city, usd in sorted(city_exposure.items(), key=lambda x: -x[1]):
                if usd > 0:
                    msg += f"  {city.title()}: ${usd:.2f}\n"

        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_ml(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show ML module statistics."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        msg = "🧠 *ML Intelligence Layer*\n\n"

        # Bias corrector stats
        bc = getattr(self.engine, 'bias_corrector', None)
        if bc:
            msg += "*Bias Corrector:*\n"
            for city in Config.WEATHER_CITIES[:5]:
                stats = bc.get_stats(city)
                if stats['observations'] > 0:
                    msg += f"  {city.title()}: MAE={stats['mae']:.1f}° ({stats['observations']} obs)\n"
            if not any(bc.get_stats(c)['observations'] > 0 for c in Config.WEATHER_CITIES[:5]):
                msg += "  No data yet\n"
            msg += "\n"

        # Momentum tracker
        pm = getattr(self.engine, 'price_momentum', None)
        if pm:
            msg += "*Price Momentum:*\n"
            active = sum(1 for v in pm.price_history.values() if len(v) > 1)
            spikes = sum(1 for s in pm.spike_cooldown.values() if s > 0)
            msg += f"  Tracked tokens: {active}\n"
            msg += f"  Active spikes: {spikes}\n\n"

        # Dynamic threshold
        dt = getattr(self.engine, 'dynamic_threshold', None)
        if dt:
            msg += "*Dynamic Thresholds:*\n"
            entry = dt.get_entry_threshold({})
            exit_t = dt.get_exit_threshold({})
            msg += f"  Entry: {entry*100:.1f}%\n"
            msg += f"  Exit: {exit_t*100:.1f}%\n\n"

        # Confidence calibrator
        cc = getattr(self.engine, 'confidence_calibrator', None)
        if cc:
            total_samples = sum(len(b['actual']) for b in cc.bins.values())
            msg += f"*Confidence Calibrator:*\n"
            msg += f"  Training samples: {total_samples}\n"
            if total_samples > 0:
                score = cc.get_overconfidence_score()
                msg += f"  Overconfidence: {score:+.2f}\n"

        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_calibration(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show confidence calibration curve."""
        if not self.engine:
            await update.message.reply_text("⚠️ Engine not ready")
            return

        cc = getattr(self.engine, 'confidence_calibrator', None)
        if not cc:
            await update.message.reply_text("⚠️ Calibrator not initialized")
            return

        msg = "📈 *Confidence Calibration*\n\n"
        msg += "```\nPredicted → Actual  (n)\n"
        msg += "─" * 28 + "\n"

        for bin_key in sorted(cc.bins.keys()):
            b = cc.bins[bin_key]
            n = len(b['actual'])
            if n == 0:
                msg += f" {bin_key:>7s}   →  n/a     (0)\n"
            else:
                actual = sum(b['actual']) / n
                bar = '█' * int(actual * 10)
                msg += f" {bin_key:>7s}   → {actual:5.0%}  ({n:2d}) {bar}\n"

        msg += "```\n"

        score = cc.get_overconfidence_score()
        if score > 0.05:
            msg += f"\n⚠️ Overconfident by {score:.0%} — thresholds tightened"
        elif score < -0.05:
            msg += f"\n✅ Underconfident by {abs(score):.0%} — thresholds relaxed"
        else:
            msg += f"\n✅ Well-calibrated (drift: {score:+.0%})"

        await update.message.reply_text(msg, parse_mode='Markdown')

    # ═══════════════════════════════════════════════════════════════════
    # CALLBACKS
    # ═══════════════════════════════════════════════════════════════════

    async def cb_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle inline button callbacks."""
        query = update.callback_query
        await query.answer()

        data = query.data
        if data == 'mode_paper':
            if self.engine:
                ok, msg = self.engine.switch_mode('paper')
                await query.edit_message_text(msg)
        elif data == 'mode_live':
            if self.engine:
                ok, msg = self.engine.switch_mode('live')
                await query.edit_message_text(msg)

    # ═══════════════════════════════════════════════════════════════════
    # NOTIFICATIONS
    # ═══════════════════════════════════════════════════════════════════

    async def send_message(self, text: str):
        """Send a message to the configured chat."""
        if not self.app or not Config.TELEGRAM_CHAT_ID:
            return
        try:
            await self.app.bot.send_message(
                chat_id=Config.TELEGRAM_CHAT_ID,
                text=text,
                parse_mode='Markdown'
            )
        except Exception:
            # Markdown parse failed — retry as plain text
            try:
                await self.app.bot.send_message(
                    chat_id=Config.TELEGRAM_CHAT_ID,
                    text=text
                )
            except Exception as e2:
                print(f"⚠️ Telegram send error: {e2}", flush=True)

    async def send_trade_alert(self, trade: dict):
        """Send trade execution notification."""
        direction = trade.get('direction', 'YES')
        city = _md_escape(trade.get('city', '').title())
        label = _md_escape(trade.get('outcome_label', ''))
        price = trade.get('entry_price', 0)
        size = trade.get('size_usd', 0)
        strategy = _md_escape(trade.get('strategy', ''))

        msg = (
            f"🌡️ *TRADE: {direction}*\n\n"
            f"City: {city}\n"
            f"Outcome: {label}\n"
            f"Price: ${price:.3f}\n"
            f"Size: ${size:.2f}\n"
            f"Strategy: {strategy}"
        )
        await self.send_message(msg)

    async def send_close_alert(self, trade: dict):
        """Send trade close notification."""
        pnl = trade.get('pnl', 0) or 0
        emoji = '✅' if pnl >= 0 else '❌'

        city = _md_escape(trade.get('city', '').title())
        label = _md_escape(trade.get('outcome_label', ''))
        reason = _md_escape(trade.get('exit_reason', ''))

        msg = (
            f"{emoji} *CLOSED*\n\n"
            f"City: {city}\n"
            f"Outcome: {label}\n"
            f"P&L: ${pnl:+.4f} ({trade.get('pnl_pct', 0):+.1f}%)\n"
            f"Reason: {reason}"
        )
        await self.send_message(msg)

    async def send_pnl_report(self, summary: dict, positions: list):
        """Send periodic P&L report."""
        msg = (
            f"📊 *P&L Report*\n\n"
            f"Balance: ${summary.get('balance', 0):.2f}\n"
            f"Trades: {summary.get('total_trades', 0)}\n"
            f"Win Rate: {summary.get('win_rate', 0):.0f}%\n"
            f"Total P&L: ${summary.get('total_pnl', 0):+.4f}\n"
            f"Open: {len(positions)}"
        )
        await self.send_message(msg)
