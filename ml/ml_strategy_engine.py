"""
ML Strategy Wrapper — The Brain That Runs Everything

This is the MASTER ORCHESTRATOR. It wraps ALL 7 strategies with
ML processing to make smarter decisions:

FLOW:
  1. Fetch forecast → ML BIAS CORRECTION → corrected forecast
  2. Get market prices → BAYESIAN UPDATE → posterior probabilities
  3. Run 7 strategies → get raw signals  
  4. For each signal:
     a. DYNAMIC THRESHOLD → should we enter? at what size?
     b. PRICE MOMENTUM → should we buy NOW or WAIT?
     c. KELLY CRITERION → optimal position sizing
  5. For existing positions:
     a. DYNAMIC EXIT → should we exit? TP/SL/trailing/edge-reversal?
  6. Output: final, ML-refined trade signals

This replaces static thresholds with intelligent, adaptive logic.
"""

import time
from typing import Dict, List, Optional
from datetime import date, datetime

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from weather_prediction.strategies.dynamic_picker import WeatherDynamicPicker
from weather_prediction.ml.bias_corrector import BiasCorrectionModel
from weather_prediction.ml.bayesian_updater import BayesianUpdater
from weather_prediction.ml.dynamic_threshold import DynamicThresholdEngine
from weather_prediction.ml.price_momentum import PriceMomentumDetector
from weather_prediction.config import Config


class MLStrategyEngine:
    """
    ML-powered strategy engine that wraps all 7 base strategies
    with intelligent pre/post-processing.
    """

    def __init__(self):
        # Base strategies
        self.picker = WeatherDynamicPicker()

        # ML modules
        self.bias_corrector = BiasCorrectionModel(data_dir='data')
        self.bayesian = BayesianUpdater()
        self.thresholds = DynamicThresholdEngine()
        self.momentum = PriceMomentumDetector()

        # State tracking
        self._scan_count = 0
        self._total_signals = 0
        self._total_filtered = 0

    async def analyze(self, weather_market: Dict, context: Dict) -> List[Dict]:
        """
        Full ML pipeline:
          Forecast → Bias Correct → Bayesian Update → Strategies → 
          Dynamic Threshold → Momentum Timing → Final Signals
        """
        self._scan_count += 1
        city = weather_market.get('city', '')
        target_date = weather_market.get('date', '')

        # ═══ STEP 1: ML Bias Correction ═══
        forecast = context.get('forecast')
        if forecast:
            correction = self.bias_corrector.get_correction(
                city=city,
                forecast_mean=forecast.get('mean_max', 0),
                forecast_std=forecast.get('std_max', 1),
                model_temps=forecast.get('models', {}),
                hours_remaining=context.get('seconds_remaining', 0) / 3600,
            )

            # Apply correction to forecast
            forecast['mean_max'] = correction['adjusted_mean']
            forecast['std_max'] = correction['adjusted_std']
            forecast['ml_correction'] = correction['correction']
            forecast['ml_method'] = correction['method']
            forecast['model_weights'] = correction.get('model_weights', {})

            # Boost confidence if ML model is confident
            forecast['confidence'] = min(
                0.98, forecast.get('confidence', 0.5) + correction['confidence_boost'])

            # Rebuild probability distribution with corrected values
            from weather_prediction.data.weather_client import WeatherClient
            wc = WeatherClient()
            unit = forecast.get('unit', 'celsius')
            forecast['probability_distribution'] = wc._build_probability_distribution(
                correction['adjusted_mean'], correction['adjusted_std'], unit)

            context['forecast'] = forecast

        # ═══ STEP 2: Bayesian Update (combine forecast + market) ═══
        if forecast:
            prob_dist = forecast.get('probability_distribution', {})
            market_prices = {}
            for outcome in weather_market.get('outcomes', []):
                temp = outcome.get('temp_low')
                if temp is not None:
                    # Use bestAsk from Gamma (most accurate market price)
                    price = outcome.get('best_ask', 0) or outcome.get('price_yes', 0)
                    market_prices[temp] = price

            hours_remaining = context.get('seconds_remaining', 0) / 3600
            total_liquidity = sum(
                o.get('liquidity', 0) for o in weather_market.get('outcomes', []))

            posterior = self.bayesian.update_probabilities(
                forecast_probs=prob_dist,
                market_prices=market_prices,
                model_confidence=forecast.get('confidence', 0.7),
                hours_remaining=hours_remaining,
                market_liquidity=total_liquidity,
                city=city,
            )

            # Store both distributions for strategies
            forecast['raw_probability_distribution'] = prob_dist
            forecast['probability_distribution'] = posterior
            forecast['bayesian_updated'] = True

        # ═══ STEP 3: Run all 7 base strategies ═══
        raw_signals = await self.picker.analyze(weather_market, context)

        # ═══ STEP 4: ML Post-Processing (filter, enhance, time) ═══
        final_signals = []
        hours = context.get('seconds_remaining', 0) / 3600

        for signal in raw_signals:
            self._total_signals += 1

            # Record price for momentum tracking
            if signal.token_id:
                price = signal.entry_price or 0
                self.momentum.record_price(signal.token_id, price)
                self.thresholds.record_price(signal.token_id, price)

            # Get edge with uncertainty (Bayesian)
            edge = signal.metadata.get('edge', 0)
            edge_info = self.bayesian.get_edge_with_uncertainty(
                forecast_prob=signal.metadata.get('forecast_prob', signal.confidence),
                market_price=signal.entry_price or 0.5,
                model_confidence=forecast.get('confidence', 0.7) if forecast else 0.5,
                hours_remaining=hours,
            )

            # Dynamic threshold check
            open_positions = context.get('open_positions', 0)
            entry_decision = self.thresholds.should_enter(
                edge=edge_info['edge'],
                model_confidence=forecast.get('confidence', 0.5) if forecast else 0.5,
                hours_remaining=hours,
                market_liquidity=weather_market.get('total_volume', 1000),
                open_positions=open_positions,
                max_positions=Config.WEATHER_MAX_TRADES_PER_RUN * 3,
                edge_uncertainty=edge_info.get('uncertainty', 0),
            )

            if not entry_decision['should_enter']:
                self._total_filtered += 1
                continue

            # Momentum-based timing
            timing = self.momentum.get_entry_timing(signal.token_id, edge_info['edge'])

            if timing['action'] == 'wait' and edge < 0.25:
                self._total_filtered += 1
                continue

            # Enhance signal with ML data
            signal.metadata.update({
                'ml_edge': edge_info['edge'],
                'ml_edge_lower': edge_info['edge_lower'],
                'ml_edge_upper': edge_info['edge_upper'],
                'ml_confidence': edge_info['confidence'],
                'ml_should_trade': edge_info['should_trade'],
                'entry_threshold': entry_decision['adjusted_threshold'],
                'position_scale': entry_decision['position_scale'],
                'entry_urgency': entry_decision['urgency'],
                'momentum_signal': timing.get('action', 'enter_now'),
                'momentum_reason': timing.get('reason', ''),
                'ml_correction': forecast.get('ml_correction', 0) if forecast else 0,
                'bayesian_updated': True,
            })

            # Adjust confidence with ML
            signal.confidence = min(0.98, signal.confidence * (
                0.7 + entry_decision['position_scale'] * 0.15))

            final_signals.append(signal)

        # Log ML pipeline stats periodically 
        if self._scan_count % 5 == 0:
            filtered = self._total_filtered
            total = self._total_signals
            rate = (filtered / total * 100) if total > 0 else 0
            print(
                f"🧠 ML Engine: {self._scan_count} scans | "
                f"{total} signals → {total - filtered} passed "
                f"({rate:.0f}% filtered)",
                flush=True)

        return final_signals

    async def check_exits(self, positions: List[Dict], context: Dict) -> List[Dict]:
        """
        ML-powered exit checking for open positions.
        Uses dynamic thresholds + momentum + edge reversal.
        """
        exit_signals = []
        forecast = context.get('forecast')
        hours = context.get('seconds_remaining', 0) / 3600

        for pos in positions:
            token_id = pos.get('token_id', '')
            entry_price = pos.get('entry_price', 0.5)
            current_price = pos.get('current_price', entry_price)
            pnl_pct = ((current_price - entry_price) / max(entry_price, 0.001)) * 100

            # Record price for momentum
            if token_id:
                self.momentum.record_price(token_id, current_price)

            # Dynamic exit decision
            forecast_prob = 0.5
            if forecast:
                prob_dist = forecast.get('probability_distribution', {})
                temp = pos.get('temp_c', 0)
                forecast_prob = prob_dist.get(int(temp), 0.5)

            exit_decision = self.thresholds.should_exit(
                pnl_pct=pnl_pct,
                current_price=current_price,
                entry_price=entry_price,
                hours_remaining=hours,
                model_confidence=forecast.get('confidence', 0.5) if forecast else 0.5,
                forecast_prob=forecast_prob,
            )

            if exit_decision['should_exit']:
                exit_signals.append({
                    'position': pos,
                    'reason': exit_decision['reason'],
                    'pnl_pct': round(pnl_pct, 1),
                    'take_profit_level': exit_decision['take_profit_level'],
                    'stop_loss_level': exit_decision['stop_loss_level'],
                    'trailing_stop': exit_decision['trailing_stop'],
                })

        return exit_signals

    def record_resolution(self, city: str, target_date: str,
                          actual_temp: float, forecast_mean: float,
                          forecast_std: float, model_temps: Dict = None):
        """
        Record actual vs forecast after market resolution.
        This trains the ML bias correction model.
        """
        self.bias_corrector.record_actual(
            city=city,
            target_date=target_date,
            actual_temp=actual_temp,
            forecast_mean=forecast_mean,
            forecast_std=forecast_std,
            model_temps=model_temps,
        )

    def record_trade_result(self, pnl: float, pnl_pct: float):
        """Record trade result for dynamic threshold learning."""
        self.thresholds.record_trade_result(pnl, pnl_pct)

    def get_stats(self) -> Dict:
        """Get ML engine statistics."""
        return {
            'scans': self._scan_count,
            'total_signals': self._total_signals,
            'filtered': self._total_filtered,
            'filter_rate': round(
                self._total_filtered / max(self._total_signals, 1) * 100, 1),
            'bias_cities': len(self.bias_corrector._history),
            'momentum_tokens': len(self.momentum._prices),
            'win_rate': round(self.thresholds._win_rate * 100, 1),
        }
