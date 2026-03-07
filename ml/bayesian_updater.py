"""
<<<<<<< HEAD
Bayesian Probability Updater — Combines Forecast + Market Signal

Instead of blindly trusting the forecast OR the market, we use 
Bayes' theorem to combine both into a POSTERIOR probability.

P(temp=X | forecast, market) ∝ P(forecast | temp=X) × P(temp=X | market)

WHY: The market contains information from OTHER traders/bots.
If the market disagrees with our forecast, there might be information
we're missing. Bayesian updating lets us weight both signals.

EXAMPLE:
  Forecast says 17°C with 33% probability
  Market prices 17°C at 35% (bidders think it's likely)
  → Posterior: ~34% (weighted average, market confirms)

  Forecast says 13°C with 1% probability  
  Market prices 13°C at 5% (some traders see something)
  → Posterior: ~3% (market suggests more likely than we think)

The KEY: when forecast and market DISAGREE significantly,
that's where trading edge exists. Bayesian updating tells us
how much to trust each signal.
"""

import math
from typing import Dict, Optional
=======
Bayesian Updater — Real-Time Probability Updates Using Bayes' Theorem

As new data arrives (hourly actual temps, updated forecasts), this module
updates the probability distribution using Bayesian inference:
  
  Prior    = ensemble forecast probability distribution
  Evidence = new hourly temperature observation
  Posterior = updated probability given the evidence

Example:
  Prior: P(max=14°C) = 25%, P(max=15°C) = 30%, P(max=16°C) = 20%
  At 1 PM, actual temp hits 15.2°C and is now cooling
  → P(max=15°C) surges to 55%, P(max=16°C) drops to 8%
  
This gives the intraday_tracker strategy much sharper probabilities
than the static ensemble forecast alone.
"""

import math
from typing import Dict, List, Optional, Tuple
from datetime import datetime
>>>>>>> a64357fa1588e8614a20f7b9abe5aaf7b7f1792a


class BayesianUpdater:
    """
<<<<<<< HEAD
    Bayesian probability updater for weather prediction markets.
    
    Combines:
      1. PRIOR: Weather forecast probability distribution
      2. LIKELIHOOD: Market prices as a signal
      3. POSTERIOR: Updated probability incorporating both
    
    The mixing weight between forecast and market depends on:
      - Time to resolution (closer → trust forecast more)
      - Model confidence (higher → trust forecast more)
      - Market liquidity (deeper → trust market more)
      - Historical forecast accuracy (better → trust forecast more)
    """

    def __init__(self):
        # How much to trust forecast vs market (0=all market, 1=all forecast)
        self.default_forecast_weight = 0.65
        self._accuracy_cache: Dict[str, float] = {}

    def update_probabilities(self, forecast_probs: Dict[int, float],
                              market_prices: Dict,
                              model_confidence: float = 0.7,
                              hours_remaining: float = 24,
                              market_liquidity: float = 1000,
                              city: str = '') -> Dict[int, float]:
        """
        Combine forecast probabilities with market prices using
        Bayesian-inspired weighted fusion.

        Args:
            forecast_probs: {temp: probability} from weather models
            market_prices: {temp: price_yes} from Polymarket (dict keyed by temp_low)
            model_confidence: 0-1 from ensemble (higher = more certain)
            hours_remaining: hours until resolution
            market_liquidity: total market liquidity in USD
            city: city name for accuracy lookup

        Returns:
            {temp: posterior_probability}
        """
        # Calculate adaptive weights
        fw = self._get_forecast_weight(
            model_confidence, hours_remaining, market_liquidity, city)

        mw = 1.0 - fw

        # Get all temperature keys from both sources
        all_temps = set()
        all_temps.update(forecast_probs.keys())

        # Parse market prices to temp-keyed dict
        market_probs = {}
        if isinstance(market_prices, dict):
            for key, val in market_prices.items():
                if isinstance(key, int):
                    market_probs[key] = val
                elif isinstance(val, dict):
                    t = val.get('temp_low')
                    if t is not None:
                        market_probs[int(t)] = val.get('price_yes', 0)
                elif isinstance(val, (int, float)):
                    try:
                        market_probs[int(key)] = float(val)
                    except (ValueError, TypeError):
                        pass

        all_temps.update(market_probs.keys())

        if not all_temps:
            return forecast_probs

        # Compute posterior for each temperature
        posterior = {}
        total = 0.0

        for temp in all_temps:
            fp = forecast_probs.get(temp, 0.001)  # Small epsilon for unseen
            mp = market_probs.get(temp, 0.001)

            # Log-space mixing for numerical stability
            if fp > 0 and mp > 0:
                log_fp = math.log(max(fp, 1e-6))
                log_mp = math.log(max(mp, 1e-6))
                log_posterior = fw * log_fp + mw * log_mp
                p = math.exp(log_posterior)
            else:
                p = fw * fp + mw * mp

            posterior[temp] = max(0, p)
            total += posterior[temp]

        # Normalize
        if total > 0:
            for temp in posterior:
                posterior[temp] = round(posterior[temp] / total, 4)

        return posterior

    def get_edge_with_uncertainty(self, forecast_prob: float, market_price: float,
                                   model_confidence: float = 0.7,
                                   hours_remaining: float = 24) -> Dict:
        """
        Calculate trading edge with uncertainty bounds.
        
        Returns not just the edge, but also how CONFIDENT we are 
        about that edge. This helps with position sizing.
        """
        if market_price <= 0 or market_price >= 1:
            return {'edge': 0, 'edge_lower': 0, 'edge_upper': 0,
                    'confidence': 0, 'should_trade': False}

        # Point estimate of edge
        edge = forecast_prob - market_price

        # Uncertainty in our forecast probability
        # Higher model confidence → narrower uncertainty band
        uncertainty = (1 - model_confidence) * 0.15

        # Time decay: further from resolution → more uncertain
        if hours_remaining > 48:
            uncertainty *= 1.5
        elif hours_remaining > 24:
            uncertainty *= 1.2
        elif hours_remaining < 6:
            uncertainty *= 0.7

        edge_lower = edge - uncertainty
        edge_upper = edge + uncertainty

        # Probability that edge is positive (assuming normal distribution)
        if uncertainty > 0:
            z = edge / uncertainty
            prob_positive = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        else:
            prob_positive = 1.0 if edge > 0 else 0.0

        # Should trade: edge is positive with high probability
        should_trade = edge_lower > 0.05 and prob_positive > 0.70

        return {
            'edge': round(edge, 4),
            'edge_lower': round(edge_lower, 4),
            'edge_upper': round(edge_upper, 4),
            'uncertainty': round(uncertainty, 4),
            'prob_positive_edge': round(prob_positive, 3),
            'should_trade': should_trade,
            'confidence': round(prob_positive, 3),
        }

    def _get_forecast_weight(self, model_confidence, hours_remaining,
                              market_liquidity, city):
        """
        Adaptive weight for forecast vs market signal.
        
        Higher weight for forecast when:
          - Model confidence is high
          - Time to resolution is short (forecast is more accurate)
          - We have good historical accuracy
          
        Higher weight for market when:
          - Market has deep liquidity (many informed traders)
          - Model confidence is low
        """
        base = self.default_forecast_weight

        # Model confidence adjustment (-0.15 to +0.15)
        confidence_adj = (model_confidence - 0.5) * 0.30
        base += confidence_adj

        # Time adjustment: closer → trust forecast more
        if hours_remaining < 6:
            base += 0.15
        elif hours_remaining < 12:
            base += 0.10
        elif hours_remaining < 24:
            base += 0.05
        elif hours_remaining > 72:
            base -= 0.10

        # Liquidity adjustment: deeper market → trust market more
        if market_liquidity > 5000:
            base -= 0.05
        elif market_liquidity < 500:
            base += 0.05

        # Historical accuracy adjustment
        accuracy = self._accuracy_cache.get(city, 0.5)
        if accuracy > 0.8:
            base += 0.10
        elif accuracy < 0.4:
            base -= 0.10

        # Clamp to [0.3, 0.9]
        return max(0.30, min(0.90, base))

    def set_city_accuracy(self, city: str, accuracy: float):
        """Set historical forecast accuracy for a city (0-1)."""
        self._accuracy_cache[city] = accuracy
=======
    Bayesian probability engine for weather outcome trading.
    Updates probability distributions as new data arrives.
    """

    def __init__(self):
        # Store prior distributions: {city_date: {temp: prob}}
        self._priors: Dict[str, Dict[int, float]] = {}
        # Store posteriors: {city_date: {temp: prob}}
        self._posteriors: Dict[str, Dict[int, float]] = {}
        # Store observations for likelihood computation
        self._observations: Dict[str, List[Tuple[float, int]]] = {}  # {city_date: [(temp, hour)]}

    def set_prior(self, city: str, target_date: str,
                  prob_dist: Dict[int, float]):
        """Set the prior probability distribution from ensemble forecast."""
        key = f"{city}_{target_date}"
        # Normalize the prior
        total = sum(prob_dist.values())
        if total > 0:
            self._priors[key] = {t: p / total for t, p in prob_dist.items()}
        else:
            self._priors[key] = prob_dist.copy()
        # Initialize posterior as prior
        self._posteriors[key] = self._priors[key].copy()

    def update(self, city: str, target_date: str,
               observed_temp: float, hour: int,
               forecast_remaining_max: float = None) -> Dict[int, float]:
        """
        Update probability distribution given a new temperature observation.
        
        Uses Gaussian likelihood centered on what each max-temp outcome
        implies about the current temperature at this hour.
        
        Args:
            city: City code
            target_date: ISO date string
            observed_temp: Current measured temperature
            hour: Current hour (0-23)
            forecast_remaining_max: Expected max for remaining hours
            
        Returns:
            Updated probability distribution {temp: probability}
        """
        key = f"{city}_{target_date}"
        
        # Record observation
        if key not in self._observations:
            self._observations[key] = []
        self._observations[key].append((observed_temp, hour))
        
        current_dist = self._posteriors.get(key, self._priors.get(key, {}))
        if not current_dist:
            return {}

        running_max = max(t for t, _ in self._observations[key])
        
        # For each possible max temperature outcome, compute likelihood
        # of seeing the current observation
        updated = {}
        
        for max_temp, prior_prob in current_dist.items():
            likelihood = self._compute_likelihood(
                max_temp, observed_temp, running_max, hour,
                forecast_remaining_max
            )
            updated[max_temp] = prior_prob * likelihood
        
        # Normalize posterior
        total = sum(updated.values())
        if total > 0:
            self._posteriors[key] = {t: p / total for t, p in updated.items()}
        
        return self._posteriors[key]

    def _compute_likelihood(self, max_temp: int, current_temp: float,
                           running_max: float, hour: int,
                           forecast_remaining_max: float = None) -> float:
        """
        Compute P(observation | max_temp = X).
        
        Logic:
        - If running_max > max_temp → this outcome is impossible (or near-zero for ranges)
        - If hour >= 16 and running_max ≈ max_temp → very likely
        - If hour < 12 → still uncertain, use Gaussian around expected profile
        """
        # Hard constraint: running max already exceeds this outcome
        if running_max > max_temp + 0.5:
            return 0.001  # Near-zero but not exactly 0 (measurement noise)
        
        # Late afternoon: running max IS the final max with high probability
        if hour >= 16:
            # How close is running_max to this outcome?
            diff = abs(running_max - max_temp)
            if diff < 0.5:
                return 2.0  # Strong match
            elif diff < 1.5:
                return 0.5
            else:
                # Still possible if this is a range or boundary
                sigma = max(1.0, diff / 2)
                return math.exp(-0.5 * (diff / sigma) ** 2)
        
        # Morning/midday: use temperature profile heuristic
        # At hour h, typical fraction of daily max reached
        if hour <= 6:
            profile_fraction = 0.7  # Early morning — far from peak
        elif hour <= 10:
            profile_fraction = 0.85
        elif hour <= 14:
            profile_fraction = 0.97  # Near peak hours
        else:
            profile_fraction = 0.99  # Post-peak
        
        # Expected current temp if max will be max_temp
        expected_current = max_temp * profile_fraction
        
        # Also consider remaining forecast
        if forecast_remaining_max is not None:
            expected_final = max(running_max, forecast_remaining_max)
            diff_from_expected = abs(max_temp - expected_final)
            sigma_forecast = max(1.0, 3.0 - hour * 0.15)
            forecast_likelihood = math.exp(-0.5 * (diff_from_expected / sigma_forecast) ** 2)
        else:
            forecast_likelihood = 1.0
        
        # Current observation likelihood
        diff = abs(current_temp - expected_current)
        sigma = max(1.0, 4.0 - hour * 0.2)  # Uncertainty decreases through day
        obs_likelihood = math.exp(-0.5 * (diff / sigma) ** 2)
        
        return obs_likelihood * forecast_likelihood

    def get_posterior(self, city: str, target_date: str) -> Dict[int, float]:
        """Get current posterior distribution."""
        key = f"{city}_{target_date}"
        return self._posteriors.get(key, self._priors.get(key, {}))

    def get_max_probability_outcome(self, city: str,
                                     target_date: str) -> Optional[Tuple[int, float]]:
        """Get the most probable outcome and its probability."""
        posterior = self.get_posterior(city, target_date)
        if not posterior:
            return None
        max_temp = max(posterior, key=posterior.get)
        return (max_temp, posterior[max_temp])

    def get_probability_for_outcome(self, city: str, target_date: str,
                                     temp: int) -> float:
        """Get probability for a specific temperature outcome."""
        posterior = self.get_posterior(city, target_date)
        return posterior.get(temp, 0.0)

    def get_cumulative_probability(self, city: str, target_date: str,
                                    temp_threshold: int,
                                    direction: str = 'above') -> float:
        """Get cumulative probability above or below a threshold."""
        posterior = self.get_posterior(city, target_date)
        if not posterior:
            return 0.0
        if direction == 'above':
            return sum(p for t, p in posterior.items() if t >= temp_threshold)
        else:
            return sum(p for t, p in posterior.items() if t <= temp_threshold)

    def confidence_ratio(self, city: str, target_date: str) -> float:
        """
        How concentrated is the posterior vs the prior?
        Higher = more information gained from observations.
        1.0 = no change, 2.0+ = significant update.
        """
        key = f"{city}_{target_date}"
        prior = self._priors.get(key, {})
        posterior = self._posteriors.get(key, {})
        if not prior or not posterior:
            return 1.0
        
        # Entropy ratio: lower posterior entropy = more concentrated
        prior_entropy = self._entropy(prior)
        posterior_entropy = self._entropy(posterior)
        
        if posterior_entropy <= 0:
            return 3.0  # Near-certainty
        return max(1.0, prior_entropy / posterior_entropy)

    def _entropy(self, dist: Dict[int, float]) -> float:
        """Shannon entropy of a probability distribution."""
        h = 0.0
        for p in dist.values():
            if p > 0:
                h -= p * math.log2(p)
        return h

    def reset(self, city: str = None, target_date: str = None):
        """Reset stored data for a city/date or all."""
        if city and target_date:
            key = f"{city}_{target_date}"
            self._priors.pop(key, None)
            self._posteriors.pop(key, None)
            self._observations.pop(key, None)
        elif city:
            keys_to_remove = [k for k in self._priors if k.startswith(f"{city}_")]
            for k in keys_to_remove:
                self._priors.pop(k, None)
                self._posteriors.pop(k, None)
                self._observations.pop(k, None)
        else:
            self._priors.clear()
            self._posteriors.clear()
            self._observations.clear()
>>>>>>> a64357fa1588e8614a20f7b9abe5aaf7b7f1792a
