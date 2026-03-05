"""
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


class BayesianUpdater:
    """
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
