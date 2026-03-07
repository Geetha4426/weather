"""
Weather Client — Multi-API Forecast Engine

Fetches weather forecasts from MULTIPLE APIs for maximum accuracy:
1. Open-Meteo (FREE, no key) — 6 models ensemble
2. Tomorrow.io (FREE tier: 500 calls/day) — ML-powered
3. WeatherAPI (FREE tier: 1M calls/month) — good for hourly
4. OpenWeatherMap (FREE tier: 1000 calls/day) — widely used

Multi-model ensemble + multi-API = highest confidence for trading.

Temperature handling:
- US cities (NYC, Chicago, etc.) → Fahrenheit (Polymarket uses °F)
- Non-US cities (London, Munich, etc.) → Celsius (Polymarket uses °C)
"""

import time
import math
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta, timezone

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.config import Config


class WeatherClient:
    """Multi-API weather forecast client for trading edge."""

    BASE_URL = "https://api.open-meteo.com/v1"

    # City configurations with Polymarket slug names
    # CRITICAL: slug names come from REAL Polymarket data
    CITIES = {
        'nyc': {
            'lat': 40.7128, 'lon': -74.0060,
            'name': 'New York City', 'station': 'KNYC',
            'timezone': 'America/New_York',
            'unit': 'fahrenheit',  # US city → °F on Polymarket
        },
        'london': {
            'lat': 51.5074, 'lon': -0.1278,
            'name': 'London', 'station': 'EGLC',
            'timezone': 'Europe/London',
            'unit': 'celsius',
        },
        'chicago': {
            'lat': 41.8781, 'lon': -87.6298,
            'name': 'Chicago', 'station': 'KORD',
            'timezone': 'America/Chicago',
            'unit': 'fahrenheit',
        },
        'miami': {
            'lat': 25.7617, 'lon': -80.1918,
            'name': 'Miami', 'station': 'KMIA',
            'timezone': 'America/New_York',
            'unit': 'fahrenheit',
        },
        'seattle': {
            'lat': 47.6062, 'lon': -122.3321,
            'name': 'Seattle', 'station': 'KSEA',
            'timezone': 'America/Los_Angeles',
            'unit': 'fahrenheit',
        },
        'atlanta': {
            'lat': 33.7490, 'lon': -84.3880,
            'name': 'Atlanta', 'station': 'KATL',
            'timezone': 'America/New_York',
            'unit': 'fahrenheit',
        },
        'dallas': {
            'lat': 32.7767, 'lon': -96.7970,
            'name': 'Dallas', 'station': 'KDAL',
            'timezone': 'America/Chicago',
            'unit': 'fahrenheit',
        },
        'munich': {
            'lat': 48.1351, 'lon': 11.5820,
            'name': 'Munich', 'station': 'EDDM',
            'timezone': 'Europe/Berlin',
            'unit': 'celsius',
        },
        'lucknow': {
            'lat': 26.8467, 'lon': 80.9462,
            'name': 'Lucknow', 'station': 'VILK',
            'timezone': 'Asia/Kolkata',
            'unit': 'celsius',
        },
        'tokyo': {
            'lat': 35.6762, 'lon': 139.6503,
            'name': 'Tokyo', 'station': 'RJTT',
            'timezone': 'Asia/Tokyo',
            'unit': 'celsius',
        },
        'paris': {
            'lat': 48.8566, 'lon': 2.3522,
            'name': 'Paris', 'station': 'LFPG',
            'timezone': 'Europe/Paris',
            'unit': 'celsius',
        },
        'los-angeles': {
            'lat': 34.0522, 'lon': -118.2437,
            'name': 'Los Angeles', 'station': 'KLAX',
            'timezone': 'America/Los_Angeles',
            'unit': 'fahrenheit',
        },
    }

    # Open-Meteo weather models
    MODELS = [
        'icon_seamless',     # DWD ICON (German)
        'gfs_seamless',      # NOAA GFS (American)
        'ecmwf_ifs025',      # ECMWF IFS (European) — best global model
        'gem_seamless',      # GEM (Canadian)
        'jma_seamless',      # JMA (Japanese)
        'ukmo_seamless',     # UKMO (British) — best for London
    ]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'weather-trade-bot/2.0',
            'Accept': 'application/json',
        })
        self._forecast_cache: Dict[str, Dict] = {}
        self._ensemble_cache: Dict[str, Dict] = {}
        self._cache_ttl = 600  # 10 minutes
        self._bias_data: Dict[str, List[Tuple[float, float]]] = {}

    # ═══════════════════════════════════════════════════════════════════
    # PUBLIC API
    # ═══════════════════════════════════════════════════════════════════

    def get_city_unit(self, city: str) -> str:
        """Get temperature unit for a city (fahrenheit or celsius)."""
        city = city.lower().replace(' ', '-')
        info = self.CITIES.get(city, {})
        return info.get('unit', 'celsius')

    def c_to_f(self, celsius: float) -> float:
        """Convert Celsius to Fahrenheit."""
        return round(celsius * 9 / 5 + 32, 1)

    def f_to_c(self, fahrenheit: float) -> float:
        """Convert Fahrenheit to Celsius."""
        return round((fahrenheit - 32) * 5 / 9, 1)

    def get_forecast(self, city: str, target_date: date = None) -> Optional[Dict]:
        """
        Get weather forecast for a city. Returns temperature in the city's
        native unit (°F for US, °C for others).
        """
        city = city.lower().replace(' ', '-')
        if city not in self.CITIES:
            print(f"⚠️ Unknown city: {city}. Available: {list(self.CITIES.keys())}", flush=True)
            return None

        if target_date is None:
            target_date = date.today()

        date_str = target_date.isoformat()
        cache_key = f"{city}_{date_str}"

        if cache_key in self._forecast_cache:
            cached = self._forecast_cache[cache_key]
            if time.time() - cached['fetched_at'] < self._cache_ttl:
                return cached

        city_info = self.CITIES[city]
        unit = city_info.get('unit', 'celsius')

        # Fetch in Celsius first (Open-Meteo default), convert if needed
        temp_unit_param = '&temperature_unit=fahrenheit' if unit == 'fahrenheit' else ''

        try:
            url = (
                f"{self.BASE_URL}/forecast"
                f"?latitude={city_info['lat']}&longitude={city_info['lon']}"
                f"&hourly=temperature_2m,precipitation,cloud_cover,wind_speed_10m"
                f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
                f"&timezone={city_info['timezone']}"
                f"&start_date={date_str}&end_date={date_str}"
                f"{temp_unit_param}"
            )
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"❌ Open-Meteo API error: {resp.status_code}", flush=True)
                return None

            data = resp.json()
            daily = data.get('daily', {})
            hourly = data.get('hourly', {})

            max_temps = daily.get('temperature_2m_max', [])
            min_temps = daily.get('temperature_2m_min', [])
            precip = daily.get('precipitation_sum', [])
            hourly_temps = hourly.get('temperature_2m', [])
            hourly_times = hourly.get('time', [])
            hourly_cloud = hourly.get('cloud_cover', [])
            hourly_wind = hourly.get('wind_speed_10m', [])

            unit_symbol = '°F' if unit == 'fahrenheit' else '°C'

            result = {
                'city': city,
                'city_name': city_info['name'],
                'date': date_str,
                'max_temp': max_temps[0] if max_temps else None,
                'min_temp': min_temps[0] if min_temps else None,
                'unit': unit,
                'unit_symbol': unit_symbol,
                'hourly_temps': hourly_temps,
                'hourly_times': hourly_times,
                'precipitation_mm': precip[0] if precip else 0,
                'cloud_cover_pct': sum(hourly_cloud) / len(hourly_cloud) if hourly_cloud else 0,
                'wind_speed_kmh': max(hourly_wind) if hourly_wind else 0,
                'model': 'best_match',
                'source': 'open-meteo',
                'fetched_at': time.time(),
            }

            self._forecast_cache[cache_key] = result
            return result

        except Exception as e:
            print(f"❌ Forecast fetch error: {e}", flush=True)
            return None

    def get_ensemble_forecast(self, city: str, target_date: date = None) -> Optional[Dict]:
        """
        Multi-source ensemble forecast combining Open-Meteo models +
        Tomorrow.io + WeatherAPI for maximum confidence.

        Returns temperatures in the city's native unit (°F for US, °C for non-US).
        """
        city = city.lower().replace(' ', '-')
        if city not in self.CITIES:
            return None

        if target_date is None:
            target_date = date.today()

        date_str = target_date.isoformat()
        cache_key = f"ensemble_{city}_{date_str}"

        if cache_key in self._ensemble_cache:
            cached = self._ensemble_cache[cache_key]
            if time.time() - cached.get('fetched_at', 0) < self._cache_ttl:
                return cached

        city_info = self.CITIES[city]
        unit = city_info.get('unit', 'celsius')
        model_temps = {}

        # ═══ Source 1: Open-Meteo multi-model ═══
        temp_unit_param = '&temperature_unit=fahrenheit' if unit == 'fahrenheit' else ''

        for model in self.MODELS:
            try:
                url = (
                    f"{self.BASE_URL}/forecast"
                    f"?latitude={city_info['lat']}&longitude={city_info['lon']}"
                    f"&daily=temperature_2m_max"
                    f"&timezone={city_info['timezone']}"
                    f"&start_date={date_str}&end_date={date_str}"
                    f"&models={model}"
                    f"{temp_unit_param}"
                )
                resp = self.session.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    daily = data.get('daily', {})
                    temps = daily.get('temperature_2m_max', [])
                    if temps and temps[0] is not None:
                        model_temps[f'openmeteo_{model}'] = temps[0]
            except Exception:
                continue

        # ═══ Source 2: Tomorrow.io (if API key set) ═══
        if Config.TOMORROW_IO_API_KEY:
            try:
                tomorrow_temp = self._fetch_tomorrow_io(city_info, date_str, unit)
                if tomorrow_temp is not None:
                    model_temps['tomorrow_io'] = tomorrow_temp
            except Exception:
                pass

        # ═══ Source 3: WeatherAPI (if API key set) ═══
        if Config.WEATHERAPI_KEY:
            try:
                weatherapi_temp = self._fetch_weatherapi(city_info, date_str, unit)
                if weatherapi_temp is not None:
                    model_temps['weatherapi'] = weatherapi_temp
            except Exception:
                pass

        # ═══ Source 4: OpenWeatherMap (if API key set) ═══
        if Config.OPENWEATHER_API_KEY:
            try:
                owm_temp = self._fetch_openweathermap(city_info, date_str, unit)
                if owm_temp is not None:
                    model_temps['openweathermap'] = owm_temp
            except Exception:
                pass

        if len(model_temps) < 2:
            print(f"⚠️ Only {len(model_temps)} models for {city}", flush=True)
            fc = self.get_forecast(city, target_date)
            if fc and fc['max_temp'] is not None:
                model_temps['best_match'] = fc['max_temp']
            if not model_temps:
                return None

        temps_list = list(model_temps.values())

        # ═══ SMART WEIGHTED ENSEMBLE (city-specific model accuracy) ═══
        # UKMO gets 2.5x weight for London (0°C error in real data)
        # GFS gets 2.2x for NYC (NOAA = best for US)
        # ICON gets 2.5x for Munich (DWD = German model)
        try:
            from weather.ml.model_weights import weighted_ensemble_mean
            mean_max, std_max = weighted_ensemble_mean(model_temps, city)
        except ImportError:
            # Fallback to equal weighting
            mean_max = sum(temps_list) / len(temps_list)
            variance = sum((t - mean_max) ** 2 for t in temps_list) / len(temps_list)
            std_max = math.sqrt(variance) if variance > 0 else 0.5

        # Bias correction
        bias = self._get_bias(city)
        adjusted_mean = mean_max + bias

        # Build probability distribution (in the city's unit)
        prob_dist = self._build_probability_distribution(adjusted_mean, std_max, unit)

        # Confidence scoring
        if std_max < 0.5:
            confidence = 0.95
        elif std_max < 1.0:
            confidence = 0.85
        elif std_max < 2.0:
            confidence = 0.70
        elif std_max < 3.5:
            confidence = 0.50
        else:
            confidence = 0.30

        # Boost confidence with more APIs
        api_count = sum(1 for k in model_temps if not k.startswith('openmeteo'))
        if api_count >= 2:
            confidence = min(0.98, confidence + 0.05)
        elif api_count >= 1:
            confidence = min(0.95, confidence + 0.03)

        unit_symbol = '°F' if unit == 'fahrenheit' else '°C'

        result = {
            'city': city,
            'date': date_str,
            'models': model_temps,
            'num_models': len(model_temps),
            'mean_max': round(adjusted_mean, 1),
            'raw_mean_max': round(mean_max, 1),
            'std_max': round(std_max, 2),
            'min_forecast': round(min(temps_list), 1),
            'max_forecast': round(max(temps_list), 1),
            'confidence': confidence,
            'bias_correction': round(bias, 2),
            'probability_distribution': prob_dist,
            'unit': unit,
            'unit_symbol': unit_symbol,
            'fetched_at': time.time(),
        }

        self._ensemble_cache[cache_key] = result
        return result

    def get_max_temp_probability(self, city: str, target_date: date = None,
                                  temp: int = None) -> Optional[Dict]:
        """
        Get probability that max temp equals a specific value.
        Temperature is in the city's native unit (°F or °C).
        """
        ensemble = self.get_ensemble_forecast(city, target_date)
        if not ensemble:
            return None

        result = {
            'city': city,
            'date': ensemble['date'],
            'mean_forecast': ensemble['mean_max'],
            'std_forecast': ensemble['std_max'],
            'probability_distribution': ensemble['probability_distribution'],
            'target_temp': temp,
            'target_probability': None,
            'confidence': ensemble['confidence'],
            'num_models': ensemble['num_models'],
            'unit': ensemble['unit'],
        }

        if temp is not None:
            result['target_probability'] = ensemble['probability_distribution'].get(temp, 0.0)

        return result

    def get_historical_accuracy(self, city: str, days_back: int = 14) -> Optional[Dict]:
        """Fetch historical forecast vs actual for bias correction."""
        city = city.lower().replace(' ', '-')
        if city not in self.CITIES:
            return None

        city_info = self.CITIES[city]
        unit = city_info.get('unit', 'celsius')
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=days_back)
        temp_unit_param = '&temperature_unit=fahrenheit' if unit == 'fahrenheit' else ''

        try:
            url = (
                f"https://archive-api.open-meteo.com/v1/archive"
                f"?latitude={city_info['lat']}&longitude={city_info['lon']}"
                f"&daily=temperature_2m_max"
                f"&timezone={city_info['timezone']}"
                f"&start_date={start_date.isoformat()}"
                f"&end_date={end_date.isoformat()}"
                f"{temp_unit_param}"
            )
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200:
                return None

            data = resp.json()
            daily = data.get('daily', {})
            dates = daily.get('time', [])
            actuals = daily.get('temperature_2m_max', [])
            if not actuals:
                return None

            forecast_url = (
                f"{self.BASE_URL}/forecast"
                f"?latitude={city_info['lat']}&longitude={city_info['lon']}"
                f"&daily=temperature_2m_max"
                f"&timezone={city_info['timezone']}"
                f"&past_days={days_back}"
                f"{temp_unit_param}"
            )
            fc_resp = self.session.get(forecast_url, timeout=15)
            forecasts = []
            if fc_resp.status_code == 200:
                fc_data = fc_resp.json()
                fc_daily = fc_data.get('daily', {})
                forecasts = fc_daily.get('temperature_2m_max', [])

            pairs = []
            errors = []
            for i, (d, actual) in enumerate(zip(dates, actuals)):
                if actual is None:
                    continue
                fc_temp = forecasts[i] if i < len(forecasts) and forecasts[i] is not None else actual
                pairs.append((d, fc_temp, actual))
                errors.append(fc_temp - actual)

            if not errors:
                return None

            bias = sum(errors) / len(errors)
            mae = sum(abs(e) for e in errors) / len(errors)
            self._bias_data[city] = [(p[1], p[2]) for p in pairs]

            return {
                'city': city, 'days': len(pairs),
                'bias': round(bias, 2), 'mae': round(mae, 2),
                'pairs': pairs, 'unit': unit,
            }
        except Exception as e:
            print(f"⚠️ Historical data error: {e}", flush=True)
            return None

    # ═══════════════════════════════════════════════════════════════════
    # ADDITIONAL API SOURCES
    # ═══════════════════════════════════════════════════════════════════

    def _fetch_tomorrow_io(self, city_info: Dict, date_str: str, unit: str) -> Optional[float]:
        """Fetch max temp from Tomorrow.io API."""
        api_key = Config.TOMORROW_IO_API_KEY
        if not api_key:
            return None
        try:
            url = (
                f"https://api.tomorrow.io/v4/weather/forecast"
                f"?location={city_info['lat']},{city_info['lon']}"
                f"&apikey={api_key}"
                f"&units={'imperial' if unit == 'fahrenheit' else 'metric'}"
            )
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                daily = data.get('timelines', {}).get('daily', [])
                for day in daily:
                    day_date = day.get('time', '')[:10]
                    if day_date == date_str:
                        values = day.get('values', {})
                        return values.get('temperatureMax')
        except Exception:
            pass
        return None

    def _fetch_weatherapi(self, city_info: Dict, date_str: str, unit: str) -> Optional[float]:
        """Fetch max temp from WeatherAPI."""
        api_key = Config.WEATHERAPI_KEY
        if not api_key:
            return None
        try:
            url = (
                f"https://api.weatherapi.com/v1/forecast.json"
                f"?key={api_key}"
                f"&q={city_info['lat']},{city_info['lon']}"
                f"&dt={date_str}"
            )
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                day = data.get('forecast', {}).get('forecastday', [{}])[0].get('day', {})
                if unit == 'fahrenheit':
                    return day.get('maxtemp_f')
                return day.get('maxtemp_c')
        except Exception:
            pass
        return None

    def _fetch_openweathermap(self, city_info: Dict, date_str: str, unit: str) -> Optional[float]:
        """Fetch max temp from OpenWeatherMap One Call API 3.0."""
        api_key = Config.OPENWEATHER_API_KEY
        if not api_key:
            return None
        try:
            # Try One Call API 3.0 first (most accurate)
            url = (
                f"https://api.openweathermap.org/data/3.0/onecall"
                f"?lat={city_info['lat']}&lon={city_info['lon']}"
                f"&appid={api_key}"
                f"&units={'imperial' if unit == 'fahrenheit' else 'metric'}"
                f"&exclude=minutely,hourly,alerts"
            )
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                daily = data.get('daily', [])
                for day in daily:
                    dt = datetime.fromtimestamp(day.get('dt', 0))
                    if dt.strftime('%Y-%m-%d') == date_str:
                        temp = day.get('temp', {})
                        return temp.get('max')

            # Fallback: 5-day forecast API (free tier)
            url2 = (
                f"https://api.openweathermap.org/data/2.5/forecast"
                f"?lat={city_info['lat']}&lon={city_info['lon']}"
                f"&appid={api_key}"
                f"&units={'imperial' if unit == 'fahrenheit' else 'metric'}"
            )
            resp2 = self.session.get(url2, timeout=10)
            if resp2.status_code == 200:
                data2 = resp2.json()
                max_temp = None
                for item in data2.get('list', []):
                    dt_txt = item.get('dt_txt', '')
                    if dt_txt.startswith(date_str):
                        temp = item.get('main', {}).get('temp_max', 0)
                        if max_temp is None or temp > max_temp:
                            max_temp = temp
                return max_temp
        except Exception:
            pass
        return None

    # ═══════════════════════════════════════════════════════════════════
    # INTERNAL
    # ═══════════════════════════════════════════════════════════════════

    def _get_bias(self, city: str) -> float:
        if city in self._bias_data and self._bias_data[city]:
            errors = [fc - actual for fc, actual in self._bias_data[city]]
            return sum(errors) / len(errors)
        return 0.0

    def _build_probability_distribution(self, mean: float, std: float,
                                         unit: str) -> Dict[int, float]:
        """
        Build discrete probability distribution for max temperature.

        For Fahrenheit cities: each integer °F (e.g., 42, 43, 44...)
        For Celsius cities: each integer °C (e.g., 12, 13, 14...)

        Uses Gaussian centered on ensemble mean with ensemble std.
        """
        if std < 0.1:
            std = 0.5 if unit == 'celsius' else 1.0

        probs = {}
        total = 0.0
        center = round(mean)

        # Range: ±15 for Fahrenheit (wider range), ±10 for Celsius
        spread = 15 if unit == 'fahrenheit' else 10

        for temp in range(center - spread, center + spread + 1):
            p = self._gaussian_cdf(temp + 0.5, mean, std) - self._gaussian_cdf(temp - 0.5, mean, std)
            if p > 0.001:
                probs[temp] = round(p, 4)
                total += p

        if total > 0:
            for temp in probs:
                probs[temp] = round(probs[temp] / total, 4)

        return probs

    @staticmethod
    def _gaussian_cdf(x: float, mean: float, std: float) -> float:
        return 0.5 * (1 + math.erf((x - mean) / (std * math.sqrt(2))))

    def clear_cache(self):
        self._forecast_cache.clear()
        self._ensemble_cache.clear()
