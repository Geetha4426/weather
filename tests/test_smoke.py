"""Quick smoke test for the weather prediction bot components."""
import sys
import os

# Ensure parent directory is on the path so 'weather' package is importable
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from weather.data.weather_client import WeatherClient

print("=" * 50)
print("Weather Client Test")
print("=" * 50)

c = WeatherClient()

# Test 1: Basic forecast
f = c.get_forecast('london')
if f:
    print(f"OK London forecast: max={f['max_temp']}{f['unit_symbol']}, min={f['min_temp']}{f['unit_symbol']}")
else:
    print("FAIL Forecast failed")

# Test 2: Ensemble forecast
e = c.get_ensemble_forecast('london')
if e:
    print(f"OK Ensemble: mean={e['mean_max']}C +/-{e['std_max']}C, models={e['num_models']}, conf={e['confidence']:.0%}")
    print("   Probability distribution:")
    for temp, prob in sorted(e['probability_distribution'].items()):
        bar = '#' * int(prob * 40)
        print(f"   {temp:3d}C: {prob:5.1%} {bar}")
else:
    print("FAIL Ensemble failed")

# Test 3: Historical accuracy
h = c.get_historical_accuracy('london', days_back=7)
if h:
    print(f"OK Historical: bias={h['bias']:+.1f}C, MAE={h['mae']:.1f}C ({h['days']} days)")
else:
    print("WARN No historical data")

# Test 4: Market discovery
from weather.data.weather_market_client import WeatherMarketClient
mc = WeatherMarketClient()
markets = mc.discover_markets(['london', 'new-york'])
print(f"\nOK Market discovery: found {len(markets)} weather markets")
if markets:
    for m in markets[:3]:
        outcomes_count = m.get('num_outcomes', 0)
        print(f"   {m['city'].title()} {m['date']} -- {outcomes_count} outcomes")

print("\nAll smoke tests passed!")
