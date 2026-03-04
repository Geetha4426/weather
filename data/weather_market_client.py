"""
Weather Market Client — Polymarket Weather Market Discovery

Discovers active weather prediction markets on Polymarket via Gamma API.

REAL SLUG PATTERNS (from actual Polymarket data):
  Event slug: highest-temperature-in-{city}-on-{month}-{day}-{year}
  Market slugs:
    - highest-temperature-in-nyc-on-march-3-2026-46forhigher     (X°F or higher)
    - highest-temperature-in-nyc-on-march-3-2026-42-43f           (range X-Y°F)
    - highest-temperature-in-london-on-march-4-2026-14c            (exact X°C)
    - highest-temperature-in-london-on-march-4-2026-12corbelow     (X°C or below)
    - highest-temperature-in-london-on-march-4-2026-20corhigher    (X°C or higher)
    - highest-temperature-in-munich-on-march-5-2026-16c            (exact X°C)

IMPORTANT:
  - negativeRisk: true for ALL weather markets
  - US cities use °F, non-US use °C
  - City names in slugs: 'nyc' (not 'new-york'), 'london', 'chicago', etc.
"""

import re
import time
import json
import requests
from typing import Dict, List, Optional
from datetime import datetime, date, timedelta, timezone

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from weather_prediction.config import Config


class WeatherMarketClient:
    """Discovers and parses weather prediction markets on Polymarket."""

    MONTH_NAMES = {
        1: 'january', 2: 'february', 3: 'march', 4: 'april',
        5: 'may', 6: 'june', 7: 'july', 8: 'august',
        9: 'september', 10: 'october', 11: 'november', 12: 'december',
    }

    # City slug aliases from Polymarket data
    CITY_ALIASES = {
        'new-york': 'nyc', 'new york': 'nyc', 'new york city': 'nyc',
        'los-angeles': 'los-angeles',
        'chicago': 'chicago', 'london': 'london',
        'miami': 'miami', 'seattle': 'seattle',
        'atlanta': 'atlanta', 'dallas': 'dallas',
        'munich': 'munich', 'lucknow': 'lucknow',
        'tokyo': 'tokyo', 'paris': 'paris',
    }

    # Temperature parsing from market titles/slugs
    # Matches: "14°C", "50°F", "42-43°F", "12°C or below", "46°F or higher"
    TEMP_PATTERNS = [
        # Range: "between 42-43°F" or "42-43f" in slug
        re.compile(r'(?:between\s+)?(\d+)\s*[-–]\s*(\d+)\s*°?\s*([FfCc])', re.I),
        # Boundary: "46°F or higher" / "12°C or below"
        re.compile(r'(\d+)\s*°?\s*([FfCc])\s+or\s+(higher|above|lower|below)', re.I),
        # Exact: "14°C" or "50°F"
        re.compile(r'(?:be\s+)?(\d+)\s*°\s*([FfCc])', re.I),
    ]

    # Slug-based temperature parsing
    SLUG_TEMP_PATTERNS = [
        # Range: 42-43f
        re.compile(r'(\d+)-(\d+)([fc])$'),
        # Boundary: 46forhigher, 12corbelow, 20corhigher
        re.compile(r'(\d+)([fc])or(higher|below)$'),
        # Exact: 14c, 50f
        re.compile(r'(\d+)([fc])$'),
    ]

    def __init__(self):
        self.base_url = Config.GAMMA_API_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'weather-trade-bot/2.0',
            'Accept': 'application/json',
        })
        self._cache: List[Dict] = []
        self._cache_ts: float = 0
        self._cache_ttl: float = 60

    def discover_markets(self, cities: List[str] = None) -> List[Dict]:
        """
        Find all active weather prediction markets on Polymarket.

        Returns list of parsed weather market dicts with full outcome details.
        """
        cities = cities or Config.WEATHER_CITIES
        cities = [c.lower().replace(' ', '-') for c in cities]
        # Map to Polymarket slug names
        cities = [self.CITY_ALIASES.get(c, c) for c in cities]

        # Check cache
        if time.time() - self._cache_ts < self._cache_ttl and self._cache:
            return [m for m in self._cache if m.get('city', '') in cities]

        all_markets = []
        found_event_slugs = set()

        # ═══ Strategy 1: Direct event slug lookup ═══
        today = date.today()
        year = today.year

        for city in cities:
            for day_offset in range(Config.WEATHER_LOOKAHEAD_DAYS + 1):
                target = today + timedelta(days=day_offset)
                month_name = self.MONTH_NAMES[target.month]

                # Real Polymarket event slug format
                event_slug = f"highest-temperature-in-{city}-on-{month_name}-{target.day}-{year}"

                try:
                    url = f"{self.base_url}/events?slug={event_slug}"
                    resp = self.session.get(url, timeout=10)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data and isinstance(data, list) and len(data) > 0:
                            event = data[0]
                            if event_slug not in found_event_slugs:
                                parsed = self._parse_weather_event(event, city, target)
                                if parsed:
                                    all_markets.append(parsed)
                                    found_event_slugs.add(event_slug)
                                    print(f"🌤️ Found: {event_slug} ({len(parsed.get('outcomes', []))} outcomes)", flush=True)
                except Exception as e:
                    continue

        # ═══ Strategy 2: Search by tag ═══
        if not all_markets:
            print("🔍 Slug lookup found nothing, trying tag search...", flush=True)
            try:
                url = f"{self.base_url}/events?tag=weather&active=true&closed=false&limit=100"
                resp = self.session.get(url, timeout=15)
                if resp.status_code == 200:
                    events = resp.json()
                    if isinstance(events, list):
                        for event in events:
                            slug = event.get('slug', '')
                            if slug in found_event_slugs:
                                continue
                            parsed = self._parse_weather_event_from_title(event, cities)
                            if parsed:
                                all_markets.append(parsed)
                                found_event_slugs.add(slug)
            except Exception as e:
                print(f"⚠️ Tag search: {e}", flush=True)

        # ═══ Strategy 3: Broad search ═══
        if not all_markets:
            print("🔍 Trying broad search for weather markets...", flush=True)
            try:
                for offset in range(0, 300, 100):
                    url = (
                        f"{self.base_url}/events"
                        f"?active=true&closed=false&limit=100&offset={offset}"
                        f"&order=startDate&ascending=false"
                    )
                    resp = self.session.get(url, timeout=20)
                    if resp.status_code != 200:
                        break
                    events = resp.json()
                    if not events:
                        break
                    for event in events:
                        title = event.get('title', '').lower()
                        slug = event.get('slug', '').lower()
                        if slug in found_event_slugs:
                            continue
                        if 'temperature' in title or 'temperature' in slug:
                            parsed = self._parse_weather_event_from_title(event, cities)
                            if parsed:
                                all_markets.append(parsed)
                                found_event_slugs.add(slug)
                    if len(all_markets) >= 10:
                        break
            except Exception as e:
                print(f"⚠️ Broad search: {e}", flush=True)

        if all_markets:
            cities_found = set(m['city'] for m in all_markets)
            total_outcomes = sum(len(m.get('outcomes', [])) for m in all_markets)
            print(
                f"🌤️ Total: {len(all_markets)} weather events, "
                f"{total_outcomes} outcomes | Cities: {cities_found}",
                flush=True
            )
        else:
            print("⚠️ No weather markets found", flush=True)

        self._cache = all_markets
        self._cache_ts = time.time()

        return [m for m in all_markets if m.get('city', '') in cities]

    def get_market(self, city: str, target_date: date) -> Optional[Dict]:
        """Get a specific weather market by city and date."""
        date_str = target_date.isoformat()
        markets = self.discover_markets([city])
        for m in markets:
            if m['city'] == city and m['date'] == date_str:
                return m
        return None

    # ═══════════════════════════════════════════════════════════════════
    # PARSING
    # ═══════════════════════════════════════════════════════════════════

    def _parse_weather_event(self, event: Dict, city: str, target_date: date) -> Optional[Dict]:
        """Parse a weather event from Gamma API."""
        markets_in_event = event.get('markets', [])
        if not markets_in_event:
            return None

        outcomes = []
        total_volume = 0

        for market in markets_in_event:
            outcome = self._parse_outcome_market(market)
            if outcome:
                outcomes.append(outcome)
                total_volume += outcome.get('volume', 0)

        if not outcomes:
            return None

        # Sort by temperature
        outcomes.sort(key=lambda x: (x.get('temp_low', 0) or 0))

        # Get the first market's resolution source (same for all outcomes)
        resolution_source = ''
        if markets_in_event:
            resolution_source = markets_in_event[0].get('resolutionSource', '')

        # Get precise endDate with time (e.g., "2026-03-05T12:00:00Z")
        end_date_precise = ''
        if markets_in_event:
            end_date_precise = markets_in_event[0].get('endDate', '')

        return {
            'city': city,
            'date': target_date.isoformat(),
            'event_id': event.get('id', ''),
            'event_slug': event.get('slug', ''),
            'title': event.get('title', ''),
            'outcomes': outcomes,
            'end_date': end_date_precise or event.get('endDate', ''),
            'total_volume': total_volume,
            'resolution_source': resolution_source or 'Weather Underground',
            'num_outcomes': len(outcomes),
            'negative_risk': True,  # All weather markets are negativeRisk
            'neg_risk_market_id': markets_in_event[0].get('negRiskMarketID', '') if markets_in_event else '',
        }

    def _parse_weather_event_from_title(self, event: Dict, cities: List[str]) -> Optional[Dict]:
        """Parse a weather event by matching title/slug."""
        slug = event.get('slug', '').lower()
        title = event.get('title', '')

        # Try to extract city and date from event slug
        # Pattern: highest-temperature-in-{city}-on-{month}-{day}-{year}
        slug_pattern = re.compile(
            r'highest-temperature-in-(.+?)-on-(\w+)-(\d+)-(\d{4})'
        )
        match = slug_pattern.search(slug)
        if not match:
            return None

        city_raw = match.group(1)
        month_name = match.group(2).lower()
        day = int(match.group(3))
        year = int(match.group(4))

        # Map city
        matched_city = None
        for c in cities:
            if c == city_raw or city_raw in c or c in city_raw:
                matched_city = c
                break
        if not matched_city:
            c_alias = self.CITY_ALIASES.get(city_raw, city_raw)
            if c_alias in cities:
                matched_city = c_alias

        if not matched_city:
            return None

        # Parse month
        month_num = None
        for num, name in self.MONTH_NAMES.items():
            if name == month_name:
                month_num = num
                break
        if not month_num:
            return None

        try:
            target_date = date(year, month_num, day)
        except ValueError:
            return None

        return self._parse_weather_event(event, matched_city, target_date)

    def _parse_outcome_market(self, market: Dict) -> Optional[Dict]:
        """
        Parse a single outcome market from a weather event.

        Extracts ALL fields from real Gamma API response including:
        - bestBid/bestAsk (no separate CLOB call needed!)
        - groupItemTitle (clean label like "17°C")
        - orderMinSize (5 shares on Polymarket)
        - spread, liquidity, volume24hr
        - negRiskMarketID, conditionId
        """
        # Use groupItemTitle first (cleaner), fall back to question
        group_title = market.get('groupItemTitle', '')
        question = market.get('question', '')
        slug = market.get('slug', '')

        if not question and not slug and not group_title:
            return None

        # Parse temperature from slug first (most reliable)
        temp_info = self._parse_temp_from_slug(slug)

        # Fall back to title parsing
        if not temp_info:
            temp_info = self._parse_temp_from_title(question or group_title)

        if not temp_info:
            return None

        # ═══ Parse token IDs and prices ═══
        tokens = market.get('tokens', [])
        clob_ids_raw = market.get('clobTokenIds', '')
        outcomes_raw = market.get('outcomes', '')
        prices_raw = market.get('outcomePrices', '')

        yes_token = ''
        no_token = ''
        yes_price = 0.5
        no_price = 0.5

        clob_ids = self._parse_json_field(clob_ids_raw)
        outcome_names = self._parse_json_field(outcomes_raw)
        prices = self._parse_json_field(prices_raw, as_float=True)

        # Method 1: tokens array
        if tokens and len(tokens) >= 2:
            for token in tokens:
                outcome = token.get('outcome', '').lower()
                if 'yes' in outcome:
                    yes_token = token.get('token_id', '')
                    yes_price = float(token.get('price', 0.5) or 0.5)
                elif 'no' in outcome:
                    no_token = token.get('token_id', '')
                    no_price = float(token.get('price', 0.5) or 0.5)

        # Method 2: clobTokenIds + outcomes (REAL API uses this)
        if not yes_token and clob_ids and len(clob_ids) >= 2:
            if outcome_names and len(outcome_names) >= 2:
                for i, name in enumerate(outcome_names):
                    name_lower = str(name).lower()
                    if 'yes' in name_lower:
                        yes_token = str(clob_ids[i])
                        if prices and len(prices) > i:
                            yes_price = prices[i]
                    elif 'no' in name_lower:
                        no_token = str(clob_ids[i])
                        if prices and len(prices) > i:
                            no_price = prices[i]
            else:
                # Default: first = YES, second = NO
                yes_token = str(clob_ids[0])
                no_token = str(clob_ids[1]) if len(clob_ids) > 1 else ''
                if prices and len(prices) >= 2:
                    yes_price = prices[0]
                    no_price = prices[1]

        # ═══ Real orderbook data from Gamma (no CLOB call needed!) ═══
        best_bid = float(market.get('bestBid', 0) or 0)
        best_ask = float(market.get('bestAsk', 0) or 0)
        last_trade = float(market.get('lastTradePrice', 0) or 0)
        spread = float(market.get('spread', 0) or 0)

        # Use best available price: bestAsk > outcomePrices > 0.5
        if best_ask > 0:
            yes_price = best_ask  # bestAsk is what you'd BUY at
        if best_bid > 0 and no_price <= 0:
            no_price = 1.0 - best_bid

        # Use groupItemTitle if available (cleaner than full question)
        label = group_title if group_title else question

        return {
            'label': label,
            'slug': slug,
            'temp_low': temp_info['temp_low'],
            'temp_high': temp_info.get('temp_high'),
            'temp_unit': temp_info['unit'],           # 'f' or 'c'
            'is_range': temp_info.get('is_range', False),
            'is_lower_bound': temp_info.get('is_lower', False),
            'is_upper_bound': temp_info.get('is_upper', False),
            'market_id': market.get('conditionId', market.get('id', '')),
            'question_id': market.get('questionID', ''),
            'token_id_yes': yes_token,
            'token_id_no': no_token,
            'price_yes': yes_price,
            'price_no': no_price,
            # Real orderbook from Gamma API
            'best_bid': best_bid,
            'best_ask': best_ask,
            'last_trade': last_trade,
            'spread': spread,
            # Order constraints
            'order_min_size': int(market.get('orderMinSize', 5) or 5),
            'tick_size': float(market.get('orderPriceMinTickSize', 0.001) or 0.001),
            # Market metrics
            'volume': float(market.get('volumeNum', 0) or market.get('volume', 0) or 0),
            'volume_24h': float(market.get('volume24hr', 0) or 0),
            'liquidity': float(market.get('liquidityNum', 0) or market.get('liquidity', 0) or 0),
            'competitive': float(market.get('competitive', 0) or 0),
            # Negative risk
            'negative_risk': market.get('negRisk', True),
            'neg_risk_market_id': market.get('negRiskMarketID', ''),
            # Status
            'accepting_orders': market.get('acceptingOrders', True),
            'active': market.get('active', True),
            # Resolution
            'resolution_source': market.get('resolutionSource', ''),
            'end_date': market.get('endDate', ''),
            # Ordering
            'threshold': int(market.get('groupItemThreshold', 0) or 0),
        }

    def _parse_temp_from_slug(self, slug: str) -> Optional[Dict]:
        """
        Parse temperature from market slug.

        Examples:
          highest-temperature-in-nyc-on-march-3-2026-42-43f  → range 42-43°F
          highest-temperature-in-nyc-on-march-3-2026-46forhigher → 46°F or higher
          highest-temperature-in-london-on-march-4-2026-14c  → exact 14°C
          highest-temperature-in-london-on-march-4-2026-12corbelow → 12°C or below
        """
        if not slug:
            return None

        # Get the last segment after the year
        parts = slug.split('-')

        # Find the temperature part (after the year)
        temp_part = ''
        for i, p in enumerate(parts):
            if p.isdigit() and len(p) == 4:  # Year found
                temp_part = '-'.join(parts[i+1:])
                break

        if not temp_part:
            # Try last segment
            temp_part = parts[-1] if parts else ''

        if not temp_part:
            return None

        # Range: 42-43f
        m = re.match(r'(\d+)-(\d+)([fc])$', temp_part, re.I)
        if m:
            return {
                'temp_low': int(m.group(1)),
                'temp_high': int(m.group(2)),
                'unit': m.group(3).lower(),
                'is_range': True, 'is_lower': False, 'is_upper': False,
            }

        # Boundary: 46forhigher, 12corbelow
        m = re.match(r'(\d+)([fc])or(higher|below)$', temp_part, re.I)
        if m:
            return {
                'temp_low': int(m.group(1)),
                'unit': m.group(2).lower(),
                'is_range': False,
                'is_lower': m.group(3).lower() == 'below',
                'is_upper': m.group(3).lower() == 'higher',
            }

        # Exact: 14c, 50f
        m = re.match(r'(\d+)([fc])$', temp_part, re.I)
        if m:
            return {
                'temp_low': int(m.group(1)),
                'unit': m.group(2).lower(),
                'is_range': False, 'is_lower': False, 'is_upper': False,
            }

        return None

    def _parse_temp_from_title(self, title: str) -> Optional[Dict]:
        """Parse temperature from market title text."""
        if not title:
            return None

        # Range: "between 42-43°F"
        m = re.search(r'(?:between\s+)?(\d+)\s*[-–]\s*(\d+)\s*°?\s*([FfCc])', title)
        if m:
            return {
                'temp_low': int(m.group(1)),
                'temp_high': int(m.group(2)),
                'unit': m.group(3).lower(),
                'is_range': True, 'is_lower': False, 'is_upper': False,
            }

        # Boundary: "46°F or higher" or "12°C or below"
        m = re.search(r'(\d+)\s*°?\s*([FfCc])\s+or\s+(higher|above|lower|below)', title, re.I)
        if m:
            return {
                'temp_low': int(m.group(1)),
                'unit': m.group(2).lower(),
                'is_range': False,
                'is_lower': m.group(3).lower() in ('lower', 'below'),
                'is_upper': m.group(3).lower() in ('higher', 'above'),
            }

        # Exact: "14°C" or "50°F" or "be 14°C"
        m = re.search(r'(?:be\s+)?(\d+)\s*°\s*([FfCc])', title, re.I)
        if m:
            return {
                'temp_low': int(m.group(1)),
                'unit': m.group(2).lower(),
                'is_range': False, 'is_lower': False, 'is_upper': False,
            }

        return None

    def _parse_json_field(self, raw, as_float=False):
        """Parse a JSON string or list field."""
        if not raw:
            return []
        try:
            if isinstance(raw, str):
                parsed = json.loads(raw)
            elif isinstance(raw, list):
                parsed = raw
            else:
                return []
            if as_float:
                return [float(x) for x in parsed]
            return parsed
        except (ValueError, TypeError):
            return []

    # ═══════════════════════════════════════════════════════════════════
    # UTILITY
    # ═══════════════════════════════════════════════════════════════════

    def get_seconds_until_resolution(self, market: Dict) -> int:
        """Calculate seconds until market resolution."""
        end_date = market.get('end_date', '')
        if not end_date:
            return 0
        try:
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            remaining = (end_dt - now).total_seconds()
            return max(0, int(remaining))
        except Exception:
            return 0

    def get_outcome_prices(self, market: Dict) -> Dict:
        """Get a mapping of temperature outcomes to prices."""
        prices = {}
        for outcome in market.get('outcomes', []):
            temp = outcome.get('temp_low')
            if temp is not None:
                label = self._format_temp_label(outcome)
                prices[label] = {
                    'price_yes': outcome.get('price_yes', 0),
                    'temp_low': temp,
                    'temp_high': outcome.get('temp_high'),
                    'is_range': outcome.get('is_range', False),
                    'is_lower_bound': outcome.get('is_lower_bound', False),
                    'is_upper_bound': outcome.get('is_upper_bound', False),
                }
        return prices

    def _format_temp_label(self, outcome: Dict) -> str:
        """Format a human-readable label for a temperature outcome."""
        unit = outcome.get('temp_unit', 'c').upper()
        temp = outcome.get('temp_low', 0)

        if outcome.get('is_range'):
            return f"{temp}-{outcome.get('temp_high', temp)}°{unit}"
        elif outcome.get('is_lower_bound'):
            return f"{temp}°{unit} or below"
        elif outcome.get('is_upper_bound'):
            return f"{temp}°{unit} or higher"
        else:
            return f"{temp}°{unit}"
