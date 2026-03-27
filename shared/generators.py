"""
Common data generation helpers for the Energy Market Demo.

All functions accept timezone-aware datetimes and produce realistic values
modelled on Polish climate and energy consumption patterns.
"""

from __future__ import annotations

import hashlib
import math
import random
from datetime import datetime


# ---------------------------------------------------------------------------
# Noise
# ---------------------------------------------------------------------------


def generate_noise(base: float, pct: float) -> float:
    """
    Add Gaussian noise to a base value.

    Parameters
    ----------
    base : float
        The centre value.
    pct : float
        Noise amplitude expressed as a fraction of *base*
        (e.g. 0.05 means ±5 %).

    Returns
    -------
    float
        ``base`` perturbed by Gaussian noise with σ = base * pct.
    """
    return base + random.gauss(0, abs(base * pct))


# ---------------------------------------------------------------------------
# Temporal multipliers
# ---------------------------------------------------------------------------


def seasonal_factor(timestamp: datetime) -> float:
    """
    Return a seasonal energy-demand multiplier (winter ≈ 1.3, summer ≈ 0.75).

    The curve follows a cosine centred on January (day-of-year ≈ 0/365),
    reflecting higher heating demand in Polish winters and lower demand
    in summer.

    Parameters
    ----------
    timestamp : datetime
        A timezone-aware datetime.

    Returns
    -------
    float
        Multiplier in the approximate range [0.70, 1.35].
    """
    day_of_year = timestamp.timetuple().tm_yday
    # cosine peaks at day 0 (≈ Jan 1) → winter high
    return 1.0 + 0.30 * math.cos(2 * math.pi * (day_of_year - 15) / 365)


def daily_factor(timestamp: datetime) -> float:
    """
    Return a daily load-curve multiplier reflecting typical Polish demand.

    Peak hours:
      - morning ramp  08:00–10:00  (multiplier ≈ 1.25)
      - evening peak   17:00–20:00  (multiplier ≈ 1.30)
    Off-peak:
      - night          01:00–05:00  (multiplier ≈ 0.60)

    Parameters
    ----------
    timestamp : datetime
        A timezone-aware datetime.

    Returns
    -------
    float
        Multiplier in the approximate range [0.55, 1.35].
    """
    hour = timestamp.hour + timestamp.minute / 60.0

    # base cosine trough at ~3 AM, peak at ~15 PM
    base = 0.90 + 0.25 * math.cos(2 * math.pi * (hour - 15) / 24)

    # morning shoulder
    if 7 <= hour < 10:
        base += 0.10 * math.sin(math.pi * (hour - 7) / 3)

    # evening peak
    if 17 <= hour < 21:
        base += 0.15 * math.sin(math.pi * (hour - 17) / 4)

    # deep night dip
    if 1 <= hour < 5:
        base -= 0.10 * math.sin(math.pi * (hour - 1) / 4)

    return base


# ---------------------------------------------------------------------------
# Weather generators
# ---------------------------------------------------------------------------


def _station_seed(station: str) -> int:
    """Deterministic seed derived from a station name."""
    return int(hashlib.md5(station.encode()).hexdigest()[:8], 16)


def weather_temperature(timestamp: datetime, station: str) -> float:
    """
    Return a realistic temperature (°C) for a Polish weather station.

    The temperature follows a seasonal cosine (coldest in January, warmest
    in July) with a diurnal cycle and per-station offset for regional
    variation (e.g. Kasprowy Wierch is colder than Wrocław).

    Parameters
    ----------
    timestamp : datetime
        A timezone-aware datetime.
    station : str
        Weather station name (used for a deterministic regional offset).

    Returns
    -------
    float
        Temperature in °C, roughly in the range [-20, 35].
    """
    day_of_year = timestamp.timetuple().tm_yday
    hour = timestamp.hour + timestamp.minute / 60.0

    # seasonal: avg 8 °C, amplitude 14 °C, peak ≈ day 200 (mid-July)
    seasonal = 8.0 + 14.0 * math.cos(2 * math.pi * (day_of_year - 200) / 365)

    # diurnal: amplitude 5 °C, peak at 14:00
    diurnal = 5.0 * math.cos(2 * math.pi * (hour - 14) / 24)

    # per-station offset (−4 … +4 °C)
    rng = random.Random(_station_seed(station) + day_of_year)
    station_offset = (rng.random() - 0.5) * 8.0

    noise = random.gauss(0, 1.2)

    return seasonal + diurnal + station_offset + noise


def wind_speed(timestamp: datetime) -> float:
    """
    Return a realistic wind speed (m/s) with seasonal and gust variation.

    Polish wind patterns: stronger in autumn/winter, lighter in summer.
    Includes random gust component.

    Parameters
    ----------
    timestamp : datetime
        A timezone-aware datetime.

    Returns
    -------
    float
        Wind speed in m/s, clipped to [0, 30].
    """
    day_of_year = timestamp.timetuple().tm_yday
    hour = timestamp.hour + timestamp.minute / 60.0

    # seasonal base: higher in winter (≈ 9 m/s), lower in summer (≈ 5 m/s)
    seasonal_base = 7.0 + 2.0 * math.cos(2 * math.pi * (day_of_year - 15) / 365)

    # slight diurnal: windier in the afternoon
    diurnal = 0.8 * math.cos(2 * math.pi * (hour - 14) / 24)

    # gust component
    gust = max(0, random.gauss(0, 1.5))
    if random.random() < 0.05:  # 5 % chance of strong gust
        gust += random.uniform(3, 8)

    speed = seasonal_base + diurnal + gust + random.gauss(0, 0.8)
    return max(0.0, min(30.0, speed))


def solar_irradiance(timestamp: datetime) -> float:
    """
    Return solar irradiance (W/m²) based on hour and season.

    Irradiance is zero at night, peaks around solar noon, and is
    significantly higher in summer (long days, high sun angle) than
    in winter for Polish latitudes (~52°N).

    Parameters
    ----------
    timestamp : datetime
        A timezone-aware datetime.

    Returns
    -------
    float
        Irradiance in W/m², in the range [0, ~1050].
    """
    day_of_year = timestamp.timetuple().tm_yday
    hour = timestamp.hour + timestamp.minute / 60.0

    # approximate sunrise / sunset for ~52°N latitude
    # day length varies from ~8 h (Dec) to ~16.5 h (Jun)
    declination = 23.45 * math.sin(2 * math.pi * (day_of_year - 81) / 365)
    lat_rad = math.radians(52.0)
    dec_rad = math.radians(declination)

    cos_hour_angle = -math.tan(lat_rad) * math.tan(dec_rad)
    cos_hour_angle = max(-1.0, min(1.0, cos_hour_angle))
    half_day_hours = math.degrees(math.acos(cos_hour_angle)) / 15.0

    sunrise = 12.0 - half_day_hours
    sunset = 12.0 + half_day_hours

    if hour < sunrise or hour > sunset:
        return 0.0

    # normalised position in the daylight window [0, 1]
    day_fraction = (hour - sunrise) / (sunset - sunrise)

    # bell-shaped curve peaking at solar noon
    solar_angle = math.sin(math.pi * day_fraction)

    # peak irradiance varies by season (higher sun angle in summer)
    max_irradiance = 600 + 400 * math.sin(2 * math.pi * (day_of_year - 81) / 365)

    # cloud factor — random attenuation
    cloud = random.uniform(0.6, 1.0)

    irradiance = max_irradiance * solar_angle * cloud
    return max(0.0, irradiance)
