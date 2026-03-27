"""
Scenariusz 04: Prognozowanie Popytu i Optymalizacja Obciążenia
Generator danych: smart meter readings, historical consumption, weather, tariffs, forecasts.

Generuje 5 plików CSV:
  - data/smart_meter_readings.csv    (~480,000 rows)
  - data/historical_consumption.csv  (~87,600 rows)
  - data/weather_data.csv            (~87,600 rows)
  - data/tariff_schedule.csv         (~50 rows)
  - data/demand_forecasts.csv        (~8,760 rows)
"""

import os
import sys
import csv
import math
import random
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.utils import now_cet, ensure_output_dir, format_ts

SEED = 2004
random.seed(SEED)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REGIONS = [
    "Mazowieckie",
    "Śląskie",
    "Wielkopolskie",
    "Małopolskie",
    "Dolnośląskie",
    "Łódzkie",
    "Pomorskie",
    "Lubelskie",
    "Podkarpackie",
    "Kujawsko-Pomorskie",
]

REGION_WEIGHTS = [0.18, 0.14, 0.11, 0.10, 0.09, 0.08, 0.08, 0.07, 0.08, 0.07]

SEGMENTS = {
    "Household":    {"share": 0.55, "tariffs": ["G11", "G12", "G13"],         "power_min": 1.0,  "power_max": 8.0},
    "Industry":     {"share": 0.10, "tariffs": ["B11", "B21", "A23"],         "power_min": 50.0, "power_max": 500.0},
    "Services":     {"share": 0.20, "tariffs": ["C11", "C21"],               "power_min": 10.0, "power_max": 100.0},
    "Agriculture":  {"share": 0.08, "tariffs": ["C11", "G11"],               "power_min": 5.0,  "power_max": 40.0},
    "Public":       {"share": 0.07, "tariffs": ["C11", "C21", "B11"],        "power_min": 15.0, "power_max": 80.0},
}

WEATHER_STATIONS = {
    "Mazowieckie":        "Warszawa-Okęcie",
    "Śląskie":            "Katowice-Pyrzowice",
    "Wielkopolskie":      "Poznań-Ławica",
    "Małopolskie":        "Kraków-Balice",
    "Dolnośląskie":       "Wrocław-Strachowice",
    "Łódzkie":            "Łódź-Lublinek",
    "Pomorskie":          "Gdańsk-Rębiechowo",
    "Lubelskie":          "Lublin-Radawiec",
    "Podkarpackie":       "Rzeszów-Jasionka",
    "Kujawsko-Pomorskie": "Bydgoszcz-Szwederowo",
}

POLISH_HOLIDAYS_MMDD = [
    "01-01", "01-06", "05-01", "05-03", "08-15",
    "11-01", "11-11", "12-25", "12-26",
]

TARIFF_DATA = [
    ("G11",  "Jednostrefowa (gospodarstwa domowe)",           "Household",    0.65,  12.50, "00:00-24:00", "-",          "Taryfa całodobowa jednolita dla gospodarstw domowych"),
    ("G12",  "Dwustrefowa dzień/noc",                         "Household",    0.72,  14.00, "06:00-13:00,15:00-22:00", "22:00-06:00,13:00-15:00", "Taryfa dwustrefowa: droższa w dzień, tańsza w nocy"),
    ("G12w", "Dwustrefowa weekendowa",                        "Household",    0.70,  14.00, "06:00-13:00,15:00-22:00 (pon-pt)", "weekend + noc", "Taryfa z tańszą energią w weekendy i w nocy"),
    ("G13",  "Trzystrefowa",                                  "Household",    0.78,  15.00, "07:00-13:00,17:00-21:00", "13:00-15:00,21:00-23:00", "Trzystrefowa: szczyt, częściowy szczyt, pozaszczyt"),
    ("C11",  "Jednostrefowa (przedsiębiorstwa)",              "Services",     0.58,  25.00, "00:00-24:00", "-",          "Taryfa jednolita dla odbiorców komercyjnych na niskim napięciu"),
    ("C12a", "Dwustrefowa komercyjna",                        "Services",     0.63,  28.00, "06:00-22:00", "22:00-06:00", "Dwustrefowa komercyjna: dzień/noc"),
    ("C12b", "Dwustrefowa komercyjna szczytowa",              "Services",     0.66,  28.00, "07:00-13:00,16:00-21:00", "reszta", "Dwustrefowa z wydzielonymi godzinami szczytu"),
    ("C21",  "Dwustrefowa z opłatą mocową",                   "Services",     0.55,  45.00, "06:00-22:00", "22:00-06:00", "Dwustrefowa komercyjna z opłatą za moc zamówioną"),
    ("C22a", "Trójstrefowa komercyjna",                       "Services",     0.60,  48.00, "07:00-13:00,17:00-21:00", "13:00-15:00,21:00-23:00", "Trzystrefowa z opłatą mocową dla większych odbiorców"),
    ("C22b", "Trójstrefowa komercyjna szczytowa",             "Services",     0.62,  48.00, "08:00-11:00,17:00-20:00", "reszta", "Trzystrefowa z krótszym szczytem"),
    ("C23",  "Trójstrefowa z mocą i strefami",                "Services",     0.52,  55.00, "07:00-13:00,17:00-21:00", "reszta", "Trzystrefowa dla dużych odbiorców komercyjnych"),
    ("B11",  "Jednostrefowa SN",                              "Industry",     0.48,  120.00, "00:00-24:00", "-",         "Jednolita taryfa na średnim napięciu"),
    ("B12",  "Dwustrefowa SN",                                "Industry",     0.52,  130.00, "06:00-22:00", "22:00-06:00", "Dwustrefowa taryfa na średnim napięciu"),
    ("B21",  "Dwustrefowa SN z mocą",                         "Industry",     0.45,  180.00, "06:00-22:00", "22:00-06:00", "Dwustrefowa SN z opłatą za moc zamówioną"),
    ("B22",  "Trójstrefowa SN z mocą",                        "Industry",     0.50,  200.00, "07:00-13:00,17:00-21:00", "reszta", "Trzystrefowa SN z opłatą za moc zamówioną"),
    ("B23",  "Trójstrefowa SN rozszerzona",                   "Industry",     0.47,  210.00, "07:00-13:00,16:00-21:00", "reszta", "Trzystrefowa SN rozszerzona z mocą"),
    ("A23",  "Trójstrefowa WN",                               "Industry",     0.38,  500.00, "07:00-13:00,17:00-21:00", "reszta", "Trzystrefowa taryfa na wysokim napięciu dla dużych odbiorców przemysłowych"),
    ("A24",  "Czterostrefowa WN",                             "Industry",     0.36,  550.00, "08:00-11:00,17:00-20:00", "reszta", "Czterostrefowa WN dla największych odbiorców"),
    ("R",    "Rolnicza",                                      "Agriculture",  0.55,  18.00, "00:00-24:00", "-",          "Taryfa dla odbiorców rolniczych"),
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def generate_noise(scale: float = 0.05) -> float:
    return random.gauss(0, scale)


def seasonal_factor(day_of_year: int) -> float:
    """Winter high (1.3), summer low (0.75), spring/autumn middle."""
    return 1.0 + 0.25 * math.cos(2 * math.pi * (day_of_year - 15) / 365)


def daily_factor_household(hour: int) -> float:
    """Households: morning peak ~7-9, evening peak ~18-21, low overnight."""
    if 0 <= hour < 5:
        return 0.3
    elif 5 <= hour < 7:
        return 0.5 + (hour - 5) * 0.25
    elif 7 <= hour < 9:
        return 1.0
    elif 9 <= hour < 12:
        return 0.65
    elif 12 <= hour < 14:
        return 0.75
    elif 14 <= hour < 17:
        return 0.6
    elif 17 <= hour < 19:
        return 0.85 + (hour - 17) * 0.1
    elif 19 <= hour < 21:
        return 1.05
    elif 21 <= hour < 23:
        return 0.7
    else:
        return 0.4


def daily_factor_industry(hour: int) -> float:
    """Industry: 6-22 workday ramp-up, peak 8-16."""
    if 0 <= hour < 6:
        return 0.2
    elif 6 <= hour < 8:
        return 0.5 + (hour - 6) * 0.2
    elif 8 <= hour < 16:
        return 0.95
    elif 16 <= hour < 18:
        return 0.8
    elif 18 <= hour < 22:
        return 0.5
    else:
        return 0.15


def daily_factor_services(hour: int) -> float:
    """Services: business hours 8-20, peak 10-17."""
    if 0 <= hour < 6:
        return 0.15
    elif 6 <= hour < 8:
        return 0.4 + (hour - 6) * 0.2
    elif 8 <= hour < 10:
        return 0.8
    elif 10 <= hour < 17:
        return 1.0
    elif 17 <= hour < 20:
        return 0.7
    elif 20 <= hour < 22:
        return 0.35
    else:
        return 0.15


def daily_factor_agriculture(hour: int) -> float:
    """Agriculture: daylight-driven, peak 6-18."""
    if 0 <= hour < 5:
        return 0.1
    elif 5 <= hour < 7:
        return 0.5
    elif 7 <= hour < 11:
        return 0.9
    elif 11 <= hour < 14:
        return 1.0
    elif 14 <= hour < 18:
        return 0.85
    elif 18 <= hour < 20:
        return 0.4
    else:
        return 0.1


def daily_factor_public(hour: int) -> float:
    """Public institutions: 7-17 main, some evening."""
    if 0 <= hour < 6:
        return 0.2
    elif 6 <= hour < 8:
        return 0.5
    elif 8 <= hour < 16:
        return 1.0
    elif 16 <= hour < 18:
        return 0.7
    elif 18 <= hour < 22:
        return 0.4
    else:
        return 0.2


DAILY_FACTORS = {
    "Household": daily_factor_household,
    "Industry": daily_factor_industry,
    "Services": daily_factor_services,
    "Agriculture": daily_factor_agriculture,
    "Public": daily_factor_public,
}


def weather_temperature(day_of_year: int, hour: int) -> float:
    """Realistic Polish temperature: winter cold, summer warm, daily cycle."""
    yearly_avg = 8.5
    yearly_amp = 12.0
    daily_amp = 5.0
    base = yearly_avg + yearly_amp * math.cos(2 * math.pi * (day_of_year - 200) / 365)
    daily = daily_amp * math.cos(2 * math.pi * (hour - 14) / 24)
    return base + daily + random.gauss(0, 1.5)


def is_polish_holiday(dt: datetime) -> bool:
    mmdd = dt.strftime("%m-%d")
    return mmdd in POLISH_HOLIDAYS_MMDD


def weekend_factor(dt: datetime, segment: str) -> float:
    """Reduce industry/services on weekends, slight increase household."""
    dow = dt.weekday()
    if dow >= 5:
        if segment == "Industry":
            return 0.25
        elif segment == "Services":
            return 0.45
        elif segment == "Public":
            return 0.30
        elif segment == "Household":
            return 1.10
        elif segment == "Agriculture":
            return 0.60
    return 1.0


def holiday_factor(dt: datetime, segment: str) -> float:
    if is_polish_holiday(dt):
        if segment == "Industry":
            return 0.15
        elif segment == "Services":
            return 0.30
        elif segment == "Public":
            return 0.20
        elif segment == "Household":
            return 1.15
        elif segment == "Agriculture":
            return 0.40
    return 1.0


def temperature_demand_factor(temp_c: float) -> float:
    """Higher demand when very cold or very hot."""
    if temp_c < 0:
        return 1.0 + abs(temp_c) * 0.04
    elif temp_c > 25:
        return 1.0 + (temp_c - 25) * 0.03
    elif temp_c < 10:
        return 1.0 + (10 - temp_c) * 0.015
    else:
        return 1.0


# ---------------------------------------------------------------------------
# Generate: smart_meter_readings.csv
# ---------------------------------------------------------------------------

def generate_smart_meter_readings(data_dir: str):
    print("Generating smart_meter_readings.csv ...")
    filepath = os.path.join(data_dir, "smart_meter_readings.csv")
    num_meters = 5000
    now = now_cet().replace(second=0, microsecond=0)
    end_time = now.replace(minute=(now.minute // 15) * 15)
    start_time = end_time - timedelta(hours=24)

    # Pre-assign meters to segments and regions
    meters = []
    meter_idx = 0
    for seg_name, seg_info in SEGMENTS.items():
        count = int(num_meters * seg_info["share"])
        for _ in range(count):
            region = random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0]
            tariff = random.choice(seg_info["tariffs"])
            base_power = random.uniform(seg_info["power_min"], seg_info["power_max"])
            meters.append({
                "meter_id": f"SM-{meter_idx:05d}",
                "customer_id": f"CUST-{meter_idx:06d}",
                "segment": seg_name,
                "region": region,
                "tariff": tariff,
                "base_power": base_power,
            })
            meter_idx += 1

    # Fill remaining meters as Household
    while len(meters) < num_meters:
        region = random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0]
        tariff = random.choice(SEGMENTS["Household"]["tariffs"])
        base_power = random.uniform(1.0, 8.0)
        meters.append({
            "meter_id": f"SM-{meter_idx:05d}",
            "customer_id": f"CUST-{meter_idx:06d}",
            "segment": "Household",
            "region": region,
            "tariff": tariff,
            "base_power": base_power,
        })
        meter_idx += 1

    random.shuffle(meters)

    timestamps = []
    t = start_time
    while t <= end_time:
        timestamps.append(t)
        t += timedelta(minutes=15)

    row_count = 0
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "meter_id", "customer_id", "customer_segment", "region",
            "tariff_zone", "active_energy_kwh", "reactive_energy_kvarh",
            "voltage_v", "current_a", "power_factor", "power_kw", "daily_max_kw",
        ])

        for ts in timestamps:
            hour = ts.hour
            doy = ts.timetuple().tm_yday
            s_factor = seasonal_factor(doy)
            temp = weather_temperature(doy, hour)
            t_factor = temperature_demand_factor(temp)

            for m in meters:
                seg = m["segment"]
                d_factor = DAILY_FACTORS[seg](hour)
                w_factor = weekend_factor(ts, seg)
                h_factor = holiday_factor(ts, seg)
                noise = 1.0 + generate_noise(0.08)

                power_kw = m["base_power"] * d_factor * s_factor * w_factor * h_factor * t_factor * noise
                power_kw = max(0.05, power_kw)

                energy_kwh = power_kw * 0.25  # 15-min interval
                pf = min(0.99, max(0.80, random.gauss(0.94, 0.03)))
                reactive = energy_kwh * math.tan(math.acos(pf))
                voltage = random.gauss(230.0, 3.0)
                voltage = max(207, min(253, voltage))
                current = (power_kw * 1000) / (voltage * pf) if voltage > 0 else 0

                # Track daily max: simplified -- add 10-30% above current
                daily_max = power_kw * random.uniform(1.1, 1.4)

                writer.writerow([
                    format_ts(ts),
                    m["meter_id"],
                    m["customer_id"],
                    seg,
                    m["region"],
                    m["tariff"],
                    round(energy_kwh, 4),
                    round(reactive, 4),
                    round(voltage, 1),
                    round(current, 2),
                    round(pf, 3),
                    round(power_kw, 3),
                    round(daily_max, 3),
                ])
                row_count += 1

    print(f"  -> {filepath}  ({row_count:,} rows)")
    return row_count


# ---------------------------------------------------------------------------
# Generate: historical_consumption.csv
# ---------------------------------------------------------------------------

def generate_historical_consumption(data_dir: str):
    print("Generating historical_consumption.csv ...")
    filepath = os.path.join(data_dir, "historical_consumption.csv")
    now = now_cet()
    start_date = (now - timedelta(days=365)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Base consumption per region in MWh (rough proportional to population)
    region_base_mwh = {
        "Mazowieckie": 520, "Śląskie": 410, "Wielkopolskie": 310,
        "Małopolskie": 290, "Dolnośląskie": 260, "Łódzkie": 220,
        "Pomorskie": 210, "Lubelskie": 180, "Podkarpackie": 170,
        "Kujawsko-Pomorskie": 180,
    }

    customer_counts = {
        "Mazowieckie": 285000, "Śląskie": 220000, "Wielkopolskie": 175000,
        "Małopolskie": 165000, "Dolnośląskie": 145000, "Łódzkie": 125000,
        "Pomorskie": 120000, "Lubelskie": 105000, "Podkarpackie": 100000,
        "Kujawsko-Pomorskie": 105000,
    }

    row_count = 0
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date", "hour", "region", "segment", "total_consumption_mwh",
            "customer_count", "avg_consumption_kwh", "peak_demand_mw",
            "temperature_c", "wind_speed_ms", "cloud_cover_pct",
            "is_holiday", "is_weekend", "day_of_week",
        ])

        current = start_date
        while current < now.replace(minute=0, second=0, microsecond=0):
            date_str = current.strftime("%Y-%m-%d")
            hour = current.hour
            doy = current.timetuple().tm_yday
            dow = current.weekday()
            is_wknd = dow >= 5
            is_hol = is_polish_holiday(current)

            s_factor = seasonal_factor(doy)
            temp = weather_temperature(doy, hour)
            t_factor = temperature_demand_factor(temp)
            wind = max(0, random.gauss(4.0, 2.5) + 2.0 * math.sin(2 * math.pi * doy / 365))
            cloud = max(0, min(100, random.gauss(55, 25) + 15 * math.cos(2 * math.pi * (doy - 180) / 365)))

            for region in REGIONS:
                base = region_base_mwh[region]
                cust_total = customer_counts[region]

                # Distribute across implicit segments for this region aggregate row
                d_factor_avg = (
                    0.55 * daily_factor_household(hour) +
                    0.10 * daily_factor_industry(hour) +
                    0.20 * daily_factor_services(hour) +
                    0.08 * daily_factor_agriculture(hour) +
                    0.07 * daily_factor_public(hour)
                )

                w_adj = 1.0
                if is_wknd:
                    w_adj = 0.55 * 1.10 + 0.10 * 0.25 + 0.20 * 0.45 + 0.08 * 0.60 + 0.07 * 0.30
                h_adj = 1.0
                if is_hol:
                    h_adj = 0.55 * 1.15 + 0.10 * 0.15 + 0.20 * 0.30 + 0.08 * 0.40 + 0.07 * 0.20

                noise = 1.0 + generate_noise(0.03)
                total_mwh = base * d_factor_avg * s_factor * t_factor * w_adj * h_adj * noise / 24.0
                total_mwh = max(0.1, total_mwh)

                peak_mw = total_mwh * random.uniform(1.3, 1.6)
                avg_kwh = (total_mwh * 1000) / cust_total if cust_total > 0 else 0

                writer.writerow([
                    date_str, hour, region, "All",
                    round(total_mwh, 3),
                    cust_total,
                    round(avg_kwh, 4),
                    round(peak_mw, 3),
                    round(temp, 1),
                    round(max(0, wind), 1),
                    round(max(0, min(100, cloud)), 1),
                    is_hol,
                    is_wknd,
                    dow,
                ])
                row_count += 1

            current += timedelta(hours=1)

    print(f"  -> {filepath}  ({row_count:,} rows)")
    return row_count


# ---------------------------------------------------------------------------
# Generate: weather_data.csv
# ---------------------------------------------------------------------------

def generate_weather_data(data_dir: str):
    print("Generating weather_data.csv ...")
    filepath = os.path.join(data_dir, "weather_data.csv")
    now = now_cet()
    start_date = (now - timedelta(days=365)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Regional temperature offsets (relative to base)
    region_temp_offset = {
        "Mazowieckie": 0.0, "Śląskie": -0.5, "Wielkopolskie": 0.3,
        "Małopolskie": -1.0, "Dolnośląskie": 0.5, "Łódzkie": 0.2,
        "Pomorskie": 1.0, "Lubelskie": -0.3, "Podkarpackie": -1.5,
        "Kujawsko-Pomorskie": 0.1,
    }

    row_count = 0
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "station_name", "region", "temperature_c",
            "feels_like_c", "humidity_pct", "wind_speed_ms",
            "wind_direction_deg", "pressure_hpa", "cloud_cover_pct",
            "precipitation_mm", "solar_radiation_wm2",
        ])

        current = start_date
        while current < now.replace(minute=0, second=0, microsecond=0):
            doy = current.timetuple().tm_yday
            hour = current.hour

            for region in REGIONS:
                station = WEATHER_STATIONS[region]
                offset = region_temp_offset[region]
                temp = weather_temperature(doy, hour) + offset

                wind = max(0, random.gauss(3.5, 2.0) + 1.5 * math.sin(2 * math.pi * doy / 365))
                wind_dir = random.randint(0, 359)

                # Wind chill / heat index approximation
                if temp < 10:
                    feels_like = temp - wind * 0.5
                elif temp > 25:
                    feels_like = temp + random.uniform(1, 4)
                else:
                    feels_like = temp - wind * 0.2

                humidity = max(20, min(100, random.gauss(70, 15) + 10 * math.cos(2 * math.pi * (doy - 180) / 365)))
                pressure = random.gauss(1013.25, 8.0) + 5 * math.cos(2 * math.pi * doy / 365)

                cloud = max(0, min(100, random.gauss(55, 25) + 15 * math.cos(2 * math.pi * (doy - 180) / 365)))

                # Precipitation: more in summer, occasional
                precip_chance = 0.15 + 0.10 * math.sin(2 * math.pi * (doy - 100) / 365)
                if random.random() < precip_chance:
                    precipitation = random.expovariate(1.0 / 0.8)
                else:
                    precipitation = 0.0

                # Solar radiation: depends on hour, season, clouds
                if 6 <= hour <= 20:
                    max_radiation = 400 + 500 * math.sin(2 * math.pi * (doy - 80) / 365)
                    hour_factor = math.sin(math.pi * (hour - 6) / 14)
                    solar = max(0, max_radiation * hour_factor * (1 - cloud / 150) + random.gauss(0, 20))
                else:
                    solar = 0.0

                writer.writerow([
                    format_ts(current),
                    station,
                    region,
                    round(temp, 1),
                    round(feels_like, 1),
                    round(humidity, 1),
                    round(max(0, wind), 1),
                    wind_dir,
                    round(pressure, 1),
                    round(max(0, min(100, cloud)), 1),
                    round(max(0, precipitation), 2),
                    round(max(0, solar), 1),
                ])
                row_count += 1

            current += timedelta(hours=1)

    print(f"  -> {filepath}  ({row_count:,} rows)")
    return row_count


# ---------------------------------------------------------------------------
# Generate: tariff_schedule.csv
# ---------------------------------------------------------------------------

def generate_tariff_schedule(data_dir: str):
    print("Generating tariff_schedule.csv ...")
    filepath = os.path.join(data_dir, "tariff_schedule.csv")

    row_count = 0
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "tariff_code", "tariff_name", "segment", "energy_rate_pln_kwh",
            "fixed_charge_pln_month", "peak_hours", "off_peak_hours", "description",
        ])
        for row in TARIFF_DATA:
            writer.writerow(row)
            row_count += 1

    print(f"  -> {filepath}  ({row_count:,} rows)")
    return row_count


# ---------------------------------------------------------------------------
# Generate: demand_forecasts.csv
# ---------------------------------------------------------------------------

def generate_demand_forecasts(data_dir: str):
    print("Generating demand_forecasts.csv ...")
    filepath = os.path.join(data_dir, "demand_forecasts.csv")
    now = now_cet()
    start_date = (now - timedelta(days=365)).replace(hour=0, minute=0, second=0, microsecond=0)

    total_base_mw = 1800.0  # Baseline total grid demand in MW

    row_count = 0
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "hour", "forecasted_demand_mw", "actual_demand_mw",
            "forecast_error_pct", "temperature_forecast_c", "is_peak_hour", "region",
        ])

        current = start_date
        while current < now.replace(minute=0, second=0, microsecond=0):
            hour = current.hour
            doy = current.timetuple().tm_yday
            dow = current.weekday()
            is_wknd = dow >= 5
            is_hol = is_polish_holiday(current)

            s_factor = seasonal_factor(doy)
            temp = weather_temperature(doy, hour)
            t_factor = temperature_demand_factor(temp)

            # Weighted average daily factor across segments
            d_factor = (
                0.55 * daily_factor_household(hour) +
                0.10 * daily_factor_industry(hour) +
                0.20 * daily_factor_services(hour) +
                0.08 * daily_factor_agriculture(hour) +
                0.07 * daily_factor_public(hour)
            )

            w_adj = 1.0
            if is_wknd:
                w_adj = 0.78
            if is_hol:
                w_adj *= 0.70

            actual_mw = total_base_mw * d_factor * s_factor * t_factor * w_adj
            actual_mw *= (1.0 + generate_noise(0.02))
            actual_mw = max(500, actual_mw)

            # Forecast: actual + controlled error (MAPE 3-5%)
            forecast_error = random.gauss(0, 0.035)
            forecasted_mw = actual_mw * (1.0 + forecast_error)
            forecasted_mw = max(400, forecasted_mw)

            error_pct = ((forecasted_mw - actual_mw) / actual_mw * 100) if actual_mw > 0 else 0

            is_peak = (7 <= hour <= 9) or (17 <= hour <= 21)

            temp_forecast = temp + random.gauss(0, 0.8)

            writer.writerow([
                format_ts(current),
                hour,
                round(forecasted_mw, 2),
                round(actual_mw, 2),
                round(error_pct, 2),
                round(temp_forecast, 1),
                is_peak,
                "All",
            ])
            row_count += 1

            current += timedelta(hours=1)

    print(f"  -> {filepath}  ({row_count:,} rows)")
    return row_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("Scenariusz 04: Prognozowanie Popytu -- Generator danych")
    print("=" * 70)

    data_dir = ensure_output_dir(__file__)
    print(f"Output directory: {data_dir}\n")

    total = 0
    total += generate_smart_meter_readings(data_dir)
    total += generate_historical_consumption(data_dir)
    total += generate_weather_data(data_dir)
    total += generate_tariff_schedule(data_dir)
    total += generate_demand_forecasts(data_dir)

    print(f"\nDone! Total rows generated: {total:,}")
    print("=" * 70)


if __name__ == "__main__":
    main()
