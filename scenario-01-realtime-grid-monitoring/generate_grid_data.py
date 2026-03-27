#!/usr/bin/env python3
"""
Scenario 01 -- Real-Time Grid Monitoring Data Generator

Generates synthetic telemetry data for 50 power grid substations across Poland:
  1. substation_readings.csv  -- ~500k rows of 10-second interval readings (24 hours)
  2. substation_events.csv    -- ~2000 correlated anomaly events
  3. substations_master.csv   -- master data for 50 substations
"""

import csv
import math
import os
import random
import sys
import uuid

# ---------------------------------------------------------------------------
# Path setup -- allow imports from the shared package
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.utils import now_cet, time_range_cet, format_ts, ensure_output_dir
from shared.constants import SUBSTATIONS
from shared.generators import generate_noise, seasonal_factor, daily_factor, weather_temperature

# ---------------------------------------------------------------------------
# Deterministic seed (reproducible data with dynamic timestamps)
# ---------------------------------------------------------------------------
SEED = 42
rng = random.Random(SEED)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _noise(base: float, pct: float) -> float:
    """generate_noise uses the global random state; wrap for deterministic rng."""
    return base + rng.gauss(0, abs(base * pct))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HOURS_BACK = 24
INTERVAL_SECONDS = 10
NOMINAL_VOLTAGE_KV = 220.0
NOMINAL_FREQUENCY_HZ = 50.0

# Build structured substation list from shared constants (tuples -> dicts)
SUBS = []
for idx, (name, lat, lon) in enumerate(SUBSTATIONS, start=1):
    SUBS.append({
        "id": f"SUB-{idx:03d}",
        "name": name,
        "lat": lat,
        "lon": lon,
    })

# Indices of substations that will have anomaly injection windows (10 of 50)
ANOMALY_SUB_IDS = [
    SUBS[2]["id"], SUBS[6]["id"], SUBS[12]["id"], SUBS[20]["id"],
    SUBS[27]["id"], SUBS[33]["id"], SUBS[40]["id"], SUBS[45]["id"],
    SUBS[48]["id"], SUBS[4]["id"],
]

# Anomaly types with their effects
ANOMALY_TYPES = [
    "Voltage_Sag",
    "Voltage_Swell",
    "Frequency_Deviation",
    "Overload",
    "Transformer_Overheat",
    "Breaker_Trip",
]

SEVERITY_LEVELS = ["Low", "Medium", "High", "Critical"]

# Region assignment based on substation name prefix
REGION_MAP = {
    "Warszawa": "Mazowieckie", "Płock": "Mazowieckie", "Radom": "Mazowieckie",
    "Kraków": "Małopolskie", "Katowice": "Śląskie", "Częstochowa": "Śląskie",
    "Poznań": "Wielkopolskie", "Konin": "Wielkopolskie", "Piła": "Wielkopolskie",
    "Kalisz": "Wielkopolskie",
    "Gdańsk": "Pomorskie", "Gdynia": "Pomorskie", "Słupsk": "Pomorskie",
    "Żarnowiec": "Pomorskie",
    "Wrocław": "Dolnośląskie", "Legnica": "Dolnośląskie", "Jelenia": "Dolnośląskie",
    "Wałbrzych": "Dolnośląskie",
    "Łódź": "Łódzkie", "Piotrków": "Łódzkie", "Bełchatów": "Łódzkie",
    "Sieradz": "Łódzkie",
    "Lublin": "Lubelskie", "Chełm": "Lubelskie", "Zamość": "Lubelskie",
    "Rzeszów": "Podkarpackie", "Stalowa": "Podkarpackie",
    "Olsztyn": "Warmińsko-mazurskie", "Elbląg": "Warmińsko-mazurskie",
    "Białystok": "Podlaskie", "Suwałki": "Podlaskie",
    "Szczecin": "Zachodniopomorskie", "Koszalin": "Zachodniopomorskie",
    "Bydgoszcz": "Kujawsko-pomorskie", "Toruń": "Kujawsko-pomorskie",
    "Grudziądz": "Kujawsko-pomorskie",
    "Zielona": "Lubuskie", "Gorzów": "Lubuskie",
    "Kielce": "Świętokrzyskie", "Połaniec": "Świętokrzyskie",
    "Opole": "Opolskie", "Dobrzeń": "Opolskie",
}


def _get_region(name: str) -> str:
    for prefix, region in REGION_MAP.items():
        if prefix in name:
            return region
    return "Mazowieckie"


# Voltage levels per substation -- 400 kV for major, 220 kV otherwise
_HIGH_VOLTAGE_KEYWORDS = [
    "Mory", "Nowa Huta", "Południe", "Plewiska", "Port", "Stadion",
    "Janów", "Abramowice", "Widełka", "Glinki", "Rogowiec", "Radziwie",
]


def _get_voltage_level(name: str) -> int:
    return 400 if any(kw in name for kw in _HIGH_VOLTAGE_KEYWORDS) else 220


# Capacity (MVA) correlated with voltage level
def _get_capacity(name: str) -> int:
    if _get_voltage_level(name) == 400:
        return rng.choice([800, 900, 1000, 1100, 1200, 1300, 1500])
    return rng.choice([350, 400, 450, 500, 550, 600, 650, 700])


# ---------------------------------------------------------------------------
# Anomaly window builder
# ---------------------------------------------------------------------------

def _build_anomaly_windows(timestamps: list) -> dict:
    """
    Create 2-4 anomaly windows per anomaly substation.
    Returns {substation_id: [(start_idx, duration_steps, anomaly_type), ...]}.
    """
    total_steps = len(timestamps)
    windows = {}

    for sub_id in ANOMALY_SUB_IDS:
        n_windows = rng.randint(2, 4)
        sub_windows = []
        for _ in range(n_windows):
            start = rng.randint(100, total_steps - 500)
            duration = rng.randint(30, 180)  # 5-30 minutes of readings
            atype = rng.choice(ANOMALY_TYPES)
            sub_windows.append((start, duration, atype))
        sub_windows.sort(key=lambda w: w[0])
        windows[sub_id] = sub_windows

    return windows


def _is_in_anomaly(step: int, windows: list) -> tuple:
    """Check whether the given step falls inside any anomaly window."""
    for start, duration, atype in windows:
        if start <= step < start + duration:
            return True, atype
    return False, None


def _apply_anomaly(base_values: dict, anomaly_type: str, progress: float) -> dict:
    """Modify base telemetry values to inject an anomaly.
    `progress` goes from 0.0 (start) to 1.0 (peak) and back.
    """
    v = dict(base_values)
    intensity = math.sin(math.pi * progress)  # 0 -> 1 -> 0

    if anomaly_type == "Voltage_Sag":
        v["voltage_kv"] -= 20 * intensity + rng.gauss(0, 2)
        v["status"] = "Critical" if intensity > 0.5 else "Warning"

    elif anomaly_type == "Voltage_Swell":
        v["voltage_kv"] += 18 * intensity + rng.gauss(0, 1.5)
        v["status"] = "Critical" if intensity > 0.6 else "Warning"

    elif anomaly_type == "Frequency_Deviation":
        direction = 1 if rng.random() > 0.5 else -1
        v["frequency_hz"] += direction * (0.8 * intensity + rng.gauss(0, 0.1))
        v["status"] = "Warning" if intensity > 0.3 else "Normal"

    elif anomaly_type == "Overload":
        v["load_pct"] = min(100.0, v["load_pct"] + 35 * intensity)
        v["current_a"] += 200 * intensity
        v["active_power_mw"] += 80 * intensity
        v["transformer_temp_c"] += 15 * intensity
        v["status"] = "Critical" if v["load_pct"] > 90 else "Warning"

    elif anomaly_type == "Transformer_Overheat":
        v["transformer_temp_c"] += 30 * intensity + rng.gauss(0, 2)
        v["status"] = "Critical" if v["transformer_temp_c"] > 85 else "Warning"

    elif anomaly_type == "Breaker_Trip":
        if intensity > 0.7:
            v["current_a"] = rng.uniform(0, 5)
            v["active_power_mw"] = 0.0
            v["load_pct"] = 0.0
            v["status"] = "Critical"
        else:
            v["status"] = "Warning"

    return v


# ---------------------------------------------------------------------------
# Main generators
# ---------------------------------------------------------------------------

def generate_readings(timestamps: list, data_dir: str) -> dict:
    """Generate substation_readings.csv and return anomaly info for event generation."""
    anomaly_windows = _build_anomaly_windows(timestamps)
    readings_path = os.path.join(data_dir, "substation_readings.csv")
    total_rows = len(timestamps) * len(SUBS)

    anomaly_events = []
    active_anomalies = {}

    fieldnames = [
        "timestamp", "substation_id", "substation_name",
        "latitude", "longitude",
        "voltage_kv", "current_a", "frequency_hz", "power_factor",
        "active_power_mw", "reactive_power_mvar", "load_pct",
        "transformer_temp_c", "ambient_temp_c", "status",
    ]

    print(f"  Generating substation_readings.csv ({total_rows:,} rows expected)...")

    row_count = 0
    with open(readings_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for step, ts in enumerate(timestamps):
            # Use shared seasonal / daily factors (they take a datetime)
            s_factor = seasonal_factor(ts)    # ~0.7 (summer) to ~1.3 (winter)
            d_factor = daily_factor(ts)       # ~0.55 (night) to ~1.35 (evening peak)

            # Ambient temperature from shared generator
            base_ambient = weather_temperature(ts, "Warszawa-Okęcie")

            for sub in SUBS:
                sub_id = sub["id"]
                sub_name = sub["name"]

                # Per-substation deterministic variation
                sub_hash = hash(sub_id) % 1000 / 1000.0

                # Base load driven by seasonal + daily multipliers
                base_load = 30 + 25 * sub_hash + 20 * d_factor * s_factor
                base_load = _clamp(base_load + rng.gauss(0, 3.0), 20, 95)

                base_voltage = NOMINAL_VOLTAGE_KV + rng.gauss(0, 3.0)
                base_voltage += -2.0 * (base_load / 100.0)

                base_current = 150 + 450 * (base_load / 100.0) + rng.gauss(0, 15.0)
                base_frequency = NOMINAL_FREQUENCY_HZ + rng.gauss(0, 0.05)
                base_pf = _clamp(
                    0.95 + rng.gauss(0, 0.02) - 0.03 * (base_load / 100.0),
                    0.85, 0.99,
                )

                capacity = _get_capacity(sub_name)
                base_active_power = capacity * (base_load / 100.0) * base_pf * 0.8
                base_active_power = _clamp(base_active_power + rng.gauss(0, 5.0), 10, 250)
                base_reactive = base_active_power * math.tan(math.acos(base_pf)) * 0.5
                base_reactive = _clamp(base_reactive + rng.gauss(0, 2.0), 5, 50)

                base_transformer_temp = 35 + 40 * (base_load / 100.0) + base_ambient * 0.1
                base_transformer_temp = _clamp(
                    base_transformer_temp + rng.gauss(0, 1.5), 30, 90
                )

                ambient_temp = _clamp(base_ambient + rng.gauss(0, 0.5), -15, 40)

                status = "Normal"

                values = {
                    "voltage_kv": round(base_voltage, 2),
                    "current_a": round(_clamp(base_current, 50, 900), 1),
                    "frequency_hz": round(_clamp(base_frequency, 49.5, 50.5), 3),
                    "power_factor": round(base_pf, 3),
                    "active_power_mw": round(base_active_power, 2),
                    "reactive_power_mvar": round(base_reactive, 2),
                    "load_pct": round(base_load, 1),
                    "transformer_temp_c": round(base_transformer_temp, 1),
                    "ambient_temp_c": round(ambient_temp, 1),
                    "status": status,
                }

                # Anomaly injection
                if sub_id in anomaly_windows:
                    windows = anomaly_windows[sub_id]
                    in_anomaly, anomaly_type = _is_in_anomaly(step, windows)
                    if in_anomaly:
                        for w_idx, (w_start, w_dur, w_type) in enumerate(windows):
                            if w_start <= step < w_start + w_dur:
                                progress = (step - w_start) / w_dur
                                values = _apply_anomaly(values, w_type, progress)

                                key = (sub_id, w_idx)
                                if key not in active_anomalies:
                                    active_anomalies[key] = True
                                    anomaly_events.append(
                                        (ts, sub_id, sub_name, w_type, True, w_dur)
                                    )
                                break

                # Clamp final values
                values["voltage_kv"] = round(_clamp(values["voltage_kv"], 190, 250), 2)
                values["current_a"] = round(_clamp(values["current_a"], 0, 900), 1)
                values["frequency_hz"] = round(_clamp(values["frequency_hz"], 49.0, 51.0), 3)
                values["load_pct"] = round(_clamp(values["load_pct"], 0, 100), 1)
                values["transformer_temp_c"] = round(
                    _clamp(values["transformer_temp_c"], 25, 105), 1
                )

                row = {
                    "timestamp": format_ts(ts),
                    "substation_id": sub_id,
                    "substation_name": sub_name,
                    "latitude": sub["lat"],
                    "longitude": sub["lon"],
                    **values,
                }
                writer.writerow(row)
                row_count += 1

            # Progress indicator
            if step % 1000 == 0 and step > 0:
                pct = (step / len(timestamps)) * 100
                print(f"    ... {pct:.0f}% ({row_count:,} rows written)")

    print(f"  [OK] substation_readings.csv -- {row_count:,} rows")
    return {"anomaly_events": anomaly_events, "anomaly_windows": anomaly_windows}


def generate_events(timestamps: list, anomaly_info: dict, data_dir: str):
    """Generate substation_events.csv correlated with anomaly windows."""
    events_path = os.path.join(data_dir, "substation_events.csv")
    anomaly_events_raw = anomaly_info["anomaly_events"]

    fieldnames = [
        "timestamp", "event_id", "substation_id", "substation_name",
        "event_type", "severity", "description", "resolved", "resolution_time_min",
    ]

    descriptions = {
        "Voltage_Sag": "Spadek napięcia poniżej dopuszczalnego zakresu na szynie głównej",
        "Voltage_Swell": "Wzrost napięcia powyżej dopuszczalnego zakresu na szynie głównej",
        "Frequency_Deviation": "Odchylenie częstotliwości sieci od wartości nominalnej 50 Hz",
        "Overload": "Przekroczenie dopuszczalnego obciążenia transformatora",
        "Transformer_Overheat": "Temperatura transformatora przekroczyła próg ostrzegawczy",
        "Breaker_Trip": "Zadziałanie wyłącznika mocy -- odłączenie obwodu",
    }

    severity_map = {
        "Voltage_Sag": ["Medium", "High", "Critical"],
        "Voltage_Swell": ["Medium", "High", "Critical"],
        "Frequency_Deviation": ["Low", "Medium", "High"],
        "Overload": ["High", "Critical"],
        "Transformer_Overheat": ["Medium", "High", "Critical"],
        "Breaker_Trip": ["High", "Critical"],
    }

    print(f"  Generating substation_events.csv...")

    events = []

    # Generate events from anomaly windows
    for ts, sub_id, sub_name, anomaly_type, is_start, duration_steps in anomaly_events_raw:
        duration_min = duration_steps * INTERVAL_SECONDS / 60.0
        resolved = rng.random() > 0.15
        resolution_time = round(rng.uniform(2, duration_min * 1.5), 1) if resolved else None

        event = {
            "timestamp": format_ts(ts),
            "event_id": f"EVT-{uuid.UUID(int=rng.getrandbits(128))}",
            "substation_id": sub_id,
            "substation_name": sub_name,
            "event_type": anomaly_type,
            "severity": rng.choice(severity_map.get(anomaly_type, ["Medium"])),
            "description": descriptions.get(anomaly_type, "Zdarzenie w stacji"),
            "resolved": str(resolved).lower(),
            "resolution_time_min": resolution_time if resolved else "",
        }
        events.append(event)

    # Generate additional random events to reach ~2000
    target_events = 2000
    while len(events) < target_events:
        sub = rng.choice(SUBS)
        ts_idx = rng.randint(0, len(timestamps) - 1)
        ts = timestamps[ts_idx]
        anomaly_type = rng.choice(ANOMALY_TYPES)
        resolved = rng.random() > 0.1
        resolution_time = round(rng.uniform(1, 120), 1) if resolved else None

        event = {
            "timestamp": format_ts(ts),
            "event_id": f"EVT-{uuid.UUID(int=rng.getrandbits(128))}",
            "substation_id": sub["id"],
            "substation_name": sub["name"],
            "event_type": anomaly_type,
            "severity": rng.choice(severity_map.get(anomaly_type, ["Medium"])),
            "description": descriptions.get(anomaly_type, "Zdarzenie w stacji"),
            "resolved": str(resolved).lower(),
            "resolution_time_min": resolution_time if resolved else "",
        }
        events.append(event)

    events.sort(key=lambda e: e["timestamp"])

    with open(events_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

    print(f"  [OK] substation_events.csv -- {len(events):,} rows")


def generate_master(data_dir: str):
    """Generate substations_master.csv -- static master data for all 50 substations."""
    master_path = os.path.join(data_dir, "substations_master.csv")

    fieldnames = [
        "substation_id", "name", "latitude", "longitude",
        "voltage_level_kv", "region", "commissioned_year",
        "capacity_mva", "transformer_count",
    ]

    print(f"  Generating substations_master.csv...")

    with open(master_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for sub in SUBS:
            row = {
                "substation_id": sub["id"],
                "name": sub["name"],
                "latitude": sub["lat"],
                "longitude": sub["lon"],
                "voltage_level_kv": _get_voltage_level(sub["name"]),
                "region": _get_region(sub["name"]),
                "commissioned_year": rng.randint(1965, 2020),
                "capacity_mva": _get_capacity(sub["name"]),
                "transformer_count": rng.randint(2, 6),
            }
            writer.writerow(row)

    print(f"  [OK] substations_master.csv -- {len(SUBS)} rows")


def main():
    print("=" * 60)
    print("  Scenario 01: Real-Time Grid Monitoring -- Data Generator")
    print("=" * 60)

    data_dir = ensure_output_dir(__file__)
    print(f"\nOutput directory: {data_dir}")

    print(f"\nGenerating timestamps (last {HOURS_BACK}h, every {INTERVAL_SECONDS}s)...")
    timestamps = time_range_cet(hours_back=HOURS_BACK, interval_seconds=INTERVAL_SECONDS)
    print(f"  Time range: {format_ts(timestamps[0])} -> {format_ts(timestamps[-1])}")
    print(f"  Total timestamps: {len(timestamps):,}")
    print(f"  Substations: {len(SUBS)}")
    print(f"  Expected readings: ~{len(timestamps) * len(SUBS):,}")

    print(f"\n[1/3] Generating telemetry readings...")
    anomaly_info = generate_readings(timestamps, data_dir)

    print(f"\n[2/3] Generating events...")
    generate_events(timestamps, anomaly_info, data_dir)

    print(f"\n[3/3] Generating master data...")
    generate_master(data_dir)

    print(f"\n{'=' * 60}")
    print(f"  Done! All files saved to: {data_dir}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
