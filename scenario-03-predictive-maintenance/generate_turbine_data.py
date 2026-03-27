"""
Scenario 03: Predictive Maintenance -- Wind Turbine Data Generator
Generates 5 datasets for wind turbine predictive maintenance demo:
  1. sensor_telemetry.csv   (~115,200 rows) -- 48h of 5-min sensor readings
  2. maintenance_history.csv (~8,000 rows)  -- 2 years of maintenance records
  3. work_orders.csv         (~1,500 rows)  -- open and closed work orders
  4. turbine_specs.csv       (200 rows)     -- turbine specifications
  5. failure_log.csv         (~500 rows)    -- failure records (ML labels)
"""

import csv
import math
import os
import random
import sys
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.utils import (
    ensure_output_dir,
    format_ts,
    now_cet,
    time_range_cet,
    time_range_daily_cet,
)

# ── Deterministic seed ──────────────────────────────────────────────
SEED = 2024_03
random.seed(SEED)

# ── Farm definitions ────────────────────────────────────────────────
FARMS = [
    {"code": "DRL", "name": "Darłowo Wind Park", "count": 45,
     "lat_base": 54.42, "lon_base": 16.41},
    {"code": "PTG", "name": "Potęgowo Wind Farm", "count": 40,
     "lat_base": 54.48, "lon_base": 17.48},
    {"code": "KRS", "name": "Korsze Wind Complex", "count": 35,
     "lat_base": 54.17, "lon_base": 21.14},
    {"code": "PRZ", "name": "Przykona Wind Park", "count": 42,
     "lat_base": 52.10, "lon_base": 18.67},
    {"code": "ZAG", "name": "Zagórz Wind Farm", "count": 38,
     "lat_base": 49.53, "lon_base": 22.27},
]

TURBINE_MODELS = [
    {"model": "Vestas V110", "rated_kw": 2200, "hub_m": 80, "rotor_m": 110},
    {"model": "Siemens SG 3.4-132", "rated_kw": 3400, "hub_m": 114, "rotor_m": 132},
    {"model": "Enercon E-138", "rated_kw": 3500, "hub_m": 131, "rotor_m": 138},
    {"model": "Nordex N131/3900", "rated_kw": 3900, "hub_m": 120, "rotor_m": 131},
]

COMPONENTS = ["Gearbox", "Generator", "Blade", "Bearing", "Hydraulic",
              "Electrical", "Pitch_System"]

MAINTENANCE_TYPES = ["Scheduled", "Corrective", "Predictive", "Emergency"]
WORK_ORDER_TYPES = ["Preventive", "Corrective", "Inspection"]
PRIORITIES = ["Low", "Medium", "High", "Critical"]
WO_STATUSES = ["Open", "In_Progress", "Completed", "Cancelled"]
SEVERITIES = ["Minor", "Major", "Critical"]

FAILURE_TYPES = {
    "Gearbox": ["Gear_Tooth_Wear", "Oil_Leak", "Bearing_Failure", "Overheating"],
    "Generator": ["Winding_Fault", "Insulation_Breakdown", "Brush_Wear", "Overheating"],
    "Blade": ["Crack", "Erosion", "Ice_Damage", "Lightning_Strike"],
    "Bearing": ["Fatigue_Failure", "Lubrication_Failure", "Cage_Damage", "Overheating"],
    "Hydraulic": ["Seal_Leak", "Pump_Failure", "Valve_Malfunction", "Pressure_Loss"],
    "Electrical": ["Cable_Damage", "Converter_Fault", "Sensor_Failure", "Grounding_Issue"],
    "Pitch_System": ["Motor_Failure", "Gear_Wear", "Battery_Degradation", "Control_Fault"],
}

ROOT_CAUSES = {
    "Gearbox": ["Lubrication degradation", "Misalignment", "Overload", "Material fatigue"],
    "Generator": ["Thermal stress", "Vibration damage", "Humidity ingress", "Age wear"],
    "Blade": ["UV degradation", "Impact damage", "Manufacturing defect", "Weather stress"],
    "Bearing": ["Insufficient lubrication", "Contamination", "Overload cycles", "Corrosion"],
    "Hydraulic": ["Seal aging", "Fluid contamination", "Pressure cycling", "Temperature cycling"],
    "Electrical": ["Moisture ingress", "Voltage surge", "Connector corrosion", "Thermal cycling"],
    "Pitch_System": ["Battery aging", "Motor brush wear", "Gear contamination", "Control board fault"],
}

TECHNICIAN_TEAMS = ["Alpha", "Beta", "Gamma", "Delta", "Omega"]

MAINTENANCE_DESCRIPTIONS = {
    "Gearbox": [
        "Wymiana oleju przekładni i inspekcja uzębienia",
        "Regulacja luzu osiowego przekładni",
        "Wymiana łożysk przekładni",
        "Kontrola temperatury i wibracji przekładni",
        "Naprawa nieszczelności oleju",
    ],
    "Generator": [
        "Inspekcja uzwojeń generatora",
        "Wymiana szczotek generatora",
        "Kontrola izolacji uzwojeń",
        "Czyszczenie i smarowanie generatora",
        "Wymiana łożysk generatora",
    ],
    "Blade": [
        "Inspekcja wizualna łopat dronem",
        "Naprawa powłoki łopat -- erozja krawędzi natarcia",
        "Dokręcenie śrub mocujących łopaty",
        "Wymiana uszczelki łopaty",
        "Odladzanie łopat",
    ],
    "Bearing": [
        "Smarowanie łożysk głównych",
        "Kontrola temperatury łożysk -- trend wzrostowy",
        "Wymiana łożyska głównego",
        "Analiza drgań łożysk",
        "Kontrola luzów łożysk",
    ],
    "Hydraulic": [
        "Wymiana płynu hydraulicznego",
        "Kontrola ciśnienia układu hydraulicznego",
        "Wymiana uszczelek cylindrów hydraulicznych",
        "Inspekcja pompy hydraulicznej",
        "Naprawa zaworu sterującego",
    ],
    "Electrical": [
        "Kontrola połączeń elektrycznych w gondoli",
        "Inspekcja przetwornika częstotliwości",
        "Wymiana czujnika temperatury",
        "Kontrola uziemienia",
        "Wymiana kabla zasilającego",
    ],
    "Pitch_System": [
        "Kontrola systemu pitch -- kalibracja kąta",
        "Wymiana akumulatora systemu pitch",
        "Smarowanie przekładni systemu pitch",
        "Kontrola silnika systemu pitch",
        "Aktualizacja oprogramowania sterowania pitch",
    ],
}


# ── Helpers ─────────────────────────────────────────────────────────

def _build_turbine_list():
    """Build the full list of 200 turbines with IDs."""
    turbines = []
    for farm in FARMS:
        for i in range(1, farm["count"] + 1):
            tid = f"T-{farm['code']}-{i:03d}"
            model = random.choice(TURBINE_MODELS)
            turbines.append({
                "turbine_id": tid,
                "farm_code": farm["code"],
                "farm_name": farm["name"],
                "model": model["model"],
                "rated_kw": model["rated_kw"],
                "hub_m": model["hub_m"],
                "rotor_m": model["rotor_m"],
                "lat": farm["lat_base"] + random.uniform(-0.05, 0.05),
                "lon": farm["lon_base"] + random.uniform(-0.05, 0.05),
            })
    return turbines


def _noise(scale=1.0):
    return random.gauss(0, scale)


def _wind_speed(hour: int, base: float = 7.0) -> float:
    """Simulate wind speed with diurnal and random variation."""
    diurnal = 1.5 * math.sin(2 * math.pi * (hour - 6) / 24)
    return max(0.0, base + diurnal + _noise(1.5))


def _seasonal_factor(day_of_year: int) -> float:
    """Winter has higher wind, summer lower."""
    return 1.0 + 0.25 * math.cos(2 * math.pi * (day_of_year - 15) / 365)


# ── Generator 1: Sensor Telemetry ──────────────────────────────────

def generate_sensor_telemetry(turbines: list, data_dir: str):
    """Generate ~115,200 rows: 200 turbines × 576 readings (48h @ 5min)."""
    print("  Generating sensor_telemetry.csv ...")
    filepath = os.path.join(data_dir, "sensor_telemetry.csv")
    timestamps = time_range_cet(hours_back=48, interval_seconds=300)

    # Select ~15 turbines that show degradation
    degrading_ids = set(random.sample([t["turbine_id"] for t in turbines], 15))
    # Select ~5 turbines currently in fault
    fault_ids = set(random.sample(list(degrading_ids), 5))

    # Per-turbine persistent state
    turbine_state = {}
    for t in turbines:
        base_wind = random.uniform(5.5, 9.5)
        base_vibration = random.uniform(1.5, 3.5)
        base_gearbox_temp = random.uniform(45.0, 58.0)
        base_bearing_temp = random.uniform(40.0, 52.0)
        base_generator_temp = random.uniform(48.0, 60.0)
        turbine_state[t["turbine_id"]] = {
            "base_wind": base_wind,
            "base_vibration": base_vibration,
            "base_gearbox_temp": base_gearbox_temp,
            "base_bearing_temp": base_bearing_temp,
            "base_generator_temp": base_generator_temp,
            "nacelle_dir": random.uniform(0, 360),
            "degrading": t["turbine_id"] in degrading_ids,
            "fault": t["turbine_id"] in fault_ids,
            "rated_kw": t["rated_kw"],
        }

    total_readings = len(timestamps)
    fields = [
        "timestamp", "turbine_id", "farm_name", "wind_speed_ms", "rotor_rpm",
        "generator_rpm", "blade_pitch_deg", "nacelle_direction_deg",
        "power_output_kw", "gearbox_temp_c", "bearing_temp_c",
        "generator_temp_c", "hydraulic_pressure_bar", "vibration_mm_s",
        "oil_viscosity", "ambient_temp_c", "humidity_pct", "status",
    ]
    row_count = 0

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for t in turbines:
            tid = t["turbine_id"]
            st = turbine_state[tid]
            rated = st["rated_kw"]

            for idx, ts in enumerate(timestamps):
                progress = idx / total_readings  # 0..1 over 48h
                hour = ts.hour + ts.minute / 60.0

                # Degradation: parameters worsen over time
                degrade_factor = 0.0
                if st["degrading"]:
                    degrade_factor = progress * random.uniform(1.5, 3.0)

                # Wind speed
                wind = _wind_speed(hour, st["base_wind"])
                wind *= _seasonal_factor(ts.timetuple().tm_yday)
                wind = round(max(0.0, wind), 2)

                # Determine status
                if st["fault"] and progress > 0.7:
                    status = "Fault"
                elif random.random() < 0.01:
                    status = "Maintenance"
                elif wind < 3.0 or wind > 25.0:
                    status = "Idle"
                else:
                    status = "Operating"

                # Rotor RPM (proportional to wind up to rated)
                if status == "Operating":
                    rpm_factor = min(wind / 12.0, 1.0)
                    rotor_rpm = round(rpm_factor * random.uniform(13.0, 17.0) + _noise(0.3), 2)
                    generator_rpm = round(rotor_rpm * random.uniform(95, 105) + _noise(5), 1)
                else:
                    rotor_rpm = round(random.uniform(0, 1.5), 2)
                    generator_rpm = round(random.uniform(0, 50), 1)

                # Blade pitch
                if status == "Operating":
                    if wind > 12.0:
                        blade_pitch = round(min(25.0, (wind - 12.0) * 2.5 + _noise(0.5)), 2)
                    else:
                        blade_pitch = round(max(0.0, 2.0 + _noise(0.5)), 2)
                else:
                    blade_pitch = round(random.uniform(85, 90), 2)  # feathered

                # Nacelle direction (slow drift)
                st["nacelle_dir"] = (st["nacelle_dir"] + _noise(2.0)) % 360
                nacelle_dir = round(st["nacelle_dir"], 1)

                # Power output
                if status == "Operating" and wind >= 3.0:
                    cut_in, rated_wind = 3.0, 12.0
                    if wind >= rated_wind:
                        power = rated
                    else:
                        power = rated * ((wind ** 3 - cut_in ** 3) /
                                         (rated_wind ** 3 - cut_in ** 3))
                    # Degradation reduces power
                    power *= max(0.4, 1.0 - degrade_factor * 0.08)
                    power = round(max(0, power + _noise(rated * 0.02)), 1)
                else:
                    power = 0.0

                # Temperatures
                load_factor = power / rated if rated > 0 else 0
                gearbox_temp = round(
                    st["base_gearbox_temp"] + load_factor * 15 +
                    degrade_factor * 5 + _noise(1.0), 1
                )
                bearing_temp = round(
                    st["base_bearing_temp"] + load_factor * 12 +
                    degrade_factor * 6 + _noise(0.8), 1
                )
                generator_temp = round(
                    st["base_generator_temp"] + load_factor * 18 +
                    degrade_factor * 3 + _noise(1.2), 1
                )

                # Vibration
                vibration = round(
                    st["base_vibration"] + load_factor * 1.5 +
                    degrade_factor * 2.5 + _noise(0.3), 3
                )
                if status == "Fault":
                    vibration = round(vibration + random.uniform(3, 8), 3)

                # Hydraulic pressure
                hydraulic = round(200 + _noise(8) - degrade_factor * 10, 1)
                if status != "Operating":
                    hydraulic = round(hydraulic - 15, 1)

                # Oil viscosity
                oil_visc = round(50 + _noise(3) - degrade_factor * 5, 1)

                # Ambient temperature
                ambient = round(10 + 8 * math.sin(2 * math.pi * (hour - 14) / 24) +
                                _noise(1.5), 1)

                # Humidity
                humidity = round(min(100, max(30, 65 - 10 *
                                math.sin(2 * math.pi * (hour - 14) / 24) +
                                _noise(5))), 1)

                writer.writerow({
                    "timestamp": format_ts(ts),
                    "turbine_id": tid,
                    "farm_name": t["farm_name"],
                    "wind_speed_ms": wind,
                    "rotor_rpm": rotor_rpm,
                    "generator_rpm": generator_rpm,
                    "blade_pitch_deg": blade_pitch,
                    "nacelle_direction_deg": nacelle_dir,
                    "power_output_kw": power,
                    "gearbox_temp_c": gearbox_temp,
                    "bearing_temp_c": bearing_temp,
                    "generator_temp_c": generator_temp,
                    "hydraulic_pressure_bar": hydraulic,
                    "vibration_mm_s": max(0, vibration),
                    "oil_viscosity": max(15, oil_visc),
                    "ambient_temp_c": ambient,
                    "humidity_pct": humidity,
                    "status": status,
                })
                row_count += 1

    print(f"    -> {filepath} ({row_count:,} rows)")
    return row_count


# ── Generator 2: Maintenance History ───────────────────────────────

def generate_maintenance_history(turbines: list, data_dir: str):
    """Generate ~8,000 rows of maintenance history over 2 years."""
    print("  Generating maintenance_history.csv ...")
    filepath = os.path.join(data_dir, "maintenance_history.csv")
    days = time_range_daily_cet(days_back=730)

    fields = [
        "record_id", "turbine_id", "farm_name", "date", "maintenance_type",
        "component", "description", "duration_hours", "cost_pln",
        "parts_replaced", "technician_team",
    ]
    rows = []
    record_counter = 0

    for t in turbines:
        # Each turbine gets ~40 maintenance events over 2 years
        n_events = random.randint(30, 55)
        event_days = sorted(random.sample(range(len(days)), min(n_events, len(days))))

        for day_idx in event_days:
            record_counter += 1
            component = random.choice(COMPONENTS)
            m_type = random.choices(
                MAINTENANCE_TYPES, weights=[40, 25, 20, 15], k=1
            )[0]
            description = random.choice(MAINTENANCE_DESCRIPTIONS[component])

            # Duration and cost depend on type
            if m_type == "Emergency":
                duration = round(random.uniform(6, 48), 1)
                cost = round(random.uniform(15000, 120000), 2)
            elif m_type == "Corrective":
                duration = round(random.uniform(4, 24), 1)
                cost = round(random.uniform(5000, 60000), 2)
            elif m_type == "Predictive":
                duration = round(random.uniform(2, 16), 1)
                cost = round(random.uniform(3000, 40000), 2)
            else:  # Scheduled
                duration = round(random.uniform(1, 8), 1)
                cost = round(random.uniform(1000, 15000), 2)

            parts = _random_parts(component)
            team = random.choice(TECHNICIAN_TEAMS)

            rows.append({
                "record_id": f"MH-{record_counter:06d}",
                "turbine_id": t["turbine_id"],
                "farm_name": t["farm_name"],
                "date": days[day_idx].strftime("%Y-%m-%d"),
                "maintenance_type": m_type,
                "component": component,
                "description": description,
                "duration_hours": duration,
                "cost_pln": cost,
                "parts_replaced": parts,
                "technician_team": team,
            })

    random.shuffle(rows)
    rows.sort(key=lambda r: r["date"])

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"    -> {filepath} ({len(rows):,} rows)")
    return len(rows)


def _random_parts(component: str) -> str:
    parts_map = {
        "Gearbox": ["Gear oil 200L", "Sun gear", "Planet gear", "Gearbox bearing",
                     "Oil filter", "Oil seal"],
        "Generator": ["Brush set", "Bearing 6320", "Winding insulation kit",
                       "Slip ring", "Cooling fan"],
        "Blade": ["Epoxy repair kit", "Leading edge tape", "Blade bolt set",
                   "Root seal", "Lightning receptor"],
        "Bearing": ["Main bearing SKF 240/600", "Grease cartridge 25kg",
                     "Bearing seal", "Retaining ring"],
        "Hydraulic": ["Hydraulic fluid 50L", "Cylinder seal kit", "Pressure valve",
                       "Hydraulic pump", "Filter element"],
        "Electrical": ["Power cable 50m", "Converter module", "Temperature sensor",
                        "Fuse set", "Grounding cable"],
        "Pitch_System": ["Pitch motor", "Pitch battery 24V", "Pitch gear set",
                          "Encoder", "Control board"],
    }
    available = parts_map.get(component, ["General parts kit"])
    n = random.randint(1, 3)
    return "; ".join(random.sample(available, min(n, len(available))))


# ── Generator 3: Work Orders ──────────────────────────────────────

def generate_work_orders(turbines: list, data_dir: str):
    """Generate ~1,500 work orders."""
    print("  Generating work_orders.csv ...")
    filepath = os.path.join(data_dir, "work_orders.csv")
    now = now_cet()

    fields = [
        "order_id", "created_date", "turbine_id", "farm_name", "priority",
        "status", "type", "description", "assigned_team", "estimated_hours",
        "actual_hours", "parts_needed", "completion_date",
    ]
    rows = []
    order_counter = 0

    descriptions_by_type = {
        "Preventive": [
            "Planowy przegląd {comp} -- kontrola parametrów",
            "Wymiana filtrów i oleju -- {comp}",
            "Inspekcja roczna -- {comp}",
            "Smarowanie i regulacja -- {comp}",
            "Kontrola momentów dokręcenia -- {comp}",
        ],
        "Corrective": [
            "Naprawa {comp} -- wykryto anomalię wibracji",
            "Wymiana uszczelki {comp} -- wyciek",
            "Naprawa {comp} -- podwyższona temperatura",
            "Korekta ustawień {comp} -- odchylenie od normy",
            "Naprawa {comp} -- nieprawidłowe ciśnienie",
        ],
        "Inspection": [
            "Inspekcja diagnostyczna {comp}",
            "Kontrola wizualna {comp} dronem",
            "Pomiar wibracji {comp} -- trend wzrostowy",
            "Termowizja {comp} -- weryfikacja alarmu",
            "Analiza oleju {comp}",
        ],
    }

    for _ in range(1500):
        order_counter += 1
        t = random.choice(turbines)
        component = random.choice(COMPONENTS)
        wo_type = random.choices(WORK_ORDER_TYPES, weights=[40, 35, 25], k=1)[0]
        priority = random.choices(PRIORITIES, weights=[25, 35, 25, 15], k=1)[0]

        # Dates
        days_ago = random.randint(0, 365)
        created = now - timedelta(days=days_ago)

        # Status depends on age
        if days_ago < 7:
            status = random.choices(
                ["Open", "In_Progress", "Completed", "Cancelled"],
                weights=[40, 35, 20, 5], k=1
            )[0]
        elif days_ago < 30:
            status = random.choices(
                ["Open", "In_Progress", "Completed", "Cancelled"],
                weights=[10, 20, 60, 10], k=1
            )[0]
        else:
            status = random.choices(
                ["Completed", "Cancelled"], weights=[90, 10], k=1
            )[0]

        est_hours = round(random.uniform(1, 48), 1)
        if status == "Completed":
            actual_hours = round(est_hours * random.uniform(0.6, 1.8), 1)
            comp_days = random.randint(1, min(days_ago, 30)) if days_ago > 0 else 0
            completion_date = (created + timedelta(days=comp_days)).strftime("%Y-%m-%d")
        else:
            actual_hours = ""
            completion_date = ""

        desc_template = random.choice(descriptions_by_type[wo_type])
        comp_names = {
            "Gearbox": "przekładni", "Generator": "generatora", "Blade": "łopat",
            "Bearing": "łożysk", "Hydraulic": "ukł. hydraulicznego",
            "Electrical": "ukł. elektrycznego", "Pitch_System": "systemu pitch",
        }
        description = desc_template.format(comp=comp_names.get(component, component))

        parts = _random_parts(component) if wo_type != "Inspection" else ""
        team = random.choice(TECHNICIAN_TEAMS)

        rows.append({
            "order_id": f"WO-{order_counter:05d}",
            "created_date": created.strftime("%Y-%m-%d"),
            "turbine_id": t["turbine_id"],
            "farm_name": t["farm_name"],
            "priority": priority,
            "status": status,
            "type": wo_type,
            "description": description,
            "assigned_team": team,
            "estimated_hours": est_hours,
            "actual_hours": actual_hours,
            "parts_needed": parts,
            "completion_date": completion_date,
        })

    rows.sort(key=lambda r: r["created_date"])

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"    -> {filepath} ({len(rows):,} rows)")
    return len(rows)


# ── Generator 4: Turbine Specs ─────────────────────────────────────

def generate_turbine_specs(turbines: list, data_dir: str):
    """Generate 200 rows of turbine specifications."""
    print("  Generating turbine_specs.csv ...")
    filepath = os.path.join(data_dir, "turbine_specs.csv")
    now = now_cet()

    fields = [
        "turbine_id", "farm_name", "model", "rated_power_kw", "hub_height_m",
        "rotor_diameter_m", "commissioned_date", "last_major_overhaul",
        "total_operating_hours", "latitude", "longitude",
    ]
    rows = []

    for t in turbines:
        # Commissioned 3-10 years ago
        comm_days_ago = random.randint(365 * 3, 365 * 10)
        commissioned = (now - timedelta(days=comm_days_ago)).strftime("%Y-%m-%d")

        # Last overhaul 0-3 years ago
        overhaul_days_ago = random.randint(30, 365 * 3)
        overhaul = (now - timedelta(days=overhaul_days_ago)).strftime("%Y-%m-%d")

        # Operating hours: ~70-90% capacity factor over years
        capacity_factor = random.uniform(0.70, 0.92)
        total_hours = int(comm_days_ago * 24 * capacity_factor)

        rows.append({
            "turbine_id": t["turbine_id"],
            "farm_name": t["farm_name"],
            "model": t["model"],
            "rated_power_kw": t["rated_kw"],
            "hub_height_m": t["hub_m"],
            "rotor_diameter_m": t["rotor_m"],
            "commissioned_date": commissioned,
            "last_major_overhaul": overhaul,
            "total_operating_hours": total_hours,
            "latitude": round(t["lat"], 6),
            "longitude": round(t["lon"], 6),
        })

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"    -> {filepath} ({len(rows):,} rows)")
    return len(rows)


# ── Generator 5: Failure Log ───────────────────────────────────────

def generate_failure_log(turbines: list, data_dir: str):
    """Generate ~500 failure records for ML training labels."""
    print("  Generating failure_log.csv ...")
    filepath = os.path.join(data_dir, "failure_log.csv")
    now = now_cet()

    fields = [
        "failure_id", "turbine_id", "farm_name", "failure_date", "component",
        "failure_type", "severity", "downtime_hours", "root_cause",
        "corrective_action", "cost_pln",
    ]
    rows = []
    failure_counter = 0

    corrective_actions = {
        "Gearbox": ["Wymiana oleju i filtrów", "Wymiana koła zębatego",
                     "Naprawa uszczelnień", "Wymiana łożysk przekładni"],
        "Generator": ["Wymiana szczotek", "Naprawa uzwojeń",
                       "Wymiana łożysk generatora", "Naprawa izolacji"],
        "Blade": ["Naprawa kompozytu epoksydowego", "Wymiana taśmy krawędzi natarcia",
                   "Wymiana łopaty", "Naprawa receptora piorunochronu"],
        "Bearing": ["Wymiana łożyska głównego", "Dosmarowanie i regulacja",
                     "Wymiana uszczelki łożyska", "Wymiana pierścienia oporowego"],
        "Hydraulic": ["Wymiana uszczelek", "Wymiana pompy hydraulicznej",
                       "Naprawa zaworu", "Wymiana płynu hydraulicznego"],
        "Electrical": ["Wymiana kabla", "Naprawa przetwornika",
                        "Wymiana czujnika", "Naprawa uziemienia"],
        "Pitch_System": ["Wymiana silnika pitch", "Wymiana akumulatora",
                          "Wymiana przekładni pitch", "Wymiana płyty sterowania"],
    }

    for _ in range(500):
        failure_counter += 1
        t = random.choice(turbines)
        component = random.choices(
            COMPONENTS, weights=[25, 20, 15, 20, 8, 7, 5], k=1
        )[0]
        f_type = random.choice(FAILURE_TYPES[component])
        severity = random.choices(SEVERITIES, weights=[35, 40, 25], k=1)[0]

        days_ago = random.randint(1, 730)
        failure_date = (now - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")

        if severity == "Critical":
            downtime = round(random.uniform(24, 168), 1)
            cost = round(random.uniform(50000, 250000), 2)
        elif severity == "Major":
            downtime = round(random.uniform(8, 72), 1)
            cost = round(random.uniform(10000, 80000), 2)
        else:
            downtime = round(random.uniform(1, 24), 1)
            cost = round(random.uniform(2000, 25000), 2)

        root_cause = random.choice(ROOT_CAUSES[component])
        action = random.choice(corrective_actions[component])

        rows.append({
            "failure_id": f"FL-{failure_counter:05d}",
            "turbine_id": t["turbine_id"],
            "farm_name": t["farm_name"],
            "failure_date": failure_date,
            "component": component,
            "failure_type": f_type,
            "severity": severity,
            "downtime_hours": downtime,
            "root_cause": root_cause,
            "corrective_action": action,
            "cost_pln": cost,
        })

    rows.sort(key=lambda r: r["failure_date"])

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"    -> {filepath} ({len(rows):,} rows)")
    return len(rows)


# ── Main ────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Scenario 03: Predictive Maintenance -- Data Generator")
    print("=" * 60)

    data_dir = ensure_output_dir(__file__)
    turbines = _build_turbine_list()
    print(f"\n  Turbine fleet: {len(turbines)} turbines across {len(FARMS)} farms\n")

    total = 0
    total += generate_sensor_telemetry(turbines, data_dir)
    total += generate_maintenance_history(turbines, data_dir)
    total += generate_work_orders(turbines, data_dir)
    total += generate_turbine_specs(turbines, data_dir)
    total += generate_failure_log(turbines, data_dir)

    print(f"\n{'=' * 60}")
    print(f"  Total rows generated: {total:,}")
    print(f"  Output directory: {data_dir}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
