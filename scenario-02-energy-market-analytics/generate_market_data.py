#!/usr/bin/env python3
"""
Scenariusz 02: Analityka Rynku Energii
Generator danych rynku energii elektrycznej w Polsce.

Generuje 5 plików CSV z 2-letnimi danymi:
  - spot_prices.csv          (~17 500 wierszy, godzinowe)
  - generation_mix.csv       (~17 500 wierszy, godzinowe)
  - bilateral_contracts.csv  (~5 000 wierszy)
  - carbon_emissions.csv     (~730 wierszy, dzienne)
  - market_participants.csv  (~50 wierszy)
"""

import os
import sys
import csv
import math
import random
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.utils import (
    now_cet,
    time_range_daily_cet,
    time_range_hourly_cet,
    format_ts,
    ensure_output_dir,
)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------
SEED = 42

ENERGY_SOURCES = ["Wiatr", "Słońce", "Gaz", "Węgiel", "Biomasa", "Woda", "Atom"]

BUYERS = [
    "PGE Obrót",
    "Tauron Sprzedaż",
    "Enea Trading",
    "Energa Obrót",
    "innogy Polska",
    "E.ON Energia",
    "Polenergia Trading",
    "Axpo Polska",
    "Statkraft Polska",
]

SELLERS = [
    "PGE Energia",
    "Tauron Wytwarzanie",
    "Enea Wytwarzanie",
    "Energa Wytwarzanie",
    "ZE PAK",
    "PKN Orlen",
    "Polenergia",
    "CEZ Polska",
    "Fortum Power",
]

CONTRACT_TYPES = ["Baseload", "Peak", "OffPeak", "Weekend"]
PRODUCTS = ["Month", "Quarter", "Year"]
STATUSES = ["Active", "Expired", "Cancelled"]


# ---------------------------------------------------------------------------
# Pomocnicze funkcje generatorów
# ---------------------------------------------------------------------------
def seasonal_factor(dt):
    """Czynnik sezonowy: wyższy zimą (grzanie), niższy latem."""
    day_of_year = dt.timetuple().tm_yday
    return 1.0 + 0.3 * math.cos(2 * math.pi * (day_of_year - 15) / 365)


def daily_factor(hour: int) -> float:
    """Czynnik dobowy -- wyższy w szczycie, niższy w nocy."""
    if 7 <= hour <= 9:
        return 1.30
    elif 10 <= hour <= 16:
        return 1.15
    elif 17 <= hour <= 20:
        return 1.35
    elif 0 <= hour <= 5:
        return 0.70
    else:
        return 0.90


def generate_noise(rng: random.Random, scale: float = 0.1) -> float:
    """Szum gaussowski o zadanej skali."""
    return rng.gauss(0, scale)


# ---------------------------------------------------------------------------
# Generator 1: Ceny spot
# ---------------------------------------------------------------------------
def generate_spot_prices(rng, timestamps, data_dir):
    """Generuje godzinowe ceny spot na rynku energii."""
    path = os.path.join(data_dir, "spot_prices.csv")
    print("  Generowanie spot_prices.csv ...")

    base_price = 450.0  # PLN/MWh -- typowa cena bazowa

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "hour", "date", "price_pln_mwh", "volume_mwh",
            "zone", "fixing_type", "min_price", "max_price",
            "weighted_avg_price",
        ])

        for ts in timestamps:
            hour = ts.hour
            date_str = ts.strftime("%Y-%m-%d")

            sf = seasonal_factor(ts)
            df = daily_factor(hour)
            noise = generate_noise(rng, scale=80)

            # Sporadyczne skoki cenowe (~2 % godzin)
            spike = 1.0
            if rng.random() < 0.02:
                spike = rng.uniform(2.0, 3.5)

            price = max(50.0, base_price * sf * df * spike + noise)
            price = round(price, 2)

            volume = round(rng.uniform(15_000, 28_000) * df, 1)
            fixing_type = rng.choice(["RDN", "RDN", "RDN", "RDB"])

            min_price = round(price * rng.uniform(0.85, 0.95), 2)
            max_price = round(price * rng.uniform(1.05, 1.20), 2)
            weighted_avg = round(price * rng.uniform(0.97, 1.03), 2)

            writer.writerow([
                format_ts(ts), hour, date_str, price, volume,
                "KSE", fixing_type, min_price, max_price, weighted_avg,
            ])

    row_count = len(timestamps)
    print(f"    -> {path}  ({row_count:,} wierszy)")
    return row_count


# ---------------------------------------------------------------------------
# Generator 2: Miks energetyczny
# ---------------------------------------------------------------------------
def generate_generation_mix(rng, timestamps, data_dir):
    """Generuje godzinowy miks energetyczny KSE."""
    path = os.path.join(data_dir, "generation_mix.csv")
    print("  Generowanie generation_mix.csv ...")

    first_ts = timestamps[0]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "hour", "date",
            "wind_mw", "solar_mw", "gas_mw", "coal_mw",
            "biomass_mw", "hydro_mw", "nuclear_mw",
            "total_mw", "oze_share_pct",
        ])

        for ts in timestamps:
            hour = ts.hour
            date_str = ts.strftime("%Y-%m-%d")
            doy = ts.timetuple().tm_yday

            # --- Wiatr: 2 000–8 000 MW, wyższy nocą, sezonowość ---
            wind_base = 4000 + 2000 * math.sin(
                2 * math.pi * (doy - 60) / 365
            )
            wind_night = 500 if (hour < 6 or hour > 21) else 0
            wind = max(200.0, wind_base + wind_night + rng.gauss(0, 1200))

            # --- Słońce: 0 w nocy, szczyt w południe, sezonowość ---
            if 6 <= hour <= 20:
                solar_peak = 3000 + 4000 * math.sin(
                    2 * math.pi * (doy - 80) / 365
                )
                hour_curve = math.sin(math.pi * (hour - 6) / 14)
                solar = max(0.0, solar_peak * hour_curve + rng.gauss(0, 500))
            else:
                solar = 0.0

            # --- Węgiel: dominujący 12 000–18 000 MW ---
            coal_base = 15000 - 2000 * math.sin(
                2 * math.pi * (doy - 80) / 365
            )
            coal = max(8000.0, coal_base + rng.gauss(0, 1500))

            # --- Gaz: peakingowy 1 000–4 000 MW ---
            gas_base = 2000 + 1000 * daily_factor(hour)
            gas = max(300.0, gas_base + rng.gauss(0, 600))

            # --- Biomasa: stabilna 500–1 200 MW ---
            biomass = max(200.0, 800 + rng.gauss(0, 200))

            # --- Woda: 500–2 000 MW, więcej wiosną ---
            hydro_base = 1000 + 500 * math.sin(
                2 * math.pi * (doy - 100) / 365
            )
            hydro = max(200.0, hydro_base + rng.gauss(0, 300))

            # --- Atom: pojawia się po ~500 dniach (nowy blok) ---
            days_elapsed = (ts - first_ts).total_seconds() / 86400
            nuclear = max(0.0, 1000 + rng.gauss(0, 100)) if days_elapsed > 500 else 0.0

            total = wind + solar + gas + coal + biomass + hydro + nuclear
            oze = wind + solar + biomass + hydro
            oze_pct = round(100.0 * oze / total, 2) if total > 0 else 0.0

            writer.writerow([
                format_ts(ts), hour, date_str,
                round(wind, 1), round(solar, 1), round(gas, 1),
                round(coal, 1), round(biomass, 1), round(hydro, 1),
                round(nuclear, 1), round(total, 1), oze_pct,
            ])

    row_count = len(timestamps)
    print(f"    -> {path}  ({row_count:,} wierszy)")
    return row_count


# ---------------------------------------------------------------------------
# Generator 3: Kontrakty bilateralne
# ---------------------------------------------------------------------------
def generate_bilateral_contracts(rng, daily_timestamps, data_dir):
    """Generuje kontrakty bilateralne na rynku energii."""
    path = os.path.join(data_dir, "bilateral_contracts.csv")
    print("  Generowanie bilateral_contracts.csv ...")

    num_contracts = 5000
    current = now_cet()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "contract_id", "contract_date", "buyer", "seller",
            "start_date", "end_date", "volume_mwh", "price_pln_mwh",
            "contract_type", "product", "status", "settlement_pln",
        ])

        for i in range(num_contracts):
            contract_date = rng.choice(daily_timestamps)
            product = rng.choice(PRODUCTS)

            duration_days = {"Month": 30, "Quarter": 90, "Year": 365}[product]
            start_date = contract_date + timedelta(days=rng.randint(1, 30))
            end_date = start_date + timedelta(days=duration_days)

            contract_type = rng.choice(CONTRACT_TYPES)

            # Wolumen zależy od produktu
            volume_ranges = {
                "Year": (50_000, 500_000),
                "Quarter": (10_000, 150_000),
                "Month": (2_000, 50_000),
            }
            vmin, vmax = volume_ranges[product]
            volume = round(rng.uniform(vmin, vmax), 1)

            price = round(
                rng.uniform(280, 650) * seasonal_factor(contract_date), 2
            )

            # Status na podstawie daty zakończenia
            if end_date < current:
                status = rng.choices(
                    ["Expired", "Cancelled"], weights=[0.90, 0.10]
                )[0]
            else:
                status = rng.choices(
                    ["Active", "Cancelled"], weights=[0.92, 0.08]
                )[0]

            settlement = round(volume * price, 2) if status != "Cancelled" else 0.0

            buyer = rng.choice(BUYERS)
            # Unikaj tego samego podmiotu jako kupujący i sprzedający
            available_sellers = [
                s for s in SELLERS if s.split()[0] != buyer.split()[0]
            ]
            seller = rng.choice(available_sellers)

            writer.writerow([
                f"CTR-{i + 1:05d}",
                contract_date.strftime("%Y-%m-%d"),
                buyer, seller,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
                volume, price,
                contract_type, product, status, settlement,
            ])

    print(f"    -> {path}  ({num_contracts:,} wierszy)")
    return num_contracts


# ---------------------------------------------------------------------------
# Generator 4: Emisje CO₂
# ---------------------------------------------------------------------------
def generate_carbon_emissions(rng, daily_timestamps, data_dir):
    """Generuje dzienne dane emisji CO₂ skorelowane z miksem energetycznym."""
    path = os.path.join(data_dir, "carbon_emissions.csv")
    print("  Generowanie carbon_emissions.csv ...")

    first_ts = daily_timestamps[0]
    eu_ets_base = 80.0  # EUR/tCO₂

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date", "total_emissions_tco2", "emission_factor_tco2_mwh",
            "coal_emissions", "gas_emissions",
            "total_generation_mwh", "oze_generation_mwh",
            "eu_ets_price_eur", "carbon_cost_pln",
        ])

        for ts in daily_timestamps:
            date_str = ts.strftime("%Y-%m-%d")
            sf = seasonal_factor(ts)

            # Dzienna generacja łączna (MWh)
            total_gen = round(rng.uniform(480_000, 720_000) * sf, 1)

            # Udział OZE rośnie w czasie (trend dekarbonizacji)
            days_elapsed = (ts - first_ts).total_seconds() / 86400
            oze_base_pct = 0.22 + 0.08 * (days_elapsed / 730)
            oze_gen = round(
                total_gen * max(0.05, oze_base_pct + rng.gauss(0, 0.03)), 1
            )

            # Podział paliw kopalnych
            fossil_gen = total_gen - oze_gen
            coal_share = rng.uniform(0.70, 0.85)
            coal_gen = fossil_gen * coal_share
            gas_gen = fossil_gen * (1 - coal_share)

            # Emisje (tys. tCO₂): węgiel ~0.9 tCO₂/MWh, gaz ~0.4 tCO₂/MWh
            coal_em = round(coal_gen * rng.uniform(0.85, 0.95) / 1000, 1)
            gas_em = round(gas_gen * rng.uniform(0.35, 0.45) / 1000, 1)
            total_em = round(coal_em + gas_em, 1)

            em_factor = (
                round(total_em * 1000 / total_gen, 4) if total_gen > 0 else 0.0
            )

            # Cena EU ETS -- trend wzrostowy + zmienność
            eu_ets = round(
                eu_ets_base + 20 * (days_elapsed / 730) + rng.gauss(0, 5), 2
            )
            eu_ets = max(50.0, eu_ets)

            # Koszt emisji w PLN (kurs EUR/PLN ~4.5)
            eur_pln = 4.5 + rng.gauss(0, 0.1)
            carbon_cost = round(total_em * eu_ets * eur_pln, 2)

            writer.writerow([
                date_str, total_em, em_factor,
                coal_em, gas_em,
                total_gen, oze_gen,
                eu_ets, carbon_cost,
            ])

    row_count = len(daily_timestamps)
    print(f"    -> {path}  ({row_count:,} wierszy)")
    return row_count


# ---------------------------------------------------------------------------
# Generator 5: Uczestnicy rynku
# ---------------------------------------------------------------------------
PARTICIPANTS = [
    # (nazwa, typ, udział_rynkowy_%, województwo, numer_licencji)
    # --- Generatorzy ---
    ("PGE Energia", "Generator", 18.5, "Mazowieckie", "GEN-001-PGE"),
    ("Tauron Wytwarzanie", "Generator", 12.3, "Śląskie", "GEN-002-TAU"),
    ("Enea Wytwarzanie", "Generator", 8.7, "Wielkopolskie", "GEN-003-ENE"),
    ("Energa Wytwarzanie", "Generator", 6.2, "Pomorskie", "GEN-004-ENG"),
    ("ZE PAK", "Generator", 4.1, "Wielkopolskie", "GEN-005-ZEP"),
    ("PKN Orlen Energia", "Generator", 3.8, "Mazowieckie", "GEN-006-ORL"),
    ("Polenergia", "Generator", 2.9, "Mazowieckie", "GEN-007-POL"),
    ("CEZ Polska", "Generator", 1.5, "Dolnośląskie", "GEN-008-CEZ"),
    ("Fortum Power", "Generator", 1.2, "Dolnośląskie", "GEN-009-FOR"),
    ("EDP Renewables Polska", "Generator", 1.8, "Zachodniopomorskie", "GEN-010-EDP"),
    ("Iberdrola Polska", "Generator", 0.9, "Pomorskie", "GEN-011-IBE"),
    ("Ørsted Polska", "Generator", 1.1, "Zachodniopomorskie", "GEN-012-ORS"),
    # --- Traderzy ---
    ("PGE Obrót", "Trader", 16.2, "Mazowieckie", "TRD-001-PGE"),
    ("Tauron Sprzedaż", "Trader", 11.8, "Śląskie", "TRD-002-TAU"),
    ("Enea Trading", "Trader", 7.5, "Wielkopolskie", "TRD-003-ENE"),
    ("Energa Obrót", "Trader", 5.8, "Pomorskie", "TRD-004-ENG"),
    ("innogy Polska", "Trader", 3.2, "Mazowieckie", "TRD-005-INN"),
    ("E.ON Energia", "Trader", 2.7, "Mazowieckie", "TRD-006-EON"),
    ("Polenergia Trading", "Trader", 1.9, "Mazowieckie", "TRD-007-POL"),
    ("Axpo Polska", "Trader", 1.4, "Mazowieckie", "TRD-008-AXP"),
    ("Statkraft Polska", "Trader", 1.1, "Mazowieckie", "TRD-009-STK"),
    ("Vattenfall Polska", "Trader", 0.8, "Mazowieckie", "TRD-010-VAT"),
    ("Shell Energy Polska", "Trader", 1.3, "Mazowieckie", "TRD-011-SHL"),
    ("RWE Supply & Trading Polska", "Trader", 0.9, "Mazowieckie", "TRD-012-RWE"),
    # --- Dystrybutorzy ---
    ("PGE Dystrybucja", "Distributor", 25.1, "Mazowieckie", "DST-001-PGE"),
    ("Tauron Dystrybucja", "Distributor", 18.7, "Śląskie", "DST-002-TAU"),
    ("Enea Operator", "Distributor", 14.3, "Wielkopolskie", "DST-003-ENE"),
    ("Energa Operator", "Distributor", 11.9, "Pomorskie", "DST-004-ENG"),
    ("Stoen Operator", "Distributor", 4.5, "Mazowieckie", "DST-005-STO"),
    # --- Odbiorcy przemysłowi ---
    ("ArcelorMittal Poland", "Consumer", 2.1, "Śląskie", "CON-001-ARC"),
    ("KGHM Polska Miedź", "Consumer", 1.8, "Dolnośląskie", "CON-002-KGH"),
    ("JSW", "Consumer", 0.9, "Śląskie", "CON-003-JSW"),
    ("Grupa Azoty", "Consumer", 1.2, "Podkarpackie", "CON-004-GAZ"),
    ("PKN Orlen", "Consumer", 1.5, "Mazowieckie", "CON-005-ORL"),
    ("Synthos", "Consumer", 0.7, "Małopolskie", "CON-006-SYN"),
    ("CMC Poland", "Consumer", 0.5, "Śląskie", "CON-007-CMC"),
    ("Huta Częstochowa", "Consumer", 0.4, "Śląskie", "CON-008-HCZ"),
    ("Toyota Motor Manufacturing Poland", "Consumer", 0.3, "Dolnośląskie", "CON-009-TOY"),
    ("Volkswagen Poznań", "Consumer", 0.35, "Wielkopolskie", "CON-010-VW"),
    ("Amazon Polska", "Consumer", 0.25, "Dolnośląskie", "CON-011-AMZ"),
    ("Google Cloud Poland", "Consumer", 0.4, "Mazowieckie", "CON-012-GOO"),
    ("Microsoft Polska", "Consumer", 0.35, "Mazowieckie", "CON-013-MSF"),
    ("Samsung Electronics Poland", "Consumer", 0.3, "Mazowieckie", "CON-014-SAM"),
    ("LG Energy Solution Wrocław", "Consumer", 0.45, "Dolnośląskie", "CON-015-LGE"),
    ("Stellantis Gliwice", "Consumer", 0.28, "Śląskie", "CON-016-STL"),
    ("Mondi Świecie", "Consumer", 0.5, "Kujawsko-Pomorskie", "CON-017-MON"),
    ("International Paper Kwidzyn", "Consumer", 0.55, "Pomorskie", "CON-018-IPC"),
    ("Bridgestone Stargard", "Consumer", 0.22, "Zachodniopomorskie", "CON-019-BRD"),
    ("Michelin Olsztyn", "Consumer", 0.2, "Warmińsko-Mazurskie", "CON-020-MIC"),
]


def generate_market_participants(rng, data_dir):
    """Generuje dane słownikowe uczestników rynku energii."""
    path = os.path.join(data_dir, "market_participants.csv")
    print("  Generowanie market_participants.csv ...")

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "participant_id", "name", "type", "market_share_pct",
            "region", "license_number",
        ])

        for idx, (name, ptype, share, region, license_no) in enumerate(
            PARTICIPANTS, start=1
        ):
            writer.writerow([
                f"PART-{idx:03d}", name, ptype, share, region, license_no,
            ])

    print(f"    -> {path}  ({len(PARTICIPANTS)} wierszy)")
    return len(PARTICIPANTS)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 65)
    print("  Scenariusz 02: Generator danych rynku energii")
    print("=" * 65)

    data_dir = ensure_output_dir(__file__)
    rng = random.Random(SEED)

    # Zakresy czasowe -- 2 lata wstecz od teraz
    print("\nPrzygotowanie zakresów czasowych (2 lata) ...")
    hourly = time_range_hourly_cet(days_back=730)
    daily = time_range_daily_cet(days_back=730)
    print(f"  Godzinowy: {len(hourly):>7,} rekordów  "
          f"({hourly[0].strftime('%Y-%m-%d')} -> {hourly[-1].strftime('%Y-%m-%d')})")
    print(f"  Dzienny:   {len(daily):>7,} rekordów  "
          f"({daily[0].strftime('%Y-%m-%d')} -> {daily[-1].strftime('%Y-%m-%d')})")

    # Generowanie plików
    print("\nGenerowanie plików CSV ...\n")
    total_rows = 0
    total_rows += generate_spot_prices(rng, hourly, data_dir)
    total_rows += generate_generation_mix(rng, hourly, data_dir)
    total_rows += generate_bilateral_contracts(rng, daily, data_dir)
    total_rows += generate_carbon_emissions(rng, daily, data_dir)
    total_rows += generate_market_participants(rng, data_dir)

    print(f"\n{'=' * 65}")
    print(f"  Zakończono! Łącznie: {total_rows:,} wierszy w 5 plikach")
    print(f"  Katalog: {data_dir}")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
