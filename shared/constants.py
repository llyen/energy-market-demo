"""
Shared constants for the Energy Market Demo.

Contains definitions of Polish energy infrastructure: substations, wind turbines,
energy sources, market zones, customer segments, weather stations, and voltage levels.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Substations — 50 substations of the Polish transmission grid
# (SE = Stacja Elektroenergetyczna)
# Each entry: (name, latitude, longitude)
# ---------------------------------------------------------------------------
SUBSTATIONS: list[tuple[str, float, float]] = [
    # Mazowieckie / Warszawa region
    ("SE Warszawa Centrum", 52.2297, 21.0122),
    ("SE Warszawa Mory", 52.1934, 20.8686),
    ("SE Warszawa Miłosna", 52.2406, 21.1755),
    ("SE Płock Radziwie", 52.5468, 19.6863),
    ("SE Radom Południe", 51.3927, 21.1471),
    # Małopolskie / Śląskie
    ("SE Kraków Nowa Huta", 50.0728, 20.0384),
    ("SE Kraków Skawina", 49.9752, 19.8283),
    ("SE Katowice Południe", 50.2389, 19.0234),
    ("SE Katowice Roździeń", 50.2624, 19.0560),
    ("SE Częstochowa Północ", 50.8233, 19.1200),
    # Wielkopolskie
    ("SE Poznań Plewiska", 52.3744, 16.8064),
    ("SE Poznań Czerwonak", 52.4632, 16.9818),
    ("SE Konin Gosławice", 52.2100, 18.2500),
    ("SE Piła Północ", 53.1622, 16.7383),
    ("SE Kalisz Wschód", 51.7556, 18.1100),
    # Pomorskie
    ("SE Gdańsk Port", 54.3520, 18.6466),
    ("SE Gdańsk Przyjaźń", 54.3406, 18.7128),
    ("SE Gdynia Wielki Kack", 54.4757, 18.4974),
    ("SE Słupsk Wierzbięcino", 54.4641, 17.0285),
    ("SE Żarnowiec", 54.7564, 18.1306),
    # Dolnośląskie
    ("SE Wrocław Stadion", 51.1079, 17.0385),
    ("SE Wrocław Klecina", 51.0765, 17.0015),
    ("SE Legnica Centrum", 51.2070, 16.1619),
    ("SE Jelenia Góra Zachód", 50.8976, 15.7136),
    ("SE Wałbrzych Południe", 50.7638, 16.2844),
    # Łódzkie
    ("SE Łódź Janów", 51.7500, 19.4350),
    ("SE Łódź Olechów", 51.7400, 19.5100),
    ("SE Piotrków Trybunalski", 51.4049, 19.6886),
    ("SE Bełchatów Rogowiec", 51.2600, 19.3300),
    ("SE Sieradz Północ", 51.6000, 18.7300),
    # Lubelskie
    ("SE Lublin Abramowice", 51.2084, 22.6010),
    ("SE Chełm Zachód", 51.1330, 23.4711),
    ("SE Zamość Południowy", 50.7075, 23.2531),
    # Podkarpackie
    ("SE Rzeszów Widełka", 50.0665, 21.9940),
    ("SE Stalowa Wola", 50.5828, 22.0539),
    # Warmińsko-Mazurskie
    ("SE Olsztyn Mątki", 53.7782, 20.4942),
    ("SE Elbląg Zachód", 54.1522, 19.3840),
    # Podlaskie
    ("SE Białystok Zubki", 53.1175, 23.1886),
    ("SE Suwałki Północ", 54.1118, 22.9307),
    # Zachodniopomorskie
    ("SE Szczecin Glinki", 53.3975, 14.5528),
    ("SE Koszalin Żydowo", 54.1836, 16.2102),
    # Kujawsko-Pomorskie
    ("SE Bydgoszcz Zachód", 53.1235, 17.9945),
    ("SE Toruń Elana", 53.0138, 18.6336),
    ("SE Grudziądz Węgrowo", 53.4837, 18.7536),
    # Lubuskie
    ("SE Zielona Góra Przylep", 51.9466, 15.4850),
    ("SE Gorzów Wielkopolski", 52.7325, 15.2369),
    # Świętokrzyskie
    ("SE Kielce Radkowice", 50.8601, 20.6286),
    ("SE Połaniec Elektrownia", 50.4333, 21.2833),
    # Opolskie
    ("SE Opole Groszowice", 50.6500, 17.9300),
    ("SE Dobrzeń Wielki", 50.7700, 17.8700),
]

# ---------------------------------------------------------------------------
# Wind Turbines — 200 turbines across 5 farms (40 turbines each)
# ---------------------------------------------------------------------------
WIND_FARMS: list[dict] = [
    {
        "name": "Farma Wiatrowa Darłowo",
        "latitude": 54.4261,
        "longitude": 16.4119,
        "region": "Zachodniopomorskie",
    },
    {
        "name": "Farma Wiatrowa Żarnowiec",
        "latitude": 54.7564,
        "longitude": 18.1306,
        "region": "Pomorskie",
    },
    {
        "name": "Farma Wiatrowa Potęgowo",
        "latitude": 54.4953,
        "longitude": 17.4839,
        "region": "Pomorskie",
    },
    {
        "name": "Farma Wiatrowa Margonin",
        "latitude": 52.9700,
        "longitude": 17.2800,
        "region": "Wielkopolskie",
    },
    {
        "name": "Farma Wiatrowa Karścino",
        "latitude": 54.0981,
        "longitude": 15.8856,
        "region": "Zachodniopomorskie",
    },
]


def _build_turbine_list() -> list[dict]:
    """Build the full 200-turbine list with farm assignments."""
    turbines = []
    for farm in WIND_FARMS:
        for i in range(1, 41):
            turbine_id = f"{farm['name'].split()[-1][:3].upper()}-{i:03d}"
            turbines.append(
                {
                    "turbine_id": turbine_id,
                    "farm": farm["name"],
                    "number": i,
                    "latitude": farm["latitude"] + (i % 8) * 0.002,
                    "longitude": farm["longitude"] + (i // 8) * 0.002,
                }
            )
    return turbines


TURBINES: list[dict] = _build_turbine_list()

# ---------------------------------------------------------------------------
# Energy sources (generation mix)
# ---------------------------------------------------------------------------
ENERGY_SOURCES: list[str] = [
    "Wiatr",
    "Słońce",
    "Gaz",
    "Węgiel",
    "Biomasa",
    "Woda",
    "Atom",
]

# ---------------------------------------------------------------------------
# Market zones (KSE = Krajowy System Elektroenergetyczny)
# ---------------------------------------------------------------------------
MARKET_ZONES: list[str] = [
    "KSE",
    "Zona A",
    "Zona B",
    "Zona C",
]

# ---------------------------------------------------------------------------
# Customer segments
# ---------------------------------------------------------------------------
CUSTOMER_SEGMENTS: list[str] = [
    "Gospodarstwa domowe",
    "Przemysł ciężki",
    "Usługi",
    "Rolnictwo",
    "Transport",
]

# ---------------------------------------------------------------------------
# Weather stations (synoptic stations across Poland)
# ---------------------------------------------------------------------------
WEATHER_STATIONS: list[str] = [
    "Warszawa-Okęcie",
    "Kraków-Balice",
    "Gdańsk-Rębiechowo",
    "Wrocław-Strachowice",
    "Poznań-Ławica",
    "Katowice-Pyrzowice",
    "Łódź-Lublinek",
    "Szczecin-Dąbie",
    "Rzeszów-Jasionka",
    "Bydgoszcz-Szwederowo",
    "Lublin-Radawiec",
    "Białystok-Krywlany",
    "Olsztyn-Mazury",
    "Kielce-Suków",
    "Zielona Góra-Babimost",
    "Koszalin-Zegrze Pomorskie",
    "Suwałki",
    "Leszno-Strzyżewice",
    "Hel",
    "Kasprowy Wierch",
]

# ---------------------------------------------------------------------------
# Voltage levels (kV) — Polish transmission grid
# ---------------------------------------------------------------------------
VOLTAGE_LEVELS: list[int] = [110, 220, 400]
