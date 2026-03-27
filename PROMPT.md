# 🔋 PROMPT: Odtworzenie Platformy Demo Microsoft Fabric dla Sektora Energetycznego

> **Ten dokument jest kompletną instrukcją pozwalającą odtworzyć od zera całą aplikację
> "Microsoft Fabric — Platforma Demo dla Firm Energetycznych". Opisuje KAŻDY element
> architektury, kodu, konfiguracji i infrastruktury.**

---

## 📋 SPIS TREŚCI

1. [Przegląd projektu](#1-przegląd-projektu)
2. [Decyzje architektoniczne](#2-decyzje-architektoniczne)
3. [Struktura repozytorium](#3-struktura-repozytorium)
4. [Moduł współdzielony (shared/)](#4-moduł-współdzielony-shared)
5. [Scenariusz 01 — Monitoring sieci w czasie rzeczywistym](#5-scenariusz-01--monitoring-sieci-w-czasie-rzeczywistym)
6. [Scenariusz 02 — Analityka rynku energii](#6-scenariusz-02--analityka-rynku-energii)
7. [Scenariusz 03 — Predykcyjne utrzymanie ruchu](#7-scenariusz-03--predykcyjne-utrzymanie-ruchu)
8. [Scenariusz 04 — Prognozowanie popytu](#8-scenariusz-04--prognozowanie-popytu)
9. [Konfiguracja .gitignore](#9-konfiguracja-gitignore)
10. [README główne](#10-readme-główne)

---

## 1. PRZEGLĄD PROJEKTU

### Cel
Kompleksowa platforma demonstracyjna prezentująca możliwości Microsoft Fabric jako zunifikowanego, opartego na AI systemu analitycznego dla sektora energetycznego w Polsce. Składa się z **4 scenariuszy demonstracyjnych**, każdy po ~15 minut prezentacji.

### Scenariusze

| # | Scenariusz | Komponenty Fabric | Czas demo |
|---|-----------|-------------------|-----------|
| 01 | ⚡ Monitoring sieci w czasie rzeczywistym (RTI) | Eventstream, Eventhouse/KQL DB, Real-Time Dashboard, Data Activator | ~15 min |
| 02 | 📈 Analityka rynku energii (Data Agents) | Lakehouse, Fabric Data Agent, Notebooks, Data Pipelines | ~15 min |
| 03 | 🔧 Predykcyjne utrzymanie ruchu (Operations Agents) | Eventhouse/KQL DB, Operations Agent, Lakehouse, Notebooks | ~15 min |
| 04 | 🔋 Prognozowanie popytu (RTI + Data Agents) | Eventstream, Eventhouse/KQL DB, Real-Time Dashboard, Data Agent, Lakehouse | ~15 min |

### Kontekst danych
Wszystkie dane opierają się na realistycznych parametrach **Krajowego Systemu Elektroenergetycznego (KSE)** — polskie nazwy stacji, farm wiatrowych, stref rynkowych, segmentów klientów, uczestników rynku (rzeczywiste firmy energetyczne).

---

## 2. DECYZJE ARCHITEKTONICZNE

1. **Język**: Cała dokumentacja, komentarze w kodzie, stałe, opisy i interfejsy agentów — w języku polskim
2. **Lokalizacja danych**: Polska infrastruktura energetyczna (KSE), 16 województw, polskie firmy
3. **Strefa czasowa**: Wszystkie timestampy w CET/CEST (`Europe/Warsaw`) — generowane relatywnie do bieżącego czasu, aby demo można było uruchomić w dowolnym momencie
4. **Deterministyczny seed**: `SEED = 42` we wszystkich generatorach danych — reprodukowalne wyniki przy tych samych timestampach
5. **Moduł współdzielony**: Pakiet `shared/` z narzędziami (`utils.py`), stałymi (`constants.py`) i generatorami danych (`generators.py`) — importowany przez wszystkie scenariusze
6. **Struktura scenariuszy**: Każdy scenariusz ma `generate_*.py` na poziomie katalogu głównego scenariusza + podkatalogi `config/`, `data/`, `kql/`, `notebooks/`
7. **Import path**: Każdy skrypt generujący ustawia `sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))` aby importować z `shared`
8. **Pliki danych**: Generowane CSV/Parquet w katalogu `data/` — dodane do `.gitignore`
9. **Python**: Wersja 3.11+, biblioteki standardowe + pandas, numpy, matplotlib, scikit-learn, mlflow, azure-eventhub, azure-identity
10. **Microsoft Fabric**: Wymagana pojemność F64 lub wyższa

---

## 3. STRUKTURA REPOZYTORIUM

```
energy-market-demo/
│
├── README.md                              # Główna dokumentacja projektu
├── requirements.txt                       # Zależności Python
├── .gitignore                             # Wykluczenia Git
│
├── shared/                                # Współdzielone moduły Python
│   ├── __init__.py                        # Docstring pakietu
│   ├── utils.py                           # Generowanie znaczników czasu (CET)
│   ├── constants.py                       # Stałe: stacje, turbiny, strefy, segmenty
│   └── generators.py                      # Generatory danych (szum, pogoda, solar, wiatr)
│
├── scenario-01-realtime-grid-monitoring/  # ⚡ RTI — Monitoring sieci
│   ├── README.md                          # Dokumentacja scenariusza
│   ├── generate_grid_data.py              # Generator danych telemetrycznych
│   ├── config/
│   │   └── eventstream_schema.json        # Schemat wiadomości Eventstream
│   ├── data/                              # Wygenerowane pliki CSV (w .gitignore)
│   ├── kql/
│   │   ├── create_tables.kql              # Tworzenie tabel KQL
│   │   ├── dashboard_queries.kql          # 10 zapytań do dashboardu
│   │   └── activator_rules.kql            # 4 reguły Data Activator
│   └── notebooks/
│       └── grid_analysis.ipynb            # Notebook analizy sieci (10 sekcji)
│
├── scenario-02-energy-market-analytics/   # 📈 Data Agent — Rynek energii
│   ├── README.md
│   ├── generate_market_data.py            # Generator danych rynkowych
│   ├── config/
│   │   ├── agent_instructions.md          # Instrukcje Fabric Data Agent
│   │   └── example_queries.md             # 35 przykładowych zapytań
│   ├── data/
│   ├── kql/
│   │   └── lakehouse_queries.sql          # 10 zapytań T-SQL
│   └── notebooks/
│       └── market_analysis.py             # Notebook analizy rynku (7 sekcji)
│
├── scenario-03-predictive-maintenance/    # 🔧 Operations Agent — Utrzymanie ruchu
│   ├── README.md
│   ├── generate_turbine_data.py           # Generator danych turbin
│   ├── config/
│   │   ├── agent_instructions.md          # Instrukcje Operations Agent
│   │   └── example_queries.md             # 32 przykładowe zapytania
│   ├── data/
│   ├── kql/
│   │   ├── create_tables.kql              # Tabela TurbineSensorData + mapowania
│   │   └── dashboard_queries.kql          # 10 zapytań KQL
│   └── notebooks/
│       └── failure_prediction.py          # Pipeline ML (RandomForest + MLflow)
│
└── scenario-04-demand-forecasting/        # 🔋 RTI + Data Agent — Prognozowanie
    ├── README.md
    ├── generate_demand_data.py            # Generator danych liczników
    ├── config/
    │   ├── agent_instructions.md          # Instrukcje Data Agent "Prognosta Popytu"
    │   ├── example_queries.md             # 38 przykładowych zapytań
    │   └── eventstream_schema.json        # Schemat SmartMeterReading
    ├── data/
    │   └── analysis_output/               # Wykresy PNG z analizy
    ├── kql/
    │   ├── create_tables.kql              # SmartMeterReadings + funkcje KQL
    │   └── dashboard_queries.kql          # 10+ zapytań dashboardowych
    └── notebooks/
        └── demand_analysis.py             # Notebook analizy popytu
```

---

## 4. MODUŁ WSPÓŁDZIELONY (`shared/`)

### 4.1 `shared/__init__.py`

```python
"""Shared utilities package for Energy Market Demo."""
```

### 4.2 `shared/utils.py`

Moduł narzędziowy do generowania znaczników czasu w strefie CET:

```python
import os, sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

CET = ZoneInfo("Europe/Warsaw")
UTC = timezone.utc
```

**Funkcje:**

| Funkcja | Opis | Parametry | Zwraca |
|---------|------|-----------|--------|
| `now_cet()` | Bieżący czas w CET | — | `datetime` |
| `now_utc()` | Bieżący czas w UTC | — | `datetime` |
| `time_range_cet(hours_back=24, interval_seconds=10)` | Lista timestampów CET od `hours_back` temu do teraz, co `interval_seconds` | `hours_back: int`, `interval_seconds: int` | `list[datetime]` |
| `time_range_daily_cet(days_back=730)` | Dzienne timestampy (północ CET) za ostatnie `days_back` dni | `days_back: int` | `list[datetime]` |
| `time_range_hourly_cet(days_back=365)` | Godzinowe timestampy CET za ostatnie `days_back` dni | `days_back: int` | `list[datetime]` |
| `format_ts(dt)` | Format ISO 8601 z timezone | `dt: datetime` | `str` |
| `format_ts_short(dt)` | Format `YYYY-MM-DD HH:MM:SS` | `dt: datetime` | `str` |
| `ensure_output_dir(script_path)` | Tworzy katalog `data/` obok skryptu | `script_path: str` | `str` (ścieżka) |
| `add_shared_to_path()` | Dodaje katalog repo do `sys.path` | — | `None` |

**Kluczowe szczegóły implementacji:**
- `time_range_cet` — generuje od `end - timedelta(hours=hours_back)` do `end`, gdzie `end = now_cet().replace(microsecond=0)`
- `time_range_daily_cet` — generuje od `end - timedelta(days=days_back)` do `end`, gdzie `end = now_cet().replace(hour=0, minute=0, second=0, microsecond=0)`
- `time_range_hourly_cet` — jak wyżej, ale `end = now_cet().replace(minute=0, second=0, microsecond=0)`
- `ensure_output_dir` — używa `os.path.dirname(os.path.abspath(script_path))` jako katalogu bazowego

### 4.3 `shared/constants.py`

#### 4.3.1 SUBSTATIONS — 50 stacji transformatorowych

Lista 50 krotek `(nazwa, szerokość_geo, długość_geo)`:

```
Mazowieckie / Warszawa:
  SE Warszawa Centrum       52.2297  21.0122
  SE Warszawa Mory          52.1934  20.8686
  SE Warszawa Miłosna       52.2406  21.1755
  SE Płock Radziwie         52.5468  19.6863
  SE Radom Południe         51.3927  21.1471

Małopolskie / Śląskie:
  SE Kraków Nowa Huta       50.0728  20.0384
  SE Kraków Skawina         49.9752  19.8283
  SE Katowice Południe      50.2389  19.0234
  SE Katowice Roździeń      50.2624  19.0560
  SE Częstochowa Północ     50.8233  19.1200

Wielkopolskie:
  SE Poznań Plewiska        52.3744  16.8064
  SE Poznań Czerwonak       52.4632  16.9818
  SE Konin Gosławice        52.2100  18.2500
  SE Piła Północ            53.1622  16.7383
  SE Kalisz Wschód          51.7556  18.1100

Pomorskie:
  SE Gdańsk Port            54.3520  18.6466
  SE Gdańsk Przyjaźń        54.3406  18.7128
  SE Gdynia Wielki Kack     54.4757  18.4974
  SE Słupsk Wierzbięcino    54.4641  17.0285
  SE Żarnowiec              54.7564  18.1306

Dolnośląskie:
  SE Wrocław Stadion        51.1079  17.0385
  SE Wrocław Klecina        51.0765  17.0015
  SE Legnica Centrum        51.2070  16.1619
  SE Jelenia Góra Zachód    50.8976  15.7136
  SE Wałbrzych Południe     50.7638  16.2844

Łódzkie:
  SE Łódź Janów             51.7500  19.4350
  SE Łódź Olechów           51.7400  19.5100
  SE Piotrków Trybunalski   51.4049  19.6886
  SE Bełchatów Rogowiec     51.2600  19.3300
  SE Sieradz Północ         51.6000  18.7300

Lubelskie:
  SE Lublin Abramowice      51.2084  22.6010
  SE Chełm Zachód           51.1330  23.4711
  SE Zamość Południowy       50.7075  23.2531

Podkarpackie:
  SE Rzeszów Widełka        50.0665  21.9940
  SE Stalowa Wola           50.5828  22.0539

Warmińsko-Mazurskie:
  SE Olsztyn Mątki          53.7782  20.4942
  SE Elbląg Zachód          54.1522  19.3840

Podlaskie:
  SE Białystok Zubki        53.1175  23.1886
  SE Suwałki Północ         54.1118  22.9307

Zachodniopomorskie:
  SE Szczecin Glinki        53.3975  14.5528
  SE Koszalin Żydowo        54.1836  16.2102

Kujawsko-Pomorskie:
  SE Bydgoszcz Zachód       53.1235  17.9945
  SE Toruń Elana            53.0138  18.6336
  SE Grudziądz Węgrowo      53.4837  18.7536

Lubuskie:
  SE Zielona Góra Przylep   51.9466  15.4850
  SE Gorzów Wielkopolski    52.7325  15.2369

Świętokrzyskie:
  SE Kielce Radkowice       50.8601  20.6286
  SE Połaniec Elektrownia   50.4333  21.2833

Opolskie:
  SE Opole Groszowice       50.6500  17.9300
  SE Dobrzeń Wielki         50.7700  17.8700
```

#### 4.3.2 WIND_FARMS — 5 farm wiatrowych

```python
WIND_FARMS = [
    {"name": "Farma Wiatrowa Darłowo",   "latitude": 54.4261, "longitude": 16.4119, "region": "Zachodniopomorskie"},
    {"name": "Farma Wiatrowa Żarnowiec", "latitude": 54.7564, "longitude": 18.1306, "region": "Pomorskie"},
    {"name": "Farma Wiatrowa Potęgowo",  "latitude": 54.4953, "longitude": 17.4839, "region": "Pomorskie"},
    {"name": "Farma Wiatrowa Margonin",  "latitude": 52.9700, "longitude": 17.2800, "region": "Wielkopolskie"},
    {"name": "Farma Wiatrowa Karścino",  "latitude": 54.0981, "longitude": 15.8856, "region": "Zachodniopomorskie"},
]
```

#### 4.3.3 Budowanie listy turbin

Funkcja `_build_turbine_list()` tworzy 200 turbin (40 na farmę):
- `turbine_id` = pierwsze 3 znaki ostatniego słowa nazwy farmy (uppercase) + "-" + numer 3-cyfrowy
  - Np. "Farma Wiatrowa Darłowo" → "DAR-001"
  - "Karścino" → "KAR-001"
- Współrzędne: `farm.latitude + (i % 8) * 0.002`, `farm.longitude + (i // 8) * 0.002`

#### 4.3.4 Pozostałe stałe

```python
ENERGY_SOURCES = ["Wiatr", "Słońce", "Gaz", "Węgiel", "Biomasa", "Woda", "Atom"]
MARKET_ZONES = ["KSE", "Zona A", "Zona B", "Zona C"]
CUSTOMER_SEGMENTS = ["Gospodarstwa domowe", "Przemysł ciężki", "Usługi", "Rolnictwo", "Transport"]
VOLTAGE_LEVELS = [110, 220, 400]

WEATHER_STATIONS = [
    "Warszawa-Okęcie", "Kraków-Balice", "Gdańsk-Rębiechowo", "Wrocław-Strachowice",
    "Poznań-Ławica", "Katowice-Pyrzowice", "Łódź-Lublinek", "Szczecin-Dąbie",
    "Rzeszów-Jasionka", "Bydgoszcz-Szwederowo", "Lublin-Radawiec", "Białystok-Krywlany",
    "Olsztyn-Mazury", "Kielce-Suków", "Zielona Góra-Babimost",
    "Koszalin-Zegrze Pomorskie", "Suwałki", "Leszno-Strzyżewice", "Hel", "Kasprowy Wierch",
]
```

### 4.4 `shared/generators.py`

Moduł generatorów danych oparty na realistycznych modelach polskiego klimatu i wzorców energetycznych.

#### 4.4.1 Szum gaussowski

```python
def generate_noise(base: float, pct: float) -> float:
    """base ± base*pct (σ = base * pct)"""
    return base + random.gauss(0, abs(base * pct))
```

#### 4.4.2 Czynnik sezonowy (seasonal_factor)

```python
def seasonal_factor(timestamp: datetime) -> float:
    day_of_year = timestamp.timetuple().tm_yday
    return 1.0 + 0.30 * math.cos(2 * math.pi * (day_of_year - 15) / 365)
```
- Kosinus wycentrowany na dzień 15 (styczeń) → szczyt zimowy ≈ 1.3, minimum letnie ≈ 0.7
- Zakres wynikowy: [0.70, 1.35]

#### 4.4.3 Czynnik dobowy (daily_factor)

```python
def daily_factor(timestamp: datetime) -> float:
    hour = timestamp.hour + timestamp.minute / 60.0
    base = 0.90 + 0.25 * math.cos(2 * math.pi * (hour - 15) / 24)
    if 7 <= hour < 10: base += 0.10 * math.sin(math.pi * (hour - 7) / 3)   # rampa poranna
    if 17 <= hour < 21: base += 0.15 * math.sin(math.pi * (hour - 17) / 4)  # szczyt wieczorny
    if 1 <= hour < 5:  base -= 0.10 * math.sin(math.pi * (hour - 1) / 4)   # dołek nocny
    return base
```
- Zakres wynikowy: [0.55, 1.35]

#### 4.4.4 Temperatura (weather_temperature)

```python
def weather_temperature(timestamp, station) -> float:
    day_of_year = timestamp.timetuple().tm_yday
    hour = timestamp.hour + timestamp.minute / 60.0
    seasonal = 8.0 + 14.0 * math.cos(2 * math.pi * (day_of_year - 200) / 365)  # średnia 8°C, amplituda 14°C, peak dzień 200 (lipiec)
    diurnal = 5.0 * math.cos(2 * math.pi * (hour - 14) / 24)  # amplituda 5°C, peak 14:00
    rng = random.Random(_station_seed(station) + day_of_year)
    station_offset = (rng.random() - 0.5) * 8.0  # ±4°C per stacja
    noise = random.gauss(0, 1.2)
    return seasonal + diurnal + station_offset + noise
```
- `_station_seed(station)` = `int(hashlib.md5(station.encode()).hexdigest()[:8], 16)`

#### 4.4.5 Prędkość wiatru (wind_speed)

```python
def wind_speed(timestamp) -> float:
    seasonal_base = 7.0 + 2.0 * math.cos(2 * math.pi * (day_of_year - 15) / 365)  # wyższy zimą
    diurnal = 0.8 * math.cos(2 * math.pi * (hour - 14) / 24)  # wietrze po południu
    gust = max(0, random.gauss(0, 1.5))
    if random.random() < 0.05: gust += random.uniform(3, 8)  # 5% szans na silny podmuch
    speed = seasonal_base + diurnal + gust + random.gauss(0, 0.8)
    return max(0.0, min(30.0, speed))
```

#### 4.4.6 Nasłonecznienie (solar_irradiance)

```python
def solar_irradiance(timestamp) -> float:
    # Obliczenie wschodu/zachodu dla szerokości 52°N
    declination = 23.45 * math.sin(2 * math.pi * (day_of_year - 81) / 365)
    cos_hour_angle = -math.tan(math.radians(52.0)) * math.tan(math.radians(declination))
    cos_hour_angle = max(-1.0, min(1.0, cos_hour_angle))
    half_day_hours = math.degrees(math.acos(cos_hour_angle)) / 15.0
    sunrise = 12.0 - half_day_hours
    sunset = 12.0 + half_day_hours
    if hour < sunrise or hour > sunset: return 0.0
    day_fraction = (hour - sunrise) / (sunset - sunrise)
    solar_angle = math.sin(math.pi * day_fraction)
    max_irradiance = 600 + 400 * math.sin(2 * math.pi * (day_of_year - 81) / 365)
    cloud = random.uniform(0.6, 1.0)
    return max(0.0, max_irradiance * solar_angle * cloud)
```
- Zakres: [0, ~1050] W/m²

---

## 5. SCENARIUSZ 01 — MONITORING SIECI W CZASIE RZECZYWISTYM

### 5.1 Przeznaczenie

Demonstracja Real-Time Intelligence (RTI) w Microsoft Fabric: streaming telemetrii z 50 stacji transformatorowych, wykrywanie anomalii, real-time dashboardy i alerty.

### 5.2 Generator danych: `generate_grid_data.py`

**Parametry generacji:**
- 50 stacji z `SUBSTATIONS`
- 24 godziny wstecz, interwał 10 sekund → ~8640 timestampów × 50 stacji = **~432 000 wierszy**
- `random.seed(42)` — deterministyczny
- Kolumny CSV: `timestamp, substation_id, substation_name, latitude, longitude, region, voltage_level_kv, active_power_mw, reactive_power_mvar, voltage_pu, current_a, frequency_hz, transformer_temp_c, load_percent, power_factor, is_anomaly, anomaly_type`

**Mapowanie regionów:**
```python
# Wyznaczane na podstawie pierwszych dwóch znaków współrzędnych
if lat >= 53.5:  region = "Północ"
elif lat >= 52.0: region = "Północny-Zachód" if lon < 18 else "Północny-Wschód"
elif lat >= 51.0: region = "Centrum"
elif lat >= 50.5: region = "Południe-Zachód" if lon < 20 else "Południe-Wschód"
else:            region = "Południe"
```
(Uwaga: rzeczywista implementacja używa prostej logiki na współrzędnych i nazwie stacji)

**Generacja wartości bazowych na stację:**
```python
# Dla każdej stacji (name, lat, lon):
substation_id = f"SE-{i+1:03d}"  # SE-001 ... SE-050
voltage_level = VOLTAGE_LEVELS[i % 3]  # cyklicznie: 110, 220, 400

# Bazowa moc czynna zależy od napięcia:
base_power_map = {110: 45.0, 220: 120.0, 400: 280.0}
base_power = base_power_map[voltage_level]
```

**Generacja odczytów (główna pętla):**
Dla każdego timestampu i stacji:
```python
sf = seasonal_factor(ts)
df = daily_factor(ts)
load_factor = sf * df

active_power   = generate_noise(base_power * load_factor, 0.08)   # ±8%
reactive_power = active_power * generate_noise(0.35, 0.15)        # ~35% mocy czynnej
voltage_pu     = generate_noise(1.0, 0.015)                       # pu ≈ 1.0 ±1.5%
current        = active_power * 1000 / (voltage_level * math.sqrt(3) * voltage_pu)
frequency      = generate_noise(50.0, 0.001)                      # 50 Hz ±0.1%
transformer_temp = generate_noise(45.0 + load_factor * 20.0, 0.05)
load_pct       = generate_noise(load_factor * 65.0, 0.08)
power_factor   = min(1.0, generate_noise(0.92, 0.02))
```

**System anomalii — 6 typów (łącznie ~1% wierszy ma anomalię):**

```python
anomaly_types = {
    "VOLTAGE_SAG":        {"probability": 0.002, "duration": (3, 15),  "effect": "voltage_pu *= uniform(0.75, 0.88)"},
    "VOLTAGE_SWELL":      {"probability": 0.0015, "duration": (2, 10), "effect": "voltage_pu *= uniform(1.08, 1.15)"},
    "OVERLOAD":           {"probability": 0.0025, "duration": (5, 30), "effect": "load_pct = uniform(95, 130), transformer_temp += 15-30"},
    "FREQUENCY_DEVIATION":{"probability": 0.001,  "duration": (1, 5),  "effect": "frequency_hz += ±uniform(0.05, 0.15)"},
    "TRANSFORMER_OVERHEATING": {"probability": 0.002, "duration": (10, 60), "effect": "transformer_temp = uniform(85, 105)"},
    "POWER_FACTOR_DROP":  {"probability": 0.0018, "duration": (5, 20), "effect": "power_factor = uniform(0.65, 0.78)"},
}
```

Mechanizm:
1. Dla każdego odczytu losuj prawdopodobieństwo z `random.random()`
2. Sprawdź kolejno każdy typ anomalii (kumulatywne prawdopodobieństwo)
3. Jeśli anomalia — modyfikuj wartości, ustaw `is_anomaly=True`, `anomaly_type=nazwa`
4. Anomalie mają "czas trwania" (duration) — śledzony per stacja w słowniku `active_anomalies`

**Format wyjścia:**
- Plik: `data/grid_telemetry.csv`
- Separator: `,`
- Floaty: 2 miejsca po przecinku (poza `voltage_pu` — 4 miejsca, `frequency_hz` — 3 miejsca, `power_factor` — 3 miejsca)
- Timestamp format: `YYYY-MM-DD HH:MM:SS+HH:MM` (ISO 8601 z timezone offset)

### 5.3 Schema Eventstream: `config/eventstream_schema.json`

```json
{
  "name": "GridTelemetryStream",
  "description": "Strumień danych telemetrycznych z stacji transformatorowych KSE",
  "format": "JSON",
  "schema": {
    "type": "object",
    "properties": {
      "timestamp":        {"type": "string", "format": "date-time"},
      "substation_id":    {"type": "string"},
      "substation_name":  {"type": "string"},
      "latitude":         {"type": "number"},
      "longitude":        {"type": "number"},
      "region":           {"type": "string"},
      "voltage_level_kv": {"type": "integer", "enum": [110, 220, 400]},
      "active_power_mw":  {"type": "number"},
      "reactive_power_mvar": {"type": "number"},
      "voltage_pu":       {"type": "number"},
      "current_a":        {"type": "number"},
      "frequency_hz":     {"type": "number"},
      "transformer_temp_c": {"type": "number"},
      "load_percent":     {"type": "number"},
      "power_factor":     {"type": "number"},
      "is_anomaly":       {"type": "boolean"},
      "anomaly_type":     {"type": "string", "nullable": true}
    }
  },
  "partitionKey": "substation_id",
  "timestampField": "timestamp"
}
```

### 5.4 KQL — Tabele: `kql/create_tables.kql`

```kql
// Tworzenie tabeli GridTelemetry
.create table GridTelemetry (
    timestamp: datetime,
    substation_id: string,
    substation_name: string,
    latitude: real,
    longitude: real,
    region: string,
    voltage_level_kv: int,
    active_power_mw: real,
    reactive_power_mvar: real,
    voltage_pu: real,
    current_a: real,
    frequency_hz: real,
    transformer_temp_c: real,
    load_percent: real,
    power_factor: real,
    is_anomaly: bool,
    anomaly_type: string
)

// Mapowanie ingestion z CSV
.create table GridTelemetry ingestion csv mapping 'GridTelemetryMapping' '[
    {"Name":"timestamp","DataType":"datetime","Ordinal":0},
    {"Name":"substation_id","DataType":"string","Ordinal":1},
    {"Name":"substation_name","DataType":"string","Ordinal":2},
    {"Name":"latitude","DataType":"real","Ordinal":3},
    {"Name":"longitude","DataType":"real","Ordinal":4},
    {"Name":"region","DataType":"string","Ordinal":5},
    {"Name":"voltage_level_kv","DataType":"int","Ordinal":6},
    {"Name":"active_power_mw","DataType":"real","Ordinal":7},
    {"Name":"reactive_power_mvar","DataType":"real","Ordinal":8},
    {"Name":"voltage_pu","DataType":"real","Ordinal":9},
    {"Name":"current_a","DataType":"real","Ordinal":10},
    {"Name":"frequency_hz","DataType":"real","Ordinal":11},
    {"Name":"transformer_temp_c","DataType":"real","Ordinal":12},
    {"Name":"load_percent","DataType":"real","Ordinal":13},
    {"Name":"power_factor","DataType":"real","Ordinal":14},
    {"Name":"is_anomaly","DataType":"bool","Ordinal":15},
    {"Name":"anomaly_type","DataType":"string","Ordinal":16}
]'

// Polityka retencji
.alter table GridTelemetry policy retention '{"SoftDeletePeriod": "365.00:00:00", "Recoverability": "Enabled"}'

// Polityka batching — szybkie ingestion dla demo
.alter table GridTelemetry policy ingestionbatching '{"MaximumBatchingTimeSpan": "00:00:30"}'
```

### 5.5 KQL — Dashboard: `kql/dashboard_queries.kql`

10 zapytań do Real-Time Dashboard:

**1. Przegląd stanu sieci (ostatnie 5 minut):**
```kql
GridTelemetry
| where timestamp > ago(5m)
| summarize
    avg_power = avg(active_power_mw),
    avg_voltage = avg(voltage_pu),
    avg_freq = avg(frequency_hz),
    max_load = max(load_percent),
    anomaly_count = countif(is_anomaly == true)
  by substation_id, substation_name, region
| order by anomaly_count desc, max_load desc
```

**2. Trend mocy czynnej (ostatnia godzina, agregacja 1 min):**
```kql
GridTelemetry
| where timestamp > ago(1h)
| summarize avg_power = avg(active_power_mw) by bin(timestamp, 1m), region
| render timechart
```

**3. Mapa anomalii:**
```kql
GridTelemetry
| where timestamp > ago(1h) and is_anomaly == true
| summarize count() by substation_name, latitude, longitude, anomaly_type
| render scatterchart with (kind=map)
```

**4. Rozkład typów anomalii:**
```kql
GridTelemetry
| where timestamp > ago(24h) and is_anomaly == true
| summarize count() by anomaly_type
| render piechart
```

**5. Top 10 stacji o najwyższym obciążeniu:**
```kql
GridTelemetry
| where timestamp > ago(15m)
| summarize avg_load = avg(load_percent), max_load = max(load_percent) by substation_id, substation_name
| top 10 by avg_load desc
```

**6. Trend częstotliwości sieci:**
```kql
GridTelemetry
| where timestamp > ago(1h)
| summarize avg_freq = avg(frequency_hz), min_freq = min(frequency_hz), max_freq = max(frequency_hz) by bin(timestamp, 1m)
| render timechart
```

**7. Temperatura transformatorów — stacje krytyczne (>80°C):**
```kql
GridTelemetry
| where timestamp > ago(30m) and transformer_temp_c > 80
| summarize avg_temp = avg(transformer_temp_c), max_temp = max(transformer_temp_c) by substation_name
| order by max_temp desc
```

**8. Profil napięcia per poziom napięcia:**
```kql
GridTelemetry
| where timestamp > ago(1h)
| summarize avg_voltage = avg(voltage_pu), p5_voltage = percentile(voltage_pu, 5), p95_voltage = percentile(voltage_pu, 95) by bin(timestamp, 5m), voltage_level_kv
| render timechart
```

**9. Współczynnik mocy — stacje poniżej normy (<0.85):**
```kql
GridTelemetry
| where timestamp > ago(1h)
| summarize avg_pf = avg(power_factor) by substation_id, substation_name
| where avg_pf < 0.85
| order by avg_pf asc
```

**10. Statystyki regionalne:**
```kql
GridTelemetry
| where timestamp > ago(1h)
| summarize
    total_power = sum(active_power_mw),
    avg_load = avg(load_percent),
    anomalies = countif(is_anomaly),
    stations = dcount(substation_id)
  by region
| order by total_power desc
```

### 5.6 KQL — Reguły Activator: `kql/activator_rules.kql`

4 reguły Data Activator:

```kql
// Reguła 1: Przeciążenie stacji (load > 95%)
GridTelemetry
| where timestamp > ago(5m)
| summarize avg_load = avg(load_percent) by substation_id, substation_name
| where avg_load > 95
// → Alert: "⚠️ Przeciążenie stacji {substation_name}: {avg_load:0.1f}%"

// Reguła 2: Odchylenie częstotliwości (|freq - 50| > 0.1 Hz)
GridTelemetry
| where timestamp > ago(2m)
| summarize avg_freq = avg(frequency_hz) by substation_id
| where abs(avg_freq - 50.0) > 0.1
// → Alert: "⚡ Odchylenie częstotliwości: {avg_freq:0.3f} Hz"

// Reguła 3: Przegrzanie transformatora (temp > 85°C)
GridTelemetry
| where timestamp > ago(5m)
| summarize max_temp = max(transformer_temp_c) by substation_id, substation_name
| where max_temp > 85
// → Alert: "🌡️ Przegrzanie: {substation_name} — {max_temp:0.1f}°C"

// Reguła 4: Klaster anomalii (>5 anomalii / 10 min na stacji)
GridTelemetry
| where timestamp > ago(10m) and is_anomaly == true
| summarize anomaly_count = count() by substation_id, substation_name
| where anomaly_count > 5
// → Alert: "🔴 Klaster anomalii: {substation_name} — {anomaly_count} zdarzeń"
```

### 5.7 Notebook: `notebooks/grid_analysis.ipynb`

Notebook Jupyter (format .ipynb) z 10 sekcjami analizy:

**Sekcje:**
1. **Ładowanie danych** — read CSV z data/ do DataFrame
2. **Przegląd danych** — `df.info()`, `df.describe()`, kształt
3. **Profil obciążenia dobowego** — wykres godzinowy avg `load_percent` z bandą min-max
4. **Analiza anomalii** — rozkład typów, timeline, heatmapa per stacja
5. **Temperatura transformatorów** — heatmapa stacja × godzina, stacje > 80°C
6. **Stabilność napięcia** — rozkład `voltage_pu`, violin plot per poziom napięcia
7. **Częstotliwość sieci** — trend z liniami granicznymi 49.95–50.05 Hz
8. **Analiza regionalna** — porównanie regionów: moc, obciążenie, anomalie
9. **Korelacja parametrów** — macierz korelacji parametrów elektrycznych
10. **Podsumowanie** — kluczowe wnioski w markdown

---

## 6. SCENARIUSZ 02 — ANALITYKA RYNKU ENERGII

### 6.1 Przeznaczenie

Demonstracja Fabric Data Agents jako inteligentnych asystentów analitycznych. Generowane dane obejmują 2 lata historii rynku energii: ceny spot, mix generacji, kontrakty bilateralne, emisje CO₂ i uczestników rynku.

### 6.2 Generator danych: `generate_market_data.py`

**Parametry globalne:**
- Okres: `time_range_daily_cet(days_back=730)` (2 lata wstecz, dziennie)
- `random.seed(42)`, `np.random.seed(42)`

#### 6.2.1 Ceny spot (spot_prices.csv)

**Kolumny:** `date, hour, price_pln_mwh, volume_mwh, zone, price_eur_mwh`

Generacja:
- 24 godziny × liczba dni
- Cena bazowa per godzina: profil dobowy

```python
hourly_base = {
    0: 180, 1: 165, 2: 155, 3: 150, 4: 152, 5: 160,
    6: 185, 7: 220, 8: 260, 9: 275, 10: 280, 11: 285,
    12: 278, 13: 270, 14: 265, 15: 260, 16: 270, 17: 290,
    18: 310, 19: 320, 20: 300, 21: 275, 22: 240, 23: 210
}
```

Modyfikatory:
- `seasonal_factor(ts)` — wyższe zimą
- Szum: `generate_noise(cena, 0.12)`
- Skoki cenowe: 2% szans na spike × `uniform(1.5, 3.0)`
- Wolumen: `generate_noise(20000, 0.15)` MWh
- Kurs EUR: `PLN_price / generate_noise(4.55, 0.02)`
- Zona: KSE (100% wierszy, bo jeden rynek)

#### 6.2.2 Mix generacji (generation_mix.csv)

**Kolumny:** `date, hour, source, generation_mwh, capacity_mw, utilization_pct, co2_intensity_kg_mwh`

Źródła i parametry bazowe:

```python
source_params = {
    "Węgiel":   {"base_gen": 10000, "capacity": 22000, "co2": 900},
    "Gaz":      {"base_gen": 3000,  "capacity": 8000,  "co2": 400},
    "Wiatr":    {"base_gen": 4000,  "capacity": 9000,  "co2": 0},    # seasonal × wind_speed
    "Słońce":   {"base_gen": 1500,  "capacity": 12000, "co2": 0},    # solar_irradiance
    "Biomasa":  {"base_gen": 1200,  "capacity": 2000,  "co2": 50},
    "Woda":     {"base_gen": 800,   "capacity": 2400,  "co2": 0},
    "Atom":     {"base_gen": 0,     "capacity": 0,     "co2": 0},    # Polska nie ma atomu (jeszcze)
}
```

Modyfikatory per źródło:
- Węgiel: `base_gen * seasonal_factor * daily_factor * noise(1.0, 0.08)`
- Gaz: `base_gen * daily_factor * noise(1.0, 0.12)` — bardziej elastyczny
- Wiatr: zależy od `wind_speed(ts)` — liniowa interpolacja 0–15 m/s → 0–capacity
- Słońce: zależy od `solar_irradiance(ts)` — proporcjonalnie do irradiancji
- Woda: `base_gen * noise(1.0, 0.10)` + sezonowy bonus wiosenny (marzec–maj +30%)
- `utilization_pct = generation / capacity * 100`

#### 6.2.3 Kontrakty bilateralne (bilateral_contracts.csv)

**Kolumny:** `contract_id, seller, buyer, start_date, end_date, volume_mwh_year, price_pln_mwh, contract_type, status`

Generacja:
- ~200 kontraktów
- `contract_type`: "baseload" (60%), "peak" (25%), "offpeak" (15%)
- `status`: "active" (70%), "expired" (20%), "negotiating" (10%)
- Sprzedawcy/kupujący: losowani z listy `MARKET_PARTICIPANTS`
- Wolumen: 50 000–500 000 MWh/rok
- Cena: bazowa 250 PLN/MWh ± sezonowo, typ kontraktu wpływa (+15% peak, -10% offpeak)
- Czas trwania: 1–5 lat

#### 6.2.4 Emisje CO₂ (carbon_emissions.csv)

**Kolumny:** `date, total_emissions_tons, emission_intensity_kg_mwh, total_generation_mwh, renewable_share_pct, ets_price_eur`

Generacja dzienna:
- `total_generation = sum(generation_mix[date])`
- `total_emissions = sum(generation[source] * co2_intensity[source])` per dzień
- `emission_intensity = total_emissions / total_generation`
- `renewable_share = (wiatr + slonce + woda + biomasa) / total_generation * 100`
- `ets_price`: bazowa 80 EUR, trend wzrostowy +0.02 EUR/dzień, szum ±5%

#### 6.2.5 Uczestnicy rynku (market_participants.csv)

**Kolumny:** `participant_id, name, type, market_share_pct, region, generation_capacity_mw, annual_revenue_mln_pln, employee_count, founded_year, specialization`

~50 uczestników — **rzeczywiste polskie firmy energetyczne**:

```python
MARKET_PARTICIPANTS = [
    {"name": "PGE Polska Grupa Energetyczna", "type": "Wytwórca", "region": "Łódzkie", "capacity": 16500, "employees": 42000, "founded": 1990, "specialization": "Energetyka konwencjonalna i OZE"},
    {"name": "TAURON Polska Energia", "type": "Wytwórca", "region": "Małopolskie", "capacity": 5600, "employees": 25000, "founded": 2006, "specialization": "Wytwarzanie i dystrybucja"},
    {"name": "Enea", "type": "Wytwórca", "region": "Wielkopolskie", "capacity": 3200, "employees": 17000, "founded": 2008, "specialization": "Wytwarzanie i obrót"},
    {"name": "Energa", "type": "Wytwórca", "region": "Pomorskie", "capacity": 1900, "employees": 9500, "founded": 2006, "specialization": "OZE i dystrybucja"},
    {"name": "PKP Energetyka", "type": "Dystrybutor", "region": "Mazowieckie", ...},
    {"name": "PSE Polskie Sieci Elektroenergetyczne", "type": "OSP", "region": "Mazowieckie", ...},
    {"name": "Orlen Synthos Green Energy", "type": "Wytwórca", "region": "Podkarpackie", "specialization": "Energetyka jądrowa SMR"},
    {"name": "Polenergia", "type": "Wytwórca", ...},
    {"name": "Respect Energy", "type": "Obrót", ...},
    {"name": "Columbus Energy", "type": "OZE", ...},
    // ... + ~40 więcej realnych polskich firm
]
```

Typy: "Wytwórca", "Dystrybutor", "Obrót", "OZE", "OSP"
Dane generowane z szumem wokół podanych wartości bazowych.

### 6.3 Instrukcje Data Agent: `config/agent_instructions.md`

Nazwa agenta: **"Analityk Rynku Energii"**

Pełna treść instrukcji:

```markdown
# Agent: Analityk Rynku Energii

## Rola
Jesteś ekspertem ds. polskiego rynku energii elektrycznej. Analizujesz dane rynkowe,
ceny spot, mix generacji, kontrakty bilateralne, emisje CO₂ i uczestników rynku.

## Dostępne tabele danych
1. **spot_prices** — Ceny godzinowe na Towarowej Giełdzie Energii (TGE)
   - Kolumny: date, hour, price_pln_mwh, volume_mwh, zone, price_eur_mwh
2. **generation_mix** — Struktura wytwarzania energii
   - Kolumny: date, hour, source, generation_mwh, capacity_mw, utilization_pct, co2_intensity_kg_mwh
3. **bilateral_contracts** — Kontrakty bilateralne
   - Kolumny: contract_id, seller, buyer, start_date, end_date, volume_mwh_year, price_pln_mwh, contract_type, status
4. **carbon_emissions** — Emisje CO₂ i ceny ETS
   - Kolumny: date, total_emissions_tons, emission_intensity_kg_mwh, total_generation_mwh, renewable_share_pct, ets_price_eur
5. **market_participants** — Uczestnicy rynku
   - Kolumny: participant_id, name, type, market_share_pct, region, generation_capacity_mw, annual_revenue_mln_pln, employee_count, founded_year, specialization

## Zasady odpowiedzi
1. Odpowiadaj **zawsze po polsku**
2. Podawaj **konkretne liczby** z danych, nie ogólniki
3. Formatuj liczby: ceny z 2 miejscami po przecinku, wolumeny zaokrąglone do tysięcy
4. Używaj jednostek: PLN/MWh, EUR/MWh, MWh, MW, tony CO₂, kg CO₂/MWh
5. Przy porównaniach podawaj wartości bezwzględne i procentowe
6. Zaznaczaj trendy: rosnący ↑, malejący ↓, stabilny →
7. Dla prognoz zaznaczaj "na podstawie trendu historycznego"

## Kontekst rynkowy
- Rynek: TGE (Towarowa Giełda Energii) — Rynek Dnia Następnego (RDN)
- Regulator: URE (Urząd Regulacji Energetyki)
- Operator systemu: PSE S.A.
- Waluta: PLN, z przeliczeniem na EUR
- System ETS: EU Emissions Trading System
- Cele OZE: Polska — 23% udziału OZE do 2030

## Terminologia
- KSE = Krajowy System Elektroenergetyczny
- OZE = Odnawialne Źródła Energii
- TGE = Towarowa Giełda Energii
- RDN = Rynek Dnia Następnego
- URE = Urząd Regulacji Energetyki
- ETS = Emissions Trading System
- PPA = Power Purchase Agreement
```

### 6.4 Przykładowe zapytania: `config/example_queries.md`

35 zapytań pogrupowanych w 7 kategorii:

**Analiza cen (5):**
- "Jaka była średnia cena spot w ostatnim miesiącu?"
- "Pokaż 10 najdroższych godzin w tym roku"
- "Jak zmieniła się cena spot rok do roku?"
- "Kiedy ceny przekroczyły 500 PLN/MWh?"
- "Jaka jest korelacja między ceną spot a udziałem OZE?"

**Mix generacji (5):**
- "Jaki jest aktualny udział OZE w miksie energetycznym?"
- "Jak zmienia się produkcja z wiatru w ciągu roku?"
- "Porównaj wykorzystanie mocy zainstalowanej per źródło"
- "Kiedy generacja ze słońca jest najwyższa?"
- "Jaki jest trend produkcji z węgla?"

**Kontrakty (5):**
- "Ile aktywnych kontraktów jest w portfelu?"
- "Jaka jest średnia cena w kontraktach baseload vs peak?"
- "Kto jest największym sprzedawcą na rynku bilateralnym?"
- "Jakie kontrakty wygasają w ciągu 6 miesięcy?"
- "Pokaż rozkład wolumenów per typ kontraktu"

**Emisje (5):**
- "Jak zmienia się intensywność emisji CO₂?"
- "Jaki jest trend ceny uprawnień ETS?"
- "Jak udział OZE wpływa na emisyjność?"
- "Porównaj emisje w zimie vs lato"
- "Jaki jest koszt CO₂ per MWh wygenerowanej energii?"

**Uczestnicy (5):**
- "Kim jest największy wytwórca na rynku?"
- "Jakie firmy specjalizują się w OZE?"
- "Porównaj top 5 firm pod kątem mocy zainstalowanej"
- "Jakie firmy powstały po 2010 roku?"
- "Jaka jest struktura rynku per typ uczestnika?"

**Porównania krzyżowe (5):**
- "Jak cena spot koreluje z obciążeniem sieci?"
- "Czy wyższa generacja wiatrowa obniża ceny?"
- "Jakie źródła dominują gdy ceny są najwyższe?"
- "Porównaj przychody firm z ich mocą zainstalowaną"
- "Jak zmienia się mix generacji w szczycie vs poza szczytem?"

**Strategiczne (5):**
- "Podsumuj sytuację na polskim rynku energii"
- "Jakie trendy kształtują transformację energetyczną?"
- "Jaki jest potencjał OZE vs obecna moc?"
- "Jak zmienić strukturę kontraktów aby zoptymalizować koszty?"
- "Jakie są ryzyka związane z cenami ETS?"

### 6.5 SQL Lakehouse: `kql/lakehouse_queries.sql`

10 zapytań T-SQL do Fabric Lakehouse:

```sql
-- 1. Średnia cena spot per miesiąc
SELECT FORMAT(date, 'yyyy-MM') as month, AVG(price_pln_mwh) as avg_price, AVG(price_eur_mwh) as avg_eur
FROM spot_prices GROUP BY FORMAT(date, 'yyyy-MM') ORDER BY month;

-- 2. Mix generacji — średni udział źródeł
SELECT source, AVG(generation_mwh) as avg_gen, AVG(utilization_pct) as avg_util
FROM generation_mix GROUP BY source ORDER BY avg_gen DESC;

-- 3. Top 10 godzin cenowych
SELECT TOP 10 date, hour, price_pln_mwh, volume_mwh
FROM spot_prices ORDER BY price_pln_mwh DESC;

-- 4. Kontrakty wygasające w 6 miesięcy
SELECT * FROM bilateral_contracts
WHERE status = 'active' AND end_date <= DATEADD(month, 6, GETDATE())
ORDER BY end_date;

-- 5. Trend emisji
SELECT FORMAT(date, 'yyyy-MM') as month,
       AVG(emission_intensity_kg_mwh) as avg_intensity,
       AVG(renewable_share_pct) as avg_renewable
FROM carbon_emissions GROUP BY FORMAT(date, 'yyyy-MM') ORDER BY month;

-- 6. Porównanie wytwórców
SELECT name, generation_capacity_mw, annual_revenue_mln_pln, employee_count
FROM market_participants WHERE type = 'Wytwórca' ORDER BY generation_capacity_mw DESC;

-- 7. Profil dobowy cen
SELECT hour, AVG(price_pln_mwh) as avg_price, MIN(price_pln_mwh) as min_price, MAX(price_pln_mwh) as max_price
FROM spot_prices GROUP BY hour ORDER BY hour;

-- 8. Korelacja cena-OZE
SELECT sp.date, AVG(sp.price_pln_mwh) as avg_price, ce.renewable_share_pct
FROM spot_prices sp JOIN carbon_emissions ce ON sp.date = ce.date
GROUP BY sp.date, ce.renewable_share_pct;

-- 9. Wolumen kontraktów per typ
SELECT contract_type, COUNT(*) as count, SUM(volume_mwh_year) as total_volume, AVG(price_pln_mwh) as avg_price
FROM bilateral_contracts GROUP BY contract_type;

-- 10. Sezonowość generacji wiatrowej
SELECT MONTH(date) as month, AVG(generation_mwh) as avg_wind_gen
FROM generation_mix WHERE source = 'Wiatr' GROUP BY MONTH(date) ORDER BY month;
```

### 6.6 Notebook: `notebooks/market_analysis.py`

Notebook Python (format `.py` z `# %%` cell markers) z 7 sekcjami:

1. **Import i ładowanie danych** — pandas, matplotlib, seaborn; wczytanie 5 CSV
2. **Analiza cen spot** — trend dzienny, profil godzinowy, histogram, skoki cenowe
3. **Mix generacji** — stacked area chart, porównanie źródeł, trend OZE
4. **Kontrakty bilateralne** — rozkład typów, wolumeny, harmonogram wygasania
5. **Emisje CO₂** — trend emisyjności, korelacja z OZE, cena ETS
6. **Analiza uczestników** — market share, mapa pojemnościowa, firmy OZE
7. **Wnioski strategiczne** — podsumowanie w markdown

---

## 7. SCENARIUSZ 03 — PREDYKCYJNE UTRZYMANIE RUCHU

### 7.1 Przeznaczenie

Demonstracja Operations Agents do monitoringu i predykcji awarii farm wiatrowych. Generator tworzy 5 datasetów: telemetrię sensorów, historię konserwacji, zlecenia pracy, specyfikacje turbin i log awarii. Pipeline ML (RandomForest + MLflow) przewiduje awarie.

### 7.2 Generator danych: `generate_turbine_data.py`

**Seed:** `SEED = 2024_03` (inny niż w shared!)

**Definicje farm (własne, nie z shared/constants.py!):**

```python
FARMS = [
    {"code": "DRL", "name": "Darłowo Wind Park",    "count": 45, "lat_base": 54.42, "lon_base": 16.41},
    {"code": "PTG", "name": "Potęgowo Wind Farm",   "count": 40, "lat_base": 54.48, "lon_base": 17.48},
    {"code": "KRS", "name": "Korsze Wind Complex",  "count": 35, "lat_base": 54.17, "lon_base": 21.14},
    {"code": "PRZ", "name": "Przykona Wind Park",   "count": 42, "lat_base": 52.10, "lon_base": 18.67},
    {"code": "ZAG", "name": "Zagórz Wind Farm",     "count": 38, "lat_base": 49.53, "lon_base": 22.27},
]
# Łącznie: 45+40+35+42+38 = 200 turbin
```

**Modele turbin:**
```python
TURBINE_MODELS = [
    {"model": "Vestas V110",         "rated_kw": 2200, "hub_m": 80,  "rotor_m": 110},
    {"model": "Siemens SG 3.4-132",  "rated_kw": 3400, "hub_m": 114, "rotor_m": 132},
    {"model": "Enercon E-138",       "rated_kw": 3500, "hub_m": 131, "rotor_m": 138},
    {"model": "Nordex N131/3900",    "rated_kw": 3900, "hub_m": 120, "rotor_m": 131},
]
```

**Komponenty, typy awarii i kategorie:**
```python
COMPONENTS = ["Gearbox", "Generator", "Blade", "Bearing", "Hydraulic", "Electrical", "Pitch_System"]

MAINTENANCE_TYPES = ["Scheduled", "Corrective", "Predictive", "Emergency"]
WORK_ORDER_TYPES = ["Preventive", "Corrective", "Inspection"]
PRIORITIES = ["Low", "Medium", "High", "Critical"]
WO_STATUSES = ["Open", "In_Progress", "Completed", "Cancelled"]
SEVERITIES = ["Minor", "Major", "Critical"]

# FAILURE_TYPES: dict[str, list[str]] — 4 typy awarii per komponent
# ROOT_CAUSES: dict[str, list[str]] — 4 przyczyny per komponent
# MAINTENANCE_DESCRIPTIONS: dict[str, list[str]] — 5 opisów po polsku per komponent
# TECHNICIAN_TEAMS = ["Alpha", "Beta", "Gamma", "Delta", "Omega"]
```

**Budowanie listy turbin (_build_turbine_list):**
```python
def _build_turbine_list():
    turbines = []
    for farm in FARMS:
        for i in range(1, farm["count"] + 1):
            tid = f"T-{farm['code']}-{i:03d}"  # np. T-DRL-001
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
```

#### 7.2.1 Dataset 1: `sensor_telemetry.csv` (~115 200 wierszy)

200 turbin × 576 odczytów (48h @ 5 min) = 115 200

**Kolumny:** `timestamp, turbine_id, farm_name, wind_speed_ms, rotor_rpm, generator_rpm, blade_pitch_deg, nacelle_direction_deg, power_output_kw, gearbox_temp_c, bearing_temp_c, generator_temp_c, hydraulic_pressure_bar, vibration_mm_s, oil_viscosity, ambient_temp_c, humidity_pct, status`

**Status:** `Operating` | `Idle` (wiatr <3 lub >25 m/s) | `Maintenance` (1% szans) | `Fault` (degradujące turbiny po 70% czasu)

**Mechanizm degradacji:**
- 15 turbin wybranych jako "degradujące" (`degrading_ids`)
- 5 z nich jako "fault" (`fault_ids`)
- `degrade_factor = progress * random.uniform(1.5, 3.0)` — rośnie od 0 do max w ciągu 48h
- Efekty degradacji:
  - Moc: `*= max(0.4, 1.0 - degrade_factor * 0.08)`
  - Temperatury: `+= degrade_factor * [5, 6, 3]` (gearbox, bearing, generator)
  - Wibracje: `+= degrade_factor * 2.5`; fault: `+= random.uniform(3, 8)`
  - Hydraulika: `-= degrade_factor * 10`
  - Olej: `-= degrade_factor * 5`

**Krzywa mocy (power curve):**
```python
cut_in, rated_wind = 3.0, 12.0
if wind >= rated_wind:
    power = rated_kw
else:
    power = rated_kw * ((wind**3 - cut_in**3) / (rated_wind**3 - cut_in**3))
```

#### 7.2.2 Dataset 2: `maintenance_history.csv` (~8 000 wierszy)

2 lata historii (730 dni), ~30-55 zdarzeń/turbinę.

**Kolumny:** `record_id, turbine_id, farm_name, date, maintenance_type, component, description, duration_hours, cost_pln, parts_replaced, technician_team`

- `record_id`: `MH-000001` (6 cyfr)
- Wagi typów: Scheduled 40%, Corrective 25%, Predictive 20%, Emergency 15%
- Czas trwania: Emergency 6–48h, Corrective 4–24h, Predictive 2–16h, Scheduled 1–8h
- Koszty: Emergency 15 000–120 000 PLN, Corrective 5 000–60 000, Predictive 3 000–40 000, Scheduled 1 000–15 000
- Opisy (po polsku) zdefiniowane w `MAINTENANCE_DESCRIPTIONS` per komponent
- Części z `_random_parts()` — 1–3 losowe z listy per komponent

#### 7.2.3 Dataset 3: `work_orders.csv` (~1 500 wierszy)

**Kolumny:** `order_id, created_date, turbine_id, farm_name, priority, status, type, description, assigned_team, estimated_hours, actual_hours, parts_needed, completion_date`

- `order_id`: `WO-00001` (5 cyfr)
- Wagi typów: Preventive 40%, Corrective 35%, Inspection 25%
- Wagi priorytetów: Low 25%, Medium 35%, High 25%, Critical 15%
- Status zależy od wieku zlecenia:
  - <7 dni: Open 40%, In_Progress 35%, Completed 20%, Cancelled 5%
  - 7-30 dni: Open 10%, In_Progress 20%, Completed 60%, Cancelled 10%
  - >30 dni: Completed 90%, Cancelled 10%
- Opisy (po polsku) generowane z szablonów per typ + komponent

#### 7.2.4 Dataset 4: `turbine_specs.csv` (200 wierszy)

**Kolumny:** `turbine_id, farm_name, model, rated_power_kw, hub_height_m, rotor_diameter_m, commissioned_date, last_major_overhaul, total_operating_hours, latitude, longitude`

- Commissioned: 3–10 lat temu
- Ostatni przegląd: 30 dni – 3 lata temu
- Operating hours: `commissioned_days × 24 × capacity_factor` (cf: 0.70–0.92)

#### 7.2.5 Dataset 5: `failure_log.csv` (~500 wierszy)

**Kolumny:** `failure_id, turbine_id, farm_name, failure_date, component, failure_type, severity, downtime_hours, root_cause, corrective_action, cost_pln`

- `failure_id`: `FL-00001` (5 cyfr)
- Wagi komponentów: Gearbox 25%, Generator 20%, Blade 15%, Bearing 20%, Hydraulic 8%, Electrical 7%, Pitch_System 5%
- Przestój: Critical 24–168h, Major 8–72h, Minor 1–24h
- Koszty: Critical 50 000–250 000, Major 10 000–80 000, Minor 2 000–25 000

### 7.3 Instrukcje Operations Agent: `config/agent_instructions.md`

Nazwa: **"Inżynier Utrzymania Ruchu"**

```markdown
# Agent: Inżynier Utrzymania Ruchu

## Rola
Jesteś ekspertem ds. utrzymania ruchu farm wiatrowych. Monitorujesz stan turbin,
analizujesz dane sensoryczne i optymalizujesz strategię konserwacji predykcyjnej.

## Dostępne dane
1. **sensor_telemetry** — dane sensoryczne (48h, interwał 5 min, 200 turbin)
   Kolumny: timestamp, turbine_id, farm_name, wind_speed_ms, rotor_rpm, generator_rpm,
   blade_pitch_deg, nacelle_direction_deg, power_output_kw, gearbox_temp_c, bearing_temp_c,
   generator_temp_c, hydraulic_pressure_bar, vibration_mm_s, oil_viscosity, ambient_temp_c,
   humidity_pct, status

2. **maintenance_history** — 2 lata historii konserwacji
   Kolumny: record_id, turbine_id, farm_name, date, maintenance_type, component,
   description, duration_hours, cost_pln, parts_replaced, technician_team

3. **work_orders** — zlecenia pracy
   Kolumny: order_id, created_date, turbine_id, farm_name, priority, status, type,
   description, assigned_team, estimated_hours, actual_hours, parts_needed, completion_date

4. **turbine_specs** — specyfikacje turbin
   Kolumny: turbine_id, farm_name, model, rated_power_kw, hub_height_m, rotor_diameter_m,
   commissioned_date, last_major_overhaul, total_operating_hours, latitude, longitude

5. **failure_log** — rejestr awarii (etykiety ML)
   Kolumny: failure_id, turbine_id, farm_name, failure_date, component, failure_type,
   severity, downtime_hours, root_cause, corrective_action, cost_pln

## Progi alarmowe
| Parametr | Normalny | Ostrzeżenie | Alarm |
|----------|----------|-------------|-------|
| Temperatura przekładni | <70°C | 70-85°C | >85°C |
| Temperatura łożysk | <65°C | 65-80°C | >80°C |
| Temperatura generatora | <75°C | 75-90°C | >90°C |
| Wibracje | <4.5 mm/s | 4.5-7.0 mm/s | >7.0 mm/s |
| Ciśnienie hydrauliki | >180 bar | 160-180 bar | <160 bar |
| Lepkość oleju | >40 | 30-40 | <30 |

## Zasady odpowiedzi
1. Odpowiadaj **po polsku**, używając terminologii branżowej
2. Klasyfikuj stan turbiny: ✅ Normalny, ⚠️ Ostrzeżenie, 🔴 Alarm
3. Wskaż priorytet: 🟢 Niski, 🟡 Średni, 🟠 Wysoki, 🔴 Krytyczny
4. Podawaj konkretne wartości odczytów i trendy
5. Rekomenduj działania: inspekcja, serwis planowy, interwencja natychmiastowa
6. Szacuj koszty i czas naprawy na podstawie historii konserwacji

## Terminologia
- SCADA = Supervisory Control And Data Acquisition
- CMS = Condition Monitoring System
- O&M = Operations and Maintenance (Obsługa i Utrzymanie)
- MTBF = Mean Time Between Failures (Średni czas między awariami)
- MTTR = Mean Time To Repair (Średni czas naprawy)
```

### 7.4 Przykładowe zapytania: `config/example_queries.md`

32 zapytania w 6 kategoriach:

**Monitoring bieżący (6):**
- "Jakie turbiny mają aktualnie status Fault?"
- "Pokaż aktualne temperatury przekładni wszystkich turbin powyżej 70°C"
- "Które turbiny mają podwyższone wibracje?"
- "Jaka jest aktualna produkcja energii per farma?"
- "Pokaż turbiny z niskim ciśnieniem hydrauliki"
- "Jakie turbiny wymagają natychmiastowej interwencji?"

**Analiza trendów (6):**
- "Pokaż trend temperatur przekładni dla turbiny T-DRL-015 z ostatnich 48h"
- "Jak zmieniały się wibracje turbin na farmie Darłowo?"
- "Porównaj trend produkcji energii per farma w ciągu ostatnich 24h"
- "Które turbiny pokazują rosnący trend temperatur?"
- "Jak zmienia się lepkość oleju w czasie?"
- "Pokaż korelację między prędkością wiatru a produkcją energii"

**Historia konserwacji (5):**
- "Ile kosztowała konserwacja w ostatnim roku per farma?"
- "Jakie komponenty najczęściej się psują?"
- "Jaki jest średni czas naprawy per typ konserwacji?"
- "Które zespoły techników mają najlepszą efektywność?"
- "Pokaż historię konserwacji turbiny T-PTG-020"

**Predykcja awarii (5):**
- "Które turbiny mają najwyższe ryzyko awarii?"
- "Na podstawie trendów sensorycznych, jakie komponenty wymagają uwagi?"
- "Ile turbiny degradujących wykrywamy?"
- "Jaki jest MTBF per komponent?"
- "Prognozuj najbliższe awarie na podstawie danych sensorycznych"

**Zlecenia pracy (5):**
- "Ile otwartych zleceń pracy jest aktualnie?"
- "Jakie zlecenia mają priorytet Krytyczny?"
- "Pokaż przeterminowane zlecenia (open > 14 dni)"
- "Jaka jest efektywność realizacji zleceń (szacowane vs rzeczywiste godziny)?"
- "Zaplanuj harmonogram konserwacji na najbliższy tydzień"

**Raporty zbiorcze (5):**
- "Wygeneruj raport stanu floty wiatrowej"
- "Porównaj wydajność farm: produkcja, dostępność, awarie"
- "Jakie jest wykorzystanie mocy zainstalowanej per model turbiny?"
- "Podsumuj koszty O&M za ostatni kwartał"
- "Jaki jest ranking turbin wg niezawodności?"

### 7.5 KQL — Tabele: `kql/create_tables.kql`

```kql
.create table TurbineSensorData (
    timestamp: datetime,
    turbine_id: string,
    farm_name: string,
    wind_speed_ms: real,
    rotor_rpm: real,
    generator_rpm: real,
    blade_pitch_deg: real,
    nacelle_direction_deg: real,
    power_output_kw: real,
    gearbox_temp_c: real,
    bearing_temp_c: real,
    generator_temp_c: real,
    hydraulic_pressure_bar: real,
    vibration_mm_s: real,
    oil_viscosity: real,
    ambient_temp_c: real,
    humidity_pct: real,
    status: string
)

.create table TurbineSensorData ingestion csv mapping 'TurbineSensorMapping' '[
    {"Name":"timestamp","DataType":"datetime","Ordinal":0},
    {"Name":"turbine_id","DataType":"string","Ordinal":1},
    {"Name":"farm_name","DataType":"string","Ordinal":2},
    {"Name":"wind_speed_ms","DataType":"real","Ordinal":3},
    {"Name":"rotor_rpm","DataType":"real","Ordinal":4},
    {"Name":"generator_rpm","DataType":"real","Ordinal":5},
    {"Name":"blade_pitch_deg","DataType":"real","Ordinal":6},
    {"Name":"nacelle_direction_deg","DataType":"real","Ordinal":7},
    {"Name":"power_output_kw","DataType":"real","Ordinal":8},
    {"Name":"gearbox_temp_c","DataType":"real","Ordinal":9},
    {"Name":"bearing_temp_c","DataType":"real","Ordinal":10},
    {"Name":"generator_temp_c","DataType":"real","Ordinal":11},
    {"Name":"hydraulic_pressure_bar","DataType":"real","Ordinal":12},
    {"Name":"vibration_mm_s","DataType":"real","Ordinal":13},
    {"Name":"oil_viscosity","DataType":"real","Ordinal":14},
    {"Name":"ambient_temp_c","DataType":"real","Ordinal":15},
    {"Name":"humidity_pct","DataType":"real","Ordinal":16},
    {"Name":"status","DataType":"string","Ordinal":17}
]'

.alter table TurbineSensorData policy retention '{"SoftDeletePeriod": "365.00:00:00"}'
.alter table TurbineSensorData policy ingestionbatching '{"MaximumBatchingTimeSpan": "00:00:30"}'
```

### 7.6 KQL — Dashboard: `kql/dashboard_queries.kql`

10 zapytań (skrócone):

1. **Przegląd floty** — avg power, max vibration, max temp per turbina, status
2. **Status farm** — count by status, avg power per farm
3. **Trend temperatury przekładni** — bin(timestamp, 15m) per turbine_id (ostatnie 6h)
4. **Mapa cieplna wibracji** — per farma i turbina
5. **Krzywa mocy** — wind_speed vs power_output scatter
6. **Top 10 turbin z najwyższymi wibracjami** — avg z ostatnich 30 min
7. **Temperatura łożysk — alarm** — turbiny z bearing_temp_c > 65
8. **Ciśnienie hydrauliki — trend** — per farma, bin(timestamp, 30m)
9. **Porównanie produkcji per model** — avg power / rated_power per turbine model
10. **Anomalie status** — turbiny ze zmianą statusu na Fault/Maintenance w ostatnich 6h

### 7.7 Notebook ML: `notebooks/failure_prediction.py`

Pipeline predykcji awarii (format `.py` z `# %%` cell markers):

**Sekcje:**
1. **Import i konfiguracja** — pandas, numpy, sklearn, mlflow, matplotlib, seaborn
2. **Ładowanie danych** — sensor_telemetry, failure_log, maintenance_history
3. **Feature engineering:**
   - Agregacja sensorów per turbinę: avg, max, std, trend (slope) z 48h danych
   - Cechy z historii: count_failures_12m, days_since_last_maintenance, count_maintenance_12m
   - Label: `failure_within_7d` — czy awaria zarejestrowana w ciągu 7 dni po ostatnim odczycie
4. **Eksploracja danych** — korelacja cech, rozkłady, pair plots degradujące vs normalne
5. **Model RandomForest:**
   - `RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')`
   - Train/test split: 80/20, stratified
   - Metryki: accuracy, precision, recall, F1, AUC-ROC
   - Feature importance plot
6. **MLflow tracking:**
   - `mlflow.set_experiment("turbine-failure-prediction")`
   - Log: parametry, metryki, model, feature importance chart
   - `mlflow.sklearn.log_model(model, "random_forest_model")`
7. **Wnioski** — top 10 turbin z najwyższym predicted risk, rekomendacje

---

## 8. SCENARIUSZ 04 — PROGNOZOWANIE POPYTU

### 8.1 Przeznaczenie

Demonstracja RTI + Data Agents do prognozowania popytu na energię. Generator tworzy 5 datasetów: odczyty 5000 smart meterów (24h), historyczną konsumpcję (1 rok), dane pogodowe (1 rok), taryfy elektryczne (polskie) i prognozy popytu.

### 8.2 Generator danych: `generate_demand_data.py`

**Seed:** `SEED = 2004`

**Stałe scenariusza (lokalne, nie z shared):**

```python
REGIONS = [
    "Mazowieckie", "Śląskie", "Wielkopolskie", "Małopolskie", "Dolnośląskie",
    "Łódzkie", "Pomorskie", "Lubelskie", "Podkarpackie", "Kujawsko-Pomorskie",
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
    "Mazowieckie": "Warszawa-Okęcie",      "Śląskie": "Katowice-Pyrzowice",
    "Wielkopolskie": "Poznań-Ławica",       "Małopolskie": "Kraków-Balice",
    "Dolnośląskie": "Wrocław-Strachowice", "Łódzkie": "Łódź-Lublinek",
    "Pomorskie": "Gdańsk-Rębiechowo",      "Lubelskie": "Lublin-Radawiec",
    "Podkarpackie": "Rzeszów-Jasionka",    "Kujawsko-Pomorskie": "Bydgoszcz-Szwederowo",
}

POLISH_HOLIDAYS_MMDD = [
    "01-01", "01-06", "05-01", "05-03", "08-15",
    "11-01", "11-11", "12-25", "12-26",
]
```

**Taryfy polskie (TARIFF_DATA) — 19 taryf:**

| Kod | Nazwa | Segment | PLN/kWh | Opłata stała PLN/mc |
|-----|-------|---------|---------|---------------------|
| G11 | Jednostrefowa (gosp. domowe) | Household | 0.65 | 12.50 |
| G12 | Dwustrefowa dzień/noc | Household | 0.72 | 14.00 |
| G12w | Dwustrefowa weekendowa | Household | 0.70 | 14.00 |
| G13 | Trzystrefowa | Household | 0.78 | 15.00 |
| C11 | Jednostrefowa (przedsiębiorstwa) | Services | 0.58 | 25.00 |
| C12a | Dwustrefowa komercyjna | Services | 0.63 | 28.00 |
| C12b | Dwustrefowa szczytowa | Services | 0.66 | 28.00 |
| C21 | Dwustrefowa z mocą | Services | 0.55 | 45.00 |
| C22a | Trójstrefowa komercyjna | Services | 0.60 | 48.00 |
| C22b | Trójstrefowa szczytowa | Services | 0.62 | 48.00 |
| C23 | Trójstrefowa z mocą i strefami | Services | 0.52 | 55.00 |
| B11 | Jednostrefowa SN | Industry | 0.48 | 120.00 |
| B12 | Dwustrefowa SN | Industry | 0.52 | 130.00 |
| B21 | Dwustrefowa SN z mocą | Industry | 0.45 | 180.00 |
| B22 | Trójstrefowa SN z mocą | Industry | 0.50 | 200.00 |
| B23 | Trójstrefowa SN rozszerzona | Industry | 0.47 | 210.00 |
| A23 | Trójstrefowa WN | Industry | 0.38 | 500.00 |
| A24 | Czterostrefowa WN | Industry | 0.36 | 550.00 |
| R | Rolnicza | Agriculture | 0.55 | 18.00 |

Każda taryfa zawiera opis godzin szczytu, pozaszczytu i pełny opis po polsku.

**Profile dobowe per segment:**

```python
# Household — szczyt rano 7-9, wieczór 19-21, noc 0-5 nisko
# Industry — 8-16 max, noc minimalna
# Services — 10-17 pełne obciążenie, noc zerowe
# Agriculture — światło dzienne 7-18
# Public — 8-16 pełne, wieczór obniżone
```

**Współczynniki weekendowe:**
```python
# Weekend: Industry=0.25, Services=0.45, Public=0.30, Household=1.10, Agriculture=0.60
```

**Współczynniki świąteczne:**
```python
# Święta: Industry=0.15, Services=0.30, Public=0.20, Household=1.15, Agriculture=0.40
```

**Wpływ temperatury:**
```python
def temperature_demand_factor(temp_c):
    if temp_c < 0:   return 1.0 + abs(temp_c) * 0.04   # zimno → grzanie
    elif temp_c > 25: return 1.0 + (temp_c - 25) * 0.03  # gorąco → klima
    elif temp_c < 10: return 1.0 + (10 - temp_c) * 0.015  # chłodno → lekkie grzanie
    else: return 1.0                                       # komfortowo
```

#### 8.2.1 Dataset 1: `smart_meter_readings.csv` (~480 000 wierszy)

5000 mierników × 96 odczytów (24h @ 15 min) + 1 = ~480 000

**Kolumny:** `timestamp, meter_id, customer_id, customer_segment, region, tariff_zone, active_energy_kwh, reactive_energy_kvarh, voltage_v, current_a, power_factor, power_kw, daily_max_kw`

Generacja:
- `meter_id`: `SM-00000` ... `SM-04999`
- `customer_id`: `CUST-000000` ... `CUST-004999`
- `power_kw = base_power × daily_factor × seasonal_factor × weekend_factor × holiday_factor × temperature_factor × noise`
- `energy_kwh = power_kw × 0.25` (interwał 15 min)
- `power_factor`: gauss(0.94, 0.03), clip [0.80, 0.99]
- `reactive_energy = energy × tan(acos(pf))`
- `voltage`: gauss(230.0, 3.0), clip [207, 253]
- `current = (power×1000) / (voltage × pf)`
- `daily_max = power × uniform(1.1, 1.4)`

#### 8.2.2 Dataset 2: `historical_consumption.csv` (~87 600 wierszy)

365 dni × 24h × 10 regionów = 87 600

**Kolumny:** `date, hour, region, segment, total_consumption_mwh, customer_count, avg_consumption_kwh, peak_demand_mw, temperature_c, wind_speed_ms, cloud_cover_pct, is_holiday, is_weekend, day_of_week`

- `segment` = "All" (zagregowane)
- Bazowa konsumpcja per region proporcjonalna do populacji:
  Mazowieckie: 520 MWh/h, Śląskie: 410, Wielkopolskie: 310, Małopolskie: 290, Dolnośląskie: 260, Łódzkie: 220, Pomorskie: 210, Lubelskie: 180, Podkarpackie: 170, Kujawsko-Pomorskie: 180
- Liczba klientów per region: Mazowieckie: 285k, Śląskie: 220k, ..., Kujawsko-Pomorskie: 105k
- Czynnik dobowy = ważona średnia segmentów (55% household + 10% industry + ...)

#### 8.2.3 Dataset 3: `weather_data.csv` (~87 600 wierszy)

**Kolumny:** `timestamp, station_name, region, temperature_c, feels_like_c, humidity_pct, wind_speed_ms, wind_direction_deg, pressure_hpa, cloud_cover_pct, precipitation_mm, solar_radiation_wm2`

- Regionalne offsety temperatury: Podkarpackie -1.5°C, Małopolskie -1.0°C, Pomorskie +1.0°C, Dolnośląskie +0.5°C, reszta ~0
- Opady: szansa 15%+10%×sin (więcej latem), exponential distribution
- Promieniowanie: 6:00–20:00, max 400+500×sin(seasonal), × (1 - cloud/150)

#### 8.2.4 Dataset 4: `tariff_schedule.csv` (19 wierszy)

**Kolumny:** `tariff_code, tariff_name, segment, energy_rate_pln_kwh, fixed_charge_pln_month, peak_hours, off_peak_hours, description`

Dokładne dane z tabeli TARIFF_DATA (19 taryf — patrz wyżej).

#### 8.2.5 Dataset 5: `demand_forecasts.csv` (~8 760 wierszy)

365 dni × 24h = 8 760

**Kolumny:** `timestamp, hour, forecasted_demand_mw, actual_demand_mw, forecast_error_pct, temperature_forecast_c, is_peak_hour, region`

- `total_base_mw = 1800.0` — bazowe zapotrzebowanie sieci w MW
- `actual_mw = 1800 × daily_factor × seasonal_factor × temp_factor × weekend_adj`
- Weekend: `w_adj = 0.78`, święta: `×= 0.70`
- Forecast: `actual × (1 + gauss(0, 0.035))` — MAPE 3–5%
- `is_peak = (7 ≤ hour ≤ 9) or (17 ≤ hour ≤ 21)`
- `region = "All"` (zagregowane)

### 8.3 Instrukcje Data Agent: `config/agent_instructions.md`

Nazwa: **"Prognosta Popytu"**

```markdown
# Agent: Prognosta Popytu

## Rola
Jesteś ekspertem ds. prognozowania popytu na energię elektryczną w Polsce.
Analizujesz dane z inteligentnych liczników, wzorce konsumpcji, warunki
meteorologiczne i optymalizujesz prognozy obciążenia sieci.

## Dostępne dane
1. **smart_meter_readings** — odczyty smart meterów (24h, co 15 min, 5000 mierników)
   Kolumny: timestamp, meter_id, customer_id, customer_segment, region,
   tariff_zone, active_energy_kwh, reactive_energy_kvarh, voltage_v, current_a,
   power_factor, power_kw, daily_max_kw

2. **historical_consumption** — historyczna konsumpcja (1 rok, godzinowa, per region)
   Kolumny: date, hour, region, segment, total_consumption_mwh, customer_count,
   avg_consumption_kwh, peak_demand_mw, temperature_c, wind_speed_ms,
   cloud_cover_pct, is_holiday, is_weekend, day_of_week

3. **weather_data** — dane meteorologiczne (1 rok, godzinowe, per stacja)
   Kolumny: timestamp, station_name, region, temperature_c, feels_like_c,
   humidity_pct, wind_speed_ms, wind_direction_deg, pressure_hpa,
   cloud_cover_pct, precipitation_mm, solar_radiation_wm2

4. **tariff_schedule** — taryfy elektryczne
   Kolumny: tariff_code, tariff_name, segment, energy_rate_pln_kwh,
   fixed_charge_pln_month, peak_hours, off_peak_hours, description

5. **demand_forecasts** — prognozy vs rzeczywistość (1 rok)
   Kolumny: timestamp, hour, forecasted_demand_mw, actual_demand_mw,
   forecast_error_pct, temperature_forecast_c, is_peak_hour, region

## Progi i benchmarki
| Metryka | Cel | Ostrzeżenie | Alarm |
|---------|-----|------------|-------|
| MAPE prognozy | <3% | 3-5% | >5% |
| Szczytowe zapotrzebowanie | <90% pojemności | 90-95% | >95% |
| Nierównomierność obciążenia | <25% | 25-35% | >35% |
| Compliance taryfowy | >95% | 90-95% | <90% |

## Zasady odpowiedzi
1. Odpowiadaj **po polsku**
2. Używaj jednostek: MW, MWh, kWh, PLN/kWh
3. Rozróżniaj segmenty: Household, Industry, Services, Agriculture, Public
4. Podawaj metryki prognozy: MAPE, RMSE, bias
5. Wskazuj wpływ pogody, świąt, weekendów na popyt
6. Dla prognoz zaznaczaj horyzont czasowy i pewność

## Terminologia
- DSR = Demand Side Response (Odpowiedź strony popytowej)
- AMI = Advanced Metering Infrastructure (Zaawansowana infrastruktura pomiarowa)
- TOU = Time-of-Use (Taryfa czasowa)
- DR = Demand Response (Zarządzanie popytem)
- MAPE = Mean Absolute Percentage Error
- RMSE = Root Mean Square Error
```

### 8.4 Przykładowe zapytania: `config/example_queries.md`

38 zapytań w 7 kategoriach:

**Smart metery (6):**
- "Jaka jest aktualna konsumpcja per segment?"
- "Pokaż top 10 mierników o najwyższym zużyciu"
- "Jak rozkłada się zużycie per taryfa?"
- "Jaka jest średnia moc per region?"
- "Które mierniki przekraczają moc zamówioną?"
- "Pokaż profil dobowy per segment klienta"

**Historia konsumpcji (5):**
- "Jak zmienia się konsumpcja sezonowo?"
- "Porównaj zużycie zimowe vs letnie per region"
- "Jaki jest wpływ temperatury na popyt?"
- "Jak weekendy wpływają na konsumpcję?"
- "Pokaż trend roczny per region"

**Pogoda i wpływ (5):**
- "Jaka jest korelacja temperatura-popyt?"
- "Jak wiatr wpływa na zapotrzebowanie?"
- "Porównaj warunki pogodowe per region"
- "Kiedy temperatury najbardziej wpływają na popyt?"
- "Jak zachmurzenie koreluje z konsumpcją?"

**Prognozy (6):**
- "Jaka jest dokładność prognoz (MAPE)?"
- "Kiedy prognozy są najgorsze?"
- "Porównaj prognozę vs rzeczywistość per godzina"
- "Jaki jest bias prognostyczny?"
- "Pokaż rozkład błędów prognoz"
- "Jak poprawić dokładność prognoz?"

**Taryfy i optymalizacja (6):**
- "Jaka jest struktura taryfowa klientów?"
- "Ile klientów mogłoby zaoszczędzić zmieniając taryfę?"
- "Porównaj koszty per taryfa"
- "Jaki jest potencjał DSR?"
- "Optymalizuj przydział taryf"
- "Szacuj oszczędności z przeniesienia obciążenia poza szczyt"

**Raporty regionalne (5):**
- "Porównaj regiony pod kątem zużycia per capita"
- "Gdzie jest największy potencjał optymalizacji?"
- "Jakie regiony mają najwyższe zapotrzebowanie?"
- "Porównaj profile dobowe regionów"
- "Wskaż regiony z największym ryzykiem przeciążeń"

**Strategia (5):**
- "Podsumuj stan zapotrzebowania w KSE"
- "Jakie trendy kształtują popyt na energię?"
- "Jak przygotować się na falę mrozów?"
- "Zaproponuj strategię zarządzania szczytami"
- "Jaki jest potencjał inteligentnych sieci?"

### 8.5 Schema Eventstream: `config/eventstream_schema.json`

```json
{
  "name": "SmartMeterStream",
  "description": "Strumień odczytów z inteligentnych liczników energii",
  "format": "JSON",
  "schema": {
    "type": "object",
    "properties": {
      "timestamp":          {"type": "string", "format": "date-time"},
      "meter_id":           {"type": "string"},
      "customer_id":        {"type": "string"},
      "customer_segment":   {"type": "string", "enum": ["Household", "Industry", "Services", "Agriculture", "Public"]},
      "region":             {"type": "string"},
      "tariff_zone":        {"type": "string"},
      "active_energy_kwh":  {"type": "number"},
      "reactive_energy_kvarh": {"type": "number"},
      "voltage_v":          {"type": "number"},
      "current_a":          {"type": "number"},
      "power_factor":       {"type": "number"},
      "power_kw":           {"type": "number"},
      "daily_max_kw":       {"type": "number"}
    }
  },
  "partitionKey": "region",
  "timestampField": "timestamp"
}
```

### 8.6 KQL — Tabele: `kql/create_tables.kql`

```kql
.create table SmartMeterReadings (
    timestamp: datetime,
    meter_id: string,
    customer_id: string,
    customer_segment: string,
    region: string,
    tariff_zone: string,
    active_energy_kwh: real,
    reactive_energy_kvarh: real,
    voltage_v: real,
    current_a: real,
    power_factor: real,
    power_kw: real,
    daily_max_kw: real
)
```

+ CSV ingestion mapping `SmartMeterMapping` (13 kolumn, Ordinal 0–12)
+ Materialized view `SmartMeterReadings_5min` — agregacja bin(timestamp, 5m) by region, customer_segment
+ Function `GetConsumptionBySegment(start, end)` — total, avg, peak per segment

### 8.7 KQL — Dashboard: `kql/dashboard_queries.kql`

10+ zapytań:

1. Podsumowanie zużycia per segment (ostatnie 15 min)
2. Trend mocy w czasie (bin 5 min)
3. Mapa zużycia per region
4. Profil dobowy per segment
5. Porównanie taryf — rozkład zużycia
6. Napięcie sieci — odchylenia od 230V
7. Współczynnik mocy — rozkład per segment
8. Peak demand tracking — max 15-min okno
9. Porównanie dzisiaj vs wczoraj
10. Prognoza vs realizacja — trend godzinowy

### 8.8 Notebook: `notebooks/demand_analysis.py`

Notebook Python (format `.py` z `# %%`) z sekcjami:

1. **Import i ładowanie** — 5 CSV + wykresy konfiguracja
2. **EDA smart meterów** — rozkłady, outliers, profil dobowy
3. **Analiza sezonowa** — heatmapa miesiąc × godzina, decompozycja
4. **Wpływ pogody** — scatter temperatura-popyt, regresja
5. **Analiza taryfowa** — konsumpcja per taryfa, potencjał optymalizacji
6. **Ewaluacja prognoz** — MAPE, RMSE, bias, worst hours
7. **Wizualizacje** — 8 wykresów PNG zapisywanych do `data/analysis_output/`
8. **Wnioski** — rekomendacje w markdown

---

## 9. KONFIGURACJA `.gitignore`

```
# Generated data
scenario-*/data/
*.csv

# Python
__pycache__/
*.pyc
*.pyo
*.egg-info/
dist/
build/
.eggs/

# Jupyter
.ipynb_checkpoints/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Virtual environments
venv/
.venv/
env/

# MLflow
mlruns/
mlflow.db
```

---

## 10. README GŁÓWNE

Główny `README.md` zawiera:

1. **Nagłówek** z emoji ⚡ i tytułem "Microsoft Fabric — Platforma Demo dla Firm Energetycznych"
2. **Opis** — platforma demonstracyjna prezentująca możliwości Microsoft Fabric jako zunifikowanego, opartego na AI systemu analitycznego
3. **Diagram architektoniczny** w ASCII:
   ```
   ┌─────────────────────────────────────────────────────────────┐
   │                    Microsoft Fabric                         │
   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
   │  │  Event-   │  │  Event-  │  │  Lake-   │  │ Data     │  │
   │  │  stream   │──│  house   │──│  house   │──│ Agents   │  │
   │  │ (Ingest)  │  │ (KQL DB) │  │ (Store)  │  │ (AI)     │  │
   │  └─────┬────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  │
   │        │             │             │              │         │
   │  ┌─────▼────┐  ┌────▼─────┐  ┌────▼─────┐  ┌────▼─────┐  │
   │  │ Real-Time│  │Dashboard │  │Notebooks │  │Operations│  │
   │  │Dashboard │  │ Queries  │  │   (ML)   │  │  Agent   │  │
   │  └─────┬────┘  └──────────┘  └──────────┘  └──────────┘  │
   │        │                                                    │
   │  ┌─────▼────┐                                              │
   │  │  Data    │                                              │
   │  │Activator │                                              │
   │  │ (Alerts) │                                              │
   │  └─────────┘                                               │
   └─────────────────────────────────────────────────────────────┘
   ```
4. **Tabela 4 scenariuszy** — z emoji, opisem i komponentami Fabric
5. **Wymagania** — Microsoft Fabric (F64+), Python 3.11+, Azure Event Hubs
6. **Quick Start** — krok po kroku: clone, venv, pip install, uruchom generatory, skonfiguruj Fabric
7. **Sekcja per scenariusz** z krótkim opisem i linkami do README
8. **Architektura danych** — opis przepływu: Generator → CSV → Eventstream/Lakehouse → KQL DB → Dashboard/Agent
9. **Uwagi** — dane syntetyczne, polskie stałe, deterministyczny seed, relatywne timestampy

---

## 11. KONFIGURACJA requirements.txt

```
pandas>=2.0
numpy>=1.24
matplotlib>=3.7
seaborn>=0.12
scikit-learn>=1.3
mlflow>=2.8
azure-eventhub>=5.11
azure-identity>=1.14
```

---

## 12. INSTRUKCJE ODTWORZENIA

### Krok 1: Utwórz strukturę katalogów
```bash
mkdir -p energy-market-demo/{shared,scenario-01-realtime-grid-monitoring/{config,data,kql,notebooks},scenario-02-energy-market-analytics/{config,data,kql,notebooks},scenario-03-predictive-maintenance/{config,data,kql,notebooks},scenario-04-demand-forecasting/{config,data,kql,notebooks}}
```

### Krok 2: Utwórz moduł `shared/`
Zaimplementuj `__init__.py`, `utils.py`, `constants.py`, `generators.py` zgodnie ze specyfikacją w sekcji 4.

### Krok 3: Utwórz generatory danych
Każdy scenariusz ma swój `generate_*.py` — zaimplementuj zgodnie ze specyfikacjami w sekcjach 5-8.

### Krok 4: Utwórz pliki konfiguracyjne
- Schematy Eventstream (JSON) dla scenariuszy 01 i 04
- Instrukcje agentów (markdown) dla scenariuszy 02, 03, 04
- Przykładowe zapytania (markdown) dla scenariuszy 02, 03, 04

### Krok 5: Utwórz pliki KQL
- `create_tables.kql` — definicje tabel, mappingi, polityki
- `dashboard_queries.kql` — zapytania do dashboardów
- `activator_rules.kql` — reguły Data Activator (scenariusz 01)

### Krok 6: Utwórz notebooki
- Scenariusz 01: `grid_analysis.ipynb` (format Jupyter)
- Scenariusze 02, 03, 04: `*.py` (format Databricks/Fabric — `# %%` cell markers)

### Krok 7: Skonfiguruj Git
- Utwórz `.gitignore` (sekcja 9)
- `git init && git add . && git commit -m "Initial commit"`

### Krok 8: Uruchom generatory
```bash
python scenario-01-realtime-grid-monitoring/generate_grid_data.py
python scenario-02-energy-market-analytics/generate_market_data.py
python scenario-03-predictive-maintenance/generate_turbine_data.py
python scenario-04-demand-forecasting/generate_demand_data.py
```

### Krok 9: Skonfiguruj Microsoft Fabric
1. Utwórz workspace "Energy Demo"
2. Dla scenariusza 01: Eventstream → Eventhouse → Real-Time Dashboard → Data Activator
3. Dla scenariusza 02: Lakehouse → załaduj 5 CSV → skonfiguruj Data Agent
4. Dla scenariusza 03: Eventhouse + Lakehouse → Operations Agent → Notebook ML
5. Dla scenariusza 04: Eventstream → Eventhouse + Lakehouse → Data Agent + Dashboard

---

## 13. UWAGI KOŃCOWE

1. **Dane syntetyczne** — wszystkie dane są generowane algorytmicznie, ale oparte na realistycznych parametrach polskiego sektora energetycznego
2. **Deterministyczność** — seed zapewnia reprodukowalność przy tych samych timestampach (ale timestampy zmieniają się z każdym uruchomieniem)
3. **Język polski** — cała dokumentacja, nazwy stacji, firmy, opisy konserwacji, instrukcje agentów — po polsku
4. **Skalowalność** — parametry (liczba stacji, turbin, mierników, okres) można łatwo modyfikować
5. **Microsoft Fabric** — projekt zaprojektowany specjalnie pod Fabric, ale generatory danych działają niezależnie
6. **Timestampy relatywne** — zawsze generowane względem `now()`, więc demo jest aktualne niezależnie od daty uruchomienia
7. **Real Polish Companies** — scenariusz 02 zawiera nazwy ~50 rzeczywistych polskich firm energetycznych do celów demonstracyjnych
