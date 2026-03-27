# Scenariusz 01: Monitoring Sieci Energetycznej w Czasie Rzeczywistym

## Opis scenariusza

Scenariusz demonstruje wykorzystanie **Microsoft Fabric Real-Time Intelligence (RTI)** do monitorowania 50 stacji elektroenergetycznych (GPZ — Głównych Punktów Zasilania) rozmieszczonych na terenie całej Polski. System zbiera dane telemetryczne w czasie rzeczywistym, wykrywa anomalie i wyzwala automatyczne alerty.

**Monitorowane parametry:**
- **Napięcie** (kV) — poziom napięcia na szynie głównej
- **Prąd** (A) — natężenie prądu obciążenia
- **Częstotliwość** (Hz) — częstotliwość sieci (nominalna: 50 Hz)
- **Współczynnik mocy** (cos φ) — jakość energii
- **Moc czynna** (MW) i **moc bierna** (MVAR)
- **Obciążenie transformatora** (%) — procent wykorzystania mocy znamionowej
- **Temperatura transformatora** (°C) — krytyczny parametr eksploatacyjny
- **Temperatura otoczenia** (°C) — warunki środowiskowe

System automatycznie klasyfikuje stan każdej stacji jako **Normal**, **Warning** lub **Critical** i wyzwala alerty przez Data Activator.

---

## Architektura rozwiązania

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        MICROSOFT FABRIC WORKSPACE                       │
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────────────┐ │
│  │   Python      │    │  Eventstream  │    │      Eventhouse            │ │
│  │   Generator   │───▶│  ES_Grid      │───▶│  EH_GridMonitoring        │ │
│  │              │    │  Telemetry    │    │  ┌──────────────────────┐  │ │
│  │  generate_   │    │              │    │  │  KQL DB:             │  │ │
│  │  grid_data.py│    │  (Custom App  │    │  │  GridTelemetry       │  │ │
│  │              │    │   Source)     │    │  │  ├─SubstationReadings│  │ │
│  └──────────────┘    └──────────────┘    │  │  └─SubstationEvents  │  │ │
│                                          │  └──────────────────────┘  │ │
│                                          └─────────┬──────────────────┘ │
│                                                    │                    │
│                              ┌─────────────────────┼────────────┐       │
│                              │                     │            │       │
│                              ▼                     ▼            ▼       │
│                   ┌──────────────────┐  ┌──────────────┐ ┌──────────┐  │
│                   │  Real-Time       │  │  KQL Query   │ │  Data    │  │
│                   │  Dashboard       │  │  Set         │ │Activator │  │
│                   │  "Monitoring     │  │  (ad-hoc     │ │  Reflex  │  │
│                   │   Sieci"         │  │   analysis)  │ │  "Alert: │  │
│                   │                  │  │              │ │ Anomalie │  │
│                   │  ┌────────────┐  │  └──────────────┘ │  Sieci"  │  │
│                   │  │ Mapa GPZ   │  │                   │          │  │
│                   │  │ Trendy U   │  │                   │  ┌─────┐ │  │
│                   │  │ Heatmapa   │  │                   │  │Email│ │  │
│                   │  │ Alerty f   │  │                   │  │Teams│ │  │
│                   │  └────────────┘  │                   │  └─────┘ │  │
│                   └──────────────────┘                   └──────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

**Przepływ danych:**
1. **Generator Python** tworzy syntetyczne dane telemetryczne z 50 stacji (odczyty co 10 sekund)
2. **Eventstream** odbiera strumień danych i kieruje go do bazy KQL
3. **Eventhouse (KQL Database)** przechowuje dane z możliwością natychmiastowego zapytania
4. **Real-Time Dashboard** wizualizuje stan sieci w czasie rzeczywistym
5. **Data Activator** monitoruje progi i wyzwala alerty

---

## Komponenty Microsoft Fabric

| Komponent | Typ w Fabric | Nazwa | Rola |
|-----------|-------------|-------|------|
| Generator danych | Python Script | `generate_grid_data.py` | Generowanie syntetycznych danych telemetrycznych |
| Strumień zdarzeń | Eventstream | `ES_GridTelemetry` | Ingestion danych w czasie rzeczywistym |
| Baza analityczna | Eventhouse | `EH_GridMonitoring` | Przechowywanie i zapytania KQL |
| Baza KQL | KQL Database | `GridTelemetry` | Tabele: SubstationReadings, SubstationEvents |
| Dashboard | Real-Time Dashboard | `Dashboard: Monitoring Sieci` | Wizualizacja stanu sieci w czasie rzeczywistym |
| Alerty | Data Activator (Reflex) | `Alert: Anomalie Sieci` | Automatyczne wyzwalanie alertów |
| Notebook | Fabric Notebook | `grid_analysis` | Analiza statystyczna i detekcja anomalii |

---

## Krok po kroku — konfiguracja

### Krok 1: Przygotowanie danych

Uruchom generator danych, który utworzy pliki CSV w katalogu `data/`:

```bash
cd scenario-01-realtime-grid-monitoring
python generate_grid_data.py
```

Generator utworzy trzy pliki:

| Plik | Opis | Rozmiar |
|------|------|---------|
| `data/substation_readings.csv` | Odczyty telemetryczne (~500 000 wierszy) | ~80 MB |
| `data/substation_events.csv` | Zdarzenia i anomalie (~2 000 wierszy) | ~200 KB |
| `data/substations_master.csv` | Dane stałe 50 stacji | ~3 KB |

**Struktura danych odczytów** (`substation_readings.csv`):
- Dane z ostatnich 24 godzin
- Odczyty co 10 sekund dla każdej z 50 stacji
- 8 640 odczytów × 50 stacji ≈ 432 000 wierszy + anomalie
- Część stacji ma wstrzyknięte okresy anomalii (spadki napięcia, odchylenia częstotliwości, przeciążenia)

### Krok 2: Utworzenie Eventhouse i bazy KQL

1. W workspace Microsoft Fabric przejdź do **+ New** → **Eventhouse**
2. Nazwij Eventhouse: **`EH_GridMonitoring`**
3. Po utworzeniu automatycznie powstanie baza KQL — zmień jej nazwę na **`GridTelemetry`**
4. Otwórz bazę KQL i uruchom polecenia z pliku `kql/create_tables.kql`:

```kql
// Tabela główna — odczyty telemetryczne
.create table SubstationReadings (
    Timestamp: datetime,
    SubstationId: string,
    SubstationName: string,
    Latitude: real,
    Longitude: real,
    VoltageKV: real,
    CurrentA: real,
    FrequencyHz: real,
    PowerFactor: real,
    ActivePowerMW: real,
    ReactivePowerMVAR: real,
    LoadPct: real,
    TransformerTempC: real,
    AmbientTempC: real,
    Status: string
)
```

5. **Włącz streaming ingestion** w ustawieniach bazy KQL:
   - Database → Settings → Streaming ingestion → **Enable**

### Krok 3: Konfiguracja Eventstream

1. W workspace utwórz **+ New** → **Eventstream** → nazwij **`ES_GridTelemetry`**

2. **Dodaj źródło danych:**
   - Kliknij **+ Add source** → **Custom App**
   - Skopiuj Connection String (Event Hub compatible)
   - Alternatywnie dla demo: użyj **Sample data** lub **Upload CSV**

3. **Dodaj miejsce docelowe:**
   - Kliknij **+ Add destination** → **KQL Database**
   - Wybierz Eventhouse: `EH_GridMonitoring`
   - Baza: `GridTelemetry`
   - Tabela: `SubstationReadings`

4. **Konfiguracja mapowania kolumn:**
   - Upewnij się, że kolumny ze schematu JSON (`config/eventstream_schema.json`) są poprawnie zmapowane
   - Timestamp → `Timestamp` (datetime)
   - Pozostałe pola — automatyczne mapowanie po nazwie

5. Uruchom Eventstream i zweryfikuj, że dane napływają

### Krok 4: Real-Time Dashboard

1. W workspace utwórz **+ New** → **Real-Time Dashboard** → nazwij **`Dashboard: Monitoring Sieci`**

2. Połącz dashboard z bazą KQL `GridTelemetry`

3. **Dodaj kafelki (tiles):**

#### Kafelek 1: Mapa stacji GPZ (Scatter Map)
```kql
SubstationReadings
| summarize arg_max(Timestamp, *) by SubstationId
| project SubstationName, Latitude, Longitude, Status, LoadPct, VoltageKV
```
- Typ wizualizacji: **Map (Scatter)**
- Rozmiar punktu: LoadPct
- Kolor: Status (Normal=zielony, Warning=żółty, Critical=czerwony)

#### Kafelek 2: Trend napięcia (Time Chart)
```kql
SubstationReadings
| where Timestamp > ago(4h)
| summarize avg(VoltageKV) by bin(Timestamp, 1m), SubstationName
| render timechart
```
- Typ: **Time chart** z podziałem na stacje

#### Kafelek 3: Heatmapa obciążenia
```kql
SubstationReadings
| where Timestamp > ago(24h)
| summarize avg(LoadPct) by SubstationName, bin(Timestamp, 1h)
| render heatmap
```

#### Kafelek 4: Alerty odchylenia częstotliwości
```kql
SubstationReadings
| where Timestamp > ago(1h)
| where abs(FrequencyHz - 50.0) > 0.3
| project Timestamp, SubstationName, FrequencyHz, Deviation = abs(FrequencyHz - 50.0)
| order by Deviation desc
```

#### Kafelek 5: Top 10 najbardziej obciążonych stacji
```kql
SubstationReadings
| summarize arg_max(Timestamp, *) by SubstationId
| top 10 by LoadPct desc
| project SubstationName, LoadPct, ActivePowerMW, Status
| render barchart
```

Wszystkie zapytania KQL znajdziesz w pliku `kql/dashboard_queries.kql`.

### Krok 5: Data Activator

1. W workspace utwórz **+ New** → **Reflex** → nazwij **`Alert: Anomalie Sieci`**

2. Połącz Reflex z Eventstreamem `ES_GridTelemetry`

3. **Skonfiguruj wyzwalacze (triggers):**

| Wyzwalacz | Warunek | Ważność |
|-----------|---------|---------|
| Anomalia napięcia | VoltageKV < 210 **lub** VoltageKV > 240 | 🔴 Critical |
| Odchylenie częstotliwości | abs(FrequencyHz − 50.0) > 0.5 | 🟡 Warning |
| Przeciążenie | LoadPct > 90 | 🔴 Critical |
| Temperatura krytyczna | TransformerTempC > 85 | 🟡 Warning |

4. **Konfiguracja akcji:**
   - Dodaj akcję: **Send Email** do zespołu operacyjnego
   - Dodaj akcję: **Post to Teams** w kanale #grid-alerts
   - Treść powiadomienia zawiera: nazwę stacji, typ anomalii, wartość odczytu, timestamp

5. Zapytania KQL dla wyzwalaczy znajdziesz w `kql/activator_rules.kql`

---

## Scenariusz demo (15 minut)

### Minuta 0–2: Wprowadzenie
- Przedstaw kontekst: monitoring sieci energetycznej w Polsce
- Pokaż diagram architektury (Eventstream → Eventhouse → Dashboard + Activator)
- Wyjaśnij, że dane pochodzą z 50 stacji GPZ monitorowanych w czasie rzeczywistym

### Minuta 2–5: Eventstream — dane w strumieniu
- Otwórz Eventstream `ES_GridTelemetry`
- Pokaż dane napływające w czasie rzeczywistym
- Zwróć uwagę na throughput (~300 zdarzeń/sekundę)
- Pokaż mapowanie kolumn i routing do bazy KQL

### Minuta 5–9: Real-Time Dashboard
- Otwórz dashboard `Monitoring Sieci`
- **Mapa GPZ**: pokaż rozmieszczenie stacji z kolorami statusów
- **Trend napięcia**: wskaż stacje z anomaliami napięcia
- **Heatmapa obciążenia**: pokaż wzorce dobowe (szczyt wieczorny ~18:00)
- **Odchylenia częstotliwości**: wyjaśnij znaczenie stabilności 50 Hz
- Włącz auto-refresh (co 30 sekund)

### Minuta 9–12: KQL — analiza ad-hoc
- Otwórz KQL Query Set
- Uruchom kilka zapytań:
  ```kql
  // Ile anomalii w ciągu ostatniej godziny?
  SubstationReadings
  | where Timestamp > ago(1h) and Status != "Normal"
  | summarize count() by Status, SubstationName
  ```
  ```kql
  // Średnie obciążenie w podziale na regiony
  SubstationReadings
  | summarize arg_max(Timestamp, *) by SubstationId
  | summarize avg(LoadPct) by Region = extract("(.*)", 1, SubstationName)
  ```
- Pokaż, jak szybko KQL przetwarza setki tysięcy wierszy

### Minuta 12–15: Data Activator — alerty
- Otwórz Reflex `Alert: Anomalie Sieci`
- Pokaż skonfigurowane wyzwalacze i ich progi
- Wskaz alert, który właśnie się wyzwolił (np. przeciążenie stacji)
- Pokaż przykładowe powiadomienie email/Teams
- Podsumuj: end-to-end od danych do akcji w czasie rzeczywistym

---

## Struktura danych

### substation_readings.csv

| Kolumna | Typ | Opis | Zakres |
|---------|-----|------|--------|
| `timestamp` | datetime (ISO 8601) | Czas odczytu w strefie CET | Ostatnie 24h |
| `substation_id` | string | Identyfikator stacji (SUB-001..SUB-050) | 50 wartości |
| `substation_name` | string | Nazwa stacji | np. "Warszawa Mory" |
| `latitude` | float | Szerokość geograficzna | 49.6–54.5 |
| `longitude` | float | Długość geograficzna | 14.2–23.3 |
| `voltage_kv` | float | Napięcie (kV) | ~220 ± 15 |
| `current_a` | float | Natężenie prądu (A) | 100–800 |
| `frequency_hz` | float | Częstotliwość sieci (Hz) | 49.8–50.2 |
| `power_factor` | float | Współczynnik mocy (cos φ) | 0.85–0.99 |
| `active_power_mw` | float | Moc czynna (MW) | 10–250 |
| `reactive_power_mvar` | float | Moc bierna (MVAR) | 5–50 |
| `load_pct` | float | Obciążenie transformatora (%) | 20–100 |
| `transformer_temp_c` | float | Temperatura transformatora (°C) | 30–90 |
| `ambient_temp_c` | float | Temperatura otoczenia (°C) | Zależna od pory roku |
| `status` | string | Stan stacji | Normal / Warning / Critical |

### substation_events.csv

| Kolumna | Typ | Opis |
|---------|-----|------|
| `timestamp` | datetime | Czas zdarzenia |
| `event_id` | string | Unikalny identyfikator zdarzenia |
| `substation_id` | string | Identyfikator stacji |
| `substation_name` | string | Nazwa stacji |
| `event_type` | string | Typ: Voltage_Sag, Voltage_Swell, Frequency_Deviation, Overload, Transformer_Overheat, Breaker_Trip |
| `severity` | string | Ważność: Low / Medium / High / Critical |
| `description` | string | Opis zdarzenia |
| `resolved` | bool | Czy zdarzenie zostało rozwiązane |
| `resolution_time_min` | float | Czas rozwiązania (minuty) |

### substations_master.csv

| Kolumna | Typ | Opis |
|---------|-----|------|
| `substation_id` | string | Identyfikator stacji |
| `name` | string | Nazwa stacji |
| `latitude` | float | Szerokość geograficzna |
| `longitude` | float | Długość geograficzna |
| `voltage_level_kv` | int | Poziom napięcia (110/220/400 kV) |
| `region` | string | Województwo |
| `commissioned_year` | int | Rok uruchomienia |
| `capacity_mva` | int | Moc znamionowa (MVA) |
| `transformer_count` | int | Liczba transformatorów |

---

## Wymagania

- **Microsoft Fabric** workspace z aktywną licencją (Trial lub F64+)
- **Python 3.10+** z bibliotekami: `random`, `csv`, `math` (standardowe)
- Przeglądarka z dostępem do portalu Fabric (app.fabric.microsoft.com)

## Pliki w scenariuszu

```
scenario-01-realtime-grid-monitoring/
├── README.md                          # Ten plik
├── generate_grid_data.py              # Generator danych telemetrycznych
├── config/
│   └── eventstream_schema.json        # Schemat wiadomości Eventstream
├── data/                              # Generowane pliki CSV (w .gitignore)
│   ├── substation_readings.csv
│   ├── substation_events.csv
│   └── substations_master.csv
├── kql/
│   ├── create_tables.kql              # Tworzenie tabel w KQL Database
│   ├── dashboard_queries.kql          # Zapytania do Real-Time Dashboard
│   └── activator_rules.kql           # Reguły Data Activator
└── notebooks/
    └── grid_analysis.ipynb            # Notebook do analizy danych
```
