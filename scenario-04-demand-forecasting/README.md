# Scenariusz 04: Prognozowanie Popytu i Optymalizacja Obciążenia

## Opis

Scenariusz demonstruje połączenie **Real-Time Intelligence (RTI)** z **Data Agent** do kompleksowego prognozowania popytu na energię elektryczną i optymalizacji obciążenia sieci.

Symulujemy infrastrukturę **10 000 inteligentnych liczników** (smart meters), z których dane strumieniowe płyną przez Eventstream do Eventhouse w czasie rzeczywistym. Jednocześnie Lakehouse przechowuje dane historyczne o zużyciu, pogodzie i taryfach.

**Data Agent** — „Prognosta Popytu" — łączy oba źródła danych i umożliwia analitykom zadawanie pytań w języku naturalnym o prognozach popytu, wzorcach zużycia, korelacjach pogodowych oraz rekomendacjach optymalizacji obciążenia.

Kluczowe elementy:
- **5 000 liczników** generuje odczyty co 15 minut (480 000 rekordów/24h)
- **Roczne dane historyczne** dla 10 regionów z danymi pogodowymi
- **Strefy taryfowe** zgodne z polskimi grupami taryfowymi (G11, G12, G13, C11, C21, B11, B21, A23)
- **Segmenty klientów**: gospodarstwa domowe, przemysł, usługi, rolnictwo, sektor publiczny

## Architektura

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────────┐
│  Smart Meter     │────▶│  Eventstream │────▶│  Eventhouse          │
│  Generator       │     │  ES_Smart    │     │  EH_SmartMeters      │
│  (5000 meters)   │     │  Meters      │     │  (real-time readings)│
└─────────────────┘     └──────┬───────┘     └──────────┬──────────┘
                               │                         │
                               │                         │
                               ▼                         ▼
                        ┌──────────────┐     ┌─────────────────────┐
                        │  Lakehouse   │     │  Real-Time           │
                        │  LH_Demand   │     │  Dashboard           │
                        │  Analytics   │     │  (live monitoring)   │
                        │  (historical)│     └──────────┬──────────┘
                        └──────┬───────┘                │
                               │                         │
                               ▼                         ▼
                        ┌──────────────┐     ┌─────────────────────┐
                        │  Data Agent  │     │  Data Activator      │
                        │  "Prognosta  │     │  (peak alerts)       │
                        │   Popytu"    │     └─────────────────────┘
                        └──────────────┘
```

**Przepływ danych:**
1. Generator smart meterów produkuje odczyty co 15 minut
2. Eventstream routuje dane do Eventhouse (analiza real-time) i Lakehouse (archiwizacja)
3. Real-Time Dashboard wizualizuje bieżące obciążenie sieci
4. Data Agent łączy dane real-time z historycznymi do prognozowania
5. Data Activator monitoruje progi i generuje alerty

## Komponenty Microsoft Fabric

| Komponent | Nazwa | Rola |
|-----------|-------|------|
| **Eventstream** | ES_SmartMeters | Ingestion danych strumieniowych z liczników |
| **Eventhouse** | EH_SmartMeters | Baza KQL do analiz real-time |
| **Lakehouse** | LH_DemandAnalytics | Przechowywanie danych historycznych (Delta) |
| **Data Agent** | Agent: Prognosta Popytu | NLP do prognozowania i analizy popytu |
| **Real-Time Dashboard** | Demand Monitoring | Wizualizacja bieżącego obciążenia sieci |
| **Data Activator** | Peak Load Alerts | Alerty o szczytowym obciążeniu |

## Krok po kroku

### Krok 1: Przygotowanie danych

Uruchom skrypt generujący dane:

```bash
cd scenario-04-demand-forecasting
python generate_demand_data.py
```

Skrypt wygeneruje następujące pliki:

| Plik | Opis | Wierszy |
|------|------|---------|
| `data/smart_meter_readings.csv` | Odczyty 5 000 liczników (24h, co 15 min) | ~480 000 |
| `data/historical_consumption.csv` | Roczne zużycie wg regionów (godzinowe) | ~87 600 |
| `data/weather_data.csv` | Dane pogodowe z 10 stacji (rok, godzinowe) | ~87 600 |
| `data/tariff_schedule.csv` | Polskie grupy taryfowe | ~50 |
| `data/demand_forecasts.csv` | Prognozy vs rzeczywistość (rok) | ~8 760 |

### Krok 2: Konfiguracja Eventhouse (RTI)

1. W Microsoft Fabric utwórz **Eventhouse** o nazwie `EH_SmartMeters`
2. Utwórz bazę KQL o nazwie `MeterReadings`
3. Wykonaj skrypt `kql/create_tables.kql`, aby utworzyć tabelę `SmartMeterReadings` z mapowaniem ingestion
4. Włącz streaming ingestion w ustawieniach bazy

```kql
// Weryfikacja tabeli
SmartMeterReadings
| take 10
```

### Krok 3: Konfiguracja Eventstream

1. Utwórz **Eventstream** o nazwie `ES_SmartMeters`
2. Skonfiguruj źródło:
   - **Custom App** — do podłączenia generatora w czasie rzeczywistym
   - Alternatywnie: **CSV upload** z pliku `smart_meter_readings.csv`
3. Dodaj dwa cele (destinations):
   - **KQL Database** → `EH_SmartMeters / MeterReadings / SmartMeterReadings` (analiza real-time)
   - **Lakehouse** → `LH_DemandAnalytics / Tables / meter_readings` (archiwizacja)
4. Skonfiguruj format danych: JSON, mapowanie zgodne z `config/eventstream_schema.json`

### Krok 4: Lakehouse z danymi historycznymi

1. Utwórz **Lakehouse** o nazwie `LH_DemandAnalytics`
2. Prześlij pliki CSV do katalogu `Files/`:
   - `historical_consumption.csv`
   - `weather_data.csv`
   - `tariff_schedule.csv`
   - `demand_forecasts.csv`
3. Utwórz tabele Delta:

```sql
-- W notebooku Spark
CREATE TABLE historical_consumption
USING DELTA
AS SELECT * FROM csv.`Files/historical_consumption.csv`;

CREATE TABLE weather_data
USING DELTA
AS SELECT * FROM csv.`Files/weather_data.csv`;

CREATE TABLE tariff_schedule
USING DELTA
AS SELECT * FROM csv.`Files/tariff_schedule.csv`;

CREATE TABLE demand_forecasts
USING DELTA
AS SELECT * FROM csv.`Files/demand_forecasts.csv`;
```

### Krok 5: Data Agent — „Prognosta Popytu"

1. Utwórz **Data Agent** o nazwie `Agent: Prognosta Popytu`
2. Podłącz źródła danych:
   - **Eventhouse**: `EH_SmartMeters` → tabela `SmartMeterReadings`
   - **Lakehouse**: `LH_DemandAnalytics` → tabele `historical_consumption`, `weather_data`, `tariff_schedule`, `demand_forecasts`
3. W zakładce **Instructions** wklej zawartość `config/agent_instructions.md`
4. Przetestuj przykładowe pytania z `config/example_queries.md`

Agent potrafi odpowiedzieć na pytania takie jak:
- *„Jaki jest prognozowany popyt na jutro?"*
- *„Jakie segmenty klientów zwiększyły zużycie w porównaniu z zeszłym rokiem?"*
- *„Jaki wpływ ma temperatura na zużycie energii w regionie mazowieckim?"*
- *„Kiedy występują godziny szczytu i jak możemy zoptymalizować obciążenie?"*

### Krok 6: Real-Time Dashboard

Utwórz **Real-Time Dashboard** z następującymi kafelkami:

| Kafelek | Typ wizualizacji | Opis |
|---------|------------------|------|
| **Bieżące obciążenie sieci** | Gauge / KPI | Sumaryczna moc [MW] z ostatnich 5 minut |
| **Zużycie wg segmentów** | Pie chart | Podział bieżącego zużycia na segmenty klientów |
| **Mapa ciepła regionów** | Heatmap / Map | Intensywność zużycia w poszczególnych regionach |
| **Pogoda vs zużycie** | Dual-axis line | Nakładka temperatury na trend zużycia |
| **Trend 24h wg segmentu** | Stacked area | Historia zużycia w podziale na segmenty |
| **Alerty progowe** | Table / Card | Przekroczenia progu szczytowego obciążenia |

Zapytania KQL dla dashboardu znajdują się w `kql/dashboard_queries.kql`.

### Krok 7: Data Activator

Skonfiguruj **Data Activator** z następującymi regułami:

1. **Alert szczytowego obciążenia**:
   - Źródło: Eventhouse → `SmartMeterReadings`
   - Warunek: Sumaryczna moc w oknie 5 min > 85% pojemności sieci
   - Akcja: Powiadomienie Teams + email do dyspozytora

2. **Wykrywanie anomalii zużycia**:
   - Źródło: Eventhouse → `SmartMeterReadings`
   - Warunek: Odczyt z licznika > 3× średniej z ostatnich 7 dni
   - Akcja: Powiadomienie Teams

3. **Prognozy vs rzeczywistość**:
   - Źródło: Lakehouse → `demand_forecasts`
   - Warunek: Błąd prognozy > 10%
   - Akcja: Powiadomienie o konieczności rekalibracji modelu

## Scenariusz demo (15 minut)

### Minuta 0–2: Wprowadzenie
- Przedstaw architekturę na diagramie
- Wyjaśnij przepływ danych: 10 000 liczników → Eventstream → Eventhouse + Lakehouse → Data Agent
- Podkreśl połączenie real-time z analizą historyczną

### Minuta 2–5: Real-Time Dashboard
- Pokaż dashboard z danymi napływającymi na żywo
- Omów bieżące obciążenie sieci, podział na segmenty
- Wskaż wzorce dzienne (szczyt poranny i wieczorny)
- Pokaż mapę ciepła regionów

### Minuta 5–8: Zapytania KQL
- Wykonaj zapytanie: bieżące zużycie sieci w MW
- Porównaj zużycie między segmentami
- Znajdź liczniki z anomalnym zużyciem
- Pokaż korelację pogody z popytem

### Minuta 8–12: Data Agent — analiza popytu
- Zadaj: *„Jaki jest aktualny popyt na energię i jak wypada w porównaniu z prognozą?"*
- Zadaj: *„Które regiony mają najwyższe zużycie i dlaczego?"*
- Zadaj: *„Jaki jest prognozowany popyt na jutro, biorąc pod uwagę prognozę pogody?"*
- Zadaj: *„Jakie rekomendacje masz dla optymalizacji obciążenia w godzinach szczytu?"*
- Pokaż, jak agent łączy dane real-time z historycznymi

### Minuta 12–15: Data Activator i optymalizacja
- Pokaż alert o zbliżaniu się do szczytu
- Omów strategie optymalizacji obciążenia (DSR, przesuwanie ładunków)
- Podsumuj wartość biznesową: redukcja kosztów szczytowych o 15–20%

## Przykładowe pytania do Data Agenta

### Prognoza popytu
- „Jaki jest prognozowany popyt na energię na najbliższe 24 godziny?"
- „Jak zmieni się zapotrzebowanie na energię, jeśli temperatura spadnie o 5 stopni?"
- „Porównaj prognozę z rzeczywistym zużyciem w tym tygodniu"

### Analiza zużycia
- „Jakie jest aktualne zużycie energii w podziale na segmenty klientów?"
- „Które regiony mają najwyższe zużycie per capita?"
- „Jak zmieniło się zużycie przemysłowe w porównaniu z zeszłym miesiącem?"

### Wpływ pogody
- „Jaka jest korelacja między temperaturą a zużyciem energii?"
- „Jak wiatr wpływa na zapotrzebowanie w regionach nadmorskich?"
- „Pokaż zużycie w dniach z temperaturą powyżej 30°C vs poniżej 0°C"

### Optymalizacja obciążenia
- „Kiedy występują godziny szczytu i jaka jest ich intensywność?"
- „Jakie segmenty klientów mogą uczestniczyć w programie DSR?"
- „O ile możemy zredukować szczyt, przesuwając ładunki przemysłowe?"

### Analiza taryfowa
- „Które grupy taryfowe generują największy przychód?"
- „Jak rozkłada się zużycie między taryfą G11 a G12?"
- „Jaki wpływ miałaby zmiana taryfy na profil obciążenia?"

## Struktura danych

### SmartMeterReadings (Eventhouse — real-time)

| Kolumna | Typ | Opis |
|---------|-----|------|
| `timestamp` | datetime | Czas odczytu (CET) |
| `meter_id` | string | Identyfikator licznika (SM-XXXXX) |
| `customer_id` | string | Identyfikator klienta |
| `customer_segment` | string | Segment: Household / Industry / Services / Agriculture / Public |
| `region` | string | Region (np. Mazowieckie, Śląskie) |
| `tariff_zone` | string | Strefa taryfowa (G11, G12, G13, C11, C21, B11, B21, A23) |
| `active_energy_kwh` | real | Energia czynna [kWh] |
| `reactive_energy_kvarh` | real | Energia bierna [kvarh] |
| `voltage_v` | real | Napięcie [V] |
| `current_a` | real | Natężenie prądu [A] |
| `power_factor` | real | Współczynnik mocy (cos φ) |
| `power_kw` | real | Moc czynna [kW] |
| `daily_max_kw` | real | Moc maksymalna dobowa [kW] |

### historical_consumption (Lakehouse)

| Kolumna | Typ | Opis |
|---------|-----|------|
| `date` | date | Data |
| `hour` | int | Godzina (0–23) |
| `region` | string | Region |
| `segment` | string | Segment klienta |
| `total_consumption_mwh` | decimal | Łączne zużycie [MWh] |
| `customer_count` | int | Liczba klientów |
| `avg_consumption_kwh` | decimal | Średnie zużycie [kWh] |
| `peak_demand_mw` | decimal | Moc szczytowa [MW] |
| `temperature_c` | decimal | Temperatura [°C] |
| `wind_speed_ms` | decimal | Prędkość wiatru [m/s] |
| `cloud_cover_pct` | decimal | Zachmurzenie [%] |
| `is_holiday` | bool | Czy dzień świąteczny |
| `is_weekend` | bool | Czy weekend |
| `day_of_week` | int | Dzień tygodnia (0=pon, 6=ndz) |

### weather_data (Lakehouse)

| Kolumna | Typ | Opis |
|---------|-----|------|
| `timestamp` | datetime | Czas pomiaru |
| `station_name` | string | Nazwa stacji meteorologicznej |
| `region` | string | Region |
| `temperature_c` | decimal | Temperatura [°C] |
| `feels_like_c` | decimal | Temperatura odczuwalna [°C] |
| `humidity_pct` | decimal | Wilgotność [%] |
| `wind_speed_ms` | decimal | Prędkość wiatru [m/s] |
| `wind_direction_deg` | int | Kierunek wiatru [°] |
| `pressure_hpa` | decimal | Ciśnienie atmosferyczne [hPa] |
| `cloud_cover_pct` | decimal | Zachmurzenie [%] |
| `precipitation_mm` | decimal | Opady [mm] |
| `solar_radiation_wm2` | decimal | Promieniowanie słoneczne [W/m²] |

### tariff_schedule (Lakehouse)

| Kolumna | Typ | Opis |
|---------|-----|------|
| `tariff_code` | string | Kod taryfy (G11, G12, …) |
| `tariff_name` | string | Pełna nazwa taryfy |
| `segment` | string | Segment klienta |
| `energy_rate_pln_kwh` | decimal | Stawka za energię [PLN/kWh] |
| `fixed_charge_pln_month` | decimal | Opłata stała [PLN/mies.] |
| `peak_hours` | string | Godziny szczytu |
| `off_peak_hours` | string | Godziny pozaszczytowe |
| `description` | string | Opis taryfy |

### demand_forecasts (Lakehouse)

| Kolumna | Typ | Opis |
|---------|-----|------|
| `timestamp` | datetime | Czas prognozy |
| `hour` | int | Godzina |
| `forecasted_demand_mw` | decimal | Prognozowany popyt [MW] |
| `actual_demand_mw` | decimal | Rzeczywisty popyt [MW] |
| `forecast_error_pct` | decimal | Błąd prognozy [%] |
| `temperature_forecast_c` | decimal | Prognozowana temperatura [°C] |
| `is_peak_hour` | bool | Czy godzina szczytu |
| `region` | string | Region |

## Wymagania

- Python 3.10+
- Biblioteki: `pandas`, `numpy`
- Microsoft Fabric workspace z licencją F64+
- Uprawnienia do tworzenia Eventhouse, Lakehouse, Data Agent
