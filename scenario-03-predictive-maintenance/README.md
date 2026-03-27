# Scenariusz 03: Predykcyjne Utrzymanie Ruchu Farm Wiatrowych

## Opis scenariusza

Scenariusz demonstruje zastosowanie **Microsoft Fabric Operations Agents** do predykcyjnego utrzymania ruchu floty **200 turbin wiatrowych** rozmieszczonych na **5 farmach wiatrowych w Polsce**.

System zbiera dane telemetryczne z czujników (wibracje, temperatura, obroty, kąt łopat, moc wyjściowa), analizuje je za pomocą modelu ML predykcji awarii, a następnie automatycznie tworzy zlecenia pracy poprzez Operations Agent.

### Kluczowe elementy:
- **Telemetria sensorowa** — wibracje, temperatura przekładni/łożysk/generatora, obroty wirnika, kąt natarcia łopat, moc wyjściowa
- **Predykcja awarii (AI/ML)** — model klasyfikacyjny przewidujący awarie w horyzoncie 7 dni
- **Automatyczne zlecenia pracy** — Operations Agent tworzy i priorytetyzuje zlecenia na podstawie predykcji
- **Analiza degradacji** — trendy zużycia komponentów w czasie

### Farmy wiatrowe:
| Farma | Region | Liczba turbin |
|-------|--------|---------------|
| Darłowo Wind Park | Zachodniopomorskie | 45 |
| Potęgowo Wind Farm | Pomorskie | 40 |
| Korsze Wind Complex | Warmińsko-Mazurskie | 35 |
| Przykona Wind Park | Wielkopolskie | 42 |
| Zagórz Wind Farm | Podkarpackie | 38 |

---

## Architektura

```
┌─────────────────────┐
│  Generator danych   │
│  sensorowych        │
│  (generate_         │
│  turbine_data.py)   │
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐     ┌─────────────────────┐
│  Eventhouse          │     │  Lakehouse           │
│  "EH_WindFarms"      │     │  "LH_Maintenance"    │
│  ─────────────────   │     │  ─────────────────   │
│  KQL Database:       │     │  Delta Tables:       │
│  TurbineTelemetry    │     │  - maintenance_hist  │
│  - TurbineSensorData │     │  - work_orders       │
│                      │     │  - turbine_specs     │
│                      │     │  - failure_log       │
└────────┬─────────────┘     └────────┬────────────┘
         │                            │
         └──────────┬─────────────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │  Notebook ML          │
         │  (failure_prediction) │
         │  ──────────────────   │
         │  RandomForest         │
         │  + MLflow Tracking    │
         └──────────┬───────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │  Operations Agent     │
         │  "Inżynier Utrzymania│
         │   Ruchu"              │
         │  ──────────────────   │
         │  • Diagnostyka turbin │
         │  • Rekomendacje       │
         │  • Zlecenia pracy     │
         │  • Priorytetyzacja    │
         └──────────────────────┘
```

---

## Komponenty Microsoft Fabric

| Komponent | Nazwa | Zastosowanie |
|-----------|-------|-------------|
| **Eventhouse** | EH_WindFarms | Dane sensorowe w czasie rzeczywistym (KQL) |
| **KQL Database** | TurbineTelemetry | Tabela TurbineSensorData z danymi z czujników |
| **Lakehouse** | LH_Maintenance | Dane historyczne: konserwacja, zlecenia, specyfikacje |
| **Notebook** | failure_prediction | Model ML predykcji awarii (RandomForest + MLflow) |
| **Operations Agent** | Inżynier Utrzymania Ruchu | Agent AI do diagnostyki i zarządzania zleceniami |

---

## Krok po kroku

### Krok 1: Przygotowanie danych

Uruchom generator danych:

```bash
cd scenario-03-predictive-maintenance
python generate_turbine_data.py
```

Generator utworzy 5 plików w katalogu `data/`:

| Plik | Opis | Wiersze |
|------|------|---------|
| `sensor_telemetry.csv` | Dane telemetryczne z czujników (48h, co 5 min) | ~115 200 |
| `maintenance_history.csv` | Historia konserwacji (2 lata) | ~8 000 |
| `work_orders.csv` | Zlecenia pracy | ~1 500 |
| `turbine_specs.csv` | Specyfikacja turbin | 200 |
| `failure_log.csv` | Rejestr awarii (etykiety dla ML) | ~500 |

### Krok 2: Konfiguracja Eventhouse

1. **Utwórz Eventhouse** — w workspace Fabric wybierz `+ New` → `Eventhouse` → nazwa: `EH_WindFarms`
2. **Utwórz KQL Database** — kliknij `+ Database` → nazwa: `TurbineTelemetry`
3. **Utwórz tabele** — otwórz edytor KQL i wykonaj skrypt z `kql/create_tables.kql`:
   - Tabela `TurbineSensorData` — wszystkie kolumny sensorowe
   - Mapowanie ingestion `TurbineSensorData_CSV_Mapping`
4. **Załaduj dane** — `Get Data` → `Local file` → załaduj `sensor_telemetry.csv` z mapowaniem CSV

### Krok 3: Konfiguracja Lakehouse

1. **Utwórz Lakehouse** — `+ New` → `Lakehouse` → nazwa: `LH_Maintenance`
2. **Załaduj pliki CSV**:
   - `maintenance_history.csv`
   - `work_orders.csv`
   - `turbine_specs.csv`
   - `failure_log.csv`
3. **Utwórz Delta Tables** — dla każdego pliku CSV kliknij PPM → `Load to Tables` → `New table`

### Krok 4: Model ML — Predykcja Awarii

1. **Otwórz notebook** `notebooks/failure_prediction.py` w Fabric
2. **Połącz z danymi** — notebook ładuje dane z Lakehouse i Eventhouse
3. **Inżynieria cech**:
   - Średnie kroczące (rolling averages) wibracji, temperatury
   - Wskaźniki degradacji (tempo zmian parametrów)
   - Statystyki okna czasowego (min, max, odchylenie standardowe)
4. **Trenowanie modelu** RandomForest Classifier:
   - Cel: predykcja awarii w horyzoncie 7 dni
   - Metryki: precision, recall, F1-score, AUC-ROC
5. **Rejestracja modelu** w MLflow
6. **Analiza ważności cech** — wibracje, temperatura łożysk i przekładni jako kluczowe predyktory

### Krok 5: Operations Agent

1. **Utwórz Operations Agent** — `+ New` → `Agent` → nazwa: `Inżynier Utrzymania Ruchu`
2. **Połącz źródła danych**:
   - Eventhouse `EH_WindFarms` → bieżące dane sensorowe
   - Lakehouse `LH_Maintenance` → historia, specyfikacje, zlecenia
3. **Konfiguracja instrukcji agenta** — skopiuj zawartość z `config/agent_instructions.md`
4. **Możliwości agenta**:
   - Diagnostyka stanu turbin na podstawie danych sensorowych
   - Rekomendacje konserwacji z priorytetyzacją
   - Tworzenie zleceń pracy z odpowiednimi parametrami
   - Analiza kosztów i planowanie zasobów

---

## Scenariusz demo (15 minut)

### Minuta 0–2: Przegląd floty
- Pokaż dashboard z KQL (`kql/dashboard_queries.kql`)
- Wizualizacja statusu wszystkich 200 turbin na mapie
- Kluczowe wskaźniki: dostępność floty, łączna moc, turbiny w awarii

### Minuta 2–5: Analiza danych sensorowych (KQL)
- Zapytanie o anomalie wibracji — turbiny z Z-score > 3
- Trend temperatury przekładni — wykrywanie przegrzania
- Krzywa mocy — porównanie rzeczywista vs. teoretyczna
- Identyfikacja turbin z degradacją parametrów

### Minuta 5–8: Model ML — Predykcja Awarii
- Otwórz notebook `failure_prediction.py`
- Pokaż trening modelu i metryki (precision > 0.85)
- Analiza ważności cech — wibracje i temperatura łożysk dominują
- Lista turbin z wysokim ryzykiem awarii w ciągu 7 dni

### Minuta 8–12: Operations Agent
- **Pytanie 1**: *"Które turbiny wymagają pilnej konserwacji?"*
  - Agent analizuje dane sensorowe i predykcje ML
  - Zwraca rankingową listę turbin z opisem problemu
- **Pytanie 2**: *"Utwórz zlecenie pracy dla turbiny T-DRL-023 — wysokie wibracje przekładni"*
  - Agent tworzy zlecenie z priorytetem, szacowanym czasem, potrzebnymi częściami
- **Pytanie 3**: *"Jaki jest koszt konserwacji farmy Darłowo w ostatnim kwartale?"*
  - Agent agreguje dane kosztowe z historii

### Minuta 12–15: Automatyczny przepływ pracy
- Pokaż pełny cykl: dane sensorowe → predykcja → zlecenie pracy
- Porównanie: konserwacja reaktywna vs. predykcyjna
- Szacowane oszczędności: 30–40% redukcja nieplanowanych przestojów

---

## Struktura danych

### sensor_telemetry.csv
| Kolumna | Typ | Opis |
|---------|-----|------|
| timestamp | datetime | Znacznik czasowy (CET) |
| turbine_id | string | ID turbiny (np. T-DRL-001) |
| farm_name | string | Nazwa farmy |
| wind_speed_ms | float | Prędkość wiatru [m/s] |
| rotor_rpm | float | Obroty wirnika [RPM] |
| generator_rpm | float | Obroty generatora [RPM] |
| blade_pitch_deg | float | Kąt natarcia łopat [°] |
| nacelle_direction_deg | float | Kierunek gondoli [°] |
| power_output_kw | float | Moc wyjściowa [kW] |
| gearbox_temp_c | float | Temperatura przekładni [°C] |
| bearing_temp_c | float | Temperatura łożysk [°C] |
| generator_temp_c | float | Temperatura generatora [°C] |
| hydraulic_pressure_bar | float | Ciśnienie hydrauliczne [bar] |
| vibration_mm_s | float | Wibracje [mm/s] |
| oil_viscosity | float | Lepkość oleju |
| ambient_temp_c | float | Temperatura otoczenia [°C] |
| humidity_pct | float | Wilgotność [%] |
| status | string | Status: Operating/Idle/Maintenance/Fault |

### maintenance_history.csv
| Kolumna | Typ | Opis |
|---------|-----|------|
| record_id | string | ID rekordu |
| turbine_id | string | ID turbiny |
| farm_name | string | Nazwa farmy |
| date | date | Data konserwacji |
| maintenance_type | string | Typ: Scheduled/Corrective/Predictive/Emergency |
| component | string | Komponent: Gearbox/Generator/Blade/Bearing/Hydraulic/Electrical/Pitch_System |
| description | string | Opis prac |
| duration_hours | float | Czas trwania [h] |
| cost_pln | float | Koszt [PLN] |
| parts_replaced | string | Wymienione części |
| technician_team | string | Zespół techników |

### work_orders.csv
| Kolumna | Typ | Opis |
|---------|-----|------|
| order_id | string | ID zlecenia |
| created_date | date | Data utworzenia |
| turbine_id | string | ID turbiny |
| farm_name | string | Nazwa farmy |
| priority | string | Priorytet: Low/Medium/High/Critical |
| status | string | Status: Open/In_Progress/Completed/Cancelled |
| type | string | Typ: Preventive/Corrective/Inspection |
| description | string | Opis zlecenia |
| assigned_team | string | Przypisany zespół |
| estimated_hours | float | Szacowany czas [h] |
| actual_hours | float | Rzeczywisty czas [h] |
| parts_needed | string | Potrzebne części |
| completion_date | date | Data zakończenia |

### turbine_specs.csv
| Kolumna | Typ | Opis |
|---------|-----|------|
| turbine_id | string | ID turbiny |
| farm_name | string | Nazwa farmy |
| model | string | Model turbiny |
| rated_power_kw | int | Moc znamionowa [kW] |
| hub_height_m | float | Wysokość piasty [m] |
| rotor_diameter_m | float | Średnica wirnika [m] |
| commissioned_date | date | Data uruchomienia |
| last_major_overhaul | date | Ostatni przegląd generalny |
| total_operating_hours | int | Łączne godziny pracy |
| latitude | float | Szerokość geograficzna |
| longitude | float | Długość geograficzna |

### failure_log.csv
| Kolumna | Typ | Opis |
|---------|-----|------|
| failure_id | string | ID awarii |
| turbine_id | string | ID turbiny |
| farm_name | string | Nazwa farmy |
| failure_date | datetime | Data awarii |
| component | string | Komponent |
| failure_type | string | Typ awarii |
| severity | string | Dotkliwość: Minor/Major/Critical |
| downtime_hours | float | Czas przestoju [h] |
| root_cause | string | Przyczyna główna |
| corrective_action | string | Działanie korygujące |
| cost_pln | float | Koszt [PLN] |

---

## Struktura katalogów

```
scenario-03-predictive-maintenance/
├── README.md
├── generate_turbine_data.py
├── config/
│   ├── agent_instructions.md
│   └── example_queries.md
├── data/
│   ├── sensor_telemetry.csv      (generowany)
│   ├── maintenance_history.csv   (generowany)
│   ├── work_orders.csv           (generowany)
│   ├── turbine_specs.csv         (generowany)
│   └── failure_log.csv           (generowany)
├── kql/
│   ├── create_tables.kql
│   └── dashboard_queries.kql
└── notebooks/
    └── failure_prediction.py
```
