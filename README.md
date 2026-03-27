# ⚡ Microsoft Fabric — Platforma Demo dla Firm Energetycznych

> **Kompleksowa platforma demonstracyjna** prezentująca możliwości Microsoft Fabric
> jako zunifikowanego, opartego na AI systemu analitycznego dla sektora energetycznego.
> Od monitoringu sieci w czasie rzeczywistym, przez analizę rynku energii,
> po predykcyjne utrzymanie ruchu i prognozowanie popytu.

---

## 📋 Spis treści

- [Przegląd](#-przegląd)
- [Architektura](#-architektura)
- [Scenariusze demonstracyjne](#-scenariusze-demonstracyjne)
- [Wymagania wstępne](#-wymagania-wstępne)
- [Szybki start](#-szybki-start)
- [Struktura repozytorium](#-struktura-repozytorium)
- [Opisy scenariuszy](#-opisy-scenariuszy)
- [Komponenty Microsoft Fabric](#-komponenty-microsoft-fabric)
- [Licencja](#-licencja)

---

## 🔍 Przegląd

Niniejsze repozytorium zawiera **cztery scenariusze demonstracyjne**, które pokazują,
jak Microsoft Fabric może służyć jako kompleksowa platforma analityczna dla branży
energetycznej. Każdy scenariusz wykorzystuje inne możliwości platformy Fabric:

- **Real-Time Intelligence (RTI)** — strumieniowe przetwarzanie danych telemetrycznych
  z sieci elektroenergetycznej i inteligentnych liczników
- **Fabric Data Agents** — zapytania w języku naturalnym do danych rynkowych,
  kontraktów i miksu energetycznego
- **Operations Agents** — agenci AI do predykcyjnego utrzymania ruchu turbin wiatrowych
- **OneLake** — zunifikowana warstwa danych łącząca wszystkie scenariusze

Dane demonstracyjne opierają się na realistycznych parametrach **Krajowego Systemu
Elektroenergetycznego (KSE)** — polskie nazwy stacji, farm wiatrowych, stref rynkowych
i segmentów klientów.

---

## 🏗 Architektura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Microsoft Fabric Workspace                        │
│                                                                            │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌────────────┐  │
│  │  Eventstream  │   │  Eventstream  │   │  Eventstream  │   │ Eventstream│  │
│  │  (Sieć 50    │   │              │   │  (Turbiny    │   │ (Liczniki  │  │
│  │  stacji)     │   │              │   │   200 szt.)  │   │  10k)      │  │
│  └──────┬───────┘   │              │   └──────┬───────┘   └─────┬──────┘  │
│         │           │              │          │                  │         │
│         ▼           │              │          ▼                  ▼         │
│  ┌──────────────┐   │              │   ┌──────────────┐   ┌────────────┐  │
│  │  Eventhouse   │   │              │   │  Eventhouse   │   │ Eventhouse │  │
│  │  / KQL DB     │   │              │   │  / KQL DB     │   │ / KQL DB   │  │
│  └──────┬───────┘   │              │   └──────┬───────┘   └─────┬──────┘  │
│         │           │              │          │                  │         │
│         ▼           │              │          ▼                  ▼         │
│  ┌──────────────┐   │              │   ┌──────────────┐   ┌────────────┐  │
│  │  Real-Time    │   │              │   │  Operations   │   │ Real-Time  │  │
│  │  Dashboard    │   │              │   │  Agent (AI)   │   │ Dashboard  │  │
│  └──────┬───────┘   │              │   └──────────────┘   └────────────┘  │
│         │           │              │                                       │
│         ▼           │              │                                       │
│  ┌──────────────┐   │              │          ┌──────────────────────┐     │
│  │  Data         │   │              │          │                      │     │
│  │  Activator    │   │              │          │      OneLake         │     │
│  │  (Alerty)     │   │              │          │  (Delta / Parquet)   │     │
│  └──────────────┘   │              │          │                      │     │
│                     │              │          └──────────┬───────────┘     │
│                     │              │                     │                 │
│                     ▼              ▼                     ▼                 │
│              ┌──────────────────────────────────────────────────┐          │
│              │                  Lakehouse                       │          │
│              │  (Ceny spot, Kontrakty, Miks energetyczny,      │          │
│              │   Emisje CO₂, Dane pogodowe, Dane liczników)    │          │
│              └──────────────────────┬──────────────────────────┘          │
│                                     │                                     │
│                                     ▼                                     │
│              ┌──────────────────────────────────────────────────┐          │
│              │              Fabric Data Agent                   │          │
│              │  (Zapytania w języku naturalnym do danych        │          │
│              │   rynkowych, prognoz popytu i optymalizacji)     │          │
│              └──────────────────────────────────────────────────┘          │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Notebooks (Python)                          │   │
│  │  Generatory danych  │  Transformacje  │  Wizualizacje  │  ML/AI    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
│  Scenariusz 01          Scenariusz 02      Scenariusz 03   Scenariusz 04  │
│  Monitoring sieci       Analityka rynku    Predykcyjne     Prognozowanie  │
│  w czasie rzecz.        energii            utrzymanie       popytu         │
│                                            ruchu                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Scenariusze demonstracyjne

| # | Scenariusz | Komponenty Fabric | Możliwości AI | Czas demo |
|---|-----------|-------------------|---------------|-----------|
| 01 | ⚡ [Monitoring sieci w czasie rzeczywistym (RTI)](#scenariusz-01) | Eventstream, Eventhouse / KQL DB, Real-Time Dashboard, Data Activator | Alerty anomalii napięcia i częstotliwości w czasie rzeczywistym | ~15 min |
| 02 | 📈 [Analityka rynku energii (Data Agents)](#scenariusz-02) | Lakehouse, Fabric Data Agent, Notebooks, Data Pipelines | Zapytania w języku naturalnym do danych rynkowych i emisji CO₂ | ~15 min |
| 03 | 🔧 [Predykcyjne utrzymanie ruchu (Operations Agents)](#scenariusz-03) | Eventhouse / KQL DB, Operations Agent, Lakehouse, Notebooks | Predykcja awarii turbin, automatyczne zlecenia serwisowe | ~15 min |
| 04 | 🔋 [Prognozowanie popytu (RTI + Data Agents)](#scenariusz-04) | Eventstream, Eventhouse / KQL DB, Real-Time Dashboard, Data Agent, Lakehouse | Strumieniowe dane liczników + zapytania NL o prognozy i optymalizację | ~15 min |

---

## ✅ Wymagania wstępne

| Wymaganie | Szczegóły |
|-----------|-----------|
| **Microsoft Fabric** | Aktywna pojemność **F64** lub wyższa |
| **Fabric Workspace** | Workspace z uprawnieniami do tworzenia zasobów (Eventstream, Lakehouse, KQL DB, Data Agent) |
| **Python** | Wersja **3.11** lub nowsza |
| **Biblioteki Python** | `pandas`, `numpy`, `azure-eventhub`, `azure-identity` (patrz `requirements.txt`) |
| **Dostęp sieciowy** | Połączenie z usługami Microsoft Fabric i Azure Event Hubs |

---

## 🚀 Szybki start

### 1. Klonowanie repozytorium

```bash
git clone https://github.com/your-org/energy-market-demo.git
cd energy-market-demo
pip install -r requirements.txt
```

### 2. Konfiguracja środowiska

Skopiuj plik konfiguracyjny i uzupełnij dane połączeniowe:

```bash
# Dla każdego scenariusza skopiuj szablon konfiguracji
cp scenario-01-realtime-grid-monitoring/config/config.template.json \
   scenario-01-realtime-grid-monitoring/config/config.json
```

Uzupełnij w pliku `config.json`:
- **Event Hub connection string** (dla scenariuszy RTI)
- **Fabric workspace ID**
- **Lakehouse / KQL Database endpoints**

### 3. Generowanie danych demonstracyjnych

```bash
# Scenariusz 01 — dane telemetryczne z 50 stacji transformatorowych
python scenario-01-realtime-grid-monitoring/notebooks/generate_grid_data.py

# Scenariusz 02 — dane rynkowe za ostatnie 2 lata
python scenario-02-energy-market-analytics/notebooks/generate_market_data.py

# Scenariusz 03 — dane sensoryczne z 200 turbin wiatrowych
python scenario-03-predictive-maintenance/notebooks/generate_turbine_data.py

# Scenariusz 04 — dane z 10 000 inteligentnych liczników
python scenario-04-demand-forecasting/notebooks/generate_meter_data.py
```

### 4. Konfiguracja w Microsoft Fabric

Szczegółowe instrukcje konfiguracji dla każdego scenariusza znajdują się
w odpowiednich plikach README:

1. Utwórz **Workspace** w Microsoft Fabric
2. Skonfiguruj **Eventhouse** i **KQL Database** (scenariusze 01, 03, 04)
3. Skonfiguruj **Lakehouse** i zaimportuj dane (scenariusze 02, 03, 04)
4. Utwórz **Eventstream** i podłącz źródła danych
5. Skonfiguruj **Data Agent** / **Operations Agent**
6. Zaimportuj dashboardy i zapytania KQL

---

## 📁 Struktura repozytorium

```
energy-market-demo/
│
├── README.md                              # Ten plik
├── requirements.txt                       # Zależności Python
├── .gitignore
│
├── shared/                                # Współdzielone moduły
│   ├── __init__.py
│   ├── utils.py                           # Generowanie znaczników czasu (CET)
│   ├── constants.py                       # Stałe: stacje, turbiny, strefy
│   └── generators.py                      # Generatory danych (szum, pogoda, etc.)
│
├── scenario-01-realtime-grid-monitoring/  # ⚡ RTI — Monitoring sieci
│   ├── config/                            #    Konfiguracja połączeń
│   ├── data/                              #    Wygenerowane dane CSV/Parquet
│   ├── kql/                               #    Zapytania KQL
│   └── notebooks/                         #    Notebooki i generatory danych
│
├── scenario-02-energy-market-analytics/   # 📈 Data Agent — Rynek energii
│   ├── config/
│   ├── data/
│   ├── kql/
│   └── notebooks/
│
├── scenario-03-predictive-maintenance/    # 🔧 Operations Agent — Utrzymanie ruchu
│   ├── config/
│   ├── data/
│   ├── kql/
│   └── notebooks/
│
└── scenario-04-demand-forecasting/        # 🔋 RTI + Data Agent — Prognozowanie
    ├── config/
    ├── data/
    ├── kql/
    └── notebooks/
```

---

## 📖 Opisy scenariuszy

### ⚡ Scenariusz 01 — Monitoring sieci w czasie rzeczywistym (RTI) {#scenariusz-01}

Monitorowanie 50 stacji transformatorowych Krajowego Systemu Elektroenergetycznego
w czasie rzeczywistym. Dane telemetryczne — napięcie, prąd, częstotliwość, współczynnik
mocy, temperatura — strumieniowane przez Eventstream do bazy KQL. Real-Time Dashboard
wizualizuje stan sieci, a Data Activator generuje alerty przy przekroczeniu progów.

📂 [Przejdź do scenariusza →](scenario-01-realtime-grid-monitoring/)

### 📈 Scenariusz 02 — Analityka rynku energii (Fabric Data Agents) {#scenariusz-02}

Analiza danych rynku energii za ostatnie 2 lata: ceny spot (TGE), kontrakty bilateralne,
miks energetyczny (wiatr, słońce, gaz, węgiel), emisje CO₂. Dane przechowywane
w Lakehouse, a Fabric Data Agent umożliwia zadawanie pytań w języku naturalnym —
np. „Jaka była średnia cena energii w styczniu?" lub „Pokaż korelację cen z udziałem OZE".

📂 [Przejdź do scenariusza →](scenario-02-energy-market-analytics/)

### 🔧 Scenariusz 03 — Predykcyjne utrzymanie ruchu (Operations Agents) {#scenariusz-03}

Telemetria z 200 turbin wiatrowych rozmieszczonych na 5 farmach w Polsce. Analiza
wibracji, temperatury łożysk, wydajności generatora i predykcja awarii na podstawie
wzorców degradacji. Operations Agent automatycznie tworzy zlecenia serwisowe
i rekomenduje priorytety napraw.

📂 [Przejdź do scenariusza →](scenario-03-predictive-maintenance/)

### 🔋 Scenariusz 04 — Prognozowanie popytu (RTI + Data Agents) {#scenariusz-04}

Strumieniowe dane z 10 000 inteligentnych liczników połączone z danymi pogodowymi.
Real-Time Intelligence przetwarza odczyty w czasie rzeczywistym, a Data Agent
odpowiada na pytania o prognozy zapotrzebowania i rekomendacje optymalizacji obciążenia
— np. „Jakie będzie zapotrzebowanie jutro o 18:00?" lub „Które segmenty klientów
generują szczytowe obciążenie?".

📂 [Przejdź do scenariusza →](scenario-04-demand-forecasting/)

---

## 🧩 Komponenty Microsoft Fabric

| Komponent | Opis | Użycie w scenariuszach |
|-----------|------|----------------------|
| **Eventstream** | Strumieniowe pozyskiwanie danych w czasie rzeczywistym | 01, 03, 04 |
| **Eventhouse / KQL DB** | Baza danych do analityki czasu rzeczywistego (Kusto) | 01, 03, 04 |
| **Real-Time Dashboard** | Dashboardy oparte na zapytaniach KQL z auto-odświeżaniem | 01, 04 |
| **Data Activator** | Automatyczne alerty i akcje na strumieniu danych | 01 |
| **Lakehouse** | Magazyn danych (Delta Lake) na dane historyczne | 02, 03, 04 |
| **Data Warehouse** | Relacyjny magazyn danych dla raportów analitycznych | 02 |
| **Fabric Data Agent** | Agent AI do zapytań w języku naturalnym | 02, 04 |
| **Operations Agent** | Agent AI do operacji i automatyzacji zadań | 03 |
| **Notebooks** | Notebooki PySpark / Python do transformacji i ML | 01, 02, 03, 04 |
| **Data Pipelines** | Orkiestracja przepływów danych (ETL/ELT) | 02, 04 |

---

## 📄 Licencja

Ten projekt jest udostępniony na licencji [MIT](LICENSE).

```
MIT License

Copyright (c) 2024 Microsoft Fabric Energy Demo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

<p align="center">
  <strong>⚡ Zbudowano z Microsoft Fabric | Demo dla sektora energetycznego ⚡</strong>
</p>
