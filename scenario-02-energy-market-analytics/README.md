# Scenariusz 02: Analityka Rynku Energii z Fabric Data Agent

## Opis scenariusza

Ten scenariusz demonstruje wykorzystanie **Microsoft Fabric Data Agent** do analizy danych rynku energii elektrycznej w Polsce. Agent umożliwia zadawanie pytań w języku naturalnym dotyczących:

- **Cen spot** na Rynku Dnia Następnego (RDN) i Rynku Dnia Bieżącego (RDB)
- **Miksu energetycznego** — udział poszczególnych źródeł w generacji
- **Kontraktów bilateralnych** — portfel umów na dostawę energii
- **Emisji CO₂** — intensywność emisji i koszty uprawnień EU ETS
- **Uczestników rynku** — generatorzy, traderzy, dystrybutorzy, odbiorcy przemysłowi

Przykładowe pytania, które można zadać agentowi:

> *„Jaka była średnia cena energii w styczniu?"*
> *„Porównaj udział OZE w miksie energetycznym Q1 vs Q2"*
> *„Który uczestnik rynku ma największy wolumen kontraktów baseload?"*

Dane obejmują **2 lata** historii godzinowej i dziennej — łącznie ponad **40 000 rekordów** w 5 tabelach.

---

## Architektura

```
┌─────────────────┐     ┌──────────┐     ┌───────────────────┐     ┌─────────────────────┐
│  Python          │     │          │     │   Lakehouse       │     │   Fabric Data Agent  │
│  Generator       │────▶│  CSV     │────▶│   (OneLake)       │────▶│   (NL Queries)       │
│  (generate_      │     │  Files   │     │   Delta Tables    │     │                      │
│   market_data.py)│     │          │     │   SQL Endpoint    │     │                      │
└─────────────────┘     └──────────┘     └───────────────────┘     └─────────────────────┘
                                                   │
                                                   ▼
                                          ┌─────────────────┐
                                          │  Power BI Report │
                                          │  (5 stron)       │
                                          └─────────────────┘
```

---

## Komponenty Fabric

| Komponent | Nazwa | Opis |
|-----------|-------|------|
| **Lakehouse** | `LH_EnergyMarket` | Przechowywanie danych w formacie Delta |
| **SQL Analytics Endpoint** | automatyczny | Zapytania T-SQL do danych w Lakehouse |
| **Fabric Data Agent** | `Agent: Analityk Rynku Energii` | Interfejs języka naturalnego do danych |
| **Power BI Report** | `Raport: Rynek Energii` | Wizualizacje i dashboardy |

---

## Krok po kroku

### Krok 1: Przygotowanie danych

Uruchom generator danych:

```bash
cd scenario-02-energy-market-analytics
python generate_market_data.py
```

Generator utworzy 5 plików CSV w katalogu `data/`:

| Plik | Opis | Wiersze | Granulacja |
|------|------|---------|------------|
| `spot_prices.csv` | Ceny spot RDN/RDB | ~17 500 | godzinowa (2 lata) |
| `generation_mix.csv` | Miks energetyczny | ~17 500 | godzinowa (2 lata) |
| `bilateral_contracts.csv` | Kontrakty bilateralne | ~5 000 | per kontrakt |
| `carbon_emissions.csv` | Emisje CO₂ | ~730 | dzienna (2 lata) |
| `market_participants.csv` | Uczestnicy rynku | ~50 | dane słownikowe |

### Krok 2: Utworzenie Lakehouse

1. W Microsoft Fabric workspace utwórz nowy **Lakehouse**:
   - Nazwa: `LH_EnergyMarket`

2. **Załaduj pliki CSV**:
   - W Lakehouse kliknij **Upload → Upload files**
   - Wybierz wszystkie 5 plików CSV z katalogu `data/`
   - Pliki pojawią się w sekcji **Files**

3. **Utwórz tabele Delta** (jeden z dwóch sposobów):

   **Sposób A — Drag & Drop**:
   - Kliknij prawym na plik CSV → **Load to Tables**
   - Powtórz dla każdego pliku

   **Sposób B — Notebook**:
   ```python
   # W Fabric Notebook podłączonym do Lakehouse:
   df = spark.read.option("header", "true").option("inferSchema", "true").csv("Files/spot_prices.csv")
   df.write.mode("overwrite").format("delta").saveAsTable("spot_prices")
   ```

4. **Weryfikacja** — otwórz **SQL Analytics Endpoint** i sprawdź:
   ```sql
   SELECT COUNT(*) FROM spot_prices;
   SELECT COUNT(*) FROM generation_mix;
   SELECT COUNT(*) FROM bilateral_contracts;
   SELECT COUNT(*) FROM carbon_emissions;
   SELECT COUNT(*) FROM market_participants;
   ```

### Krok 3: Konfiguracja Fabric Data Agent

1. W workspace kliknij **+ New item** → **Data Agent** (Preview)

2. Skonfiguruj agenta:
   - **Nazwa**: `Agent: Analityk Rynku Energii`
   - **Źródło danych**: dodaj Lakehouse `LH_EnergyMarket`
   - **Instrukcje**: skopiuj zawartość pliku [`config/agent_instructions.md`](config/agent_instructions.md)
   - **Przykładowe zapytania**: dodaj zapytania z pliku [`config/example_queries.md`](config/example_queries.md)

3. **Testowanie** — w panelu czatu zadaj pytania testowe:
   - *„Ile wyniosła średnia cena energii w ostatnim miesiącu?"*
   - *„Jaki był udział OZE w generacji wczoraj?"*
   - *„Pokaż 10 najdroższych godzin w ostatnim kwartale"*

### Krok 4: Power BI Report

Utwórz raport Power BI podłączony do SQL Analytics Endpoint:

| Strona | Zawartość |
|--------|-----------|
| **Market Overview** | KPI: śr. cena, wolumen, OZE share. Trendy miesięczne. |
| **Generation Mix** | Stacked area chart miksu generacji. Udział OZE w czasie. |
| **Price Trends** | Ceny godzinowe, heatmapa godzina × dzień, rozkład cen. |
| **Carbon Emissions** | Trend emisji dziennych, cena EU ETS, koszt węglowy. |
| **Contract Portfolio** | Rozkład kontraktów wg typu, produktu, statusu. Top uczestnicy. |

---

## Scenariusz demo (15 minut)

| Czas | Temat | Szczegóły |
|------|-------|-----------|
| **0:00–2:00** | Wprowadzenie | Pokaż Lakehouse z danymi — 5 tabel, zakresy dat, liczność |
| **2:00–5:00** | SQL Endpoint | Uruchom kilka zapytań z `kql/lakehouse_queries.sql` |
| **5:00–10:00** | Data Agent | Zadaj pytania w języku naturalnym — agent generuje SQL i odpowiedzi |
| **10:00–13:00** | Power BI | Pokaż raport z drill-through między stronami |
| **13:00–15:00** | Zaawansowane | Pokaż jak Agent łączy dane z wielu tabel (np. korelacja cena–generacja) |

### Przebieg demo Data Agent

1. Zacznij od prostego pytania: *„Jaka jest średnia cena energii w tym miesiącu?"*
2. Przejdź do porównań: *„Porównaj ceny w Q1 i Q2 tego roku"*
3. Zapytaj o generację: *„Jaki jest udział wiatru w miksie energetycznym?"*
4. Połącz dane: *„Czy istnieje korelacja między ceną spot a udziałem OZE?"*
5. Analiza kontraktów: *„Który trader ma największy portfel kontraktów baseload?"*
6. Emisje: *„Jak zmieniała się intensywność emisji CO₂ w ostatnim roku?"*

---

## Przykładowe pytania do Data Agenta

### Ceny rynkowe
1. Jaka była średnia cena energii na RDN w styczniu 2024?
2. Pokaż 10 najdroższych godzin w ostatnim kwartale
3. Jaka jest różnica cenowa między szczytem a doliną w dni robocze?
4. Jak zmieniała się cena spot miesiąc do miesiąca?
5. Ile wyniosła maksymalna cena w ostatnim roku?

### Miks energetyczny
6. Jaki jest średni udział OZE w generacji w tym roku?
7. Porównaj generację z wiatru latem i zimą
8. Kiedy fotowoltaika generowała najwięcej energii?
9. Ile wynosi średnia generacja z węgla w godzinach nocnych?
10. Jak zmienia się miks energetyczny w weekendy vs dni robocze?

### Kontrakty bilateralne
11. Ile aktywnych kontraktów baseload jest w portfelu?
12. Który uczestnik rynku kupił najwięcej energii?
13. Jaka jest średnia cena kontraktów kwartalnych?
14. Pokaż rozkład kontraktów wg typu produktu
15. Ile wynosi łączna wartość rozliczeń kontraktów w 2024?

### Emisje CO₂
16. Jaka jest średnia intensywność emisji CO₂ na MWh?
17. Jak zmieniał się koszt emisji CO₂ w ostatnim roku?
18. Jaki jest trend ceny uprawnień EU ETS?
19. Porównaj emisje w miesiącach letnich i zimowych

### Analizy porównawcze
20. Porównaj udział OZE w miksie energetycznym Q1 vs Q2
21. Jak zmienił się miks energetyczny rok do roku?
22. Czy istnieje korelacja między ceną spot a udziałem OZE?
23. Porównaj wolumeny kontraktów PGE i Tauron
24. Która pora roku ma najniższe ceny energii?
25. Jak cena EU ETS wpływa na koszty emisji w Polsce?

---

## Struktura danych

### spot_prices
| Kolumna | Typ | Opis |
|---------|-----|------|
| `timestamp` | datetime | Znacznik czasu (CET) |
| `hour` | int | Godzina (0–23) |
| `date` | date | Data |
| `price_pln_mwh` | float | Cena PLN/MWh |
| `volume_mwh` | float | Wolumen MWh |
| `zone` | string | Strefa rynkowa (KSE) |
| `fixing_type` | string | Typ fixingu (RDN/RDB) |
| `min_price` | float | Cena minimalna |
| `max_price` | float | Cena maksymalna |
| `weighted_avg_price` | float | Średnia ważona wolumenem |

### generation_mix
| Kolumna | Typ | Opis |
|---------|-----|------|
| `timestamp` | datetime | Znacznik czasu (CET) |
| `hour` | int | Godzina (0–23) |
| `date` | date | Data |
| `wind_mw` | float | Generacja wiatrowa (MW) |
| `solar_mw` | float | Generacja fotowoltaiczna (MW) |
| `gas_mw` | float | Generacja gazowa (MW) |
| `coal_mw` | float | Generacja węglowa (MW) |
| `biomass_mw` | float | Generacja z biomasy (MW) |
| `hydro_mw` | float | Generacja wodna (MW) |
| `nuclear_mw` | float | Generacja jądrowa (MW) |
| `total_mw` | float | Generacja łączna (MW) |
| `oze_share_pct` | float | Udział OZE (%) |

### bilateral_contracts
| Kolumna | Typ | Opis |
|---------|-----|------|
| `contract_id` | string | Identyfikator kontraktu |
| `contract_date` | date | Data zawarcia |
| `buyer` | string | Kupujący |
| `seller` | string | Sprzedający |
| `start_date` | date | Data rozpoczęcia |
| `end_date` | date | Data zakończenia |
| `volume_mwh` | float | Wolumen (MWh) |
| `price_pln_mwh` | float | Cena (PLN/MWh) |
| `contract_type` | string | Typ (Baseload/Peak/OffPeak/Weekend) |
| `product` | string | Produkt (Month/Quarter/Year) |
| `status` | string | Status (Active/Expired/Cancelled) |
| `settlement_pln` | float | Wartość rozliczenia (PLN) |

### carbon_emissions
| Kolumna | Typ | Opis |
|---------|-----|------|
| `date` | date | Data |
| `total_emissions_tco2` | float | Łączne emisje (tys. tCO₂) |
| `emission_factor_tco2_mwh` | float | Wskaźnik emisji (tCO₂/MWh) |
| `coal_emissions` | float | Emisje z węgla (tys. tCO₂) |
| `gas_emissions` | float | Emisje z gazu (tys. tCO₂) |
| `total_generation_mwh` | float | Łączna generacja (MWh) |
| `oze_generation_mwh` | float | Generacja OZE (MWh) |
| `eu_ets_price_eur` | float | Cena uprawnienia EU ETS (EUR/tCO₂) |
| `carbon_cost_pln` | float | Koszt emisji (PLN) |

### market_participants
| Kolumna | Typ | Opis |
|---------|-----|------|
| `participant_id` | string | Identyfikator uczestnika |
| `name` | string | Nazwa firmy |
| `type` | string | Typ (Generator/Trader/Distributor/Consumer) |
| `market_share_pct` | float | Udział rynkowy (%) |
| `region` | string | Województwo |
| `license_number` | string | Numer licencji/koncesji |

---

## Struktura plików

```
scenario-02-energy-market-analytics/
├── README.md                          # Ten plik
├── generate_market_data.py            # Generator danych
├── config/
│   ├── agent_instructions.md          # Instrukcje dla Fabric Data Agent
│   └── example_queries.md             # Przykładowe zapytania
├── data/                              # Wygenerowane pliki CSV (po uruchomieniu generatora)
│   ├── spot_prices.csv
│   ├── generation_mix.csv
│   ├── bilateral_contracts.csv
│   ├── carbon_emissions.csv
│   └── market_participants.csv
├── kql/
│   └── lakehouse_queries.sql          # Zapytania T-SQL dla SQL Endpoint
└── notebooks/
    └── market_analysis.py             # Notebook analizy rynku (Fabric)
```
