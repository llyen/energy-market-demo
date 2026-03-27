-- =============================================================================
-- Scenariusz 02: Analityka Rynku Energii — zapytania T-SQL
-- Przeznaczone do uruchomienia na SQL Analytics Endpoint Lakehouse
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Średnia cena spot wg miesiąca
-- ---------------------------------------------------------------------------
SELECT
    FORMAT(CAST([date] AS DATE), 'yyyy-MM')   AS miesiac,
    ROUND(AVG(price_pln_mwh), 2)              AS srednia_cena,
    ROUND(MIN(price_pln_mwh), 2)              AS cena_min,
    ROUND(MAX(price_pln_mwh), 2)              AS cena_max,
    ROUND(AVG(volume_mwh), 0)                 AS sredni_wolumen,
    COUNT(*)                                   AS liczba_godzin
FROM spot_prices
WHERE fixing_type = 'RDN'
GROUP BY FORMAT(CAST([date] AS DATE), 'yyyy-MM')
ORDER BY miesiac;

-- ---------------------------------------------------------------------------
-- 2. Analiza udziału źródeł w miksie energetycznym
-- ---------------------------------------------------------------------------
SELECT
    FORMAT(CAST([date] AS DATE), 'yyyy-MM')        AS miesiac,
    ROUND(AVG(wind_mw / total_mw) * 100, 2)       AS udzial_wiatr_pct,
    ROUND(AVG(solar_mw / total_mw) * 100, 2)      AS udzial_slonce_pct,
    ROUND(AVG(gas_mw / total_mw) * 100, 2)        AS udzial_gaz_pct,
    ROUND(AVG(coal_mw / total_mw) * 100, 2)       AS udzial_wegiel_pct,
    ROUND(AVG(biomass_mw / total_mw) * 100, 2)    AS udzial_biomasa_pct,
    ROUND(AVG(hydro_mw / total_mw) * 100, 2)      AS udzial_woda_pct,
    ROUND(AVG(nuclear_mw / total_mw) * 100, 2)    AS udzial_atom_pct
FROM generation_mix
WHERE total_mw > 0
GROUP BY FORMAT(CAST([date] AS DATE), 'yyyy-MM')
ORDER BY miesiac;

-- ---------------------------------------------------------------------------
-- 3. Trend udziału OZE (kwartalnie)
-- ---------------------------------------------------------------------------
SELECT
    CONCAT(YEAR(CAST([date] AS DATE)), '-Q',
           DATEPART(QUARTER, CAST([date] AS DATE)))  AS kwartal,
    ROUND(AVG(oze_share_pct), 2)                     AS sredni_udzial_oze,
    ROUND(MIN(oze_share_pct), 2)                     AS min_oze,
    ROUND(MAX(oze_share_pct), 2)                     AS max_oze,
    COUNT(*)                                          AS liczba_obserwacji
FROM generation_mix
GROUP BY
    YEAR(CAST([date] AS DATE)),
    DATEPART(QUARTER, CAST([date] AS DATE))
ORDER BY kwartal;

-- ---------------------------------------------------------------------------
-- 4. Analiza zmienności cen (odchylenie standardowe)
-- ---------------------------------------------------------------------------
SELECT
    FORMAT(CAST([date] AS DATE), 'yyyy-MM')   AS miesiac,
    ROUND(AVG(price_pln_mwh), 2)              AS srednia_cena,
    ROUND(STDEV(price_pln_mwh), 2)            AS odchylenie_std,
    ROUND(STDEV(price_pln_mwh)
          / AVG(price_pln_mwh) * 100, 2)      AS wspolczynnik_zmiennosci_pct,
    COUNT(*)                                   AS liczba_godzin
FROM spot_prices
WHERE fixing_type = 'RDN'
GROUP BY FORMAT(CAST([date] AS DATE), 'yyyy-MM')
ORDER BY miesiac;

-- ---------------------------------------------------------------------------
-- 5. Top 10 najdroższych godzin
-- ---------------------------------------------------------------------------
SELECT TOP 10
    [timestamp],
    [date],
    [hour],
    price_pln_mwh,
    volume_mwh,
    fixing_type,
    max_price
FROM spot_prices
ORDER BY price_pln_mwh DESC;

-- ---------------------------------------------------------------------------
-- 6. Podsumowanie portfela kontraktów
-- ---------------------------------------------------------------------------
SELECT
    contract_type,
    product,
    status,
    COUNT(*)                               AS liczba_kontraktow,
    ROUND(SUM(volume_mwh), 0)             AS laczny_wolumen_mwh,
    ROUND(AVG(price_pln_mwh), 2)          AS srednia_cena,
    ROUND(SUM(settlement_pln), 2)         AS laczne_rozliczenie_pln
FROM bilateral_contracts
GROUP BY contract_type, product, status
ORDER BY contract_type, product, status;

-- ---------------------------------------------------------------------------
-- 7. Trend intensywności emisji CO₂
-- ---------------------------------------------------------------------------
SELECT
    FORMAT(CAST([date] AS DATE), 'yyyy-MM')   AS miesiac,
    ROUND(AVG(emission_factor_tco2_mwh), 4)   AS sredni_wskaznik_emisji,
    ROUND(SUM(total_emissions_tco2), 1)        AS laczne_emisje_tco2,
    ROUND(AVG(eu_ets_price_eur), 2)            AS srednia_cena_ets_eur,
    ROUND(SUM(carbon_cost_pln), 2)             AS laczny_koszt_co2_pln
FROM carbon_emissions
GROUP BY FORMAT(CAST([date] AS DATE), 'yyyy-MM')
ORDER BY miesiac;

-- ---------------------------------------------------------------------------
-- 8. Korelacja ceny spot z generacją OZE (dzienne agregaty)
-- ---------------------------------------------------------------------------
SELECT
    sp.[date],
    ROUND(AVG(sp.price_pln_mwh), 2)       AS srednia_cena,
    ROUND(AVG(gm.oze_share_pct), 2)       AS sredni_udzial_oze,
    ROUND(AVG(gm.wind_mw), 0)             AS srednia_generacja_wiatr,
    ROUND(AVG(gm.solar_mw), 0)            AS srednia_generacja_slonce,
    ROUND(AVG(gm.total_mw), 0)            AS srednia_generacja_total
FROM spot_prices sp
INNER JOIN generation_mix gm
    ON sp.[date] = gm.[date] AND sp.[hour] = gm.[hour]
WHERE sp.fixing_type = 'RDN'
GROUP BY sp.[date]
ORDER BY sp.[date];

-- ---------------------------------------------------------------------------
-- 9. Porównanie cen: szczyt vs pozaszczyt
-- ---------------------------------------------------------------------------
SELECT
    FORMAT(CAST([date] AS DATE), 'yyyy-MM') AS miesiac,
    ROUND(AVG(CASE WHEN [hour] BETWEEN 7 AND 21
              THEN price_pln_mwh END), 2)   AS cena_szczyt,
    ROUND(AVG(CASE WHEN [hour] < 7 OR [hour] > 21
              THEN price_pln_mwh END), 2)   AS cena_pozaszczyt,
    ROUND(AVG(CASE WHEN [hour] BETWEEN 7 AND 21
              THEN price_pln_mwh END)
        - AVG(CASE WHEN [hour] < 7 OR [hour] > 21
              THEN price_pln_mwh END), 2)   AS spread_szczyt_pozaszczyt
FROM spot_prices
WHERE fixing_type = 'RDN'
GROUP BY FORMAT(CAST([date] AS DATE), 'yyyy-MM')
ORDER BY miesiac;

-- ---------------------------------------------------------------------------
-- 10. Porównanie rok do roku (YoY)
-- ---------------------------------------------------------------------------
WITH roczne AS (
    SELECT
        YEAR(CAST([date] AS DATE))             AS rok,
        ROUND(AVG(price_pln_mwh), 2)          AS srednia_cena,
        ROUND(AVG(volume_mwh), 0)             AS sredni_wolumen,
        ROUND(MAX(price_pln_mwh), 2)          AS cena_max,
        COUNT(*)                                AS liczba_godzin
    FROM spot_prices
    WHERE fixing_type = 'RDN'
    GROUP BY YEAR(CAST([date] AS DATE))
),
generacja AS (
    SELECT
        YEAR(CAST([date] AS DATE))             AS rok,
        ROUND(AVG(oze_share_pct), 2)          AS sredni_udzial_oze,
        ROUND(AVG(coal_mw / total_mw) * 100, 2) AS sredni_udzial_wegiel
    FROM generation_mix
    WHERE total_mw > 0
    GROUP BY YEAR(CAST([date] AS DATE))
)
SELECT
    r.rok,
    r.srednia_cena,
    r.sredni_wolumen,
    r.cena_max,
    g.sredni_udzial_oze,
    g.sredni_udzial_wegiel,
    r.liczba_godzin
FROM roczne r
LEFT JOIN generacja g ON r.rok = g.rok
ORDER BY r.rok;
