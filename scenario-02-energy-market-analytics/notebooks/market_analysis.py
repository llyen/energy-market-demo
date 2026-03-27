# Fabric notebook  –  Scenariusz 02: Analityka Rynku Energii
# Uruchom w Microsoft Fabric Notebook podłączonym do Lakehouse LH_EnergyMarket

# %% [markdown]
# # Analityka Rynku Energii — Notebook eksploracyjny
#
# Notebook ładuje dane z Lakehouse i przeprowadza analizy:
# 1. Trend cen spot
# 2. Miks energetyczny (stacked area)
# 3. Korelacja: generacja wiatrowa vs cena
# 4. Prognoza udziału OZE (regresja liniowa)
# 5. Analiza emisji CO₂

# %% [markdown]
# ## 0. Konfiguracja i import bibliotek

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

plt.rcParams.update({
    "figure.figsize": (14, 5),
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
})

# %% [markdown]
# ## 1. Wczytanie danych z Lakehouse
#
# Przy uruchomieniu w Fabric Notebook tabele są dostępne przez Spark SQL.
# Poniżej wariant z `spark.sql()` (Fabric) z automatycznym fallback na CSV
# dla lokalnego uruchomienia.

# %%
def load_table(table_name: str) -> pd.DataFrame:
    """Wczytaj tabelę z Lakehouse (Spark) lub z lokalnego CSV."""
    try:
        return spark.sql(f"SELECT * FROM {table_name}").toPandas()  # noqa: F821
    except NameError:
        import os
        csv_path = os.path.join(
            os.path.dirname(os.path.abspath("__file__")),
            "..", "data", f"{table_name}.csv",
        )
        if os.path.exists(csv_path):
            return pd.read_csv(csv_path)
        csv_path_alt = os.path.join("data", f"{table_name}.csv")
        return pd.read_csv(csv_path_alt)

spot = load_table("spot_prices")
gen  = load_table("generation_mix")
contracts = load_table("bilateral_contracts")
emissions = load_table("carbon_emissions")
participants = load_table("market_participants")

# Konwersja typów
spot["date"] = pd.to_datetime(spot["date"])
spot["timestamp"] = pd.to_datetime(spot["timestamp"])
gen["date"] = pd.to_datetime(gen["date"])
gen["timestamp"] = pd.to_datetime(gen["timestamp"])
emissions["date"] = pd.to_datetime(emissions["date"])
contracts["contract_date"] = pd.to_datetime(contracts["contract_date"])

print(f"spot_prices:          {len(spot):>8,} wierszy")
print(f"generation_mix:       {len(gen):>8,} wierszy")
print(f"bilateral_contracts:  {len(contracts):>8,} wierszy")
print(f"carbon_emissions:     {len(emissions):>8,} wierszy")
print(f"market_participants:  {len(participants):>8,} wierszy")

# %% [markdown]
# ## 2. Trend cen spot (średnia miesięczna + zakres min–max)

# %%
monthly_prices = (
    spot[spot["fixing_type"] == "RDN"]
    .groupby(spot["date"].dt.to_period("M"))
    .agg(
        srednia=("price_pln_mwh", "mean"),
        mediana=("price_pln_mwh", "median"),
        min_cena=("price_pln_mwh", "min"),
        max_cena=("price_pln_mwh", "max"),
        wolumen=("volume_mwh", "sum"),
    )
)
monthly_prices.index = monthly_prices.index.to_timestamp()

fig, ax1 = plt.subplots(figsize=(14, 6))

ax1.fill_between(
    monthly_prices.index,
    monthly_prices["min_cena"],
    monthly_prices["max_cena"],
    alpha=0.15, color="royalblue", label="Zakres min–max",
)
ax1.plot(
    monthly_prices.index, monthly_prices["srednia"],
    color="royalblue", linewidth=2, label="Średnia cena",
)
ax1.plot(
    monthly_prices.index, monthly_prices["mediana"],
    color="darkorange", linewidth=1.5, linestyle="--", label="Mediana",
)

ax1.set_ylabel("Cena (PLN/MWh)")
ax1.set_title("Trend cen spot na RDN — średnia miesięczna")
ax1.legend(loc="upper left")
ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 3. Miks energetyczny — stacked area chart

# %%
daily_gen = (
    gen.groupby("date")
    .agg({
        "wind_mw": "mean",
        "solar_mw": "mean",
        "gas_mw": "mean",
        "coal_mw": "mean",
        "biomass_mw": "mean",
        "hydro_mw": "mean",
        "nuclear_mw": "mean",
    })
    .rolling(7)
    .mean()
    .dropna()
)

fig, ax = plt.subplots(figsize=(14, 7))

sources = ["coal_mw", "gas_mw", "wind_mw", "solar_mw", "biomass_mw", "hydro_mw", "nuclear_mw"]
labels  = ["Węgiel", "Gaz", "Wiatr", "Słońce", "Biomasa", "Woda", "Atom"]
colors  = ["#555555", "#FF9800", "#2196F3", "#FFC107", "#4CAF50", "#00BCD4", "#9C27B0"]

ax.stackplot(
    daily_gen.index,
    [daily_gen[s] for s in sources],
    labels=labels,
    colors=colors,
    alpha=0.85,
)

ax.set_ylabel("Średnia moc (MW)")
ax.set_title("Miks energetyczny KSE — średnia dzienna (7-dniowa średnia krocząca)")
ax.legend(loc="upper left", ncol=4)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 4. Korelacja: generacja wiatrowa vs cena spot

# %%
# Złączenie danych godzinowych
merged = spot[spot["fixing_type"] == "RDN"].merge(
    gen[["timestamp", "wind_mw", "oze_share_pct", "total_mw"]],
    on="timestamp",
    how="inner",
)

fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Scatter: wiatr vs cena
axes[0].scatter(
    merged["wind_mw"], merged["price_pln_mwh"],
    alpha=0.05, s=3, color="steelblue",
)
# Linia trendu
z = np.polyfit(merged["wind_mw"], merged["price_pln_mwh"], 1)
x_line = np.linspace(merged["wind_mw"].min(), merged["wind_mw"].max(), 100)
axes[0].plot(x_line, np.polyval(z, x_line), "r-", linewidth=2,
             label=f"Trend: {z[0]:.3f}x + {z[1]:.1f}")
axes[0].set_xlabel("Generacja wiatrowa (MW)")
axes[0].set_ylabel("Cena spot (PLN/MWh)")
axes[0].set_title("Generacja wiatrowa vs cena spot")
axes[0].legend()

# Scatter: udział OZE vs cena
axes[1].scatter(
    merged["oze_share_pct"], merged["price_pln_mwh"],
    alpha=0.05, s=3, color="forestgreen",
)
z2 = np.polyfit(merged["oze_share_pct"], merged["price_pln_mwh"], 1)
x_line2 = np.linspace(merged["oze_share_pct"].min(), merged["oze_share_pct"].max(), 100)
axes[1].plot(x_line2, np.polyval(z2, x_line2), "r-", linewidth=2,
             label=f"Trend: {z2[0]:.3f}x + {z2[1]:.1f}")
axes[1].set_xlabel("Udział OZE (%)")
axes[1].set_ylabel("Cena spot (PLN/MWh)")
axes[1].set_title("Udział OZE vs cena spot")
axes[1].legend()

plt.tight_layout()
plt.show()

corr_wind = merged["wind_mw"].corr(merged["price_pln_mwh"])
corr_oze = merged["oze_share_pct"].corr(merged["price_pln_mwh"])
print(f"Korelacja Pearsona — wiatr vs cena: {corr_wind:.4f}")
print(f"Korelacja Pearsona — OZE%  vs cena: {corr_oze:.4f}")

# %% [markdown]
# ## 5. Prognoza udziału OZE — regresja liniowa

# %%
quarterly_oze = (
    gen.groupby(gen["date"].dt.to_period("Q"))["oze_share_pct"]
    .mean()
    .reset_index()
)
quarterly_oze.columns = ["kwartal", "oze_pct"]
quarterly_oze["kwartal_ts"] = quarterly_oze["kwartal"].dt.to_timestamp()
quarterly_oze["kwartal_num"] = range(len(quarterly_oze))

# Regresja liniowa
coeffs = np.polyfit(quarterly_oze["kwartal_num"], quarterly_oze["oze_pct"], 1)
trend_fn = np.poly1d(coeffs)

# Prognoza na 4 kolejne kwartały
n_forecast = 4
future_nums = range(len(quarterly_oze), len(quarterly_oze) + n_forecast)
future_vals = [trend_fn(x) for x in future_nums]
last_q = quarterly_oze["kwartal"].iloc[-1]
future_quarters = pd.period_range(
    start=last_q + 1, periods=n_forecast, freq="Q"
)

fig, ax = plt.subplots(figsize=(14, 5))

ax.plot(
    quarterly_oze["kwartal_ts"], quarterly_oze["oze_pct"],
    "o-", color="forestgreen", linewidth=2, markersize=6, label="Dane historyczne",
)
ax.plot(
    [q.to_timestamp() for q in future_quarters], future_vals,
    "s--", color="coral", linewidth=2, markersize=6, label="Prognoza (regresja liniowa)",
)
# Linia trendu przez cały zakres
all_x = list(quarterly_oze["kwartal_num"]) + list(future_nums)
all_ts = list(quarterly_oze["kwartal_ts"]) + [q.to_timestamp() for q in future_quarters]
ax.plot(all_ts, [trend_fn(x) for x in all_x],
        ":", color="gray", linewidth=1, alpha=0.7)

ax.set_ylabel("Średni udział OZE (%)")
ax.set_title("Udział OZE w miksie energetycznym — trend i prognoza")
ax.legend()
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-Q%q"))
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

print(f"Współczynnik trendu: +{coeffs[0]:.3f} pp / kwartał")
print(f"Prognoza na kolejne {n_forecast} kwartały: "
      f"{', '.join(f'{v:.1f}%' for v in future_vals)}")

# %% [markdown]
# ## 6. Analiza emisji CO₂

# %%
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# 6a. Dzienny trend emisji z 30-dniową średnią kroczącą
emissions_sorted = emissions.sort_values("date")
emissions_sorted["em_rolling"] = (
    emissions_sorted["total_emissions_tco2"].rolling(30).mean()
)

axes[0, 0].plot(
    emissions_sorted["date"], emissions_sorted["total_emissions_tco2"],
    alpha=0.3, color="gray", linewidth=0.5,
)
axes[0, 0].plot(
    emissions_sorted["date"], emissions_sorted["em_rolling"],
    color="crimson", linewidth=2, label="30-dniowa śr. krocząca",
)
axes[0, 0].set_title("Dzienne emisje CO₂ (tys. tCO₂)")
axes[0, 0].set_ylabel("Emisje (tys. tCO₂)")
axes[0, 0].legend()

# 6b. Cena EU ETS
axes[0, 1].plot(
    emissions_sorted["date"], emissions_sorted["eu_ets_price_eur"],
    color="darkblue", linewidth=1.5,
)
axes[0, 1].set_title("Cena uprawnień EU ETS (EUR/tCO₂)")
axes[0, 1].set_ylabel("EUR/tCO₂")

# 6c. Wskaźnik emisji (tCO₂/MWh)
emissions_sorted["ef_rolling"] = (
    emissions_sorted["emission_factor_tco2_mwh"].rolling(30).mean()
)
axes[1, 0].plot(
    emissions_sorted["date"], emissions_sorted["emission_factor_tco2_mwh"],
    alpha=0.3, color="gray", linewidth=0.5,
)
axes[1, 0].plot(
    emissions_sorted["date"], emissions_sorted["ef_rolling"],
    color="darkgreen", linewidth=2, label="30-dniowa śr. krocząca",
)
axes[1, 0].set_title("Wskaźnik emisji (tCO₂/MWh)")
axes[1, 0].set_ylabel("tCO₂/MWh")
axes[1, 0].legend()

# 6d. Miesięczny koszt emisji
monthly_cost = (
    emissions_sorted
    .groupby(emissions_sorted["date"].dt.to_period("M"))["carbon_cost_pln"]
    .sum()
)
monthly_cost.index = monthly_cost.index.to_timestamp()

axes[1, 1].bar(
    monthly_cost.index, monthly_cost.values / 1e6,
    width=25, color="darkorange", alpha=0.8,
)
axes[1, 1].set_title("Miesięczny koszt emisji CO₂ (mln PLN)")
axes[1, 1].set_ylabel("mln PLN")

for ax in axes.flat:
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for label in ax.get_xticklabels():
        label.set_rotation(45)

plt.suptitle("Analiza emisji CO₂", fontsize=16, y=1.01)
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 7. Podsumowanie — kluczowe wskaźniki

# %%
print("=" * 60)
print("  PODSUMOWANIE RYNKU ENERGII")
print("=" * 60)

avg_price = spot[spot["fixing_type"] == "RDN"]["price_pln_mwh"].mean()
max_price = spot["price_pln_mwh"].max()
avg_oze = gen["oze_share_pct"].mean()
total_contracts = len(contracts[contracts["status"] == "Active"])
total_settlement = contracts[contracts["status"] == "Active"]["settlement_pln"].sum()
avg_emission = emissions["emission_factor_tco2_mwh"].mean()
avg_ets = emissions["eu_ets_price_eur"].mean()

print(f"  Średnia cena RDN:           {avg_price:>10.2f} PLN/MWh")
print(f"  Maksymalna cena spot:       {max_price:>10.2f} PLN/MWh")
print(f"  Średni udział OZE:          {avg_oze:>10.2f} %")
print(f"  Aktywne kontrakty:          {total_contracts:>10,}")
print(f"  Wartość portfela:           {total_settlement:>10,.0f} PLN")
print(f"  Śr. wskaźnik emisji:        {avg_emission:>10.4f} tCO₂/MWh")
print(f"  Śr. cena EU ETS:            {avg_ets:>10.2f} EUR/tCO₂")
print(f"  Uczestnicy rynku:           {len(participants):>10,}")
print("=" * 60)
