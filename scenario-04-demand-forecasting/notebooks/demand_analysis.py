"""
Scenariusz 04: Prognozowanie Popytu — Analiza popytu i prognozowanie
Notebook-style analysis: correlation, seasonal decomposition, forecasting, optimization.

Przeznaczony do uruchomienia w Microsoft Fabric Notebook lub lokalnie z pandas/numpy/matplotlib.
"""

# %% [markdown]
# # Analiza Popytu na Energię Elektryczną
# Notebook łączy dane historyczne o zużyciu z danymi pogodowymi,
# wykonuje analizę korelacji, dekompozycję sezonową, prosty model prognostyczny
# oraz identyfikuje wzorce szczytowe i rekomendacje optymalizacyjne.

# %% Imports and setup
import os
import sys
import math
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# Ensure UTF-8 output on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

HAS_MATPLOTLIB = False
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB = True
except ImportError:
    print("matplotlib not available — charts will be skipped.")

HAS_SCIPY = False
try:
    from scipy import stats as scipy_stats
    HAS_SCIPY = True
except ImportError:
    print("scipy not available — some statistical tests will be skipped.")

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "analysis_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# %% Load data
print("Loading data...")

consumption_df = pd.read_csv(
    os.path.join(DATA_DIR, "historical_consumption.csv"),
    parse_dates=["date"],
)
weather_df = pd.read_csv(
    os.path.join(DATA_DIR, "weather_data.csv"),
    parse_dates=["timestamp"],
)
forecasts_df = pd.read_csv(
    os.path.join(DATA_DIR, "demand_forecasts.csv"),
    parse_dates=["timestamp"],
)
tariff_df = pd.read_csv(os.path.join(DATA_DIR, "tariff_schedule.csv"))

print(f"  historical_consumption: {len(consumption_df):,} rows")
print(f"  weather_data:           {len(weather_df):,} rows")
print(f"  demand_forecasts:       {len(forecasts_df):,} rows")
print(f"  tariff_schedule:        {len(tariff_df):,} rows")


# %% [markdown]
# ## 1. Eksploracja danych — podstawowe statystyki

# %% Basic statistics
print("\n" + "=" * 60)
print("1. PODSTAWOWE STATYSTYKI")
print("=" * 60)

print("\n--- Zużycie historyczne (per region per hour) ---")
print(consumption_df[["total_consumption_mwh", "peak_demand_mw", "temperature_c"]].describe().round(2))

print("\n--- Pogoda ---")
print(weather_df[["temperature_c", "wind_speed_ms", "humidity_pct", "solar_radiation_wm2"]].describe().round(2))

print("\n--- Prognozy vs Rzeczywistość ---")
print(forecasts_df[["forecasted_demand_mw", "actual_demand_mw", "forecast_error_pct"]].describe().round(2))

# Aggregate total consumption by hour across all regions
hourly_total = consumption_df.groupby(["date", "hour"]).agg(
    total_mwh=("total_consumption_mwh", "sum"),
    avg_temp=("temperature_c", "mean"),
    is_weekend=("is_weekend", "first"),
    is_holiday=("is_holiday", "first"),
    day_of_week=("day_of_week", "first"),
).reset_index()
hourly_total["datetime"] = pd.to_datetime(hourly_total["date"]) + pd.to_timedelta(hourly_total["hour"], unit="h")

print(f"\nŁączna liczba godzin danych: {len(hourly_total):,}")
print(f"Średnie godzinowe zużycie:   {hourly_total['total_mwh'].mean():.1f} MWh")
print(f"Maks. godzinowe zużycie:     {hourly_total['total_mwh'].max():.1f} MWh")
print(f"Min. godzinowe zużycie:      {hourly_total['total_mwh'].min():.1f} MWh")


# %% [markdown]
# ## 2. Korelacja: Temperatura vs Popyt

# %% Temperature-demand correlation
print("\n" + "=" * 60)
print("2. KORELACJA: TEMPERATURA vs POPYT")
print("=" * 60)

corr = hourly_total["total_mwh"].corr(hourly_total["avg_temp"])
print(f"Współczynnik korelacji Pearsona (temp vs consumption): {corr:.4f}")

# Temperature bins analysis
temp_bins = [-20, -10, -5, 0, 5, 10, 15, 20, 25, 30, 40]
hourly_total["temp_bin"] = pd.cut(hourly_total["avg_temp"], bins=temp_bins)
temp_analysis = hourly_total.groupby("temp_bin", observed=True).agg(
    avg_consumption_mwh=("total_mwh", "mean"),
    count=("total_mwh", "count"),
).round(2)
print("\nŚrednie zużycie [MWh] wg przedziałów temperatury:")
print(temp_analysis)

if HAS_SCIPY:
    slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(
        hourly_total["avg_temp"].dropna(), hourly_total["total_mwh"].dropna()
    )
    print(f"\nRegresja liniowa: consumption = {slope:.2f} * temp + {intercept:.2f}")
    print(f"  R² = {r_value**2:.4f},  p-value = {p_value:.2e}")

if HAS_MATPLOTLIB:
    fig, ax = plt.subplots(figsize=(10, 6))
    sample = hourly_total.sample(min(5000, len(hourly_total)), random_state=42)
    ax.scatter(sample["avg_temp"], sample["total_mwh"], alpha=0.15, s=5, c="steelblue")
    ax.set_xlabel("Temperatura [°C]")
    ax.set_ylabel("Zużycie [MWh]")
    ax.set_title("Korelacja: Temperatura vs Zużycie Energii")
    ax.grid(True, alpha=0.3)
    if HAS_SCIPY:
        x_line = np.linspace(hourly_total["avg_temp"].min(), hourly_total["avg_temp"].max(), 100)
        ax.plot(x_line, slope * x_line + intercept, "r-", linewidth=2, label=f"y={slope:.1f}x+{intercept:.0f}")
        ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "temperature_vs_consumption.png"), dpi=150)
    plt.close()
    print("  -> Saved: temperature_vs_consumption.png")


# %% [markdown]
# ## 3. Dekompozycja sezonowa

# %% Seasonal decomposition
print("\n" + "=" * 60)
print("3. DEKOMPOZYCJA SEZONOWA")
print("=" * 60)

# Daily average for seasonal analysis
daily_total = hourly_total.groupby("date").agg(
    daily_mwh=("total_mwh", "sum"),
    avg_temp=("avg_temp", "mean"),
    is_weekend=("is_weekend", "first"),
).reset_index()
daily_total["date"] = pd.to_datetime(daily_total["date"])
daily_total = daily_total.sort_values("date").reset_index(drop=True)

# Rolling averages for trend extraction
daily_total["trend_7d"] = daily_total["daily_mwh"].rolling(7, center=True).mean()
daily_total["trend_30d"] = daily_total["daily_mwh"].rolling(30, center=True).mean()
daily_total["detrended"] = daily_total["daily_mwh"] - daily_total["trend_30d"]

print(f"Średnie dzienne zużycie:     {daily_total['daily_mwh'].mean():.0f} MWh")
print(f"Odchylenie standardowe:      {daily_total['daily_mwh'].std():.0f} MWh")

# Monthly seasonality
daily_total["month"] = daily_total["date"].dt.month
monthly_avg = daily_total.groupby("month")["daily_mwh"].mean().round(0)
print("\nŚrednie dzienne zużycie wg miesiąca [MWh]:")
month_names = {
    1: "Styczeń", 2: "Luty", 3: "Marzec", 4: "Kwiecień",
    5: "Maj", 6: "Czerwiec", 7: "Lipiec", 8: "Sierpień",
    9: "Wrzesień", 10: "Październik", 11: "Listopad", 12: "Grudzień",
}
for m, val in monthly_avg.items():
    print(f"  {month_names.get(m, m):12s}: {val:,.0f} MWh")

# Day-of-week pattern
dow_names = {0: "Poniedziałek", 1: "Wtorek", 2: "Środa", 3: "Czwartek",
             4: "Piątek", 5: "Sobota", 6: "Niedziela"}
daily_total["dow"] = daily_total["date"].dt.dayofweek
dow_avg = daily_total.groupby("dow")["daily_mwh"].mean().round(0)
print("\nŚrednie dzienne zużycie wg dnia tygodnia:")
for d, val in dow_avg.items():
    print(f"  {dow_names.get(d, d):14s}: {val:,.0f} MWh")

if HAS_MATPLOTLIB:
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))

    ax = axes[0]
    ax.plot(daily_total["date"], daily_total["daily_mwh"], alpha=0.3, linewidth=0.5, color="gray", label="Dzienne")
    ax.plot(daily_total["date"], daily_total["trend_7d"], linewidth=1, color="blue", label="Średnia 7-dniowa")
    ax.plot(daily_total["date"], daily_total["trend_30d"], linewidth=2, color="red", label="Średnia 30-dniowa")
    ax.set_ylabel("Zużycie [MWh]")
    ax.set_title("Dekompozycja sezonowa — dzienne zużycie energii")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

    ax = axes[1]
    ax.bar(range(12), [monthly_avg.get(m+1, 0) for m in range(12)],
           color=["#2196F3" if m+1 in [12,1,2] else "#FF9800" if m+1 in [6,7,8] else "#4CAF50" for m in range(12)])
    ax.set_xticks(range(12))
    ax.set_xticklabels([month_names[m+1][:3] for m in range(12)])
    ax.set_ylabel("Średnie dzienne zużycie [MWh]")
    ax.set_title("Sezonowość — średnie dzienne zużycie wg miesiąca")
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "seasonal_decomposition.png"), dpi=150)
    plt.close()
    print("  -> Saved: seasonal_decomposition.png")


# %% [markdown]
# ## 4. Prosty model prognostyczny (regresja wielokrotna)

# %% Forecasting model
print("\n" + "=" * 60)
print("4. MODEL PROGNOSTYCZNY")
print("=" * 60)

# Feature engineering
model_df = hourly_total.copy()
model_df["hour_sin"] = np.sin(2 * np.pi * model_df["hour"] / 24)
model_df["hour_cos"] = np.cos(2 * np.pi * model_df["hour"] / 24)
model_df["dow"] = pd.to_datetime(model_df["date"]).dt.dayofweek
model_df["dow_sin"] = np.sin(2 * np.pi * model_df["dow"] / 7)
model_df["dow_cos"] = np.cos(2 * np.pi * model_df["dow"] / 7)
model_df["month"] = pd.to_datetime(model_df["date"]).dt.month
model_df["month_sin"] = np.sin(2 * np.pi * model_df["month"] / 12)
model_df["month_cos"] = np.cos(2 * np.pi * model_df["month"] / 12)
model_df["is_weekend_int"] = model_df["is_weekend"].astype(int)
model_df["is_holiday_int"] = model_df["is_holiday"].astype(int)
model_df["temp_squared"] = model_df["avg_temp"] ** 2

features = ["avg_temp", "temp_squared", "hour_sin", "hour_cos",
            "dow_sin", "dow_cos", "month_sin", "month_cos",
            "is_weekend_int", "is_holiday_int"]

model_df = model_df.dropna(subset=features + ["total_mwh"])

# Simple train/test split: last 30 days = test
split_date = model_df["date"].max() - timedelta(days=30)
train = model_df[pd.to_datetime(model_df["date"]) <= split_date]
test = model_df[pd.to_datetime(model_df["date"]) > split_date]

print(f"Training set: {len(train):,} hours")
print(f"Test set:     {len(test):,} hours")

# Linear regression using numpy (no sklearn dependency)
X_train = train[features].values
y_train = train["total_mwh"].values
X_test = test[features].values
y_test = test["total_mwh"].values

# Add intercept column
X_train_b = np.column_stack([np.ones(len(X_train)), X_train])
X_test_b = np.column_stack([np.ones(len(X_test)), X_test])

# Normal equation: beta = (X'X)^-1 X'y
try:
    beta = np.linalg.lstsq(X_train_b, y_train, rcond=None)[0]

    y_pred_train = X_train_b @ beta
    y_pred_test = X_test_b @ beta

    # Metrics
    def mape(y_true, y_pred):
        mask = y_true != 0
        return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

    def rmse(y_true, y_pred):
        return np.sqrt(np.mean((y_true - y_pred) ** 2))

    def mae(y_true, y_pred):
        return np.mean(np.abs(y_true - y_pred))

    def r_squared(y_true, y_pred):
        ss_res = np.sum((y_true - y_pred) ** 2)
        ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
        return 1 - ss_res / ss_tot

    print(f"\n--- Wyniki na zbiorze treningowym ---")
    print(f"  MAPE:  {mape(y_train, y_pred_train):.2f}%")
    print(f"  RMSE:  {rmse(y_train, y_pred_train):.2f} MWh")
    print(f"  MAE:   {mae(y_train, y_pred_train):.2f} MWh")
    print(f"  R²:    {r_squared(y_train, y_pred_train):.4f}")

    print(f"\n--- Wyniki na zbiorze testowym ---")
    print(f"  MAPE:  {mape(y_test, y_pred_test):.2f}%")
    print(f"  RMSE:  {rmse(y_test, y_pred_test):.2f} MWh")
    print(f"  MAE:   {mae(y_test, y_pred_test):.2f} MWh")
    print(f"  R²:    {r_squared(y_test, y_pred_test):.4f}")

    print(f"\n--- Współczynniki modelu ---")
    coef_names = ["intercept"] + features
    for name, coef in zip(coef_names, beta):
        print(f"  {name:20s}: {coef:12.4f}")

    if HAS_MATPLOTLIB:
        fig, axes = plt.subplots(2, 1, figsize=(14, 10))

        test_dates = pd.to_datetime(test["date"]) + pd.to_timedelta(test["hour"], unit="h")

        ax = axes[0]
        ax.plot(test_dates, y_test, linewidth=0.8, alpha=0.7, label="Rzeczywiste", color="blue")
        ax.plot(test_dates, y_pred_test, linewidth=0.8, alpha=0.7, label="Prognoza", color="red")
        ax.set_ylabel("Zużycie [MWh]")
        ax.set_title("Prognoza vs Rzeczywistość — ostatnie 30 dni")
        ax.legend()
        ax.grid(True, alpha=0.3)

        ax = axes[1]
        errors = y_test - y_pred_test
        ax.hist(errors, bins=50, color="steelblue", alpha=0.7, edgecolor="white")
        ax.axvline(0, color="red", linestyle="--", linewidth=1.5)
        ax.set_xlabel("Błąd prognozy [MWh]")
        ax.set_ylabel("Częstość")
        ax.set_title(f"Rozkład błędów prognozy (MAPE={mape(y_test, y_pred_test):.2f}%)")
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, "forecast_model_results.png"), dpi=150)
        plt.close()
        print("  -> Saved: forecast_model_results.png")

except np.linalg.LinAlgError:
    print("  [ERROR] Could not fit linear model — matrix is singular.")


# %% [markdown]
# ## 5. Analiza segmentów — profil dobowy

# %% Segment analysis
print("\n" + "=" * 60)
print("5. ANALIZA REGIONÓW — PROFIL DOBOWY I RANKING")
print("=" * 60)

region_hourly = consumption_df.groupby(["region", "hour"]).agg(
    avg_mwh=("total_consumption_mwh", "mean"),
    total_mwh=("total_consumption_mwh", "sum"),
).reset_index()

print("\n--- Ranking regionów wg średniego godzinowego zużycia [MWh] ---")
region_ranking = consumption_df.groupby("region")["total_consumption_mwh"].mean().sort_values(ascending=False)
for i, (region, val) in enumerate(region_ranking.items(), 1):
    print(f"  {i:2d}. {region:25s}: {val:.1f} MWh")

# Peak hour identification per region
print("\n--- Godzina szczytu wg regionu ---")
peak_hours = region_hourly.loc[region_hourly.groupby("region")["avg_mwh"].idxmax()]
for _, row in peak_hours.iterrows():
    print(f"  {row['region']:25s}: godzina {int(row['hour']):2d}:00  ({row['avg_mwh']:.1f} MWh)")

# Weekend vs workday comparison
print("\n--- Średnie dzienne zużycie: dzień roboczy vs weekend ---")
workday = consumption_df[consumption_df["is_weekend"] == False].groupby("region")["total_consumption_mwh"].mean()
weekend = consumption_df[consumption_df["is_weekend"] == True].groupby("region")["total_consumption_mwh"].mean()
comparison = pd.DataFrame({"workday_mwh": workday, "weekend_mwh": weekend})
comparison["reduction_pct"] = ((comparison["workday_mwh"] - comparison["weekend_mwh"]) / comparison["workday_mwh"] * 100).round(1)
print(comparison.round(1).to_string())

if HAS_MATPLOTLIB:
    fig, ax = plt.subplots(figsize=(14, 8))
    top_regions = region_ranking.head(5).index.tolist()
    colors = ["#2196F3", "#FF5722", "#4CAF50", "#FF9800", "#9C27B0"]
    for i, region in enumerate(top_regions):
        data = region_hourly[region_hourly["region"] == region].sort_values("hour")
        ax.plot(data["hour"], data["avg_mwh"], linewidth=2, label=region, color=colors[i])
    ax.set_xlabel("Godzina")
    ax.set_ylabel("Średnie zużycie [MWh]")
    ax.set_title("Profil dobowy — top 5 regionów")
    ax.set_xticks(range(0, 24))
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.axvspan(7, 9, alpha=0.1, color="red", label="Szczyt poranny")
    ax.axvspan(17, 21, alpha=0.1, color="orange", label="Szczyt wieczorny")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "segment_daily_profile.png"), dpi=150)
    plt.close()
    print("  -> Saved: segment_daily_profile.png")


# %% [markdown]
# ## 6. Wykrywanie szczytu i analiza obciążenia

# %% Peak detection
print("\n" + "=" * 60)
print("6. ANALIZA SZCZYTÓW I KRZYWA TRWANIA OBCIĄŻENIA")
print("=" * 60)

# Peak threshold: 90th percentile
p90 = hourly_total["total_mwh"].quantile(0.90)
p95 = hourly_total["total_mwh"].quantile(0.95)
p99 = hourly_total["total_mwh"].quantile(0.99)

print(f"Percentyl 90: {p90:.1f} MWh")
print(f"Percentyl 95: {p95:.1f} MWh")
print(f"Percentyl 99: {p99:.1f} MWh")

peak_hours_df = hourly_total[hourly_total["total_mwh"] >= p95].copy()
print(f"\nLiczba godzin powyżej P95: {len(peak_hours_df)}")
if len(peak_hours_df) > 0:
    print(f"Średnia moc w szczycie: {peak_hours_df['total_mwh'].mean():.1f} MWh")
    print(f"Maks. moc w szczycie:  {peak_hours_df['total_mwh'].max():.1f} MWh")

    peak_by_hour = peak_hours_df.groupby("hour").size()
    print("\nRozkład godzin szczytowych:")
    for h in range(24):
        count = peak_by_hour.get(h, 0)
        bar = "█" * (count // 2)
        if count > 0:
            print(f"  {h:2d}:00  {count:4d}  {bar}")

# Load duration curve data
sorted_load = hourly_total["total_mwh"].sort_values(ascending=False).reset_index(drop=True)
sorted_load_pct = (sorted_load.index / len(sorted_load) * 100)

print(f"\n--- Krzywa trwania obciążenia ---")
checkpoints = [0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100]
for pct in checkpoints:
    idx = min(int(pct / 100 * len(sorted_load)), len(sorted_load) - 1)
    print(f"  {pct:3d}% czasu: obciążenie >= {sorted_load.iloc[idx]:.1f} MWh")

if HAS_MATPLOTLIB:
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    ax = axes[0]
    ax.plot(sorted_load_pct, sorted_load.values, linewidth=2, color="steelblue")
    ax.axhline(y=p95, color="red", linestyle="--", alpha=0.7, label=f"P95 = {p95:.0f} MWh")
    ax.axhline(y=p90, color="orange", linestyle="--", alpha=0.7, label=f"P90 = {p90:.0f} MWh")
    ax.set_xlabel("% czasu")
    ax.set_ylabel("Obciążenie [MWh]")
    ax.set_title("Krzywa trwania obciążenia")
    ax.legend()
    ax.grid(True, alpha=0.3)

    ax = axes[1]
    hour_dist = hourly_total.groupby("hour")["total_mwh"].mean()
    colors_bar = ["#FF5722" if h in [7,8,17,18,19,20] else "#2196F3" for h in range(24)]
    ax.bar(range(24), hour_dist.values, color=colors_bar, edgecolor="white")
    ax.set_xlabel("Godzina")
    ax.set_ylabel("Średnie zużycie [MWh]")
    ax.set_title("Średni profil dobowy (czerwone = godziny szczytu)")
    ax.set_xticks(range(24))
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "peak_analysis.png"), dpi=150)
    plt.close()
    print("  -> Saved: peak_analysis.png")


# %% [markdown]
# ## 7. Ocena dokładności prognoz

# %% Forecast accuracy
print("\n" + "=" * 60)
print("7. OCENA DOKŁADNOŚCI PROGNOZ")
print("=" * 60)

forecasts_df["abs_error_pct"] = forecasts_df["forecast_error_pct"].abs()
overall_mape = forecasts_df["abs_error_pct"].mean()
overall_rmse = np.sqrt(((forecasts_df["forecasted_demand_mw"] - forecasts_df["actual_demand_mw"]) ** 2).mean())

print(f"Ogólny MAPE: {overall_mape:.2f}%")
print(f"Ogólny RMSE: {overall_rmse:.2f} MW")

# MAPE by hour
mape_by_hour = forecasts_df.groupby("hour")["abs_error_pct"].mean().round(2)
print("\nMAPE wg godziny:")
for h, val in mape_by_hour.items():
    bar = "█" * int(val * 5)
    print(f"  {h:2d}:00  {val:5.2f}%  {bar}")

# MAPE by peak vs off-peak
peak_mape = forecasts_df[forecasts_df["is_peak_hour"] == True]["abs_error_pct"].mean()
offpeak_mape = forecasts_df[forecasts_df["is_peak_hour"] == False]["abs_error_pct"].mean()
print(f"\nMAPE w szczycie:         {peak_mape:.2f}%")
print(f"MAPE poza szczytem:      {offpeak_mape:.2f}%")

# Monthly MAPE
forecasts_df["timestamp"] = pd.to_datetime(forecasts_df["timestamp"], utc=True)
forecasts_df["month"] = forecasts_df["timestamp"].dt.month
monthly_mape = forecasts_df.groupby("month")["abs_error_pct"].mean().round(2)
print("\nMAPE wg miesiąca:")
for m, val in monthly_mape.items():
    print(f"  {month_names.get(m, m):12s}: {val:.2f}%")


# %% [markdown]
# ## 8. Rekomendacje optymalizacji obciążenia

# %% Optimization recommendations
print("\n" + "=" * 60)
print("8. REKOMENDACJE OPTYMALIZACJI OBCIĄŻENIA")
print("=" * 60)

avg_peak = hourly_total[hourly_total["hour"].isin([7, 8, 17, 18, 19, 20])]["total_mwh"].mean()
avg_offpeak = hourly_total[hourly_total["hour"].isin([0, 1, 2, 3, 4, 5])]["total_mwh"].mean()
avg_total = hourly_total["total_mwh"].mean()
peak_ratio = avg_peak / avg_offpeak if avg_offpeak > 0 else 0

print(f"Średnie zużycie w szczycie (7-9, 17-21):  {avg_peak:.1f} MWh")
print(f"Średnie zużycie w dolinie (0-5):           {avg_offpeak:.1f} MWh")
print(f"Stosunek szczyt/dolina:                    {peak_ratio:.2f}x")
print(f"Średnia dobowa:                            {avg_total:.1f} MWh")

# Potential DSR savings
dsr_potential_pct = 0.08  # 8% of peak load can be shifted
dsr_mw_saved = avg_peak * dsr_potential_pct
peak_cost_premium = 0.35  # 35% cost premium during peak

print(f"\n--- Potencjał DSR (Demand Side Response) ---")
print(f"Zakładany udział obciążeń elastycznych:  {dsr_potential_pct*100:.0f}%")
print(f"Możliwa redukcja szczytu:                {dsr_mw_saved:.1f} MWh")
print(f"Szacowane oszczędności roczne:            {dsr_mw_saved * 365 * peak_cost_premium * 0.65:.0f} tys. PLN")

print(f"\n--- Rekomendacje ---")
recommendations = [
    f"1. Przesunięcie {dsr_potential_pct*100:.0f}% obciążeń przemysłowych z godzin 17-21 na 22-06 "
    f"  → redukcja szczytu o ~{dsr_mw_saved:.0f} MWh",
    f"2. Promowanie taryfy G12 (dwustrefowej) wśród gospodarstw domowych "
    f"  → zachęta do przenoszenia zużycia na godziny nocne",
    f"3. Implementacja dynamicznego cennika real-time "
    f"  → sygnał cenowy zachęcający do wyrównania krzywej obciążenia",
    f"4. Instalacja magazynów energii w regionach o najwyższym szczycie "
    f"  → {region_ranking.head(3).index.tolist()} — top 3 regiony",
    f"5. Integracja z prognozą pogody dla lepszego planowania "
    f"  → korelacja temp-popyt: {corr:.2f}",
]
for rec in recommendations:
    print(f"  {rec}")

print(f"\n--- Potencjał redukcji kosztów szczytowych ---")
print(f"  Obecny koszt szczytowy (estymacja):  ~{avg_peak * 365 * 24 * 0.25 / 1000:.0f} mln PLN/rok")
print(f"  Potencjalna redukcja (15-20%):       ~{avg_peak * 365 * 24 * 0.25 * 0.175 / 1000:.0f} mln PLN/rok")


# %% [markdown]
# ## 9. Korelacja wieloczynnikowa (pogoda)

# %% Multi-factor weather analysis
print("\n" + "=" * 60)
print("9. ANALIZA WIELOCZYNNIKOWA — WPŁYW POGODY")
print("=" * 60)

weather_hourly = weather_df.copy()
weather_hourly["timestamp"] = pd.to_datetime(weather_hourly["timestamp"], utc=True)
weather_hourly["date"] = weather_hourly["timestamp"].dt.date.astype(str)
weather_hourly["hour"] = weather_hourly["timestamp"].dt.hour

weather_agg = weather_hourly.groupby(["date", "hour"]).agg(
    avg_temp=("temperature_c", "mean"),
    avg_wind=("wind_speed_ms", "mean"),
    avg_humidity=("humidity_pct", "mean"),
    avg_cloud=("cloud_cover_pct", "mean"),
    avg_solar=("solar_radiation_wm2", "mean"),
    avg_pressure=("pressure_hpa", "mean"),
).reset_index()

# Ensure consistent date types for merge
hourly_total_str = hourly_total.copy()
hourly_total_str["date"] = hourly_total_str["date"].astype(str)

merged = hourly_total_str.merge(
    weather_agg,
    left_on=["date", "hour"],
    right_on=["date", "hour"],
    how="inner",
    suffixes=("_cons", "_weather"),
)

weather_features = ["avg_temp_weather", "avg_wind", "avg_humidity", "avg_cloud", "avg_solar", "avg_pressure"]
available_features = [f for f in weather_features if f in merged.columns]

if len(available_features) > 0 and len(merged) > 100:
    print("\nKorelacja ze zmiennymi pogodowymi:")
    feature_labels = {
        "avg_temp_weather": "Temperatura",
        "avg_wind": "Wiatr",
        "avg_humidity": "Wilgotność",
        "avg_cloud": "Zachmurzenie",
        "avg_solar": "Promieniowanie",
        "avg_pressure": "Ciśnienie",
    }
    correlations = {}
    for feat in available_features:
        c = merged["total_mwh"].corr(merged[feat])
        label = feature_labels.get(feat, feat)
        correlations[label] = c
        bar = "█" * int(abs(c) * 40)
        sign = "+" if c > 0 else "-"
        print(f"  {label:20s}: {sign}{abs(c):.4f}  {bar}")

    if HAS_MATPLOTLIB:
        fig, ax = plt.subplots(figsize=(10, 5))
        labels = list(correlations.keys())
        values = list(correlations.values())
        colors_corr = ["#FF5722" if v < 0 else "#4CAF50" for v in values]
        ax.barh(labels, values, color=colors_corr, edgecolor="white")
        ax.set_xlabel("Współczynnik korelacji Pearsona")
        ax.set_title("Korelacja zmiennych pogodowych z zużyciem energii")
        ax.axvline(0, color="black", linewidth=0.8)
        ax.grid(True, alpha=0.3, axis="x")
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, "weather_correlation.png"), dpi=150)
        plt.close()
        print("  -> Saved: weather_correlation.png")
else:
    print("  Insufficient merged data for multi-factor analysis.")


# %% Summary
print("\n" + "=" * 60)
print("PODSUMOWANIE ANALIZY")
print("=" * 60)
print(f"""
Kluczowe wnioski:
  1. Sezonowość: zużycie zimowe (sty) jest o ~{((monthly_avg.get(1,0) - monthly_avg.get(7,0)) / monthly_avg.get(7,0) * 100):.0f}% wyższe niż letnie (lip)
  2. Szczyt dobowy: stosunek szczyt/dolina = {peak_ratio:.1f}x
  3. Korelacja z temperaturą: r = {corr:.3f}
  4. Dokładność prognoz: MAPE = {overall_mape:.2f}%
  5. Potencjał DSR: ~{dsr_mw_saved:.0f} MWh redukcji szczytu

Pliki wyjściowe zapisane w: {OUTPUT_DIR}
""")

print("Analiza zakończona.")
