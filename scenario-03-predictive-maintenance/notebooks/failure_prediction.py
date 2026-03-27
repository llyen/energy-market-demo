# Databricks notebook source / Microsoft Fabric Notebook
# MAGIC %md
# MAGIC # Predykcja Awarii Turbin Wiatrowych
# MAGIC
# MAGIC Model ML do predykcji awarii turbin w horyzoncie 7 dni.
# MAGIC Wykorzystuje dane sensorowe z Eventhouse i historię awarii z Lakehouse.
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. Załaduj dane sensorowe i rejestr awarii
# MAGIC 2. Inżynieria cech (rolling averages, degradation rates)
# MAGIC 3. Trenowanie RandomForest Classifier
# MAGIC 4. Ewaluacja modelu (confusion matrix, precision, recall, AUC)
# MAGIC 5. Analiza ważności cech
# MAGIC 6. Predykcja na bieżących danych — turbiny zagrożone awarią

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Konfiguracja i import bibliotek

# COMMAND ----------

import warnings

import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

warnings.filterwarnings("ignore")
sns.set_theme(style="whitegrid")

RANDOM_STATE = 42
PREDICTION_HORIZON_DAYS = 7

print("Libraries loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ładowanie danych
# MAGIC
# MAGIC W środowisku Fabric dane są ładowane bezpośrednio z Lakehouse / Eventhouse.
# MAGIC Poniżej alternatywna ścieżka z plików CSV (dla testów lokalnych).

# COMMAND ----------

# --- Option A: Load from Fabric Lakehouse (uncomment in Fabric) ---
# sensor_df = spark.sql("SELECT * FROM LH_Maintenance.sensor_telemetry").toPandas()
# failure_df = spark.sql("SELECT * FROM LH_Maintenance.failure_log").toPandas()
# specs_df = spark.sql("SELECT * FROM LH_Maintenance.turbine_specs").toPandas()

# --- Option B: Load from local CSV files ---
import os

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

sensor_df = pd.read_csv(
    os.path.join(data_dir, "sensor_telemetry.csv"), parse_dates=["timestamp"]
)
failure_df = pd.read_csv(
    os.path.join(data_dir, "failure_log.csv"), parse_dates=["failure_date"]
)
specs_df = pd.read_csv(os.path.join(data_dir, "turbine_specs.csv"))

print(f"Sensor data: {sensor_df.shape[0]:,} rows, {sensor_df.shape[1]} columns")
print(f"Failure log: {failure_df.shape[0]:,} rows")
print(f"Turbine specs: {specs_df.shape[0]:,} rows")
print(f"\nSensor columns: {list(sensor_df.columns)}")
print(f"Date range: {sensor_df['timestamp'].min()} to {sensor_df['timestamp'].max()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Eksploracja danych

# COMMAND ----------

print("=== Status distribution ===")
print(sensor_df["status"].value_counts())
print(f"\n=== Failure severity distribution ===")
print(failure_df["severity"].value_counts())
print(f"\n=== Failures by component ===")
print(failure_df["component"].value_counts())
print(f"\n=== Farms ===")
print(sensor_df["farm_name"].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inżynieria cech (Feature Engineering)
# MAGIC
# MAGIC Tworzymy cechy opisujące stan turbiny w oknach czasowych:
# MAGIC - Średnie kroczące (6h, 12h, 24h) — wibracje, temperatury, moc
# MAGIC - Wskaźniki degradacji — tempo zmian parametrów
# MAGIC - Statystyki okna (std, min, max, percentyle)
# MAGIC - Etykiety: czy w ciągu 7 dni wystąpi awaria

# COMMAND ----------

def create_features(sensor_data: pd.DataFrame, failure_data: pd.DataFrame) -> pd.DataFrame:
    """
    Build feature matrix from sensor telemetry with rolling statistics
    and binary failure labels (7-day prediction horizon).
    """
    df = sensor_data.copy()
    df = df[df["status"] == "Operating"].copy()
    df.sort_values(["turbine_id", "timestamp"], inplace=True)

    # ── Rolling window features ─────────────────────────────────────
    sensor_cols = [
        "vibration_mm_s", "gearbox_temp_c", "bearing_temp_c",
        "generator_temp_c", "power_output_kw", "hydraulic_pressure_bar",
        "oil_viscosity", "rotor_rpm", "wind_speed_ms",
    ]

    # Window sizes: 6h = 72 readings (at 5min), 12h = 144, 24h = 288
    windows = {"6h": 72, "12h": 144, "24h": 288}

    for col in sensor_cols:
        for label, win in windows.items():
            grp = df.groupby("turbine_id")[col]
            df[f"{col}_mean_{label}"] = grp.transform(
                lambda x: x.rolling(win, min_periods=1).mean()
            )
            df[f"{col}_std_{label}"] = grp.transform(
                lambda x: x.rolling(win, min_periods=1).std().fillna(0)
            )

    # ── Degradation rate (slope over 24h window) ───────────────────
    for col in ["vibration_mm_s", "gearbox_temp_c", "bearing_temp_c"]:
        grp = df.groupby("turbine_id")[col]
        rolling_min = grp.transform(lambda x: x.rolling(288, min_periods=1).min())
        rolling_max = grp.transform(lambda x: x.rolling(288, min_periods=1).max())
        df[f"{col}_range_24h"] = rolling_max - rolling_min

        # Rate of change
        df[f"{col}_diff"] = grp.transform(lambda x: x.diff().fillna(0))

    # ── Derived features ────────────────────────────────────────────
    df["power_efficiency"] = np.where(
        df["wind_speed_ms"] > 3,
        df["power_output_kw"] / (df["wind_speed_ms"] ** 3 + 1) * 1000,
        0,
    )
    df["temp_vibration_product"] = df["bearing_temp_c"] * df["vibration_mm_s"]
    df["gearbox_bearing_temp_diff"] = df["gearbox_temp_c"] - df["bearing_temp_c"]

    # ── Create failure labels ───────────────────────────────────────
    # For each turbine, mark rows within PREDICTION_HORIZON_DAYS before a failure
    df["failure_within_7d"] = 0

    for _, fail_row in failure_data.iterrows():
        tid = fail_row["turbine_id"]
        fail_date = fail_row["failure_date"]
        horizon_start = fail_date - pd.Timedelta(days=PREDICTION_HORIZON_DAYS)

        mask = (
            (df["turbine_id"] == tid)
            & (df["timestamp"] >= horizon_start)
            & (df["timestamp"] <= fail_date)
        )
        df.loc[mask, "failure_within_7d"] = 1

    # ── Aggregate to hourly level for training ──────────────────────
    df["hour"] = df["timestamp"].dt.floor("h")
    feature_cols = [c for c in df.columns if c not in [
        "timestamp", "status", "hour", "nacelle_direction_deg",
    ]]

    numeric_cols = df[feature_cols].select_dtypes(include=[np.number]).columns.tolist()
    non_numeric = ["turbine_id", "farm_name"]

    agg_dict = {col: "mean" for col in numeric_cols if col not in non_numeric}
    agg_dict["failure_within_7d"] = "max"
    for col in non_numeric:
        if col in df.columns:
            agg_dict[col] = "first"

    hourly = df.groupby(["turbine_id", "hour"]).agg(agg_dict).reset_index()
    hourly.drop(columns=["hour"], inplace=True, errors="ignore")

    # Drop rows with NaN in key feature columns
    hourly.dropna(subset=[c for c in numeric_cols if c in hourly.columns], inplace=True)

    print(f"Feature matrix: {hourly.shape[0]:,} rows, {hourly.shape[1]} columns")
    print(f"Positive labels (failure within 7d): {hourly['failure_within_7d'].sum():,}")
    print(f"Negative labels: {(hourly['failure_within_7d'] == 0).sum():,}")

    return hourly


features_df = create_features(sensor_df, failure_df)
features_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Przygotowanie danych do treningu

# COMMAND ----------

# Select feature columns (exclude IDs and labels)
exclude_cols = ["turbine_id", "farm_name", "failure_within_7d"]
feature_columns = [
    c for c in features_df.select_dtypes(include=[np.number]).columns
    if c not in exclude_cols
]

X = features_df[feature_columns].copy()
y = features_df["failure_within_7d"].astype(int)

# Handle any remaining NaN/inf
X.replace([np.inf, -np.inf], np.nan, inplace=True)
X.fillna(X.median(), inplace=True)

print(f"Feature matrix shape: {X.shape}")
print(f"Label distribution:\n{y.value_counts()}")
print(f"Positive rate: {y.mean():.2%}")

# Train/test split (stratified)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=RANDOM_STATE, stratify=y
)

print(f"\nTrain set: {X_train.shape[0]:,} rows")
print(f"Test set:  {X_test.shape[0]:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Trenowanie modelu RandomForest

# COMMAND ----------

# Start MLflow experiment
mlflow.set_experiment("Wind_Turbine_Failure_Prediction")

with mlflow.start_run(run_name="RandomForest_v1") as run:
    # Log parameters
    params = {
        "n_estimators": 200,
        "max_depth": 15,
        "min_samples_split": 10,
        "min_samples_leaf": 5,
        "class_weight": "balanced",
        "random_state": RANDOM_STATE,
        "prediction_horizon_days": PREDICTION_HORIZON_DAYS,
        "n_features": len(feature_columns),
        "n_train_samples": X_train.shape[0],
        "n_test_samples": X_test.shape[0],
    }
    mlflow.log_params(params)

    # Train model
    model = RandomForestClassifier(
        n_estimators=params["n_estimators"],
        max_depth=params["max_depth"],
        min_samples_split=params["min_samples_split"],
        min_samples_leaf=params["min_samples_leaf"],
        class_weight=params["class_weight"],
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # Predictions
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    # Metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    try:
        auc = roc_auc_score(y_test, y_prob)
    except ValueError:
        auc = 0.0

    mlflow.log_metrics({
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "auc_roc": auc,
    })

    # Log model
    mlflow.sklearn.log_model(model, "failure_prediction_model")

    print("=" * 50)
    print("MODEL EVALUATION")
    print("=" * 50)
    print(f"Accuracy:  {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1 Score:  {f1:.4f}")
    print(f"AUC-ROC:   {auc:.4f}")
    print(f"\nMLflow Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Macierz pomyłek i raport klasyfikacji

# COMMAND ----------

print("Classification Report:")
print("=" * 50)
target_names = ["No Failure (0)", "Failure within 7d (1)"]
print(classification_report(y_test, y_pred, target_names=target_names, zero_division=0))

# Confusion Matrix
cm = confusion_matrix(y_test, y_pred)
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Confusion matrix heatmap
sns.heatmap(
    cm, annot=True, fmt="d", cmap="Blues",
    xticklabels=target_names, yticklabels=target_names,
    ax=axes[0],
)
axes[0].set_title("Macierz pomyłek (Confusion Matrix)")
axes[0].set_ylabel("Wartość rzeczywista")
axes[0].set_xlabel("Predykcja modelu")

# ROC curve
if auc > 0:
    fpr, tpr, _ = roc_curve(y_test, y_prob)
    axes[1].plot(fpr, tpr, color="darkorange", lw=2,
                 label=f"ROC curve (AUC = {auc:.3f})")
    axes[1].plot([0, 1], [0, 1], color="gray", lw=1, linestyle="--")
    axes[1].set_xlim([0.0, 1.0])
    axes[1].set_ylim([0.0, 1.05])
    axes[1].set_xlabel("False Positive Rate")
    axes[1].set_ylabel("True Positive Rate")
    axes[1].set_title("Krzywa ROC")
    axes[1].legend(loc="lower right")

plt.tight_layout()
plt.savefig(os.path.join(data_dir, "model_evaluation.png"), dpi=150, bbox_inches="tight")
plt.show()
print("Evaluation chart saved to data/model_evaluation.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Ważność cech (Feature Importance)

# COMMAND ----------

importances = model.feature_importances_
feature_importance_df = pd.DataFrame({
    "feature": feature_columns,
    "importance": importances,
}).sort_values("importance", ascending=False)

print("Top 20 Features by Importance:")
print("=" * 60)
for i, row in feature_importance_df.head(20).iterrows():
    bar = "█" * int(row["importance"] * 200)
    print(f"  {row['feature']:45s} {row['importance']:.4f}  {bar}")

# Plot
fig, ax = plt.subplots(figsize=(10, 8))
top_n = 20
top_features = feature_importance_df.head(top_n)
sns.barplot(
    data=top_features, x="importance", y="feature",
    palette="viridis", ax=ax,
)
ax.set_title(f"Top {top_n} — Ważność cech (Feature Importance)")
ax.set_xlabel("Importance")
ax.set_ylabel("")
plt.tight_layout()
plt.savefig(os.path.join(data_dir, "feature_importance.png"), dpi=150, bbox_inches="tight")
plt.show()
print("Feature importance chart saved to data/feature_importance.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Predykcje na bieżących danych — turbiny zagrożone awarią

# COMMAND ----------

# Get the most recent readings per turbine
latest_readings = sensor_df.sort_values("timestamp").groupby("turbine_id").tail(12)

# Aggregate to single row per turbine
agg_latest = latest_readings.groupby("turbine_id").agg({
    "farm_name": "first",
    **{col: "mean" for col in latest_readings.select_dtypes(include=[np.number]).columns},
}).reset_index()

# Build features for current data
current_features = agg_latest.reindex(columns=["turbine_id", "farm_name"] + feature_columns)
X_current = current_features[feature_columns].copy()
X_current.replace([np.inf, -np.inf], np.nan, inplace=True)
X_current.fillna(X_current.median(), inplace=True)

# Predict
risk_probs = model.predict_proba(X_current)[:, 1]
risk_preds = model.predict(X_current)

risk_df = pd.DataFrame({
    "turbine_id": agg_latest["turbine_id"],
    "farm_name": agg_latest["farm_name"],
    "failure_probability": np.round(risk_probs, 4),
    "failure_predicted": risk_preds,
    "vibration_mm_s": np.round(agg_latest.get("vibration_mm_s", 0), 2),
    "bearing_temp_c": np.round(agg_latest.get("bearing_temp_c", 0), 1),
    "gearbox_temp_c": np.round(agg_latest.get("gearbox_temp_c", 0), 1),
    "power_output_kw": np.round(agg_latest.get("power_output_kw", 0), 1),
})

risk_df.sort_values("failure_probability", ascending=False, inplace=True)

print("=" * 70)
print("TURBINES AT RISK — Failure Prediction (7-day horizon)")
print("=" * 70)

at_risk = risk_df[risk_df["failure_probability"] > 0.3]
if len(at_risk) > 0:
    print(f"\n⚠️  {len(at_risk)} turbines with elevated failure risk (probability > 30%):\n")
    for _, row in at_risk.iterrows():
        risk_icon = "🔴" if row["failure_probability"] > 0.7 else (
            "🟠" if row["failure_probability"] > 0.5 else "🟡"
        )
        print(
            f"  {risk_icon} {row['turbine_id']} ({row['farm_name']})"
            f"  P={row['failure_probability']:.1%}"
            f"  | vib={row['vibration_mm_s']:.1f} mm/s"
            f"  | bearing={row['bearing_temp_c']:.0f}°C"
            f"  | gearbox={row['gearbox_temp_c']:.0f}°C"
        )
else:
    print("\n✅ No turbines with elevated failure risk at this time.")

print(f"\nTotal turbines analyzed: {len(risk_df)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Eksport wyników do Lakehouse

# COMMAND ----------

# Save risk assessment to CSV (in Fabric, write to Lakehouse Delta table)
risk_output_path = os.path.join(data_dir, "current_risk_assessment.csv")
risk_df.to_csv(risk_output_path, index=False)
print(f"Risk assessment saved to: {risk_output_path}")

# --- In Fabric, write to Delta table: ---
# spark_risk_df = spark.createDataFrame(risk_df)
# spark_risk_df.write.mode("overwrite").format("delta").saveAsTable(
#     "LH_Maintenance.current_risk_assessment"
# )
# print("Risk assessment written to LH_Maintenance.current_risk_assessment")

# Save feature importance
importance_output_path = os.path.join(data_dir, "feature_importance.csv")
feature_importance_df.to_csv(importance_output_path, index=False)
print(f"Feature importance saved to: {importance_output_path}")

print("\n✅ Pipeline complete. Model registered in MLflow.")
print(f"   Turbines at risk (P > 30%): {len(at_risk)}")
