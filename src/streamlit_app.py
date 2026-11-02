"""Streamlit dashboard reading predictions from MongoDB."""
import json

import pandas as pd
import plotly.express as px
import pymongo
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:  # optional dependency
    st_autorefresh = None

st.set_page_config(page_title="Paris Traffic Dashboard", layout="wide")
st.title("Paris traffic state monitoring")

REFRESH_SECONDS = 60
if st_autorefresh is not None:
    st_autorefresh(interval=REFRESH_SECONDS * 1000, key="auto_refresh")


@st.cache_resource
def get_collection():
    client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000)
    return client["traffic"]["predictions"]


@st.cache_data(ttl=REFRESH_SECONDS)
def load_data():
    return pd.DataFrame(list(get_collection().find()))


try:
    df = load_data()
except pymongo.errors.PyMongoError as exc:
    st.error(f"Cannot connect to MongoDB: {exc}")
    st.stop()

if df.empty:
    st.warning("No predictions in MongoDB yet. Start spark_predictor.py and kafka_producer.py first.")
    st.stop()

df = df.drop(columns=["_id"], errors="ignore")


def extract_lat_lon(gps_str):
    try:
        coords = json.loads(gps_str)
        return pd.Series({"latitude": coords["lat"], "longitude": coords["lon"]})
    except (TypeError, ValueError, KeyError):
        return pd.Series({"latitude": None, "longitude": None})


if "geo_point_2d" in df.columns:
    df = pd.concat([df, df["geo_point_2d"].apply(extract_lat_lon)], axis=1)
    df = df.dropna(subset=["latitude", "longitude"])

df["timestamp"] = pd.to_datetime(df["t_1h"], errors="coerce")
df = df.dropna(subset=["timestamp"])
df["hour"] = df["timestamp"].dt.hour

# Fallback for old documents without "etat_predit": fixed index mapping
status_map = {0: "Fluide", 1: "Pre-sature", 2: "Sature", 3: "Bloque"}
if "etat_predit" in df.columns:
    df["etat_trafic_prediction"] = df["etat_predit"]
else:
    df["etat_trafic_prediction"] = df["prediction"].map(status_map)

if df.empty:
    st.warning("No valid rows left after cleaning (check coordinates and timestamps).")
    st.stop()

# ---------------- Filters ----------------
st.sidebar.header("Filters")
hours = sorted(df["hour"].dropna().unique())
states = sorted(df["etat_trafic_prediction"].dropna().unique())

selected_hours = st.sidebar.multiselect("Hours", hours, default=hours)
selected_states = st.sidebar.multiselect("Predicted traffic states", states, default=states)

df_filtered = df[df["hour"].isin(selected_hours) & df["etat_trafic_prediction"].isin(selected_states)]

# ---------------- KPIs ----------------
st.subheader("Number of predictions per state (filtered)")
cols = st.columns(max(len(states), 1))
for i, status in enumerate(states):
    count = int((df_filtered["etat_trafic_prediction"] == status).sum())
    cols[i].metric(status, count)

# ---------------- Hourly distribution ----------------
st.subheader("Hourly distribution of predictions")
if not df_filtered.empty:
    fig = px.histogram(
        df_filtered, x="hour", color="etat_trafic_prediction",
        title="Traffic state distribution by hour", barmode="group",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data matches the selected filters.")

# ---------------- Map ----------------
st.subheader("Map of predicted traffic locations")
if not df_filtered.empty:
    st.map(df_filtered[["latitude", "longitude"]])
else:
    st.warning("No coordinates available after filtering.")

# ---------------- Trend ----------------
st.subheader("Prediction volume over time")
if not df_filtered.empty:
    df_trend = (
        df_filtered.groupby(["timestamp", "etat_trafic_prediction"])
        .size().reset_index(name="count")
    )
    fig_line = px.line(
        df_trend, x="timestamp", y="count", color="etat_trafic_prediction",
        title="Predicted states over time",
    )
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.info("No data available for the trend chart.")

# ---------------- Table ----------------
st.subheader("Filtered data preview")
st.dataframe(
    df_filtered.sort_values("timestamp", ascending=False).reset_index(drop=True),
    use_container_width=True,
    hide_index=True,
)

# ---------------- CSV export ----------------
st.subheader("Download filtered data")
csv = df_filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download CSV",
    data=csv,
    file_name="filtered_traffic_data.csv",
    mime="text/csv",
)

st.caption(f"Data refreshes automatically every {REFRESH_SECONDS} seconds")
EOFcat > src/streamlit_app.py <<'EOF'
"""Streamlit dashboard reading predictions from MongoDB."""
import json

import pandas as pd
import plotly.express as px
import pymongo
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:  # optional dependency
    st_autorefresh = None

st.set_page_config(page_title="Paris Traffic Dashboard", layout="wide")
st.title("Paris traffic state monitoring")

REFRESH_SECONDS = 60
if st_autorefresh is not None:
    st_autorefresh(interval=REFRESH_SECONDS * 1000, key="auto_refresh")


@st.cache_resource
def get_collection():
    client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000)
    return client["traffic"]["predictions"]


@st.cache_data(ttl=REFRESH_SECONDS)
def load_data():
    return pd.DataFrame(list(get_collection().find()))


try:
    df = load_data()
except pymongo.errors.PyMongoError as exc:
    st.error(f"Cannot connect to MongoDB: {exc}")
    st.stop()

if df.empty:
    st.warning("No predictions in MongoDB yet. Start spark_predictor.py and kafka_producer.py first.")
    st.stop()

df = df.drop(columns=["_id"], errors="ignore")


def extract_lat_lon(gps_str):
    try:
        coords = json.loads(gps_str)
        return pd.Series({"latitude": coords["lat"], "longitude": coords["lon"]})
    except (TypeError, ValueError, KeyError):
        return pd.Series({"latitude": None, "longitude": None})


if "geo_point_2d" in df.columns:
    df = pd.concat([df, df["geo_point_2d"].apply(extract_lat_lon)], axis=1)
    df = df.dropna(subset=["latitude", "longitude"])

df["timestamp"] = pd.to_datetime(df["t_1h"], errors="coerce")
df = df.dropna(subset=["timestamp"])
df["hour"] = df["timestamp"].dt.hour

# Fallback for old documents without "etat_predit": fixed index mapping
status_map = {0: "Fluide", 1: "Pre-sature", 2: "Sature", 3: "Bloque"}
if "etat_predit" in df.columns:
    df["etat_trafic_prediction"] = df["etat_predit"]
else:
    df["etat_trafic_prediction"] = df["prediction"].map(status_map)

if df.empty:
    st.warning("No valid rows left after cleaning (check coordinates and timestamps).")
    st.stop()

# ---------------- Filters ----------------
st.sidebar.header("Filters")
hours = sorted(df["hour"].dropna().unique())
states = sorted(df["etat_trafic_prediction"].dropna().unique())

selected_hours = st.sidebar.multiselect("Hours", hours, default=hours)
selected_states = st.sidebar.multiselect("Predicted traffic states", states, default=states)

df_filtered = df[df["hour"].isin(selected_hours) & df["etat_trafic_prediction"].isin(selected_states)]

# ---------------- KPIs ----------------
st.subheader("Number of predictions per state (filtered)")
cols = st.columns(max(len(states), 1))
for i, status in enumerate(states):
    count = int((df_filtered["etat_trafic_prediction"] == status).sum())
    cols[i].metric(status, count)

# ---------------- Hourly distribution ----------------
st.subheader("Hourly distribution of predictions")
if not df_filtered.empty:
    fig = px.histogram(
        df_filtered, x="hour", color="etat_trafic_prediction",
        title="Traffic state distribution by hour", barmode="group",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data matches the selected filters.")

# ---------------- Map ----------------
st.subheader("Map of predicted traffic locations")
if not df_filtered.empty:
    st.map(df_filtered[["latitude", "longitude"]])
else:
    st.warning("No coordinates available after filtering.")

# ---------------- Trend ----------------
st.subheader("Prediction volume over time")
if not df_filtered.empty:
    df_trend = (
        df_filtered.groupby(["timestamp", "etat_trafic_prediction"])
        .size().reset_index(name="count")
    )
    fig_line = px.line(
        df_trend, x="timestamp", y="count", color="etat_trafic_prediction",
        title="Predicted states over time",
    )
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.info("No data available for the trend chart.")

# ---------------- Table ----------------
st.subheader("Filtered data preview")
st.dataframe(
    df_filtered.sort_values("timestamp", ascending=False).reset_index(drop=True),
    use_container_width=True,
    hide_index=True,
)

# ---------------- CSV export ----------------
st.subheader("Download filtered data")
csv = df_filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download CSV",
    data=csv,
    file_name="filtered_traffic_data.csv",
    mime="text/csv",
)

st.caption(f"Data refreshes automatically every {REFRESH_SECONDS} seconds")
