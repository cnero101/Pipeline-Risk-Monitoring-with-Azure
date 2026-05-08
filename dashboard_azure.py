"""
dashboard_azure.py
Real-Time Pipeline Risk Monitor — Azure Edition

Reads Parquet files from Azure Data Lake Storage Gen2 and displays
live pipeline status, risk distribution, and sensor trends.

Usage:
    streamlit run dashboard_azure.py

Environment variables (set in .env or export directly):
    STORAGE_CONN_STR — Azure Storage Account connection string
"""

import os
from io import BytesIO

import pandas as pd
import plotly.express as px
import streamlit as st
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

load_dotenv()

STORAGE_CONN_STR = os.getenv("STORAGE_CONN_STR")
CONTAINER        = "sensordata"

PIPES = ["P-101", "P-102", "P-103", "P-104", "P-105"]


# ── Data Loading ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_data() -> pd.DataFrame:
    """
    Load the last 10 Parquet files per pipeline from ADLS Gen2.
    Only scans today's partition folder to keep load times fast.
    """
    if not STORAGE_CONN_STR:
        st.error("STORAGE_CONN_STR is not set. Check your .env file.")
        return pd.DataFrame()

    try:
        client           = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        container_client = client.get_container_client(CONTAINER)
        now              = pd.Timestamp.utcnow()
        frames           = []

        for pipe in PIPES:
            prefix = (
                f"processed_data/pipe_id={pipe}/"
                f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            )
            blobs = list(container_client.list_blobs(name_starts_with=prefix))
            blobs = [b for b in blobs if b.size > 0]
            # Take only the most recent 10 blobs per pipe
            recent = blobs[-10:] if len(blobs) > 10 else blobs

            for blob in recent:
                try:
                    b   = container_client.get_blob_client(blob.name)
                    buf = BytesIO(b.download_blob().readall())
                    frames.append(pd.read_parquet(buf))
                except Exception:
                    continue

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
        df = df.sort_values("timestamp")

        # Filter to last 30 minutes
        cutoff = pd.to_datetime("now") - pd.Timedelta(minutes=30)
        df = df[df["timestamp"] >= cutoff]
        return df

    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


# ── Page Config ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Pipeline Risk Monitor",
    page_icon="🛢️",
    layout="wide"
)

# Auto-refresh every 30 seconds
st_autorefresh(interval=30000, limit=None, key="autorefresh")

# ── Header ─────────────────────────────────────────────────────────────────────

st.markdown("""
    <div style='background-color:#1a1a2e;padding:20px 24px;border-radius:8px;margin-bottom:24px'>
        <h1 style='color:white;margin:0;font-size:24px;letter-spacing:1px'>
            🛢️ REAL-TIME PIPELINE RISK MONITOR
        </h1>
        <p style='color:#aaa;margin:4px 0 0 0;font-size:13px'>
            Alberta Oil & Gas Pipelines — Azure Edition
        </p>
    </div>
""", unsafe_allow_html=True)

# ── Load Data ──────────────────────────────────────────────────────────────────

df = load_data()

if df.empty:
    st.warning("No data yet. Start the sensor simulator.")
    st.stop()

# ── Pipeline Status Cards ──────────────────────────────────────────────────────

st.subheader("Pipeline Status Overview")

latest = df.sort_values("timestamp").groupby("pipe_id").last().reset_index()
cols   = st.columns(len(latest))

for i, row in latest.iterrows():
    risk  = row["risk_level"]
    color = {"Normal": "#28a745", "Anomaly": "#fd7e14", "Critical": "#dc3545"}.get(risk, "#888")
    icon  = {"Normal": "✅", "Anomaly": "⚠️", "Critical": "🚨"}.get(risk, "❓")
    cols[i].markdown(f"""
        <div style='background-color:#1e1e2e;border-left:5px solid {color};
                    border-radius:8px;padding:14px 16px;margin-bottom:8px'>
            <div style='font-size:15px;font-weight:bold;color:white'>{icon} {row["pipe_id"]}</div>
            <div style='font-size:12px;color:#aaa;margin-top:4px'>
                <span style='background:{color};color:white;padding:2px 8px;
                             border-radius:4px;font-size:11px'>{risk}</span>
            </div>
            <div style='margin-top:10px;font-size:12px;color:#ccc;line-height:1.8'>
                🔵 Pressure: <b>{row["pressure_MPa"]} MPa</b><br>
                🌡️ Temp: <b>{row["temperature_C"]} °C</b><br>
                💧 Flow: <b>{row["flow_rate_percent"]} %</b><br>
                🕐 <span style='color:#888'>{str(row["timestamp"])[:19]}</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

st.divider()

# ── KPI Row ────────────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Records",   len(df))
c2.metric("Pipes Monitored", df["pipe_id"].nunique())
c3.metric("Normal",          int((df.risk_level == "Normal").sum()))
c4.metric("Anomaly",         int((df.risk_level == "Anomaly").sum()))
c5.metric("Critical",        int((df.risk_level == "Critical").sum()))

st.divider()

# ── Charts ─────────────────────────────────────────────────────────────────────

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Risk Distribution")
    fig_pie = px.pie(
        df, names="risk_level",
        color="risk_level",
        color_discrete_map={"Normal": "#28a745", "Anomaly": "#fd7e14", "Critical": "#dc3545"},
        hole=0.4
    )
    fig_pie.update_layout(margin=dict(t=20, b=20))
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.subheader("Risk Events Over Time")
    risk_only = df[df.risk_level != "Normal"].copy()
    if not risk_only.empty:
        fig_timeline = px.scatter(
            risk_only, x="timestamp", y="pipe_id",
            color="risk_level",
            color_discrete_map={"Anomaly": "#fd7e14", "Critical": "#dc3545"},
        )
        fig_timeline.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("No anomalies or critical events in the last 30 minutes.")

st.divider()

# ── Sensor Trends ──────────────────────────────────────────────────────────────

st.subheader("Sensor Trends by Pipeline")

pipe_options  = ["All Pipes"] + sorted(df["pipe_id"].unique().tolist())
selected_pipe = st.selectbox("Select Pipeline", pipe_options)
filtered      = df if selected_pipe == "All Pipes" else df[df.pipe_id == selected_pipe]

tab1, tab2, tab3 = st.tabs(["Pressure (MPa)", "Temperature (°C)", "Flow Rate (%)"])

with tab1:
    st.plotly_chart(
        px.line(filtered, x="timestamp", y="pressure_MPa", color="pipe_id", title="Pressure over Time"),
        use_container_width=True
    )
with tab2:
    st.plotly_chart(
        px.line(filtered, x="timestamp", y="temperature_C", color="pipe_id", title="Temperature over Time"),
        use_container_width=True
    )
with tab3:
    st.plotly_chart(
        px.line(filtered, x="timestamp", y="flow_rate_percent", color="pipe_id", title="Flow Rate over Time"),
        use_container_width=True
    )

st.divider()

# ── Recent Alerts Table ────────────────────────────────────────────────────────

st.subheader("Recent Risk Events")
alerts = df[df.risk_level != "Normal"].sort_values("timestamp", ascending=False).head(20)
if not alerts.empty:
    st.dataframe(
        alerts[["timestamp", "pipe_id", "risk_level", "pressure_MPa", "temperature_C", "flow_rate_percent"]],
        use_container_width=True
    )
else:
    st.info("No risk events in the last 30 minutes.")

# ── Raw Data Expander ──────────────────────────────────────────────────────────

with st.expander("View Raw Data (last 100 records)"):
    st.dataframe(df.tail(100), use_container_width=True)

# ── Footer ─────────────────────────────────────────────────────────────────────

st.caption("Showing last 30 minutes of data. Auto-refreshes every 30 seconds.")
if st.button("Refresh Now"):
    st.cache_data.clear()
    st.rerun()
