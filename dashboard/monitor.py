import streamlit as st
import pandas as pd
import plotly.express as px
import time

# Page Config
st.set_page_config(page_title="Bluegrey Live Monitor", layout="wide", page_icon="📈")

st.title("⚡ Bluegrey StatArb Engine")

# Auto-Refresh Logic (The dashboard will reload every 2 seconds)
if st.button('🔄 Refresh Data Now'):
    st.rerun()

# 1. Load Data
try:
    df = pd.read_csv('data/live_monitor.csv')
    # Convert timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    latest = df.iloc[-1]
except FileNotFoundError:
    st.warning("⚠️ Waiting for Bot to start... (No data file found)")
    st.stop()

# 2. Key Metrics Row
col1, col2, col3, col4 = st.columns(4)
col1.metric("Amazon (AMZN)", f"${latest['AMZN']:.2f}")
col2.metric("Tesla (TSLA)", f"${latest['TSLA']:.2f}")
col3.metric("Spread Value", f"{latest['Spread']:.2f}")

# Z-Score Color Logic
z_val = latest['Z-Score']
z_color = "normal"
if z_val > 2.0: z_color = "inverse" # Red
if z_val < -2.0: z_color = "off"     # Green (conceptually)

col4.metric("Live Z-Score", f"{z_val:.2f}", delta="Signal Strength", delta_color=z_color)

# 3. The "Heartbeat" Chart
st.subheader("🧬 Strategy Pulse (Z-Score)")

# We create a reference zone for the Entry Thresholds (+2 and -2)
fig = px.line(df, x='Timestamp', y='Z-Score', title='Mean Reversion Tracking')
fig.add_hline(y=2.0, line_dash="dash", line_color="red", annotation_text="Short Threshold")
fig.add_hline(y=-2.0, line_dash="dash", line_color="green", annotation_text="Long Threshold")
fig.add_hline(y=0.0, line_dash="dot", line_color="white", opacity=0.5)

fig.update_layout(template="plotly_dark", height=400)
st.plotly_chart(fig, use_container_width=True)

# 4. Raw Data Table
with st.expander("📝 Raw Data Log"):
    st.dataframe(df.sort_values(by='Timestamp', ascending=False).head(10))

# Auto-Rerun loop (Trick to make Streamlit look "Live")
time.sleep(2)
st.rerun()