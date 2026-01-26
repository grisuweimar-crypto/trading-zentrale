import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Trading Zentrale Online", layout="wide")

st.title("🚀 Trading Zentrale: Elliott-Scanner")

if os.path.exists("watchlist.csv"):
    df = pd.read_csv("watchlist.csv")
    
    # Sortierung nach Score
    if 'Score' in df.columns:
        df = df.sort_values(by="Score", ascending=False)

    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.warning("Noch keine watchlist.csv gefunden. Lass den Scanner in Thonny laufen!")