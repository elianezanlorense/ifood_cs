import streamlit as st
from pathlib import Path
import pandas as pd
BASE_PATH = Path("/Users/maceli/ifood_cs/dados") 
df = pd.read_parquet(BASE_PATH / "gold" / "df_app.parquet")
  

estado = st.selectbox("Escolha o estado", df["weekday"].unique())

df_filtrado = df[df["weekday"] == estado]

st.dataframe(df_filtrado)
