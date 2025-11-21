import streamlit as st
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
BASE_PATH = Path("/Users/maceli/ifood_cs/dados") 
df = pd.read_parquet(BASE_PATH / "gold" / "df_app.parquet")
  
dia = st.selectbox("Escolha o dia da semana", sorted(df["weekday"].unique()))

df_filtrado = df[df["weekday"] == dia]
df_grafico = (
    df_filtrado
      .groupby("hour")["total_clientes"]
      .count()
      .reset_index()
      .rename(columns={"total_clientes": "qtd_clientes"})
)

fig, ax = plt.subplots()
ax.bar(df_grafico["hour"], df_grafico["qtd_clientes"])
ax.set_xlabel("Hora do dia")
ax.set_ylabel("Quantidade de pedidos")
ax.set_title(f"Pedidos por hora — {dia}")

st.pyplot(fig)