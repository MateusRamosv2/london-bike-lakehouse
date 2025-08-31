# Databricks notebook source
# ============================================
# CAMADA GOLD
# Objetivo: Agregações e métricas de negócio
# ============================================

import pandas as pd

caminho_silver = "../silver/london_bike_silver.parquet"


df = pd.read_parquet(caminho_silver)

# print("Dimensões do dataset Gold (entrada):", df.shape)
# display(df.head)

# 2. Criação de colunas legíveis a partir do timestamp

df["data_hora"] = pd.to_datetime(df["timestamp"]).dt.strftime("%d/%m/%Y %H:%M")
df["data"] = pd.to_datetime(df["timestamp"]).dt.strftime("%d/%m/%Y")
df["hora"] = pd.to_datetime(df["timestamp"]).dt.strftime("%H:%M")
df["ano"] = pd.to_datetime(df["timestamp"]).dt.year
df["mes"] = pd.to_datetime(df["timestamp"]).dt.month
df["dia_da_semana"] = pd.to_datetime(df["timestamp"]).dt.day

df = df.drop(columns=["timestamp","year","month","day","hour","weekday"])

# display(df)

# 3. Construção de datasets analíticos

# Agregações básicas

df_agg_mes = df.groupby(["ano", "mes"])["qtd_bikes"].sum().reset_index()
df_agg_weekday = df.groupby("dia_da_semana")["qtd_bikes"].mean().reset_index()
df_agg_hora = df.groupby("hora")["qtd_bikes"].mean().reset_index()

# display(df_agg_mes)
# display(df_agg_weekday)
# display(df_agg_hora)

# df_corr = df[["qtd_bikes", "temp_real", "temp_percebida", "umidade", "velocidade_vento"]]

df_corr = df[["qtd_bikes", "temp_real", "temp_percebida", "umidade", "velocidade_vento"]].corr()


# caminho_gold = "../gold/london_bike_gold.parquet"

# Salva tanto as agregações quanto o dataset principal com colunas legíveis
# df.to_parquet(caminho_gold + "london_bike_gold_base.parquet", index=False)
# df_agg_mes.to_parquet(caminho_gold + "london_bike_gold_mes.parquet", index=False)
# df_agg_weekday.to_parquet(caminho_gold + "london_bike_gold_weekday.parquet", index=False)
# df_agg_hora.to_parquet(caminho_gold + "london_bike_gold_hora.parquet", index=False)
# df_corr.to_parquet(caminho_gold + "london_bike_gold_corr.parquet", index=True)

print("\n Dataset Gold processado e salvo com sucesso!")

display(df)

# df.to_parquet("../gold/london_bike_gold.parquet", index=False)

