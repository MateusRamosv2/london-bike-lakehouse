# Databricks notebook source
# ============================================
# CAMADA SILVER
# Objetivo: Limpeza, tratamento e enriquecimento
# ============================================


import pandas as pd

caminho_bronze = "../bronze/london_bike_bronze.parquet"
df = pd.read_parquet(caminho_bronze)


df["timestamp"] = pd.to_datetime(df["timestamp"])


df["year"] = df["timestamp"].dt.year
df["month"] = df["timestamp"].dt.month
df["day"] = df["timestamp"].dt.day
df["hour"] = df["timestamp"].dt.hour
df["weekday"] = df["timestamp"].dt.weekday

df = df.rename(columns={
    "cnt" : "qtd_bikes",
    "t1" : "temp_real",
    "t2" : "temp_percebida",
    "hum" : "umidade",
    "wind_speed" : "velocidade_vento",
    "weather_code" : "codigo_clima",
    "is_holiday" : "feriado",
    "is_weekend" : "final_semana",
    "season" : "estacao"
 })

# print(df.head())

# display(df)

# 6. (Opcional) salvar dataset tratado
# Salva a versão tratada em Silver
df.to_parquet("../silver/london_bike_silver.parquet", index=False)
