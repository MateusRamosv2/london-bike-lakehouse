# Databricks notebook source
# ============================================
# CAMADA SILVER
# Objetivo: Limpeza, tratamento e enriquecimento
# ============================================


import pandas as pd

caminho_bronze = "/Volumes/Workspace/default/arquivos-projetos/london_merged.csv"
df = pd.read_csv(caminho_bronze)


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

print(df.head())

display(df)

# 6. (Opcional) salvar dataset tratado
# df.to_parquet("/Volumes/workspace/default/silver/london_bike_silver.parquet", index=False)

