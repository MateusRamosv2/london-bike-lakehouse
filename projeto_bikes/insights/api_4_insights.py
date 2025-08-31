# Databricks notebook source
# =============================================
# NOTEBOOK DE INSIGHTS (versão final)
# Objetivo: Exploração, métricas e visualização
# =============================================


import pandas as pd
import matplotlib.pyplot as plt

caminho_gold = "../gold/london_bike_gold.parquet"

df = pd.read_parquet(caminho_gold)


df_agg_mes = pd.read_parquet(caminho_gold + "london_bike_gold_mes.parquet")
df_agg_weekday = pd.read_parquet(caminho_gold + "london_bike_gold_weekday.parquet")
df_agg_hora = pd.read_parquet(caminho_gold + "london_bike_gold_hora.parquet")
df_corr = pd.read_parquet(caminho_gold + "london_bike_gold_corr.parquet")

# 2. Insights principais

mes_top = df_agg_mes.sort_values('qtd_bikes', ascending=False).head(1)

hora_top = df_agg_hora.sort_values('qtd_bikes', ascending=False).head(1)

corr_temp = df_corr.loc['qtd_bikes', "temp_real"]

corr_umidade = df_corr.loc["qtd_bikes", "umidade"]
