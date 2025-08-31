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

print(f" Mês com maior número de viagens: {mes_top['mes'].values[0]}/{mes_top['ano'].values[0]}")
print(f" Hora do dia com maior movimento: {hora_top['hora'].values[0]}h")
print(f" Correlação uso x temperatura: {corr_temp:.2f}")
print(f" Correlação uso x umidade: {corr_umidade:.2f}")


plt.figure(figsize=(10,5))
plt.plot(df_agg_mes["mes"].astype(str) + "/" + df_agg_mes["ano"].astype(str),
         df_agg_mes["qtd_bikes"], marker="o")
plt.xticks(rotation=45)
plt.title("Quantidade de Bikes por Mês")
plt.xlabel("Mês/Ano")
plt.ylabel("Qtd de Bike")
plt.show()

# Média de bikes por hora do dia
plt.figure(figsize=(10,5))
plt.bar(df_agg_hora["hora"], df_agg_hora["qtd_bikes"])
plt.title("Média de Bikes por Hora do Dia")
plt.xlabel("Hora")
plt.ylabel("Qtd. Média de Bikes")
plt.show()
