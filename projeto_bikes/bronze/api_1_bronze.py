# Databricks notebook source
# ============================================
# CAMADA BRONZE
# Objetivo: Ingestão dos dados crus (raw)
# ============================================


# 1. Imports
import pandas as pd

# 2. Configurações iniciais
caminho_arquivo = "/Volumes/workspace/default/arquivos-projetos/london_merged.csv"

# 3. Leitura dos dados crus
df_bronze = pd.read_csv(caminho_arquivo)

# 4. Exibir amostra dos dados crus
print("Dados crus - camada Bronze")
display(df_bronze.head())

#salvar em formato parquet para futuras camadas
df_bronze.to_parquet("../bronze/london_bike_bronze.parquet", index=False)


