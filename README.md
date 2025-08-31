#  London Bike Lakehouse

## Visão Geral
Este projeto implementa um **pipeline de dados no Databricks (Free Edition)** usando a **Arquitetura Medallion (Bronze → Silver → Gold)** para processar o dataset público **London Bike Sharing (Kaggle)**.  
O objetivo é transformar os dados crus de uso de bicicletas em **datasets analíticos prontos para consumo**, permitindo responder perguntas de negócio como sazonalidade de uso, picos por horário e impacto do clima.  
O resultado final inclui **arquivos Parquet por camada** e um **notebook de Insights** com análises e visualizações.

---

## Arquitetura do Pipeline de Dados

### Bronze (Raw Data)
- **O que faz:** ingestão dos dados **exatamente como recebidos** do Kaggle.
- **Como:** leitura do `london_merged.csv` e persistência como Parquet.
- **Saída:** `../bronze/london_bike_bronze.parquet`.

```python
# api_1_bronze.py
import pandas as pd
df_bronze = pd.read_csv("../london_merged.csv")
df_bronze.to_parquet("../bronze/london_bike_bronze.parquet", index=False)
```

---


### Silver (Cleansed / Validated)

- **O que faz: limpeza, padronização e enriquecimento**.

- **Principais tratamentos:**
  * Conversão de timestamp para datetime.

  * Criação de colunas derivadas: year, month, day, hour, weekday.

  * Padronização de nomes (português): cnt → qtd_bikes, t1 → temp_real, t2 → temp_percebida, etc.


**Saída:** ```../silver/london_bike_silver.parquet.```

```python
# api_2_silver.py
import pandas as pd

df = pd.read_parquet("../bronze/london_bike_bronze.parquet")
df["timestamp"] = pd.to_datetime(df["timestamp"])

# features de tempo
df["year"] = df["timestamp"].dt.year
df["month"] = df["timestamp"].dt.month
df["day"] = df["timestamp"].dt.day
df["hour"] = df["timestamp"].dt.hour
df["weekday"] = df["timestamp"].dt.weekday

# padronização de nomes
df = df.rename(columns={
    "cnt": "qtd_bikes",
    "t1": "temp_real",
    "t2": "temp_percebida",
    "hum": "umidade",
    "wind_speed": "velocidade_vento",
    "weather_code": "codigo_clima",
    "is_holiday": "feriado",
    "is_weekend": "final_semana",
    "season": "estacao"
})

df.to_parquet("../silver/london_bike_silver.parquet", index=False)

```

---


### Gold (Business-Ready)

- **O que faz: prepara dados legíveis ao negócio e remove duplicidades técnicas.**

- **Principais passos:**
  * Criação de colunas legíveis: data_hora, data, hora, ano, mes, dia_da_semana.

  * Remoção das colunas técnicas originais (timestamp, year, month, day, hour, weekday).

  * (Opcional) Agregações por mês, hora e dia da semana.

**Saída:** ```../gold/london_bike_gold.parquet.```


```python
# api_3_gold.py
import pandas as pd

df = pd.read_parquet("../silver/london_bike_silver.parquet")

# colunas legíveis
ts = pd.to_datetime(df["timestamp"])
df["data_hora"] = ts.dt.strftime("%d/%m/%Y %H:%M")
df["data"] = ts.dt.strftime("%d/%m/%Y")
df["hora"] = ts.dt.strftime("%H:%M")
df["ano"] = ts.dt.year
df["mes"] = ts.dt.month
df["dia_da_semana"] = ts.dt.day

# remove colunas técnicas
df = df.drop(columns=["timestamp","year","month","day","hour","weekday"], errors="ignore")

df.to_parquet("../gold/london_bike_gold.parquet", index=False)
```

---

### Fluxo (diagrama textual)


    Kaggle (london_merged.csv)
     -> Ingestão (Notebook Bronze)
     -> Bronze (Parquet cru)
     -> Limpeza/Padronização (Notebook Silver)
     -> Silver (Parquet tratado + features)
     -> Enriquecimento/Agregação (Notebook Gold)
     -> Gold (Parquet pronto p/ negócio)
     -> Notebook de Insights (gráficos e métricas)

---

### Tech Stack (Tecnologias Utilizadas)
- **Plataforma: Databricks (Free Edition)**
- **Linguagem: Python 3.x**
- **Bibliotecas: ```pandas```, ```matplotlib```**
- **Formato de dados: Parquet**
- **Arquitetura: Medallion (Bronze/Silver/Gold) — Lakehouse**
- **Fonte dos dados: Kaggle – London Bike Sharing Dataset**
- **Versionamento: GitHub (projeto estruturado por camadas)**

---

### Estrutura do Repositório

    projeto_bikes/
    ├─ bronze/
    │  ├─ api_1_bronze.py              # Ingestão CSV → Parquet (cru)
    │  ├─ london_bike_bronze.parquet
    │
    ├─ silver/
    │  ├─ api_2_silver.py              # Limpeza/renomeação/enriquecimento
    │  ├─ london_bike_silver.parquet
    │
    ├─ gold/
    │  ├─ api_3_gold.py                # Colunas legíveis e dataset de negócio
    │  ├─ london_bike_gold.parquet
    │  ├─ london_bike_gold_base.parquet
    │  ├─ london_bike_gold_corr.parquet
    │  ├─ london_bike_gold_hora.parquet
    │  ├─ london_bike_gold_mes.parquet
    │  ├─ london_bike_gold_weekday.parquet
    │
    ├─ insights/
    │  ├─ api_4_insights.py            # Visualizações e insights (consome Gold)
    │
    ├─ london_merged.csv               # Dataset Kaggle (entrada)
    ├─ README.md




### Observação Importante

* No Databricks Free Edition os caminhos são relativos entre pastas ```(../silver/...)```, pois não há Unity Catalog/Volumes.
Isso foi proposital para simular a arquitetura Lakehouse dentro das limitações do ambiente.
