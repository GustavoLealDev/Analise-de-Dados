import pandas as pd
import pyspark
from pyspark.sql import SparkSession
#Bibliotecas

# Criando sessao do Spark
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Lê o arquivo e pula as 6 primeiras linhas
df_pandas = pd.read_csv("2295.csv", sep=",", encoding="UTF-8", skiprows=6)

# Renomeando a primeira coluna
df_pandas.rename(columns={df_pandas.columns[0]: "Ano"}, inplace=True)

# Converte a coluna Ano e so seliciona linhas com valores validos
df_pandas = df_pandas[pd.to_numeric(df_pandas["Ano"], errors="coerce").notnull()]

# Lista dos meses 
meses = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]

# Limpa espacos e converte para int todas as colunas
for roubos in meses:
    df_pandas[roubos] = df_pandas[roubos].astype(str).str.replace(" ", "").replace("", "0")
    df_pandas[roubos] = pd.to_numeric(df_pandas[roubos], errors="coerce").fillna(0).astype(int)

# Seleciona apenas Ano e meses
dados = spark.createDataFrame(df_pandas[["Ano"] + meses])

# Exibe as 25 primeiras linhas
dados.show(25)

dados.printSchema()