import pandas as pd
import numpy as np
import pyspark
from pyspark.sql import SparkSession
#Bibliotecas

#Criando sessao do Spark
spark = SparkSession.builder.master("local[*]").getOrCreate()

#le o arquivo e pula as 6 primeiras linhas
df_pandas = pd.read_csv("2295.csv", sep=",", encoding="UTF-8", skiprows=6)

#Renomeando a primeira coluna
df_pandas.rename(columns={df_pandas.columns[0]: "Ano"}, inplace=True)

#Converte a coluna Ano em numerico e filtra apenas linhas com valores validos
df_pandas = df_pandas[pd.to_numeric(df_pandas["Ano"], errors="coerce").notnull()]

#Remove os espacos e converte para int
df_pandas["Jan"] = df_pandas["Jan"].str.replace(" ", "").astype(int)

#Seleciona apenas os Anos e Janeiro de cada ano
dados = spark.createDataFrame(df_pandas[["Ano", "Jan"]])

#Exibe as 25 primeiras linhas
dados.show(25)