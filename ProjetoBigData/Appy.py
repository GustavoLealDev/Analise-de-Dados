import pandas as pd
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
import matplotlib.pyplot as plt 
#Bibliotecas 

# criando sessao do spark
spark = SparkSession.builder.master('local[*]').getOrCreate()

spark
# le o arquivo e pula as 6 primeiras linhas
df_roubos = pd.read_csv("https://raw.githubusercontent.com/GustavoLealDev/Analise-de-Dados/main/ProjetoBigData/2295.csv", 
                        sep=",", 
                        encoding="UTF-8", 
                        skiprows=6)

# Renomeando a primeira coluna
df_roubos.rename(columns={df_roubos.columns[0]: "Ano"}, inplace=True)

# Converte a coluna Ano e so seliciona linhas com valores validos
df_roubos = df_roubos[pd.to_numeric(df_roubos["Ano"], errors="coerce").notnull()]

#Remove os espacos e converte para int
df_roubos["Total2"] = df_roubos["Total2"].str.replace(" ", "").astype(int)

#Seleciona apenas os Anos e os valores totais de roubo de cada ano
dados_RPA = spark.createDataFrame(df_roubos[["Ano", "Total2"]])

dados_RPA.show(25)

dados_RPA = dados_RPA.withColumn("Ano", col("Ano").cast("int"))

assembler = VectorAssembler(inputCols=["Ano"], outputCol="Ano_Vector")
df_vetorizado = assembler.transform(dados_RPA).select("Ano_Vector", "Total2")

lr = LinearRegression(featuresCol="Ano_Vector", labelCol="Total2")
modelo = lr.fit(df_vetorizado)

print("Coeficiente Angular (W)", modelo.coefficients[0])
print("Intercepto (b)", modelo.intercept)

dados_pd = dados_RPA.toPandas().sort_values(by="Ano")

X = dados_pd["Ano"].values
y = dados_pd["Total2"].values

y_pred = modelo.coefficients * X + modelo.intercept

plt.scatter(X, y, label="Dados Originais")
plt.plot(X, y_pred, color="red", linewidth=2, label="Linha de Regressão")
plt.xlabel("Ano")
plt.ylabel("Quantidade de Roubos por Ano")
plt.legend()
plt.title("Regressão Linear Simples")
plt.grid(True)
plt.show()

media = dados_pd["Total2"].mean()
print(f"Média de roubos por ano: {media:.0f}")

desvio_padrao = dados_pd["Total2"].std()
print(f"Desvio padrão dos roubos: {desvio_padrao:.0f}")

faixas = pd.cut(dados_pd["Total2"],bins=5)
tabela_freq = dados_pd["Total2"].groupby(faixas, observed=False).count().reset_index(name="Frequência")

plt.figure(figsize=(8, 5))
plt.bar(tabela_freq["Total2"].astype(str), tabela_freq["Frequência"], color="skyblue", edgecolor="black")
plt.xlabel("Faixas de Quantidade de Roubos")
plt.ylabel("Frequência")
plt.title("Distribuição de Frequência dos Roubos por Faixas")
plt.xticks(rotation=45)
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.show()

