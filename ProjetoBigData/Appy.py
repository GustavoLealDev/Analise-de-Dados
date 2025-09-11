import pandas as pd
import numpy as np
import pyspark 
from pyspark.sql import SparkSession
#Bibliotecas 

#criando sessao do spark
spark = SparkSession.builder.master('local[*]').getOrCreate()

spark

#lendo o arquivo csv como um dataframe
dados_roubos = spark.read.csv(
path = "RouboRJ.csv",
inferSchema= True,
header= True,
sep= ';',
encoding= "UTF-8"
)

type(dados_roubos)

#Exibir primeiras linhas
dados_roubos.show()

