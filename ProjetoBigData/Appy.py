import pandas as pd
import numpy as np
import pyspark 
from pyspark.sql import SparkSession

sc = SparkSession.builder.master('local[*]').getOrCreate()

sc

dados_roubos = sc.read.csv(
path = "RouboRJ.csv",
inferSchema= True,
header= True,
sep= ';',
encoding= "UTF-8"
)

type(dados_roubos)

dados_roubos.show()

