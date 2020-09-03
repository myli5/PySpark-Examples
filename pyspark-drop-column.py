# -*- coding: utf-8 -*-

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
simpleData = (("James","","Smith","36636","NewYork",13100), \
    ("Michael","Rose","","40288","California",14300), \
    ("Robert","","Williams","42114","Florida",11400), \
    ("Maria","Anne","Jones","39192","Florida",15500), \
    ("Jen","Mary","Brown","34561","NewYork",13000) \
  )
columns= ["firstname","middlename","lastname","id","location","salary"]

df = spark.createDataFrame(data = simpleData, schema = columns)

df.printSchema()
df.show(truncate=False)

df.drop("firstname") \
  .printSchema()
  
df.drop(col("firstname")) \
  .printSchema()  
  
df.drop(df.firstname) \
  .printSchema()

df.drop("firstname","middlename","lastname") \
    .printSchema()

cols = ("firstname","middlename","lastname")

df.drop(*cols) \
   .printSchema()