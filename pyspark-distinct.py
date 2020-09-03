# -*- coding: utf-8 -*-

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.appName('Distinct').getOrCreate()

data = [("James", "Sales", 13000), \
    ("Michael", "Sales", 14600), \
    ("Robert", "Sales", 14100), \
    ("Maria", "Finance",13000), \
    ("James", "Sales", 13000), \
    ("Scott", "Finance",13300), \
    ("Jen", "Finance", 13900), \
    ("Jeff", "Marketing", 13000), \
    ("Kumar", "Marketing", 12000), \
    ("Saif", "Sales", 14100) \
  ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

distinctDF = df.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)

df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
df2.show(truncate=False)

dropDisDF = df.dropDuplicates(["department","salary"])
print("Distinct count of department salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)