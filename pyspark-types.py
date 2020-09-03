# -*- coding: utf-8 -*-

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import DataType
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.appName('types').getOrCreate()

arrayType = ArrayType(IntegerType(),False)
print(arrayType.jsonValue)  

print(arrayType.simpleString)

print(arrayType.typeName)

