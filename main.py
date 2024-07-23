from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import array_contains

spark = SparkSession.builder.appName('Practice').getOrCreate()

df_object = spark.createDataFrame([
    Row(id=1, name="abc", collections=[1, 2, 3]),
    Row(id=2, name="def", collections=[3]),
    Row(id=3, name="ghi", collections=None)
])
df_collection = spark.createDataFrame([
    Row(id=1, name="category a", objects=[1]),
    Row(id=2, name="category b", objects=[1]),
    Row(id=3, name="category c or d", objects=[1, 2])
])

# SELECT Object.name, Collection.name FROM Object LEFT JOIN Collection ON Collection.id in Object.collections
df_res = df_object.join(df_collection, array_contains(df_object['collections'], df_collection['id']), 'left').select(df_object['name'], df_collection['name'])

df_res.show()
