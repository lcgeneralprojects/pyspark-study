from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName('Practice').getOrCreate()

df_object = spark.createDataFrame([
    Row(id=1, name="abc"),
    Row(id=2, name="def"),
    Row(id=3, name="ghi")
])
df_junction = spark.createDataFrame([
    Row(id=1, object_id=1, collection_id=1),
    Row(id=2, object_id=1, collection_id=2),
    Row(id=3, object_id=1, collection_id=3),
    Row(id=4, object_id=2, collection_id=3)
])
df_collection = spark.createDataFrame([
    Row(id=1, name="category a"),
    Row(id=2, name="category b"),
    Row(id=3, name="category c or d")
])

# SELECT Object.name, Collection.name FROM Object LEFT JOIN Junction ON Object.id = Junction.object_id LEFT JOIN Collection ON Junction.collection_id = Collection.id
df_res = df_object.join(df_junction.join(df_collection, df_junction['collection_id'] == df_collection['id'], 'left'), df_object['id'] == df_junction['object_id'], 'left').select(df_object['name'].alias('object_name'), df_collection['name'].alias('collection_name'))

df_res.show()
