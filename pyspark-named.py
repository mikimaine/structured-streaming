from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from kafka import KafkaProducer
import spacy

nlp = spacy.load("en_core_web_sm")

def extract_named_entities(text):
    doc = nlp(text)
    entities = [ent.text for ent in doc.ents]
    return entities

spark = SparkSession.builder \
    .appName("KafkaNERToTopic2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

extract_named_entities_udf = udf(extract_named_entities, ArrayType(StringType()))

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "topic1") \
    .load()

messages = df.selectExpr("CAST(value AS STRING) as text")
entities_df = messages.withColumn("entities", extract_named_entities_udf(col("text")))

exploded_entities = entities_df.select("text", explode(col("entities")).alias("entity"))

query = exploded_entities.selectExpr("CAST(entity AS STRING) as key", "CAST(entity AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("topic", "topic2") \
    .option("checkpointLocation", "./tmp") \
    .outputMode("append") \
    .start()

query.awaitTermination()