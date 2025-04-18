from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_json, struct, slice
import os
from scripts.schema import schema_enum, expression_enum

def get_spark_session():
    session = (SparkSession.builder
               .appName('crypto_pipeline')
               .master("local[*]")
               .config("spark.jars.packages", os.environ['PYSPARK_PACKAGES'].strip(','))
               .config("spark.sql.caseSensitive", "true")
               .config("spark.ui.enabled", "true")
               .config("spark.ui.port", os.environ['PYSPARK_UI_PORT'])
               .config("spark.ui.host", os.environ['PYSPARK_UI_HOST'])
               .config("spark.sql.shuffle.partitions",  os.environ['PYSPARK_SHUFFLE_PARTITIONS'])
               .config("spark.default.parallelism", os.environ['PYSPARK_PARALLELISM'])
               .config("spark.streaming.backpressure.enabled", "true")
               .config("spark.streaming.backpressure.initialRate", os.environ['PYSPARK_BACKPRESSURE_INITIAL_RATE'])
               .config("spark.streaming.backpressure.pid.minRate", os.environ['PYSPARK_BACKPRESSURE_PID_MIN_RATE'])
               .getOrCreate()
               )

    print(f"Spark session initialized on host '{session.conf.get('spark.ui.host')}', "
                      f"port '{session.conf.get('spark.ui.port')}'")

    return session

def get_spark_consumer(topic):
    batch_size = int(os.environ['PYSPARK_BATCH_SIZE_RARE']) if topic.split('_')[0] in ['kline', 'avgPrice', 'miniTicker', 'ticker', 'depth'] \
        else int(os.environ['PYSPARK_BATCH_SIZE_FREQUENT'])

    consumer = (spark_session.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", os.environ['KAFKA_BOOTSTRAP_SERVERS'])
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", batch_size)
                .option("minPartitions", int(os.environ['KAFKA_PARTITIONS']))
                .option("failOnDataLoss", "false")
                .load()
                )

    return apply_spark_consumer_schema(consumer, topic)

def apply_spark_consumer_schema(consumer, topic):
    consumer = consumer.withColumn("kafka_partition", col("partition"))

    consumer = consumer.withColumn(
        "values_json", from_json(col("value").cast("string"), schema_enum[topic.split('_')[0]])
    )

    consumer = consumer.select("values_json.*", "kafka_partition")

    expression = expression_enum[topic.split('_')[0]]
    if "kafka_partition" not in expression:
        expression.append("kafka_partition")

    consumer = consumer.selectExpr(*expression)

    if 'bids' in consumer.columns:
        consumer = consumer.withColumn("bids", slice(col("bids"), 1, 5))
    if 'asks' in consumer.columns:
        consumer = consumer.withColumn("asks", slice(col("asks"), 1, 5))

    for column_name in consumer.columns:
        if column_name.endswith("_utc"):
            consumer = consumer.withColumn(
                column_name, from_unixtime(col(column_name) / 1000).cast('timestamp')
            )

    return consumer

def process_kafka_stream():
    global spark_session
    spark_session = get_spark_session()

    streams = []
    try:
        topic_set = set()
        for topic in os.environ['WEBSOCKET_ENDPOINTS'].split(','):
            topic_set.add(topic.split('_')[0])

        for topic in topic_set:
            consumer = get_spark_consumer(topic + '_raw')

            output_data = consumer.select(
                col("kafka_partition").cast("int").alias("partition"),
                to_json(struct(*[c for c in consumer.columns if c != "kafka_partition"])).alias("value")
            )

            stream = (output_data.writeStream
                      .outputMode("append")
                      .format("kafka")
                      .option("kafka.bootstrap.servers", os.environ['KAFKA_BOOTSTRAP_SERVERS'])
                      .option("topic", f"{topic}_processed")
                      .option("checkpointLocation", f"/home/pipeline_user/crypto_pipeline/checkpoints/spark_checkpoints/{topic}")
                      .start())

            print(f"Topic '{topic}_raw' consumer initialized")
            streams.append(stream)

        print("All streams started. Awaiting termination...")
        spark_session.streams.awaitAnyTermination()
    except ConnectionRefusedError:
        print("Disconnecting Spark session via keyboard interrupt...")
    except Exception as exception:
        print(exception)
    finally:
        print("Spark session connection closed")
