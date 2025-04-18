from scripts.environment import spark_environ, clickhouse_environ, kafka_environ, websocket_environ, config_environ

from scripts.kafka_handler import create_kafka_topics
from scripts.websocket_handler import process_kafka_stream
from scripts.spark_handler import process_kafka_stream
from scripts.clickhouse_handler import create_clickhouse_tables, process_kafka_stream

config_environ()

def invoke_extract():
    websocket_environ()
    kafka_environ()

    kafka_handler.create_kafka_topics()
    websocket_handler.process_kafka_stream()


def invoke_transform():
    spark_environ()
    kafka_environ()

    kafka_handler.create_kafka_topics()
    spark_handler.process_kafka_stream()

def invoke_load():
    spark_environ()
    clickhouse_environ()

    clickhouse_handler.create_clickhouse_tables()
    clickhouse_handler.process_kafka_stream()


