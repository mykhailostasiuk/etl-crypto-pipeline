from pathlib import Path
import sys
import os

from scripts.config_handler import Config

default_values = {
    'CLICKHOUSE_HOST': 'localhost',
    'CLICKHOUSE_USER': 'default',
    'CLICKHOUSE_PASSWORD': '',
    'CLICKHOUSE_FLUSH_INTERVAL_FREQUENT': 0.5,
    'CLICKHOUSE_FLUSH_INTERVAL_RARE': 1,

    'PYSPARK_SHUFFLE_PARTITIONS': 200,
    'PYSPARK_PARALLELISM': 300,
    'PYSPARK_BACKPRESSURE_INITIAL_RATE': 1000,
    'PYSPARK_BACKPRESSURE_PID_MIN_RATE': 1000,
    'PYSPARK_UI_HOST': 'localhost',
    'PYSPARK_UI_PORT': '7070',
    'PYSPARK_BATCH_SIZE_FREQUENT': 1000,
    'PYSPARK_BATCH_SIZE_RARE': 10,

    'KAFKA_PARTITIONS': 9,
    'KAFKA_BATCH_SIZE': 1000,
    'KAFKA_FLUSH_INTERVAL': 100,
}

def env_exists(var_name):
    return bool(os.getenv(var_name))

def env_get(var_name):
    return os.getenv(var_name)

def env_set(var_name, value):
    os.environ[var_name] = str(value)

# KAFKA

def kafka_environ():
    for kafka_variable in ['KAFKA_PARTITIONS', 'KAFKA_BATCH_SIZE', 'KAFKA_FLUSH_INTERVAL']:
        if env_exists(kafka_variable):
            try:
                value = int(env_get(kafka_variable))
            except ValueError:
                print(f"Kafka variable '{kafka_variable}' was specified incorrectly - value must be an integer. "
                      f"Setting the default value {kafka_variable}={default_values[kafka_variable]}")
                env_set(kafka_variable, default_values[kafka_variable])
            else:
                if value < 1:
                    raise ValueError(f"Kafka variable '{kafka_variable}' must be greater than 1")
        else:
            env_set(kafka_variable, default_values[kafka_variable])
            print(f"Kafka variable '{kafka_variable}' was not specified. "
                  f"Setting the default value {kafka_variable}={default_values[kafka_variable]}")

# WEBSOCKET

def websocket_environ():
    for websocket_variable in ['API_KEY', 'SECRET_KEY']:
        if not env_exists(websocket_variable):
            raise ValueError(f"Websocket variable '{websocket_variable}' was not specified")

# CONFIG

def config_environ():
    config_path = '/config.yml'
    config = Config(config_path)

    os.environ['WEBSOCKET_SUBSCRIPTIONS'] = config.websocket_subscriptions()
    os.environ['WEBSOCKET_ENDPOINTS'] = config.endpoints_list()

# CLICKHOUSE

def clickhouse_environ():
    for clickhouse_variable in ['CLICKHOUSE_USER', 'CLICKHOUSE_PASSWORD']:
        if not env_exists(clickhouse_variable):
            env_set(clickhouse_variable, default_values[clickhouse_variable])
            print(f"Spark variable '{clickhouse_variable}' was not specified. "
                  f"Setting the default value {clickhouse_variable}={default_values[clickhouse_variable]}")

    for clickhouse_variable in ['CLICKHOUSE_FLUSH_INTERVAL_FREQUENT', 'CLICKHOUSE_FLUSH_INTERVAL_RARE']:
        if env_exists(clickhouse_variable):
            try:
                value = int(env_get(clickhouse_variable))
            except ValueError:
                print(f"Spark variable '{clickhouse_variable}' was specified incorrectly - value must be an integer. "
                      f"Setting the default value {clickhouse_variable}={default_values[clickhouse_variable]}")
                env_set(clickhouse_variable, default_values[clickhouse_variable])
            else:
                if value < 1:
                    raise ValueError(f"Spark variable '{clickhouse_variable}' must be greater than 1")
        else:
            env_set(clickhouse_variable, default_values[clickhouse_variable])
            print(f"Spark variable '{clickhouse_variable}' was not specified. "
                  f"Setting the default value {clickhouse_variable}={default_values[clickhouse_variable]}")

# SPARK

def spark_environ():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    os.environ['PYSPARK_PACKAGES'] = ','.join([
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        f"org.apache.kafka:kafka-clients:3.8.0",
        f"org.apache.httpcomponents.client5:httpclient5:5.3.1",
    ])

    if not env_exists('PYSPARK_UI_HOST'):
        env_set('PYSPARK_UI_HOST', default_values['PYSPARK_UI_HOST'])
        print("Spark variable 'PYSPARK_UI_HOST' was not specified. "
              f"Setting the default value PYSPARK_UI_HOST={default_values['PYSPARK_UI_HOST']}")

    for spark_variable in ['PYSPARK_SHUFFLE_PARTITIONS', 'PYSPARK_PARALLELISM',
                           'PYSPARK_BATCH_SIZE_FREQUENT', 'PYSPARK_BATCH_SIZE_RARE', 'PYSPARK_UI_PORT',
                           'PYSPARK_BACKPRESSURE_INITIAL_RATE', 'PYSPARK_BACKPRESSURE_PID_MIN_RATE']:
        if env_exists(spark_variable):
            try:
                value = int(env_get(spark_variable))
            except ValueError:
                print(f"Spark variable '{spark_variable}' was specified incorrectly - value must be an integer. "
                      f"Setting the default value {spark_variable}={default_values[spark_variable]}")
                env_set(spark_variable, default_values[spark_variable])
            else:
                if value < 1:
                    raise ValueError(f"Spark variable '{spark_variable}' must be greater than 1")
        else:
            env_set(spark_variable, default_values[spark_variable])
            print(f"Spark variable '{spark_variable}' was not specified. "
                  f"Setting the default value {spark_variable}={default_values[spark_variable]}")
