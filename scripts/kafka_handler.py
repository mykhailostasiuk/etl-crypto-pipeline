import os
from confluent_kafka import Producer
from kafka.admin import KafkaAdminClient, NewTopic

def create_kafka_topics():
    admin_client = KafkaAdminClient(bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'])
    topic_set = set()
    for topic in  os.environ['WEBSOCKET_ENDPOINTS'].split(','):
        topic_set.add(topic.split('_')[0])

    for topic in topic_set:
        if topic + '_raw' not in admin_client.list_topics():
            admin_client.create_topics([
                NewTopic(
                    name=topic + '_raw',
                    num_partitions=int(os.environ['KAFKA_PARTITIONS']),
                    replication_factor=int(os.environ['KAFKA_DEFAULT_REPLICATION_FACTOR'])
                ),
                NewTopic(
                    name=topic + '_processed',
                    num_partitions=int(os.environ['KAFKA_PARTITIONS']),
                    replication_factor=int(os.environ['KAFKA_DEFAULT_REPLICATION_FACTOR'])
                )
            ])

            print(
                f"Created topics '{topic}_raw' and '{topic}_processed' "
                f"with {os.environ['KAFKA_PARTITIONS']} partitions "
                f"and {os.environ['KAFKA_DEFAULT_REPLICATION_FACTOR']} replicas")

    admin_client.close()

def produce_message(producer, topic, partition, message):
    try:
        producer.produce(
            topic=topic,
            partition=partition,
            value=message,
            callback=delivery_report,
        )
        producer.poll(0)
    except Exception as exception:
        print(exception)

def delivery_report(err, msg):
    if err is not None:
        print(err)
    else:
        print(f"Message sent. Topic: '{msg.topic()}'. Partition {msg.partition()}. Value: {msg.value()}")

def get_kafka_producer():
    kafka_producer = Producer({
        'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        'client.id': 'binance-websocket-producer',
        'compression.type': 'snappy',
        'acks': 'all',
        'retries': 3,
        'linger.ms': int(os.environ['KAFKA_FLUSH_INTERVAL']),
        'batch.num.messages': int(os.environ['KAFKA_BATCH_SIZE']),
    })

    print(f"Kafka producer connected to {os.environ['KAFKA_BOOTSTRAP_SERVERS']}")

    return kafka_producer
