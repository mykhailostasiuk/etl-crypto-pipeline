from confluent_kafka import Consumer, TopicPartition
import clickhouse_driver
import json
import time
import os
from decimal import Decimal
from datetime import datetime
from scripts.schema import table_enum
from threading import Thread, Event

stop_clickhouse_consumer_thread_event = Event()

def get_clickhouse_client():
    client = clickhouse_driver.Client(host='clickhouse',
                                      user=os.environ['CLICKHOUSE_USER'],
                                      password=os.environ['CLICKHOUSE_PASSWORD'] if 'CLICKHOUSE_PASSWORD' in os.environ else None,
                                      database='crypto_pipeline')

    return client

def create_clickhouse_tables():
    ch_client = get_clickhouse_client()
    topic_set = set()
    for topic in os.environ['WEBSOCKET_ENDPOINTS'].split(','):
        topic_set.add(topic.split('_')[0])

    for topic in topic_set:
        ch_client.execute(table_enum[topic])
        print(f"Table '{topic}s' initialized in Clickhouse")

def flush_consumer_buffer(buffer, table_name, ch_client):
    if not buffer:
        return
    try:
        ch_client.execute(f"INSERT INTO {table_name} VALUES", buffer)
    except Exception as exception:
        print(f"{table_name}: {exception}")
    finally:
        buffer.clear()

def get_clickhouse_consumer(topic):
    ch_client = get_clickhouse_client()
    consumer = Consumer({
        "bootstrap.servers": os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        "group.id": f"clickhouse-{topic.split('_')[0]}-consumer-group",
        "auto.offset.reset": 'earliest',
    })

    metadata = consumer.list_topics(topic, timeout=10)
    partitions = [TopicPartition(topic, p.id) for p in metadata.topics[topic].partitions.values()]
    consumer.assign(partitions)

    buffer = []
    last_flush = time.time()
    table_name = topic.split('_')[0] + 's'

    flush_interval = float(os.environ['CLICKHOUSE_FLUSH_INTERVAL_RARE']) if topic.split('_')[0] in ['kline', 'avgPrice', 'miniTicker', 'ticker', 'depth']\
        else float(os.environ['CLICKHOUSE_FLUSH_INTERVAL_FREQUENT'])
    batch_size = int(os.environ['PYSPARK_BATCH_SIZE_RARE']) if topic.split('_')[0] in ['kline', 'avgPrice', 'miniTicker', 'ticker', 'depth'] \
        else int(os.environ['PYSPARK_BATCH_SIZE_FREQUENT'])

    try:
        while not stop_clickhouse_consumer_thread_event.is_set():
            messages = consumer.consume(timeout=0.1)
            if not messages:
                if time.time() - last_flush > flush_interval:
                    flush_consumer_buffer(buffer, table_name, ch_client)
                    last_flush = time.time()
                continue

            for message in messages:
                if message.error():
                    print(message.error())
                    continue

                try:
                    data = json.loads(message.value().decode('utf-8'))

                    print(f"Writing message {data} to Clickhouse")

                    if table_name == 'aggTrades':
                        buffer.append([
                            data['first_trade_id'],
                            data['last_trade_id'],
                            data['aggregate_trade_id'],
                            datetime.fromisoformat(data['timestamp_utc']),
                            data['symbol'],
                            Decimal(str(data['price'])),
                            Decimal(str(data['quantity'])),
                            bool(data['is_buyer_market_maker']),
                        ])
                    elif table_name == 'trades':
                        buffer.append([
                            data['trade_id'],
                            datetime.fromisoformat(data['timestamp_utc']),
                            data['symbol'],
                            Decimal(str(data['price'])),
                            Decimal(str(data['quantity'])),
                            bool(data['is_buyer_market_maker']),
                        ])
                    elif table_name == 'klines':
                        buffer.append([
                            abs(data['first_trade_id']),
                            abs(data['last_trade_id']),
                            datetime.fromisoformat(data['timestamp_utc']),
                            datetime.fromisoformat(data['open_timestamp_utc']),
                            datetime.fromisoformat(data['close_timestamp_utc']),
                            data['symbol'],
                            data['interval'],
                            data['num_trades'],
                            bool(data['is_closed']),
                            Decimal(str(data['open_price'])),
                            Decimal(str(data['close_price'])),
                            Decimal(str(data['high_price'])),
                            Decimal(str(data['low_price'])),
                            Decimal(str(data['base_asset_volume'])),
                            Decimal(str(data['quote_asset_volume'])),
                            Decimal(str(data['taker_buy_base_asset_volume'])),
                            Decimal(str(data['taker_buy_quote_asset_volume'])),
                        ])
                    elif table_name == 'miniTickers':
                        buffer.append([
                            datetime.fromisoformat(data['timestamp_utc']),
                            data['symbol'],
                            Decimal(str(data['open_price'])),
                            Decimal(str(data['close_price'])),
                            Decimal(str(data['high_price'])),
                            Decimal(str(data['low_price'])),
                            Decimal(str(data['total_base_asset_volume'])),
                            Decimal(str(data['total_quote_asset_volume'])),
                        ])
                    elif table_name == 'tickers':
                        buffer.append([
                            data['first_trade_id'],
                            data['last_trade_id'],
                            datetime.fromisoformat(data['timestamp_utc']),
                            datetime.fromisoformat(data['open_timestamp_utc']),
                            datetime.fromisoformat(data['close_timestamp_utc']),
                            data['symbol'],
                            Decimal(str(data['price_change'])),
                            Decimal(str(data['price_change_percent'])),
                            Decimal(str(data['weighted_avg_price'])),
                            data['num_trades'],
                            Decimal(str(data['first_trade_price'])),
                            Decimal(str(data['open_price'])),
                            Decimal(str(data['close_price'])),
                            Decimal(str(data['high_price'])),
                            Decimal(str(data['low_price'])),
                            Decimal(str(data['best_bid_price'])),
                            Decimal(str(data['best_bid_quantity'])),
                            Decimal(str(data['best_ask_price'])),
                            Decimal(str(data['best_ask_quantity'])),
                            data['last_quantity'],
                            Decimal(str(data['total_base_asset_volume'])),
                            Decimal(str(data['total_quote_asset_volume'])),
                        ])
                    elif table_name == 'avgPrices':
                        buffer.append([
                            datetime.fromisoformat(data['timestamp_utc']),
                            datetime.fromisoformat(data['last_trade_timestamp_utc']),
                            data['symbol'],
                            data['avg_price_interval'],
                            Decimal(str(data['avg_price'])),
                        ])
                    elif table_name == 'depths':
                        buffer.append([
                            datetime.fromisoformat(data['timestamp_utc']),
                            data['symbol'],
                            data['first_update_id'],
                            data['final_update_id'],
                            data['bids'],
                            data['asks'],
                        ])
                except Exception as exception:
                    print(exception)

                if len(buffer) >= batch_size or time.time() - last_flush > flush_interval:
                    flush_consumer_buffer(buffer, table_name, ch_client)
                    last_flush = time.time()
    finally:
        flush_consumer_buffer(buffer, table_name, ch_client)
        consumer.close()

def start_consumer_thread(topic):
    consumer_thread = Thread(target=get_clickhouse_consumer, args=(topic,))
    consumer_thread.daemon = True
    consumer_thread.start()
    return consumer_thread

def process_kafka_stream():
    topic_set = set()
    for topic in os.environ['WEBSOCKET_ENDPOINTS'].split(','):
        topic_set.add(topic.split('_')[0])

    threads = []
    for topic in topic_set:
        thread = start_consumer_thread(topic + '_processed')
        threads.append(thread)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Disconnecting Clickhouse consumer via keyboard interrupt...")
        stop_clickhouse_consumer_thread_event.set()
        for thread in threads:
            thread.join()
        print("Clickhouse consumer connection closed")