import json
import time
import websocket
from threading import Thread, Event
import os
from scripts.kafka_handler import create_kafka_topics, get_kafka_producer, produce_message
from scripts.schema import endpoint_enum

stop_kafka_producer_thread_event = Event()

def on_message(ws, message):
    try:
        message_data = json.loads(message)
        if 'result' in message_data and message_data['result'] is None:
            return

        endpoint = endpoint_enum.get(message_data.get('e'), None)
        if endpoint is None:
            print(f"Received NotImplemented endpoint: {endpoint}, skipping...")
            return

        message_event_time = message_data.get('E', None)
        if message_event_time is None:
            print(f"Message event time is missing: {message_data}, skipping...")
            return

        partition = hash(message_event_time) % int(os.environ['KAFKA_PARTITIONS'])

        produce_message(producer, endpoint + '_raw', partition, json.dumps(message_data))

    except Exception as exception:
        print(exception)

def on_error(ws, error):
    if isinstance(error, KeyboardInterrupt):
        stop_kafka_producer_thread_event.set()
        producer.flush()
        print("Disconnecting websocket via keyboard interrupt...")
        print("Disconnecting Kafka producer via keyboard interrupt...")
        exit(0)
    else:
        print(error)

def on_close(ws, close_status_code, close_msg):
    stop_kafka_producer_thread_event.set()
    producer.flush()
    print("Websocket connection closed")
    print("Kafka producer connection closed")

def on_open(ws):
    payload = {
        "method": "SUBSCRIBE",
        "params": os.environ['WEBSOCKET_SUBSCRIPTIONS'].split(','),
        "id": 1
    }
    print(f"Websocket subscribed to: {os.environ['WEBSOCKET_SUBSCRIPTIONS'].split(',')}")
    ws.send(json.dumps(payload))

def kafka_producer_thread():
    while not stop_kafka_producer_thread_event.is_set():
        producer.flush()
        time.sleep(1)

def process_kafka_stream():
    global producer
    producer = get_kafka_producer()

    producer_thread = Thread(target=kafka_producer_thread)
    producer_thread.start()

    ws_url = "wss://stream.binance.com:9443/ws"

    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.on_open = on_open

    ws.run_forever()
