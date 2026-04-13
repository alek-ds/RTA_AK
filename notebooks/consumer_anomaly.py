from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='consumer-anomaly-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Dla każdego usera trzymamy czasy jego ostatnich transakcji
user_transactions = defaultdict(deque) # klucz suer_id, wartość kolejka czasów transakcji

print("Nasłuchuję i wykrywam anomalie prędkości transakcji...")

for message in consumer:
    event = message.value
    user_id = event["user_id"]
    tx_id = event["tx_id"]
    amount = event["amount"]
    store = event["store"]
    category = event["category"]
    timestamp_str = event["timestamp"]

    # Zamiana tekstu ISO na datetime
    event_time = datetime.fromisoformat(timestamp_str)

    # Pobranie kolejki czasów dla danego usera
    tx_times = user_transactions[user_id]

    # Dodanie nową transakcję
    tx_times.append(event_time)

    # Usuńnięcie transakcje starsze niż 60 sekund
    window_start = event_time - timedelta(seconds=60)
    while tx_times and tx_times[0] < window_start:
        tx_times.popleft()

    # Jeśli w ostatnich 60 sekundach są więcej niż 3 transakcje -> alert
    if len(tx_times) > 3:
        print(
            f"ALERT: user {user_id} wykonał {len(tx_times)} transakcji "
            f"w ciągu 60 sekund | ostatnia: {tx_id} | {amount:.2f} PLN | "
            f"{store} | {category} | {timestamp_str}"
        )
