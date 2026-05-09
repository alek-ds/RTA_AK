from kafka import KafkaConsumer
from collections import defaultdict
import json

# TWÓJ KOD

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers = 'broker:9092',
    group_id = 'consumer-stats-group',
    auto_offset_reset = 'earliest',
    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    "count": 0,
    "sum" : 0.0,
    "min": float("inf"),
    "max": float("-inf")
})

msg_count = 0

print("Nasłuchuję i liczę statystyki per kategoria...")

for message in consumer:
    event = message.value
    category = event['category']
    amount = event['amount']

    stats[category]['count'] += 1
    stats[category]['sum'] += amount
    stats[category]['min'] = min(stats[category]['min'], amount)
    stats[category]['max'] = max(stats[category]['max'], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n" + "=" * 65)
        print(f"Statystyki po {msg_count} wiadomościach")
        print("=" * 65)
        print(f"{'Kategoria':<15} {'Liczba':<8} {'Przychód':<12} {'Min':<12} {'Max':<12}")

        for cat in sorted(stats):
            count = stats[cat]['count']
            total = stats[cat]['sum']
            min_amount = stats[cat]['min']
            max_amount = stats[cat]['max']
    
            print(f"{cat:<15} {count:<8} {total:<12.2f} {min_amount:<12.2f} {max_amount:<12.2f}")
