from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id = 'consumer-count_group',
    auto_offset_reset = 'earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Zmienne stanu
store_counts = Counter() # Liczba transakcji dla każdego sklepu
total_amount = {}        # Słownik sum wartości transakcji dla każdego sklepu
msg_count = 0            # Licznik odebranych wiadomości

# TWÓJ KOD
# Dla każdej wiadomości:
#   1. Zwiększ store_counts[store]
#   2. Dodaj amount do total_amount[store]
#   3. Co 10 wiadomości wypisz tabelę:
#      Sklep | Liczba | Suma | Średnia

print("Nasłuchuję i zliczam transakcje per sklep...")

# Wyciąganie danych z wiadomości
for message in consumer:
    event = message.value     # słownik transakcji
    store = event["store"]    # nazwa sklepu
    amount = event["amount"]  # kwota transakcji

    # 1. Zwiększ store_counts[store]
    store_counts[store] += 1

    # 2. Dodaj amount do total_amount[store]
    if store not in total_amount:
        total_amount[store] = 0.0
    total_amount[store] += amount

    msg_count += 1

    # 3. Co 10 wiadomości wypisz tabelę
    if msg_count % 10 == 0:
        print("\n" + "=" * 50)
        print(f"Podsumowanie po {msg_count} wiadomościach")
        print("=" * 50)
        print(f"{'Sklep':<12} {'Liczba':<8} {'Suma':<12} {'Średnia':<12}")

        for store_name in store_counts:
            count = store_counts[store_name]
            total = total_amount[store_name]
            avg = total / count
            print(f"{store_name:<12} {count:<8} {total:<12.2f} {avg:<12.2f}")
