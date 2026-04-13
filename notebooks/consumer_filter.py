from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Teraz ładujemy dane
)

for message in consumer:
    if message.value['amount'] > 3000:
        print(f"ALERT: {message.value}")
    
# TWÓJ KOD
# Dla każdej wiadomości: sprawdź amount > 1000, jeśli tak — wypisz ALERT
