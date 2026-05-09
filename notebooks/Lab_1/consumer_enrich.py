from kafka import KafkaConsumer # Konsument łączy się z kafką i odbiera wiadomości
import json                     # Zamiana bajtów na pythonowy dict

# TWÓJ KOD
# Czytaj z 'transactions' (użyj INNEGO group_id!)
# Dodaj pole risk_level na podstawie amount
# Wypisz wzbogaconą transakcję

consumer = KafkaConsumer(
    'transactions',                       # Czytanie z tematu utw. w części 1
    bootstrap_servers='broker:9092',      # Kafka działa pod nazwą `broker` w Dockerze na procie `9092`
    group_id='consumer-enrich-group-v2',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))   # bajty -> tekst -> słownik                      
)

print("Nasłuchuje i dodaje risk_level...")
for message in consumer:
    event = message.value # dane wiadomości (message - obiekt wiadomości, treść wiadomości JSON message.value
    amount = event['amount']
    if amount > 3000:
        risk_level = "HIGH"
    elif amount > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    event["risk_level"] = risk_level # Dodanie pola ryzyka do słownika wiadomości
    print(event)
                                            
