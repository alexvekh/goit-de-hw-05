# цей код створює споживача Kafka, підписується на топік, обробляє отримані повідомлення й створює повідомлення для інших топіків.

from kafka import KafkaConsumer
from kafka import KafkaProducer
import uuid
from configs import kafka_config
import json


# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_4'   # Ідентифікатор групи споживачів
)

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "vekh"
topic_name = f'{my_name}__building_sensors'

# Підписка на тему
consumer.subscribe([topic_name])
print(f"Subscribed to topic '{topic_name}'")
# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} , partition {message.partition}")
        if message.value["temperature"] > 40:
            try:
                data = {
                    "sensor_ID": message.value["sensor_ID"],
                    "timestamp": message.value["timestamp"],
                    "temperature": message.value["temperature"],
                    "humidity": message.value["humidity"]
                }
                producer.send("vekh__temperature_alerts", key=str(uuid.uuid4()), value=data)
                producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
                print("    Alert has sent to topic vekh__temperature_alerts successfully.")
            except Exception as e:
                print(f"An error occurred: {e}")
        if message.value["humidity"] > 80 or message.value["humidity"] < 20:
            try:
                data = {
                    "sensor_ID": message.value["sensor_ID"],
                    "timestamp": message.value["timestamp"],
                    "temperature": message.value["temperature"],
                    "humidity": message.value["humidity"]
                }
                producer.send("vekh__humidity_alerts", key=str(uuid.uuid4()), value=data)
                producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
                print("    Alert has sent to topic vekh__humidity_alerts successfully.")
            except Exception as e:
                print(f"An error occurred: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer

