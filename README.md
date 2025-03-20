# goit-de-hw-05

### Create_topics.py 
- Створює 3 топіки для роботи з трьома партиціями та коефіцієнтом реплікації 1 в Apache Kafka, використовуючи конфігурацію з файлу kafka_config
    - name__humidity_alerts
    - name__building_sensors
    - name__temperature_alerts

### Producer.py 
- Імітує роботу датчика і періодично відправляє випадково згенеровані дані (температуру та вологість) у топік building_sensors.
 
### consumer_producer.py 
- створює споживача Kafka, підписується на топік building_sensors, обробляє отримані повідомлення й створює повідомлення для інших двох топіків.

### alert_consumer.py 
- обробляє отримані повідомлення з humidity_alerts й temperature_alerts та виводить їх на екран.