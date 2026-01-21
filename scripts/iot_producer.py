import time
import json
import random
from datetime import datetime
from google.cloud import pubsub_v1

# SEU PROJECT ID
project_id = "quick-cache-484111-j4"
topic_id = "iot-readings"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Sensores Simulados
sensors = ["sensor_A", "sensor_B", "sensor_C", "sensor_D"]

print(f"📡 Enviando dados para: {topic_path}...")

try:
    while True:
        data = {
            "sensor_id": random.choice(sensors),
            "temperature": round(random.uniform(15.0, 40.0), 2),
            "humidity": round(random.uniform(30.0, 90.0), 2),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Publicar
        future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
        print(f"Enviado: {data['sensor_id']} | {data['temperature']}C")
        
        time.sleep(1) # 1 msg/seg

except KeyboardInterrupt:
    print("Parado.")
