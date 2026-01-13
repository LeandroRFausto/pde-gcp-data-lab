import os
import json
import time
import random
from datetime import datetime
from google.cloud import pubsub_v1
from faker import Faker

# Configuração
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
topic_id = "iot-telemetry-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
fake = Faker()

print(f"--- Iniciando Simulador IoT ---")
print(f"Projeto: {project_id}")
print(f"Topico: {topic_id}")
print("Pressione CTRL+C para parar.\n")

sensores = [f"SENSOR-{i:03d}" for i in range(1, 6)]

try:
    while True:
        for sensor in sensores:
            data = {
                "sensor_id": sensor,
                "timestamp": datetime.utcnow().isoformat(),
                "temperature": round(random.uniform(20.0, 45.0), 2),
                "vibration": round(random.uniform(0.0, 0.5), 4),
                "status": random.choice(["OK", "OK", "OK", "WARNING"]),
                "location": fake.city()
            }
            
            # Converter para JSON e bytes
            data_str = json.dumps(data)
            data_bytes = data_str.encode("utf-8")
            
            # Publicar
            future = publisher.publish(topic_path, data_bytes)
            print(f"Enviado: {data['sensor_id']} | Temp: {data['temperature']}")
            
        time.sleep(2) # Pausa de 2 segundos entre lotes

except KeyboardInterrupt:
    print("\nSimulacao parada.")
