import time
import json
import random
from google.cloud import pubsub_v1
import argparse
from datetime import datetime

def run(project_id, topic_id):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    print(f"🚀 Iniciando envio de dados para: {topic_path}")
    print("Pressione Ctrl+C para parar.")

    sensors = ['sensor_1', 'sensor_2', 'sensor_3', 'sensor_4', 'sensor_5']

    try:
        while True:
            for sensor_id in sensors:
                # Simula uma leitura de temperatura
                data = {
                    "sensor_id": sensor_id,
                    "temperature": round(random.uniform(20.0, 35.0), 2), # Temp aleatória entre 20 e 35
                    "timestamp": datetime.now().isoformat(),
                    "source_type": "streaming"
                }
                
                # Prepara e envia a mensagem
                data_str = json.dumps(data)
                data_bytes = data_str.encode("utf-8")

                future = publisher.publish(topic_path, data=data_bytes)
                # print(f"Enviado: {data_str}") # Descomente se quiser ver o log detalhado
            
            print(f"📡 Enviados dados de {len(sensors)} sensores...")
            time.sleep(2) # Espera 2 segundos antes do próximo lote
    except KeyboardInterrupt:
        print("\n🛑 Parando o publisher...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--topic_id", required=True)
    args = parser.parse_args()

    run(args.project_id, args.topic_id)
