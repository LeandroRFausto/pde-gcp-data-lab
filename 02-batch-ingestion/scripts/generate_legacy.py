import csv
import random
from datetime import datetime, timedelta
import os

# Configurações
NUM_SENSORS = 20
LOCATIONS = ['Warehouse_A', 'Warehouse_B', 'Cold_Storage_1', 'Distribution_Center']
MODELS = ['TMP-2000', 'TMP-X1', 'Legacy-99']
FILENAME_METADATA = 'sensors_metadata.csv'
FILENAME_HISTORY = 'historical_logs.csv'

def generate_metadata():
    print(f"Gerando {FILENAME_METADATA}...")
    with open(FILENAME_METADATA, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['sensor_id', 'location', 'model', 'install_date'])
        
        for i in range(1, NUM_SENSORS + 1):
            s_id = f"sensor_{i}"
            loc = random.choice(LOCATIONS)
            mod = random.choice(MODELS)
            days_old = random.randint(100, 700)
            date = (datetime.now() - timedelta(days=days_old)).strftime("%Y-%m-%d")
            writer.writerow([s_id, loc, mod, date])
    print("Metadata concluído.")

def generate_history():
    print(f"Gerando {FILENAME_HISTORY}...")
    with open(FILENAME_HISTORY, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['sensor_id', 'timestamp', 'temperature'])
        
        start_date = datetime.now() - timedelta(days=30)
        
        for _ in range(1000):
            s_id = f"sensor_{random.randint(1, NUM_SENSORS)}"
            random_seconds = random.randint(0, 30 * 24 * 60 * 60)
            ts = (start_date + timedelta(seconds=random_seconds)).isoformat()
            temp = round(random.uniform(20.0, 30.0), 2)
            writer.writerow([s_id, ts, temp])
    print("Histórico concluído.")

if __name__ == "__main__":
    generate_metadata()
    generate_history()
    print(f"\nArquivos gerados na pasta: {os.getcwd()}")
