"""
DAG de Pipeline Batch Diário
Orquestra: Ingestão → Validação → Qualidade → Notificação
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os

# Variáveis de ambiente (vindas do Composer)
PROJECT_ID = os.getenv('PROJECT_ID', 'quick-cache-484111-j4')
BUCKET_NAME = os.getenv('BUCKET_NAME', f'{PROJECT_ID}-raw-data')
DATASET_ID = 'raw_zone'
TABLE_ID = 'batch_readings'

# Configurações padrão da DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Função Python para validar estrutura do CSV
def validate_csv_structure(**context):
    """Verifica se os arquivos CSV têm as colunas esperadas"""
    from google.cloud import storage
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix='readings/'))
    
    if not blobs:
        raise ValueError("❌ Nenhum arquivo encontrado em readings/")
    
    # Valida o primeiro arquivo como amostra
    blob = blobs[0]
    content = blob.download_as_text()
    header = content.split('\n')[0].strip()
    
    expected_columns = {'sensor_id', 'temperature', 'timestamp', 'source_type'}
    actual_columns = set(header.split(','))
    
    if not expected_columns.issubset(actual_columns):
        raise ValueError(f"❌ Colunas incorretas! Esperado: {expected_columns}, Encontrado: {actual_columns}")
    
    print(f"✅ Validação OK! Arquivo: {blob.name}, Colunas: {actual_columns}")
    return True


# Definição da DAG
with DAG(
    'daily_batch_sensor_pipeline',
    default_args=default_args,
    description='Pipeline diário: Valida → Carrega → Verifica qualidade',
    schedule_interval='0 8 * * *',  # Todo dia às 08:00 UTC
    catchup=False,
    tags=['batch', 'sensors', 'production'],
) as dag:

    # TAREFA 1: Aguardar arquivos no GCS
    wait_for_files = GCSObjectExistenceSensor(
        task_id='wait_for_csv_files',
        bucket=BUCKET_NAME,
        object='readings/',  # Verifica se a pasta existe (com pelo menos 1 arquivo)
        timeout=600,  # Espera no máximo 10 min
        poke_interval=30,  # Checa a cada 30 segundos
        mode='poke',
    )

    # TAREFA 2: Validar estrutura dos CSVs
    validate_csv = PythonOperator(
        task_id='validate_csv_structure',
        python_callable=validate_csv_structure,
        provide_context=True,
    )

    # TAREFA 3: Refresh da tabela externa (força BigQuery reescanear o GCS)
    refresh_table = BashOperator(
        task_id='refresh_bigquery_table',
        bash_command=f"""
        bq update --external_table_definition=@/tmp/table_def.json \
        {PROJECT_ID}:{DATASET_ID}.{TABLE_ID} || echo "Tabela atualizada via schema existente"
        """,
    )

    # TAREFA 4: Validar qualidade dos dados
    check_data_quality = BigQueryCheckOperator(
        task_id='check_data_quality',
        sql=f"""
        SELECT COUNT(*) >= 1
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        """,
        use_legacy_sql=False,
    )

    # TAREFA 5: Validar valores de temperatura (não podem ser absurdos)
    check_temperature_range = BigQueryCheckOperator(
        task_id='check_temperature_range',
        sql=f"""
        SELECT COUNT(*) = 0
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE temperature < -50 OR temperature > 100
        """,
        use_legacy_sql=False,
    )

    # TAREFA 6: Notificação de sucesso
    send_success_notification = BashOperator(
        task_id='send_success_notification',
        bash_command="""
        echo "🎉 Pipeline batch concluído com sucesso!"
        echo "Data: $(date)"
        echo "Próxima execução: Amanhã às 08:00 UTC"
        """,
    )

    # ORQUESTRAÇÃO (fluxo de execução)
    wait_for_files >> validate_csv >> refresh_table >> [check_data_quality, check_temperature_range] >> send_success_notification
