import argparse
import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from google.cloud import firestore

# --- Classes de Transformação ---

class AddTimestamp(beam.DoFn):
    def process(self, element):
        # Adiciona timestamp de processamento se não vier na mensagem
        if 'timestamp' not in element:
            element['timestamp'] = datetime.now().isoformat()
        yield element

class WriteToFirestore(beam.DoFn):
    """Escreve o último estado de cada sensor no Firestore"""
    def start_bundle(self):
        # Inicializa o cliente uma vez por bundle
        self.db = firestore.Client()

    def process(self, element):
        try:
            # Converte o ID para string para usar como chave do documento
            sensor_id = str(element.get('sensor_id', 'unknown'))
            
            # Escreve na coleção 'sensors'. 
            # O .set() funciona como UPSERT (cria ou atualiza)
            doc_ref = self.db.collection('sensors').document(sensor_id)
            doc_ref.set(element)
        except Exception as e:
            logging.error(f"Erro ao escrever no Firestore: {e}")

# --- Definição do Pipeline ---

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True, help='Tópico do Pub/Sub para ler')
    parser.add_argument('--output_table', required=True, help='Tabela do BigQuery para escrever')
    
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    # Esquema da tabela do BigQuery
    table_schema = 'sensor_id:STRING, temperature:FLOAT, timestamp:TIMESTAMP, source_type:STRING'

    with beam.Pipeline(options=pipeline_options) as p:
        
        # 1. Leitura e Processamento Inicial
        messages = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | 'ParseJSON' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | 'AddTimestamp' >> beam.ParDo(AddTimestamp())
        )

        # 2. Ramificação A: Cold Storage (Histórico no BigQuery)
        messages | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=known_args.output_table,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # 3. Ramificação B: Hot Storage (Tempo Real no Firestore)
        messages | 'WriteToFirestore' >> beam.ParDo(WriteToFirestore())

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
