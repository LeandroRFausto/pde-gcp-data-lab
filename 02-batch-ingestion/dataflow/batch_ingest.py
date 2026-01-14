import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Esquemas das tabelas (Schemas)
# Tabela de Dimensão: Dados cadastrais dos sensores
SCHEMA_METADATA = "sensor_id:STRING, location:STRING, model:STRING, install_date:DATE"

# Tabela de Fato: Histórico de leituras (mesmo schema da tabela IoT, mas batch)
SCHEMA_HISTORY = "sensor_id:STRING, timestamp:TIMESTAMP, temperature:FLOAT"

class ParseMetadata(beam.DoFn):
    def process(self, element):
        # O CSV vem como string única: "sensor_1,Warehouse_A,TMP-2000,2023-01-15"
        try:
            row = element.split(',')
            # Pula o cabeçalho se ele for lido como dado (simples check)
            if row[0] == 'sensor_id':
                return

            yield {
                'sensor_id': row[0],
                'location': row[1],
                'model': row[2],
                'install_date': row[3]
            }
        except Exception as e:
            logging.error(f"Erro ao parsear metadata: {element} - {e}")

class ParseHistory(beam.DoFn):
    def process(self, element):
        # CSV: "sensor_5,2025-01-10T14:30:00,25.5"
        try:
            row = element.split(',')
            if row[0] == 'sensor_id':
                return
            
            yield {
                'sensor_id': row[0],
                'timestamp': row[1],
                'temperature': float(row[2])
            }
        except Exception as e:
            logging.error(f"Erro ao parsear history: {element} - {e}")

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_bucket', required=True, help='Bucket de entrada (Landing Zone)')
    parser.add_argument('--output_dataset', required=True, help='Dataset do BigQuery')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Caminhos dos arquivos no Bucket
    metadata_path = f"gs://{known_args.input_bucket}/batch_input/metadata/*.csv"
    history_path = f"gs://{known_args.input_bucket}/batch_input/history/*.csv"
    
    # Referências das tabelas no BQ: dataset.tabela
    table_metadata = f"{known_args.output_dataset}.dim_sensors"
    table_history = f"{known_args.output_dataset}.history_logs"

    with beam.Pipeline(options=pipeline_options) as p:
        
        # --- FLUXO 1: METADATA (Dimensão) ---
        (
            p 
            | 'Read Metadata CSV' >> beam.io.ReadFromText(metadata_path)
            | 'Parse Metadata' >> beam.ParDo(ParseMetadata())
            | 'Write Metadata BQ' >> beam.io.WriteToBigQuery(
                table_metadata,
                schema=SCHEMA_METADATA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, # Substitui a tabela toda (Full Load)
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # --- FLUXO 2: HISTORY (Fatos) ---
        (
            p
            | 'Read History CSV' >> beam.io.ReadFromText(history_path)
            | 'Parse History' >> beam.ParDo(ParseHistory())
            | 'Write History BQ' >> beam.io.WriteToBigQuery(
                table_history,
                schema=SCHEMA_HISTORY,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # Adiciona dados novos
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
