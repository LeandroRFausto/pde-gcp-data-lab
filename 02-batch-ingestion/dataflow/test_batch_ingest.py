import logging
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
# Importa as classes do nosso script original 
from batch_ingest import ParseMetadata, ParseHistory

def test_parse_metadata_sucesso():
    input_data = [
        "sensor_1,Galpao_A,TMP-2000,2023-01-01",
        "sensor_2,Galpao_B,TMP-X1,2023-02-01"
    ]
    expected_output = [
        {'sensor_id': 'sensor_1', 'location': 'Galpao_A', 'model': 'TMP-2000', 'install_date': '2023-01-01'},
        {'sensor_id': 'sensor_2', 'location': 'Galpao_B', 'model': 'TMP-X1', 'install_date': '2023-02-01'}
    ]
    with TestPipeline() as p:
        output = (p | beam.Create(input_data) | beam.ParDo(ParseMetadata()))
        assert_that(output, equal_to(expected_output))

def test_parse_history_ignora_cabecalho():
    input_data = ["sensor_id,timestamp,temperature", "sensor_5,2025-01-10T14:00,25.5"]
    expected_output = [{'sensor_id': 'sensor_5', 'timestamp': '2025-01-10T14:00', 'temperature': 25.5}]
    with TestPipeline() as p:
        output = (p | beam.Create(input_data) | beam.ParDo(ParseHistory()))
        assert_that(output, equal_to(expected_output))
