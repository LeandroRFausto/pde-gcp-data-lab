# GCP Data Engineering Lab  
### Pipeline IoT End-to-End (Batch + Streaming + Governança + BI + ML)

## 🎯 Objetivo

Este projeto implementa um **pipeline de dados moderno e escalável**, cobrindo ingestão **batch** e **streaming**, orquestração, transformações analíticas, governança de dados, BI e ML, utilizando diversos **serviços gerenciados da Google Cloud Platform (GCP)**.

O foco está na **qualidade arquitetural**, **boas práticas** e **decisões conscientes**, consolidadas durante a preparação e obtenção da certificação **Google Professional Data Engineer**.

## 🧰 Tech Stack (GCP)

- **Storage & Analytics:** Cloud Storage (GCS), BigQuery  
- **Batch Processing:** Dataproc (Apache Spark)  
- **Streaming:** Pub/Sub, Dataflow (Apache Beam)  
- **Orchestration:** Cloud Composer (Airflow)  
- **Transformations:** Dataform  
- **Governance:** Dataplex (lakes, zones, tags)  
- **Machine Learning:** Vertex AI Workbench  
- **State / Observability (dev):** Firestore  
- **Infrastructure as Code:** Terraform  

---

## 🧱 Architecture Overview

<p align="center">
  <img src="docs/architecture/architecture_overview.svg"
       alt="GCP Data Engineering Architecture"
       width="900"/>
</p>

<details>
<summary>📐 View architecture as Mermaid code</summary>

```mermaid
---
config:
  look: classic
  layout: fixed
---
flowchart LR

subgraph Legend["Legend"]
  L1["Batch flow"]
  L2["Streaming flow"]
  L3["Governance coverage (Dataplex)"]
end

subgraph Sources["Data Sources"]
  CSV["CSV Local / Batch<br>(historical_logs.csv, sensors_metadata.csv)"]
  IoT["IoT Devices / Simulator"]
end

subgraph GCS_Landing["GCS - Landing Zone (ephemeral)"]
  LZ["landing-zone bucket"]
end

subgraph Orchestration["Cloud Composer / Airflow"]
  DAG["DAG: 03_end_to_end_diagram<br>+ Dataform invoke"]
end

subgraph Dataproc["Dataproc (ephemeral)"]
  Spark["Spark Batch Job<br>normalize/validate"]
end

subgraph Raw_GCS["GCS - Raw Zone"]
  Parquet["Parquet Files (v1)<br>dims/ + facts/<br>partitioned by event_date"]
end

subgraph Raw_BQ["BigQuery - raw_zone"]
  BQ_Dim_Ext["raw_zone.dim_sensors_ext"]
  BQ_Batch_Ext["raw_zone.iot_readings_batch_ext"]
  BQ_Stream["raw_zone.iot_readings_stream"]
  BQ_Raw_DLQ["raw_zone.iot_readings_dlq"]
  BQ_ParquetDims["raw_zone.readings_v1_dims_dim_sensors"]
  BQ_ParquetFacts["raw_zone.readings_v1_facts_iot_readings_batch"]
end

subgraph Streaming["Streaming Pipeline"]
  PS["Pub/Sub<br>iot-readings"]
  DF["Dataflow (Apache Beam)"]
end

subgraph DLQ["BigQuery - dlq_layer"]
  BQ_DLQ["dlq_layer.iot_readings_dlq"]
end

subgraph Firestore["Firestore (state / observability)"]
  FS["sensors/{sensor_id}<br>payload + last_update"]
end

subgraph DW["BigQuery - sensor_dw (Dataform)"]
  Dim["sensor_dw.dim_sensors"]
  Fact["sensor_dw.fact_iot_readings"]
  FactE["sensor_dw.fact_iot_readings_enriched"]
end

subgraph Gold["BigQuery - gold_layer (consumption)"]
  GoldIoT["gold_layer.iot_readings"]
  GoldDaily["gold_layer.iot_daily_temperature"]
  GoldAnom["gold_layer.iot_temperature_anomalies"]
end

subgraph Dataform_WS["Dataform (workspace)"]
  DF_Repo["dataform dataset / actions"]
  DF_View1["first_view"]
  DF_View2["second_view"]
end

subgraph Dataplex["Dataplex (Governança)"]
  Lake["Lake: main-lake"]
  ZRaw["Zone: raw-zone (RAW)"]
  ZDW["Zone: dw-zone (CURATED)"]
  ZGold["Zone: gold-zone (CURATED)"]
  Tags["Tag Template: data_governance<br>(layer/owner/sla/sensitivity)"]
end

subgraph BI["Looker Studio"]
  Looker["Dashboards<br>Overview + Anomalias"]
end

subgraph ML["Vertex AI Workbench"]
  Vertex["Notebook<br>Anomaly Detection (Z-score)"]
end

CSV --> LZ
LZ --> Spark
Spark --> Parquet
Parquet --> BQ_Batch_Ext & BQ_Dim_Ext & BQ_ParquetDims & BQ_ParquetFacts

IoT --> PS
PS --> DF
DF --> BQ_Stream & BQ_DLQ & BQ_Raw_DLQ & FS

BQ_Dim_Ext --> Dim
BQ_Batch_Ext --> Fact
BQ_Stream --> Fact
Dim --> FactE
Fact --> FactE
FactE --> GoldIoT

GoldIoT --> Vertex & GoldDaily & Looker
Vertex --> GoldAnom
GoldDaily --> Looker
GoldAnom --> Looker

DAG --> LZ & Spark & DF_Repo
DF_Repo --> DF_View1 & DF_View2

Lake --> ZRaw & ZDW & ZGold
ZRaw --> Raw_GCS & Raw_BQ & DLQ
ZDW --> DW
ZGold --> Gold
Tags --> ZRaw & ZDW & ZGold
```
</details> 


## 📂 Repository Structure

```text
gcp-data-engineering-lab/
│
├─ dags/
│  └─ 03_end_to_end_diagram.py
│
├─ data/
│  └─ local/
│     └─ legacy/
│        ├─ historical_logs.csv
│        └─ sensors_metadata.csv
│
├─ infra/
│  ├─ main.tf
│  ├─ providers.tf
│  ├─ variables.tf
│  ├─ outputs.tf
│  ├─ versions.tf
│  └─ modules/
│     ├─ 01-storage/
│     ├─ 02-compute/
│     └─ 03-governance/
│
├─ pipelines/
│  ├─ batch/
│  │  └─ dataproc/jobs/spark_batch_ingest.py
│  └─ streaming/
│     └─ dataflow/streaming_job.py
│
├─ scripts/
│  └─ check_firestore.py
│
├─ requirements.txt
└─ .gitignore
```
Dados locais, estado do Terraform e artefatos temporários são ignorados por design.

🔁 Data Ingestion
📦 Batch Pipeline
Geração de dados históricos em CSV
Upload para GCS Landing Zone (efêmera)
Processamento via Dataproc (Spark):
normalização
validação
escrita em Parquet particionado
Persistência na RAW Zone

📡 Streaming Pipeline
Simulação de dispositivos IoT
Publicação em Pub/Sub
Processamento via Dataflow (Apache Beam):
escrita em BigQuery (streaming table)
escrita em DLQ (BigQuery)
escrita em Firestore como state store (dev)
Semântica aplicada:
at-least-once no streaming
deduplicação garantida na camada GOLD

🧠 Orchestration (Airflow / Cloud Composer)
A DAG 03_end_to_end_diagram coordena o fluxo end-to-end:
Aguarda arquivos na Landing Zone
Executa Spark em Dataproc efêmero
Remove o cluster após execução (otimização de custo)
Dispara transformações analíticas via Dataform
Características:
DAG idempotente
Infra não persistente
Execuções limpas e controladas
<p align="center"> <img src="docs/architecture/airflow.png" alt="Airflow DAG - End-to-End Pipeline" width="900"/> </p>
<br/>


🗄️ Data Warehouse (BigQuery)
O BigQuery atua como single source of truth, organizado em camadas:
RAW: dados brutos batch + streaming
DW: modelo canônico (fatos e dimensões)
GOLD: dados consolidados para consumo
Transformações declarativas e versionadas via Dataform.
<p align="center"> <img src="docs/architecture/bigquery.png" alt="BigQuery Datasets and Tables" width="900"/> </p>
<br/>


🏛️ Data Governance (Dataplex)
Lake: main-lake
Zones: RAW, CURATED (DW), CURATED (GOLD)
Assets: GCS e BigQuery
Tag Template: data_governance
layer
owner
sla_horas
sensitivity
Governança visível, auditável e desacoplada do pipeline.
<p align="center"> <img src="docs/architecture/dataplex.png" alt="Dataplex Governance" width="900"/> </p>
<br/>


📊 BI (Looker Studio)
Dashboards consomem diretamente a camada GOLD, sem SQL para o usuário final.
Inclui:
visão geral de temperatura
tendências temporais
anomalias detectadas
<p align="center"> <img src="docs/architecture/looker.png" alt="Looker Studio Dashboards" width="900"/> </p>
<br/>


🤖 Machine Learning (Vertex AI)
Uso do Vertex AI Workbench
Extração de dados da camada GOLD
Anomaly Detection baseado em Z-score global
Escrita em gold_layer.iot_temperature_anomalies
Decisão consciente:
foco em explicabilidade
base sólida para evolução futura

🧪 Validations Performed
Contagem consistente entre camadas
Deduplicação correta
Partições coerentes
IAM validado
Batch e streaming coexistindo
DLQ funcional
Governança visível no Dataplex

🏁 Conclusion
Este projeto entrega:
✅ Pipeline real e end-to-end
✅ Batch + Streaming integrados
✅ Governança aplicada corretamente
✅ BI e ML fechando o ciclo
✅ Infraestrutura desacoplada e escalável
✅ Portfólio tecnicamente defendível em entrevista

Arquitetura completa de engenharia de dados, com decisões explícitas e trade-offs conscientes.