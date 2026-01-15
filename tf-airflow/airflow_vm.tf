# -----------------------
# Airflow VM (Docker Compose)
# Stack separado (tf-airflow)
# -----------------------

resource "google_service_account" "airflow_sa" {
  account_id   = "airflow-sa"
  display_name = "Airflow Orchestration SA"
}

# IAM (mínimo pro lab)
resource "google_project_iam_member" "airflow_sa_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_project_iam_member" "airflow_sa_bq_jobuser" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_project_iam_member" "airflow_sa_bq_dataeditor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_project_iam_member" "airflow_sa_dataflow" {
  project = var.project_id
  role    = "roles/dataflow.developer"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_project_iam_member" "airflow_sa_dataform" {
  project = var.project_id
  role    = "roles/dataform.editor"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# Firewall: liberar UI do Airflow (8080)
# (Para lab rápido; em produção restrinja source_ranges pro seu IP)
resource "google_compute_firewall" "allow_airflow_ui" {
  name    = "${var.project_id}-allow-airflow-ui"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  target_tags   = ["airflow"]
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_instance" "airflow_vm" {
  name         = "airflow-vm"
  machine_type = "e2-standard-2"
  zone         = "${var.region}-b"

  tags = ["airflow"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 30
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  service_account {
    email  = google_service_account.airflow_sa.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    set -euo pipefail

    apt-get update -y
    apt-get install -y docker.io docker-compose-plugin git curl jq

    systemctl enable docker
    systemctl start docker

    mkdir -p /opt/airflow/{dags,logs,plugins,config}

    cat > /opt/airflow/docker-compose.yml <<'YAML'
    services:
      postgres:
        image: postgres:15
        environment:
          POSTGRES_USER: airflow
          POSTGRES_PASSWORD: airflow
          POSTGRES_DB: airflow
        volumes:
          - postgres-db-volume:/var/lib/postgresql/data
        restart: unless-stopped

      airflow-webserver:
        image: apache/airflow:2.8.4-python3.11
        depends_on:
          - postgres
        environment:
          AIRFLOW__CORE__EXECUTOR: LocalExecutor
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
          AIRFLOW__CORE__LOAD_EXAMPLES: "false"
          AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
          _PIP_ADDITIONAL_REQUIREMENTS: >-
            apache-airflow-providers-google==10.16.0
            google-auth==2.*
            requests==2.*
        volumes:
          - /opt/airflow/dags:/opt/airflow/dags
          - /opt/airflow/logs:/opt/airflow/logs
          - /opt/airflow/plugins:/opt/airflow/plugins
        ports:
          - "8080:8080"
        command: webserver
        restart: unless-stopped

      airflow-scheduler:
        image: apache/airflow:2.8.4-python3.11
        depends_on:
          - postgres
        environment:
          AIRFLOW__CORE__EXECUTOR: LocalExecutor
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
          AIRFLOW__CORE__LOAD_EXAMPLES: "false"
          _PIP_ADDITIONAL_REQUIREMENTS: >-
            apache-airflow-providers-google==10.16.0
            google-auth==2.*
            requests==2.*
        volumes:
          - /opt/airflow/dags:/opt/airflow/dags
          - /opt/airflow/logs:/opt/airflow/logs
          - /opt/airflow/plugins:/opt/airflow/plugins
        command: scheduler
        restart: unless-stopped

    volumes:
      postgres-db-volume:
    YAML

    cd /opt/airflow

    docker compose up -d postgres
    sleep 10

    docker compose run --rm airflow-webserver airflow db init || true
    docker compose run --rm airflow-webserver airflow users create \
      --username admin --password admin \
      --firstname Admin --lastname User --role Admin --email admin@example.com || true

    docker compose up -d
  EOT
}

output "airflow_vm_external_ip" {
  value = google_compute_instance.airflow_vm.network_interface[0].access_config[0].nat_ip
}
