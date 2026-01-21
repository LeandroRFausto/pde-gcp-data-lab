# Base oficial do Dataflow para Flex Template (Python)
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

WORKDIR /template

# Dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Seu pipeline
COPY streaming_job.py .

# Diz ao launcher qual arquivo executar
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/streaming_job.py

# (Opcional) requirements via env também (não precisa, mas ok)
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/template/requirements.txt
