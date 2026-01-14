FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# Define o diretório de trabalho
WORKDIR /dataflow/template

# Copia o script do pipeline e requirements (se houver)
COPY batch_ingest.py .

# Variável de ambiente necessária para o Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/dataflow/template/batch_ingest.py"

# Instala dependências extras se necessário (ex: pandas, requests)
# RUN pip install -U apache-beam[gcp] 
# (A imagem base já tem o Beam instalado, mas é bom garantir se usar libs extras)
