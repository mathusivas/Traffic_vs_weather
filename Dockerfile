FROM apache/airflow:2.9.2-python3.11

# Install extra Python deps
USER root
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
      requests==2.32.3 \
      pandas==2.2.2 \
      azure-storage-blob==12.19.1 \
      python-dotenv==1.0.1

# Back to airflow user
USER airflow

# Copy DAGs and includes (compose also mounts them, but this helps for image builds)
COPY dags/ /opt/airflow/dags/
COPY include/ /opt/airflow/include/
