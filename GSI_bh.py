from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

# Define os argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 3, 15),
    'schedule_interval': '@daily',  # Define a frequência de execução da DAG
}

# Define o nome da DAG e suas configurações
dag = DAG(
    'tb_gsi_bh',
    default_args=default_args,
    description='Copia dados do Google Cloud Storage para o BigQuery',
    catchup=False
)

# Define o esquema da tabela no BigQuery
schema = [
    {"name": "ID_GSI", "type": "STRING", "mode": "NULLABLE"},
    {"name": "NOME_GRUPO", "type": "STRING", "mode": "NULLABLE"},
    {"name": "NOME_SUBGRUPO", "type": "STRING", "mode": "NULLABLE"},
    {"name": "NOME_ITEM", "type": "STRING", "mode": "NULLABLE"}
]


# Define o operador para copiar dados do Google Cloud Storage para o BigQuery
copy_to_bigquery = GCSToBigQueryOperator(
    task_id='copy_to_bigquery',
    bucket='basebh',  # Substitua pelo nome do seu bucket do GCS
    source_objects=['financeiro_bh/GSI_bh.csv'],  # Substitua pelo caminho do seu arquivo no GCS
    destination_project_dataset_table='etlgcp-416120.FINANCEIRO.tb_gsi_bh',  # Substitua pelo destino no BigQuery
    schema_fields=schema,  # Define o esquema da tabela
    source_format='CSV',  # Formato dos dados no GCS
    field_delimiter=';',  # Define o separador dos campos
    create_disposition='CREATE_IF_NEEDED',  # Se a tabela no BigQuery deve ser criada se não existir
    write_disposition='WRITE_TRUNCATE',  # O que fazer com os dados na tabela existente no BigQuery
    dag=dag
)

# Define a ordem de execução dos operadores
copy_to_bigquery