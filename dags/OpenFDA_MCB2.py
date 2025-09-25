# dags/openfda_mcb2_test.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from datetime import timedelta
import pendulum
import pandas as pd
import requests

# ============================================================
# CONFIGURAÇÕES (ajuste se necessário)
# ============================================================
GCP_PROJECT  = "365846072239"      # seu project id (numérico ou string)
BQ_DATASET   = "dataset_fda"       # dataset no BigQuery
BQ_TABLE     = "openfda_sildenafil_daily_test"   # tabela de destino (teste!)
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"  # conexão GCP configurada no Airflow
POOL_NAME    = "openfda_api"           # pode reaproveitar o pool já existente
# ============================================================

# Criamos uma sessão HTTP com um cabeçalho "educado"
SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "mda-openfda-etl/1.0 (contato: voce@exemplo.gov.br)"}
)


def _openfda_get(url: str):
    """
    Consulta simples à API openFDA (SEM retries, para testes).
    Se a API responder com erro, a execução falha imediatamente.
    """
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()   # se vier erro 4xx ou 5xx, levanta exceção
    return r.json()


@task(pool=POOL_NAME, retries=0)  # <<< sem retries no Airflow
def fetch_day_and_to_gbq():
    """
    Task de teste:
      - pega o dia do run (data_interval_start)
      - consulta a API openFDA para "sildenafil citrate"
      - agrega no nível diário (semana não é necessária aqui)
      - grava no BigQuery
    """
    # -----------------------------------------------------------------
    # 1) Descobre o dia de referência do run
    # -----------------------------------------------------------------
    ctx = get_current_context()
    run_day = ctx["data_interval_start"].date()   # exemplo: 2025-09-23
    day_str = run_day.strftime("%Y%m%d")          # formato AAAAMMDD

    # -----------------------------------------------------------------
    # 2) Monta a URL para consultar a API openFDA
    # -----------------------------------------------------------------
    url = (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22"
        f"+AND+receivedate:[{day_str}+TO+{day_str}]"
        "&count=receivedate"
    )
    print("[openFDA] URL:", url)

    # -----------------------------------------------------------------
    # 3) Chama a API e coleta os resultados
    # -----------------------------------------------------------------
    data = _openfda_get(url)
    results = data.get("results", [])
    if not results:
        print("Sem resultados para o dia:", run_day)
        return

    # -----------------------------------------------------------------
    # 4) Converte em DataFrame pandas
    # -----------------------------------------------------------------
    df = pd.DataFrame(results)  # colunas: time (AAAAMMDD), count
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)

    # preview no log
    print(df.head().to_string())

    # -----------------------------------------------------------------
    # 5) Prepara escrita no BigQuery
    # -----------------------------------------------------------------
    bq_hook = BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        use_legacy_sql=False
    )
    credentials = bq_hook.get_credentials()

    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"
    schema = [
        {"name": "time",   "type": "TIMESTAMP"},
        {"name": "events", "type": "INTEGER"},
        {"name": "day",    "type": "DATE"},   # dia de referência do run
    ]

    df = df.rename(columns={"count": "events"})
    df["day"] = pd.to_datetime(run_day)

    # -----------------------------------------------------------------
    # 6) Grava no BigQuery (append simples)
    # -----------------------------------------------------------------
    df.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=credentials,
        table_schema=schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )

    print(f"Gravados {len(df)} registros em {GCP_PROJECT}.{destination_table}.")


# ============================================================
# DEFINIÇÃO DA DAG
# ============================================================
@dag(
    dag_id="openfda_mcb2_test",
    schedule="@daily",   # <<< agora roda todo dia
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["openfda", "bigquery", "test"],
)
def openfda_pipeline_test():
    fetch_day_and_to_gbq()

# Instancia a DAG
dag = openfda_pipeline_test()
