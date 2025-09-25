# dags/openfda_to_gbq.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from datetime import timedelta
import pendulum
import pandas as pd
import requests
import time
from datetime import datetime, date

# ===== CONFIG =====
GCP_PROJECT  = "365846072239"            # seu project id (numérico funciona)
BQ_DATASET   = "crypto"                  # ajuste se preferir outro dataset
BQ_TABLE     = "openfda_sildenafil_weekly"
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"    # conexão GCP no Airflow
POOL_NAME    = "openfda_api"             # crie em Admin -> Pools (1 slot)
# ==================

# Sessão HTTP com cabeçalho "educado"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "mda-openfda-etl/1.0 (contato: voce@exemplo.gov.br)"})


def _end_of_month(dt: date) -> date:
    # pega último dia do mês
    first_next = (dt.replace(day=1) + timedelta(days=32)).replace(day=1)
    return first_next - timedelta(days=1)


def _openfda_get(url: str, max_attempts=6):
    # retry com backoff simples (openFDA tem limites; count já ajuda)
    for attempt in range(1, max_attempts + 1):
        r = SESSION.get(url, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            wait = min(60, 2 ** attempt)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"OpenFDA falhou após {max_attempts} tentativas: {url}")


@task(pool=POOL_NAME, retries=5, retry_exponential_backoff=True, retry_delay=timedelta(seconds=15))
def fetch_month_and_to_gbq():
    """
    Para o mês do data_interval_start (mensal), consulta a API openFDA:
      - medicamento: "sildenafil citrate"
      - janela: [YYYYMM01 TO YYYYMMúltimo]
      - count=receivedate  -> retorna contagem diária
    Agrega por semana e grava no BigQuery.
    """
    ctx = get_current_context()
    # esta DAG é mensal; usamos o mês do run
    start_month = ctx["data_interval_start"].date().replace(day=1)
    end_month   = _end_of_month(start_month)

    start_str = start_month.strftime("%Y%m%d")
    end_str   = end_month.strftime("%Y%m%d")

    # query usando count (agregado diário), depois somamos por semana
    url = (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22"
        f"+AND+receivedate:[{start_str}+TO+{end_str}]"
        "&count=receivedate"
    )
    print("[openFDA] URL:", url)

    data = _openfda_get(url)
    results = data.get("results", [])
    if not results:
        print("Sem resultados para o mês:", start_month)
        return

    df = pd.DataFrame(results)  # colunas: time (string AAAAMMDD) e count
    # converte para datetime UTC neutro
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    # agrega por semana (fim de semana padrão = domingo)
    weekly = (
        df.groupby(pd.Grouper(key="time", freq="W"))["count"]
          .sum()
          .reset_index()
          .rename(columns={"count": "events"})
    )

    # preview
    print(weekly.head().to_string())

    # prepara para BQ
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"
    schema = [
        {"name": "time",   "type": "TIMESTAMP"},
        {"name": "events", "type": "INTEGER"},
        {"name": "month",  "type": "DATE"},  # mês de referência do run
    ]
    weekly["month"] = pd.to_datetime(start_month)

    # grava
    weekly.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=credentials,
        table_schema=schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )

    print(f"Gravados {len(weekly)} registros em {GCP_PROJECT}.{destination_table}.")


@dag(
    dag_id="openfda_to_gbq",
    schedule="@monthly",
    start_date=pendulum.datetime(2020, 11, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,     # importante p/ evitar rajadas
    tags=["openfda", "bigquery"],
)
def openfda_pipeline():
    fetch_month_and_to_gbq()

dag = openfda_pipeline()
