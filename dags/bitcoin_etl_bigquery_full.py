# dags/bitcoin_etl_bigquery_full.py
from __future__ import annotations

# Airflow
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

# Datas / terceiros
from datetime import timedelta
import pendulum
import requests
import pandas as pd

# HTTP retry/backoff
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# BigQuery (para obter credenciais do Airflow Connection)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# ------------------------------------------------------------
# Objetivo do arquivo
# ------------------------------------------------------------
# - Executar diariamente (00:00 UTC) uma coleta de preço do Bitcoin
#   na CoinGecko em granularidade DIÁRIA.
# - Garantir 1 linha por dia e carregar em tabela do BigQuery.
# - Tratar rate limit/erros transitórios (429/5xx) com retry/backoff.
# - Ler API key Demo via Airflow Variable (segurança básica).
# - Usar credenciais do GCP a partir da conexão do Airflow.
#
# Observação:
# - Em produção, considere idempotência/UPSERT (MERGE) e monitoria (SLAs).
# ------------------------------------------------------------

# ====== CONFIG ===============================================================
GCP_PROJECT  = "365846072239"           # Project ID do GCP
BQ_DATASET   = "crypto"                 # Dataset de destino no BigQuery
BQ_TABLE     = "bitcoin_history_daily"  # Tabela diária (1 linha por dia)
BQ_LOCATION  = "US"                     # Região do dataset
GCP_CONN_ID  = "google_cloud_default"   # Conexão GCP no Airflow
# ============================================================================

DEFAULT_ARGS = {
    "email_on_failure": True,
    "owner": "Alex Lopes,Open in Cloud IDE",
}

@task
def fetch_and_to_gbq():
    """
    Tarefa principal:
    - Define a janela "ontem" (UTC) a partir do contexto do Airflow.
    - Chama /coins/bitcoin/market_chart/range com interval='daily'.
    - Constrói um DataFrame, agrega para 1 linha/dia e carrega no BigQuery.
    - Implementa retry/backoff e validação da API key Demo.
    """

    # 1) Janela de tempo (ontem em UTC): [D-1 00:00, D 00:00)
    ctx = get_current_context()
    end_time = ctx["data_interval_start"]  # datetime em UTC
    start_time = end_time - timedelta(days=1)
    print(f"[UTC] target window: {start_time} -> {end_time}")

    start_s = int(start_time.timestamp())  # CoinGecko espera segundos
    end_s   = int(end_time.timestamp())

    # 2) Sessão HTTP com API key (Demo) + retry/backoff
    session = requests.Session()
    session.headers.update({"User-Agent": "mda-coingecko-etl/1.0"})

    # Lê a chave do Airflow Variables (falha explícita se não existir)
    DEMO_KEY = Variable.get("intro-ciencia-dados", default_var=None)
    if not DEMO_KEY:
        raise AirflowFailException(
            "A variável Airflow 'intro-ciencia-dados' não está definida. "
            "Cadastre a sua Demo API Key em Admin > Variables."
        )
    session.headers.update({"x-cg-demo-api-key": DEMO_KEY})

    retry = Retry(
        total=5,
        backoff_factor=2,                 # 1s, 2s, 4s, 8s...
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))

    # 3) Chamada à API (granularidade diária)
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_s,
        "to": end_s,
        "interval": "daily",  # reduz volume e risco de rate limit
        "precision": "full",
    }

    r = session.get(url, params=params, timeout=30)
    if r.status_code == 401:
        raise AirflowFailException(
            "CoinGecko retornou 401 Unauthorized. Verifique se a Demo API Key é válida "
            "e se está ativa na sua conta."
        )
    r.raise_for_status()
    payload = r.json()

    # 4) Extrai arrays (preços, market cap e volume)
    prices = payload.get("prices", [])
    caps   = payload.get("market_caps", [])
    vols   = payload.get("total_volumes", [])

    if not prices:
        print("No data returned for the specified window.")
        return

    # 5) Constrói DataFrame e normaliza o timestamp
    df_p = pd.DataFrame(prices, columns=["time_ms", "price_usd"])
    df_c = pd.DataFrame(caps,   columns=["time_ms", "market_cap_usd"])
    df_v = pd.DataFrame(vols,   columns=["time_ms", "volume_usd"])

    df = df_p.merge(df_c, on="time_ms", how="outer").merge(df_v, on="time_ms", how="outer")
    df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    df.drop(columns=["time_ms"], inplace=True)
    df.sort_values("time", inplace=True)

    # 6) Garante 1 linha por dia (bordas do range podem trazer 2 pontos)
    df["date"] = df["time"].dt.date
    df = (
        df.groupby("date", as_index=False)
          .agg({
              "time": "last",             # timestamp representativo
              "price_usd": "last",        # troque para "mean" se quiser média diária
              "market_cap_usd": "last",
              "volume_usd": "last",
          })
    )
    df.drop(columns=["date"], inplace=True)

    print("Preview daily rows to load:")
    print(df.head(10).to_string(index=False))

    # 7) Carrega no BigQuery (append)
    # Nota: append pode duplicar se reprocessar o mesmo dia.
    # Para produção, considere particionar por DATE(time) + dedupe, ou usar MERGE.
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()
    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"

    table_schema = [
        {"name": "time",            "type": "TIMESTAMP"},
        {"name": "price_usd",       "type": "FLOAT"},
        {"name": "market_cap_usd",  "type": "FLOAT"},
        {"name": "volume_usd",      "type": "FLOAT"},
    ]

    if df.index.name == "time":
        df = df.reset_index()

    df.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=credentials,
        table_schema=table_schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )

    print(f"Loaded {len(df)} rows to {GCP_PROJECT}.{destination_table} (location={BQ_LOCATION}).")


@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",  # diário às 00:00 UTC
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,          # processa runs pendentes em série
    max_active_runs=1,     # evita concorrência
    owner_links={
        "Alex Lopes": "mailto:alexlopespereira@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/cm3webulw15k701npm2uhu77t/cloud-ide/cm42rbvn10lqk01nlco70l0b8/cm44gkosq0tof01mxajutk86g",
    },
    tags=["bitcoin", "etl", "coingecko", "bigquery", "pandas-gbq", "daily"],
)
def bitcoin_etl_bigquery_full():
    fetch_and_to_gbq()

dag = bitcoin_etl_bigquery_full()
