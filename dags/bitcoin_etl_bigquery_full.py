# dags/bitcoin_etl_bigquery_full.py
from __future__ import annotations

# ------------------------------------------------------------
# Objetivo do arquivo
# ------------------------------------------------------------
# - Executar diariamente (00:00 UTC) uma coleta de preço do Bitcoin
#   usando a API pública da CoinGecko, já em granularidade DIÁRIA.
# - Garantir que apenas 1 linha por dia seja gravada (agregação).
# - Carregar o resultado em uma tabela do BigQuery.
# - Tratar rate limit/erros transitórios (HTTP 429/5xx) com retries/backoff.
# - Usar credenciais do GCP vindas da conexão do Airflow (BigQueryHook).
#
# Observação:
# - Este DAG foi pensado como um exercício/estudo. Em produção, avalie
#   adicionar idempotência forte (MERGE em vez de append) e monitoria (SLAs).

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context  # pega contexto (datas etc.) da execução
from datetime import timedelta
import pendulum
import requests
import pandas as pd

# Retries HTTP (para mitigar 429/5xx sem falhar de primeira)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Hook do BigQuery: apenas para obter credenciais (pandas-gbq fará o load)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# ====== CONFIG ===============================================================
# Dica: deixe esses valores em variáveis de ambiente/Connections quando levar
# para produção — aqui ficam hardcoded apenas para facilitar o estudo.
GCP_PROJECT  = "365846072239"            # Project ID do GCP
BQ_DATASET   = "crypto"                  # Dataset de destino no BigQuery
BQ_TABLE     = "bitcoin_history_daily"   # Tabela diária (1 linha por dia)
BQ_LOCATION  = "US"                      # Região do dataset (ex.: "US" ou "EU")
GCP_CONN_ID  = "google_cloud_default"    # Conexão GCP no Airflow com permissão para BQ
# ============================================================================

# Parâmetros padrão do DAG (email_on_failure útil para avisos)
DEFAULT_ARGS = {
    "email_on_failure": True,
    "owner": "Alex Lopes,Open in Cloud IDE",
}

@task
def fetch_and_to_gbq():
    """
    Tarefa principal do DAG:
    - Determina a janela de interesse do "ontem" (UTC), usando o contexto do Airflow
      (data_interval_start -> é o início do intervalo que o scheduler definiu).
    - Faz 1 chamada à API /market_chart/range com interval=daily para reduzir volume.
    - Constrói um DataFrame "tidy" e garante 1 linha por dia (agregação).
    - Envia os dados para o BigQuery via pandas-gbq (append).
    - Inclui sessão HTTP com retries/backoff para mitigar rate limits (429) e 5xx.

    Por que DIÁRIO e não HORÁRIO?
    - Para DAG de estudo, reduzir chamadas diminui a chance de 429 e simplifica
      análises; se você precisa granularidade horária, troque o "interval"
      ou use outra função.
    """

    # ------------------------------------------------------------------------
    # 1) Pega o contexto do Airflow (datas do intervalo)
    # ------------------------------------------------------------------------
    ctx = get_current_context()

    # Janela "ontem" no fuso UTC:
    # - Para um run do dia D (data_interval_start=D 00:00:00),
    #   coletamos [D-1 00:00:00, D 00:00:00).
    end_time = ctx["data_interval_start"]          # datetime em UTC
    start_time = end_time - timedelta(days=1)
    print(f"[UTC] target window: {start_time} -> {end_time}")

    # CoinGecko espera timestamps em SEGUNDOS (não ms)
    start_s = int(start_time.timestamp())
    end_s   = int(end_time.timestamp())

    # ------------------------------------------------------------------------
    # 2) Monta sessão HTTP com retry/backoff
    # ------------------------------------------------------------------------
    # Por que retries?
    # - A CoinGecko possui rate limit e pode responder 429 ou falhas transitórias.
    # - Usamos backoff exponencial e respeitamos Retry-After quando enviado.
    session = requests.Session()
    session.headers.update({"User-Agent": "mda-coingecko-etl/1.0"})  # header "educado" :)
    retry = Retry(
        total=5,                          # nº total de tentativas
        backoff_factor=2,                 # 1s, 2s, 4s, 8s...
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,  # se vier Retry-After, obedecer
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))

    # ------------------------------------------------------------------------
    # 3) Chamada à API com granularidade DIÁRIA
    # ------------------------------------------------------------------------
    # Endpoint: /coins/{id}/market_chart/range
    # - Retorna arrays: prices, market_caps, total_volumes
    # - Com interval="daily", reduzimos os pontos para 1/dia (ou próximos disso)
    #   ainda assim, por segurança, agregamos localmente (groupby date) abaixo.
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_s,
        "to": end_s,
        "interval": "daily",  # << chave para diminuir volume e evitar 429
        "precision": "full",  # mais casas decimais
    }

    # Faz a requisição com timeout e aplica política de retry acima
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()  # lança HTTPError se não for 2xx
    payload = r.json()

    # ------------------------------------------------------------------------
    # 4) Extrai arrays e valida retorno
    # ------------------------------------------------------------------------
    prices = payload.get("prices", [])
    caps   = payload.get("market_caps", [])
    vols   = payload.get("total_volumes", [])

    # Se não vierem preços, não há o que carregar (não falha o task)
    if not prices:
        print("No data returned for the specified window.")
        return

    # ------------------------------------------------------------------------
    # 5) Constrói DataFrame "tidy"
    # ------------------------------------------------------------------------
    # Estrutura típica dos arrays da CoinGecko:
    # - Cada item é [timestamp_ms, valor]
    # - Vamos juntar por timestamp_ms: preço, market cap e volume
    df_p = pd.DataFrame(prices, columns=["time_ms", "price_usd"])
    df_c = pd.DataFrame(caps,   columns=["time_ms", "market_cap_usd"])
    df_v = pd.DataFrame(vols,   columns=["time_ms", "volume_usd"])

    # Merge externo para preservar o máximo de sinais (em geral os três arrays alinham)
    df = df_p.merge(df_c, on="time_ms", how="outer").merge(df_v, on="time_ms", how="outer")

    # Converte timestamp (ms) para datetime UTC
    df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    df.drop(columns=["time_ms"], inplace=True)
    df.sort_values("time", inplace=True)

    # ------------------------------------------------------------------------
    # 6) Garante 1 linha por dia
    # ------------------------------------------------------------------------
    # Mesmo com interval="daily", o range pode trazer 2 pontos nas bordas do período.
    # Para evitar duplicidade, agrupamos por data (yyyy-mm-dd) e ficamos com o último.
    # Se preferir a média do dia, troque "last" por "mean" em price/market/volume.
    df["date"] = df["time"].dt.date
    df = (
        df.groupby("date", as_index=False)
          .agg({
              "time": "last",             # timestamp representativo do dia
              "price_usd": "last",        # ou "mean" (média diária), conforme necessidade
              "market_cap_usd": "last",
              "volume_usd": "last",
          })
    )
    df.drop(columns=["date"], inplace=True)

    # Mostra amostra para debug
    print("Preview daily rows to load:")
    print(df.head(10).to_string(index=False))

    # ------------------------------------------------------------------------
    # 7) Carrega no BigQuery (append)
    # ------------------------------------------------------------------------
    # Observação importante:
    # - "append" grava sem deduplicar. Se você reexecutar (re-run) o mesmo dia,
    #   poderá duplicar linhas. Em produção, considere:
    #   (a) Tabela particionada por DATE(time) + job de dedupe depois, ou
    #   (b) Usar uma tabela staging e fazer MERGE alvo (idempotência).
    bq_hook = BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        use_legacy_sql=False
    )
    credentials = bq_hook.get_credentials()
    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"

    # Schema explícito ajuda na criação da tabela na 1ª vez
    table_schema = [
        {"name": "time",            "type": "TIMESTAMP"},
        {"name": "price_usd",       "type": "FLOAT"},
        {"name": "market_cap_usd",  "type": "FLOAT"},
        {"name": "volume_usd",      "type": "FLOAT"},
    ]

    # Garante que 'time' não é índice
    if df.index.name == "time":
        df = df.reset_index()

    # Envio usando pandas-gbq
    df.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="append",         # evita sobrescrever histórico
        credentials=credentials,
        table_schema=table_schema,  # usado apenas na criação inicial
        location=BQ_LOCATION,
        progress_bar=False,
    )

    print(f"Loaded {len(df)} rows to {GCP_PROJECT}.{destination_table} (location={BQ_LOCATION}).")


# ----------------------------------------------------------------------------
# Definição do DAG
# ----------------------------------------------------------------------------
@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",  # agenda diária às 00:00 UTC
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,          # True = processa datas passadas se o DAG ficar pausado
    max_active_runs=1,     # evita múltiplos runs concorrentes do mesmo DAG
    # Dica: crie um Pool no Airflow (ex.: "coingecko_pool", 1 slot) e
    # aplique na task se for fazer muito catchup, para serializar as chamadas.
    owner_links={
        "Alex Lopes": "mailto:alexlopespereira@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/cm3webulw15k701npm2uhu77t/cloud-ide/cm42rbvn10lqk01nlco70l0b8/cm44gkosq0tof01mxajutk86g",
    },
    tags=["bitcoin", "etl", "coingecko", "bigquery", "pandas-gbq", "daily"],
)
def bitcoin_etl_bigquery_full():
    # Encadeamento simples: apenas 1 tarefa neste DAG
    fetch_and_to_gbq()

# Instancia o DAG
dag = bitcoin_etl_bigquery_full()
