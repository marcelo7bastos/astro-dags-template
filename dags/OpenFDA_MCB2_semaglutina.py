# dags/api_fda_semaglutine_to_db_dag.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import pendulum
import pandas as pd
import requests
from datetime import date

# ============================================================
# CONFIGURAÇÕES GERAIS (edite aqui para ajustes rápidos)
# ============================================================

# --- Alvo no BigQuery ---
GCP_PROJECT  = "365846072239"
BQ_DATASET   = "dataset_fda"
BQ_TABLE     = "openfda_semaglutina"   # tabela de teste (intervalo fixo; agregação diária)
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"

# --- Controle de execução ---
USE_POOL     = True
POOL_NAME    = "openfda_api"    # se não quiser pool, defina USE_POOL=False

# --- Parâmetros do teste (INTERVALO FIXO) ---
# Esta seção faz a DAG consultar SEMPRE o mesmo intervalo,
# igual ao que você testou no navegador.
TEST_START = date(2025, 1, 1)   # AAAA, M, D
TEST_END   = date(2025, 3, 29)  # inclusive
DRUG_QUERY = "semaglutide"      # ✅ alinhado ao seu teste por generic_name

# Observação:
# - Mantemos agregação DIÁRIA (count=receivedate) — exatamente como na URL.
# - Se quiser voltar ao modo “um dia por execução”, basta criar outra task
#   com `data_interval_start` e trocar a montagem da URL (ver notas no fim).
# ============================================================

# Sessão HTTP com cabeçalho “educado”
SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "mda-openfda-etl/1.0 (contato: marceloc.bastos@hotmail.com)"}
)


def _openfda_get(url: str) -> dict:
    """
    Chamada simples à openFDA **sem retries**.
    IMPORTANTE: a openFDA retorna **404** quando não há resultados para a query.
    Aqui tratamos 404 como "vazio" (results=[]), para o task concluir com sucesso.
    """
    r = SESSION.get(url, timeout=30)
    if r.status_code == 404:
        return {"results": []}
    r.raise_for_status()
    return r.json()


def _build_openfda_url(start: date, end: date, drug_query: str) -> str:
    """
    Monta a URL da openFDA exatamente como no seu teste:
      - filtro em patient.drug.openfda.generic_name:"<drug_query>"
      - janela de receivedate [start .. end] (formato AAAAMMDD)
      - count=receivedate  -> buckets diários
    """
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    # Observação: o termo da droga é simples (sem espaços) no generic_name.
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.openfda.generic_name:%22{drug_query}%22"
        f"+AND+receivedate:[{start_str}+TO+{end_str}]"
        "&count=receivedate"
    )


# Decorator do task: sem retries; pool opcional
_task_kwargs = dict(retries=0)
if USE_POOL:
    _task_kwargs["pool"] = POOL_NAME


@task(**_task_kwargs)
def fetch_fixed_range_and_to_bq():
    """
    Task de TESTE por intervalo fixo:
      1) Monta a URL com TEST_START..TEST_END e DRUG_QUERY.
      2) Chama a API openFDA (404 => "sem resultados").
      3) Converte retorno em DataFrame diário (time, events).
      4) Grava no BigQuery (append) com colunas:
           - time      (TIMESTAMP UTC do dia)
           - events    (contagem diária)
           - win_start (DATE)  -> início do intervalo consultado
           - win_end   (DATE)  -> fim do intervalo consultado
           - drug      (STRING)-> termo consultado (p/ rastreio)
    """
    # 1) Monta a URL EXATAMENTE como a que funcionou no navegador
    url = _build_openfda_url(TEST_START, TEST_END, DRUG_QUERY)
    print("[openFDA] URL:", url)

    # 2) Chama a API e trata “sem resultados”
    data = _openfda_get(url)
    results = data.get("results", [])
    if not results:
        print(f"[openFDA] Sem resultados para {TEST_START}..{TEST_END} (drug={DRUG_QUERY}).")
        return

    # 3) Converte para DataFrame
    df = pd.DataFrame(results)  # colunas: time (AAAAMMDD), count
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    df = df.rename(columns={"count": "events"})
    df["win_start"] = pd.to_datetime(TEST_START)
    df["win_end"]   = pd.to_datetime(TEST_END)
    df["drug"]      = DRUG_QUERY  # generic_name já está legível

    print("[openFDA] Prévia dos dados:\n", df.head().to_string())

    # 4) Escreve no BigQuery
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

    # Esquema explícito (bom para evitar inferências ruins do pandas-gbq)
    schema = [
        {"name": "time",      "type": "TIMESTAMP"},
        {"name": "events",    "type": "INTEGER"},
        {"name": "win_start", "type": "DATE"},
        {"name": "win_end",   "type": "DATE"},
        {"name": "drug",      "type": "STRING"},
    ]

    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",           # teste: acrescenta linhas
        credentials=credentials,
        table_schema=schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )
    print(f"[BQ] Gravados {len(df)} registros em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}.")


# ============================================================
# DEFINIÇÃO DA DAG
# ============================================================
@dag(
    dag_id="openfda_semaglutina_full",
    schedule="@once",  # roda uma vez (é teste de intervalo fixo); mude para @daily se quiser
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,     # como é intervalo fixo, não queremos runs históricos
    max_active_runs=1,
    tags=["openfda", "bigquery", "test", "range"],
)
def openfda_semaglutina_full():
    fetch_fixed_range_and_to_bq()

# ✅ Corrigido: instanciando a função correta
dag = openfda_semaglutina_full()
