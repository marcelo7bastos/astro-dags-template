# dags/api_fda_semaglutine_to_db_dag.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # ainda usado pela task antiga

import pendulum
import pandas as pd
import requests
from datetime import date

# ============================================================
# CONFIGURAÇÕES GERAIS (edite aqui para ajustes rápidos)
# ============================================================

# --- Alvo no BigQuery (usado pela task agregada antiga) ---
GCP_PROJECT  = "365846072239"
BQ_DATASET   = "dataset_fda"
BQ_TABLE     = "openfda_semaglutina"   # tabela de teste (intervalo fixo; agregação diária)
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"

# --- Controle de execução ---
USE_POOL     = True
POOL_NAME    = "openfda_api"    # se não quiser pool, defina USE_POOL=False

# --- Parâmetros do teste (INTERVALO FIXO) ---
TEST_START = date(2025, 1, 1)   # AAAA, M, D
TEST_END   = date(2025, 3, 29)  # inclusive
DRUG_QUERY = "semaglutide"      # alinhado ao teste por generic_name

# --- Parâmetros do passo 4 (eventos brutos / paginação simples) ---
RAW_LIMIT      = 100   # registros por página
RAW_MAX_PAGES  = 5     # nº máximo de páginas (100 * 5 = 500 eventos)
RAW_TIMEOUT    = 45    # segundos

# Sessão HTTP com cabeçalho “educado”
SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "mda-openfda-etl/1.0 (contato: marceloc.bastos@hotmail.com)"}
)

# ============================================================
# Helpers HTTP
# ============================================================

def _openfda_get(url: str, params: dict | None = None, *, timeout: int = 30) -> dict:
    """
    Chamada simples à openFDA (sem retries).
    404 => retorna {"results": []} para concluir sem erro.
    """
    r = SESSION.get(url, params=params, timeout=timeout)
    if r.status_code == 404:
        return {"results": []}
    r.raise_for_status()
    return r.json()


def _build_search_expr(start: date, end: date, drug_query: str) -> str:
    """
    Expressão de busca SEM 'count' (para eventos brutos):
      patient.drug.openfda.generic_name:"<drug_query>"
      AND receivedate:[YYYYMMDD TO YYYYMMDD]
    """
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    return (
        f'patient.drug.openfda.generic_name:"{drug_query}"'
        f'+AND+receivedate:[{start_str}+TO+{end_str}]'
    )


def _build_openfda_count_url(start: date, end: date, drug_query: str) -> str:
    """
    URL com count=receivedate (sua task antiga de agregação diária).
    """
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
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

# ============================================================
# TASK (antiga): agregação diária -> BigQuery
# ============================================================

@task(**_task_kwargs)
def fetch_fixed_range_and_to_bq():
    url = _build_openfda_count_url(TEST_START, TEST_END, DRUG_QUERY)
    print("[openFDA] URL (count):", url)

    data = _openfda_get(url, timeout=RAW_TIMEOUT)
    results = data.get("results", [])
    if not results:
        print(f"[openFDA] Sem resultados (count) para {TEST_START}..{TEST_END} (drug={DRUG_QUERY}).")
        return

    df = pd.DataFrame(results)  # colunas: time (AAAAMMDD), count
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    df = df.rename(columns={"count": "events"})
    df["win_start"] = pd.to_datetime(TEST_START)
    df["win_end"]   = pd.to_datetime(TEST_END)
    df["drug"]      = DRUG_QUERY

    print("[openFDA] Prévia (count):\n", df.head().to_string())

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

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
        if_exists="append",
        credentials=credentials,
        table_schema=schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )
    print(f"[BQ] Gravados {len(df)} registros em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}.")

# ============================================================
# PASSO 4 — TASK NOVA: Eventos brutos (preview, sem BQ)
# ============================================================

@task(**_task_kwargs)
def fetch_events_raw_preview():
    """
    Busca eventos BRUTOS (sem count) com paginação limit/skip.
    Não grava; apenas imprime uma prévia tabular com campos-chave.
    """
    base_url = "https://api.fda.gov/drug/event.json"
    search_expr = _build_search_expr(TEST_START, TEST_END, DRUG_QUERY)
    print("[openFDA] search (raw):", search_expr)

    all_rows = []
    for page in range(RAW_MAX_PAGES):
        params = {
            "search": search_expr,
            "limit": str(RAW_LIMIT),
            "skip": str(page * RAW_LIMIT),
            # "sort": "receivedate:desc"  # opcional
        }
        payload = _openfda_get(base_url, params=params, timeout=RAW_TIMEOUT)
        batch = payload.get("results", []) or []
        print(f"[openFDA] Página {page+1}: {len(batch)} registros")
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < RAW_LIMIT:
            break

    print(f"[openFDA] Total coletado (raw) = {len(all_rows)}")

    if not all_rows:
        print("[openFDA] Sem eventos no período para o filtro informado.")
        return

    # Prévia "flat" para inspeção rápida
    def first_or_none(seq, key):
        try:
            return (seq or [])[0].get(key)
        except Exception:
            return None

    preview = []
    for ev in all_rows[:50]:  # corta para não poluir logs
        patient = ev.get("patient", {}) or {}
        reactions = patient.get("reaction", []) or []
        drugs     = patient.get("drug", []) or []
        preview.append({
            "safetyreportid": ev.get("safetyreportid"),
            "receivedate":    ev.get("receivedate"),
            "serious":        ev.get("serious"),
            "patientsex":     patient.get("patientsex"),
            "primarysourcecountry": ev.get("primarysourcecountry"),
            "reaction_pt":    first_or_none(reactions, "reactionmeddrapt"),
            "drug_product":   first_or_none(drugs, "medicinalproduct"),
        })

    df_prev = pd.DataFrame(preview)
    # Normaliza data para visualização
    df_prev["receivedate"] = pd.to_datetime(df_prev["receivedate"], format="%Y%m%d", errors="coerce")

    print("[openFDA] Prévia (raw) — até 50 linhas:")
    print(df_prev.to_string(index=False))

# ============================================================
# DEFINIÇÃO DA DAG
# ============================================================

@dag(
    dag_id="openfda_semaglutina_full",
    schedule="@once",  # mude para @daily quando avançar ao passo 8
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "faers", "preview", "raw", "range"],
)
def openfda_semaglutina_full():
    # 👉 Para este passo, executa só o preview de eventos brutos:
    fetch_events_raw_preview()

    # Se quiser rodar também a task antiga de agregação e carga no BQ,
    # basta descomentar a linha abaixo.
    # fetch_fixed_range_and_to_bq()

dag = openfda_semaglutina_full()
