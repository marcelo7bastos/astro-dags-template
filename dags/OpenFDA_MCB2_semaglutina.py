# dags/api_fda_semaglutine_to_db_dag.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # mantido p/ task antiga opcional

import os
import pendulum
import pandas as pd
import requests
from datetime import date, timedelta

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

# --- Parâmetros do passo 4 (eventos brutos / paginação + janelas) ---
RAW_LIMIT       = 100     # registros por página
RAW_MAX_PAGES   = 5       # nº máximo de páginas por janela (100 * 5 = 500 eventos)
RAW_TIMEOUT     = 45      # segundos
RAW_WINDOW_DAYS = 14      # fatiamento do intervalo em janelas de 14 dias (reduz 500)

# --- Retries/backoff para 429/5xx ---
MAX_RETRIES   = 4
BACKOFF_SECS  = [1, 2, 4, 8]

# (Sem API key) -> não usar param api_key
OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY", None)  # se não existir, ignorado

# Sessão HTTP com cabeçalho “educado”
SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "mda-openfda-etl/1.0 (contato: marceloc.bastos@hotmail.com)"}
)

# ============================================================
# Helpers
# ============================================================

def _openfda_get(url: str, params: dict | None = None, *, timeout: int = RAW_TIMEOUT) -> dict:
    """
    GET com retries e tratamento de 404 -> {"results": []}.
    Re-tenta em 429/5xx com backoff exponencial simples.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=timeout)
        # 404 significa "sem resultados" na openFDA
        if r.status_code == 404:
            return {"results": []}
        # Sucesso
        if 200 <= r.status_code < 300:
            return r.json()
        # Erros transitórios -> retry
        if r.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
            wait = BACKOFF_SECS[min(attempt - 1, len(BACKOFF_SECS) - 1)]
            print(f"[openFDA] {r.status_code} (tentativa {attempt}) -> retry em {wait}s | URL={r.url}")
            try:
                print("[openFDA] corpo do erro:", str(r.json())[:500])
            except Exception:
                print("[openFDA] corpo do erro (texto):", r.text[:500])
            import time as _t; _t.sleep(wait)
            continue
        # Última tentativa ou erro não transitório
        r.raise_for_status()

def _iter_windows(start: date, end: date, step_days: int):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=step_days - 1), end)
        yield cur, nxt
        cur = nxt + timedelta(days=1)

def _build_openfda_count_url(start: date, end: date, drug_query: str) -> str:
    """
    URL com count=receivedate (task antiga de agregação diária -> BQ).
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
# TASK (OPCIONAL/LEGADO): agregação diária -> BigQuery
# ============================================================

@task(**_task_kwargs)
def fetch_fixed_range_and_to_bq():
    url = _build_openfda_count_url(TEST_START, TEST_END, DRUG_QUERY)
    print("[openFDA] URL (count):", url)

    data = _openfda_get(url)
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
# PASSO 4 — TASK PRINCIPAL: Eventos brutos (preview, sem BQ)
# ============================================================

@task(**_task_kwargs)
def fetch_events_raw_preview():
    """
    Busca eventos BRUTOS (sem count) com:
      - fatiamento do intervalo em janelas menores (RAW_WINDOW_DAYS)
      - paginação limit/skip
      - retries/backoff para 429/5xx
      - prévia no log (até 50 linhas), sem gravar no BQ
    """
    base_url = "https://api.fda.gov/drug/event.json"
    total_coletado = 0
    preview_rows = []

    for (w_start, w_end) in _iter_windows(TEST_START, TEST_END, RAW_WINDOW_DAYS):
        start_str = w_start.strftime("%Y%m%d")
        end_str   = w_end.strftime("%Y%m%d")
        search_expr = (
            f'patient.drug.openfda.generic_name:"{DRUG_QUERY}"+AND+'
            f'receivedate:[{start_str}+TO+{end_str}]'
        )
        print(f"[openFDA] Janela {w_start}..{w_end} | search={search_expr}")

        for page in range(RAW_MAX_PAGES):
            params = {
                "search": search_expr,
                "limit": str(RAW_LIMIT),
                "skip":  str(page * RAW_LIMIT),
                # "sort": "receivedate:desc",  # opcional
            }
            # Sem API key: não enviar api_key; se existir em env, será usada
            if OPENFDA_API_KEY:
                params["api_key"] = OPENFDA_API_KEY

            payload = _openfda_get(base_url, params=params)
            batch = payload.get("results", []) or []
            print(f"[openFDA]  Janela {w_start}..{w_end} | página {page+1}: {len(batch)} registros")

            if not batch:
                break

            total_coletado += len(batch)

            # Monta preview leve (primeiras 50 linhas no total)
            for ev in batch:
                if len(preview_rows) >= 50:
                    break
                patient   = ev.get("patient", {}) or {}
                reactions = patient.get("reaction", []) or []
                drugs     = patient.get("drug", []) or []
                preview_rows.append({
                    "safetyreportid": ev.get("safetyreportid"),
                    "receivedate":    ev.get("receivedate"),
                    "serious":        ev.get("serious"),
                    "patientsex":     patient.get("patientsex"),
                    "primarysourcecountry": ev.get("primarysourcecountry"),
                    "reaction_pt":    (reactions[0].get("reactionmeddrapt") if reactions else None),
                    "drug_product":   (drugs[0].get("medicinalproduct") if drugs else None),
                })

            if len(batch) < RAW_LIMIT:
                break  # última página da janela

    print(f"[openFDA] Total coletado (raw) = {total_coletado}")

    if not preview_rows:
        print("[openFDA] Sem eventos no período/filtro.")
        return

    df_prev = pd.DataFrame(preview_rows)
    df_prev["receivedate"] = pd.to_datetime(df_prev["receivedate"], format="%Y%m%d", errors="coerce")
    print("[openFDA] Prévia (raw) — até 50 linhas:")
    print(df_prev.to_string(index=False))

# ============================================================
# DEFINIÇÃO DA DAG
# ============================================================

@dag(
    dag_id="openfda_semaglutina_full",
    schedule="@once",  # mude para @daily quando evoluir ao passo 8
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "faers", "preview", "raw", "range"],
)
def openfda_semaglutina_full():
    # Para este passo, executa só o preview de eventos brutos:
    fetch_events_raw_preview()

    # Se quiser rodar também a task de agregação + carga no BQ, descomente:
    # fetch_fixed_range_and_to_bq()

dag = openfda_semaglutina_full()
