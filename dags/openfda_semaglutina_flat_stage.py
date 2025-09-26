# dags/api_fda_semaglutine_to_db_dag.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import os
import pendulum
import pandas as pd
import requests
from datetime import date, timedelta
from typing import Any, Dict, List

# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

# BigQuery (STAGING FLAT)
GCP_PROJECT   = "365846072239"
BQ_DATASET    = "dataset_fda"
BQ_TABLE_STAGE = "semaglutide_events_stage"  # staging flat (passo 5)
BQ_LOCATION   = "US"
GCP_CONN_ID   = "google_cloud_default"

# Execução
USE_POOL   = True
POOL_NAME  = "openfda_api"

# Intervalo fixo do teste
TEST_START = date(2025, 1, 1)
TEST_END   = date(2025, 3, 29)
DRUG_QUERY = "semaglutide"   # patient.drug.openfda.generic_name

# Busca RAW com janelas + paginação
RAW_LIMIT        = 100     # por página
RAW_MAX_PAGES    = 5       # páginas por janela
RAW_TIMEOUT      = 45
RAW_WINDOW_DAYS  = 14

# Retries/backoff
MAX_RETRIES   = 4
BACKOFF_SECS  = [1, 2, 4, 8]

# API key (opcional). Se não houver, não enviaremos nada.
OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY")

# HTTP session
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "mda-openfda-etl/1.0 (contato: marceloc.bastos@hotmail.com)"})


# ============================================================
# Helpers
# ============================================================

def _openfda_get(url: str, params: dict | None = None, *, timeout: int = RAW_TIMEOUT) -> dict:
    """GET com retries; 404 -> {'results': []}."""
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=timeout)
        if r.status_code == 404:
            return {"results": []}
        if 200 <= r.status_code < 300:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
            wait = BACKOFF_SECS[min(attempt - 1, len(BACKOFF_SECS) - 1)]
            print(f"[openFDA] {r.status_code} (tentativa {attempt}) -> retry em {wait}s | URL={r.url}")
            try:
                print("[openFDA] corpo do erro:", str(r.json())[:500])
            except Exception:
                print("[openFDA] corpo do erro (texto):", r.text[:500])
            import time as _t; _t.sleep(wait)
            continue
        r.raise_for_status()

def _iter_windows(start: date, end: date, step_days: int):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=step_days - 1), end)
        yield cur, nxt
        cur = nxt + timedelta(days=1)

def _build_search_expr(start: date, end: date, drug_query: str) -> str:
    """NUNCA usar '+' manual; use espaços para AND/TO."""
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    return (
        f'patient.drug.openfda.generic_name:"{drug_query}" '
        f'AND receivedate:[{start_str} TO {end_str}]'
    )


# Decorator default
_task_kwargs = dict(retries=0)
if USE_POOL:
    _task_kwargs["pool"] = POOL_NAME


# ============================================================
# PASSO 5 — Tasks: fetch RAW -> normalize_minimal -> load BQ (staging)
# ============================================================

@task(**_task_kwargs)
def fetch_events_raw() -> List[Dict[str, Any]]:
    """
    Busca eventos BRUTOS (sem count) com janelas e paginação.
    Retorna lista de dicts (payload openFDA 'results' coletados).
    """
    base_url = "https://api.fda.gov/drug/event.json"
    all_rows: List[Dict[str, Any]] = []
    for (w_start, w_end) in _iter_windows(TEST_START, TEST_END, RAW_WINDOW_DAYS):
        search_expr = _build_search_expr(w_start, w_end, DRUG_QUERY)
        print(f"[openFDA] Janela {w_start}..{w_end} | search={search_expr}")
        for page in range(RAW_MAX_PAGES):
            params = {"search": search_expr, "limit": str(RAW_LIMIT), "skip": str(page * RAW_LIMIT)}
            if OPENFDA_API_KEY:
                params["api_key"] = OPENFDA_API_KEY
            payload = _openfda_get(base_url, params=params)
            batch = payload.get("results", []) or []
            print(f"[openFDA]  Janela {w_start}..{w_end} | pág {page+1}: {len(batch)} registros")
            if not batch:
                break
            all_rows.extend(batch)
            if len(batch) < RAW_LIMIT:
                break
    print(f"[openFDA] Total coletado (raw) = {len(all_rows)}")
    return all_rows


@task(**_task_kwargs)
def normalize_minimal(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Achata campos principais em formato FLAT (staging):
      safetyreportid (STRING)
      receivedate (DATE)
      patientsex (INTEGER)
      primarysourcecountry (STRING)
      serious (INTEGER)
      reaction_pt (STRING)            # primeira reação (medDRA PT)
      drug_product (STRING)           # primeiro 'medicinalproduct'
    Faz dedupe por safetyreportid.
    """
    if not rows:
        print("[normalize] Nenhuma linha para normalizar.")
        return []

    flat: List[Dict[str, Any]] = []
    for ev in rows:
        patient   = ev.get("patient", {}) or {}
        reactions = patient.get("reaction", []) or []
        drugs     = patient.get("drug", []) or []
        flat.append({
            "safetyreportid":        ev.get("safetyreportid"),
            "receivedate":           ev.get("receivedate"),  # YYYYMMDD (será convertido)
            "patientsex":            patient.get("patientsex"),
            "primarysourcecountry":  ev.get("primarysourcecountry"),
            "serious":               ev.get("serious"),
            "reaction_pt":           (reactions[0].get("reactionmeddrapt") if reactions else None),
            "drug_product":          (drugs[0].get("medicinalproduct") if drugs else None),
        })

    # DataFrame para limpeza + dedupe
    df = pd.DataFrame(flat)

    # Converter receivedate YYYYMMDD -> DATE
    if "receivedate" in df.columns:
        df["receivedate"] = pd.to_datetime(df["receivedate"], format="%Y%m%d", errors="coerce").dt.date

    # Dedupe por safetyreportid
    before = len(df)
    df = df.drop_duplicates(subset=["safetyreportid"], keep="first")
    after = len(df)
    print(f"[normalize] Dedupe: {before} -> {after}")

    # Tipos simples (inteiros onde faz sentido)
    for col in ["patientsex", "serious"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Preview
    print("[normalize] Preview:\n", df.head(10).to_string(index=False))

    # Retorna como lista de dict (para o próximo task)
    return df.to_dict(orient="records")


@task(**_task_kwargs)
def load_bq_stage(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Grava o FLAT em BigQuery (staging).
    Tabela: dataset_fda.semaglutide_events_stage (WRITE_APPEND)
    """
    if not rows:
        print("[load_bq_stage] Nada a gravar.")
        return {"inserted": 0}

    df = pd.DataFrame(rows)

    # Schema explícito do STAGING (flat)
    schema = [
        {"name": "safetyreportid",       "type": "STRING"},
        {"name": "receivedate",          "type": "DATE"},
        {"name": "patientsex",           "type": "INTEGER"},
        {"name": "primarysourcecountry", "type": "STRING"},
        {"name": "serious",              "type": "INTEGER"},
        {"name": "reaction_pt",          "type": "STRING"},
        {"name": "drug_product",         "type": "STRING"},
    ]

    # Credenciais do Airflow
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

    # Grava
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE_STAGE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=credentials,
        table_schema=schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )
    print(f"[BQ] Gravados {len(df)} registros em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}.")
    return {"inserted": len(df)}


# ============================================================
# DEFINIÇÃO DA DAG
# ============================================================

@dag(
    dag_id="openfda_semaglutina_flat_stage",
    schedule="@once",   # mantenha @once para o teste; depois podemos ir para @daily
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "faers", "raw", "stage", "flat"],
)
def openfda_semaglutina_flat_stage():
    raw = fetch_events_raw()
    flat = normalize_minimal(raw)
    load_bq_stage(flat)

dag = openfda_semaglutina_flat_stage()

# Dependências (string Python):
# fetch_events_raw >> normalize_minimal >> load_bq_stage
