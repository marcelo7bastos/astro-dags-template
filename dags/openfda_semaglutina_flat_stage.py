# dags/openfda_semaglutina_stage_simple.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import pendulum
import pandas as pd
import requests
from datetime import date
from typing import Any, Dict, List

# ========================= Config (edite aqui) =========================
GCP_PROJECT   = "365846072239"
BQ_DATASET    = "dataset_fda"
BQ_TABLE      = "semaglutide_events_stage"   # staging simples
BQ_LOCATION   = "US"
GCP_CONN_ID   = "google_cloud_default"

# Janela fixa p/ o exemplo (curta ajuda a evitar erros e manter rápido)
TEST_START = date(2025, 1, 1)
TEST_END   = date(2025, 1, 15)
DRUG_QUERY = "semaglutide"                   # generic_name

# Para manter didático, pegamos só um lote pequeno
API_LIMIT   = 200        # quantidade total (uma chamada)
TIMEOUT_S   = 30
MAX_RETRIES = 3

# HTTP session (educado)
SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "didactic-openfda-etl/1.0 (contato: exemplo@dominio.com)"}
)

# ========================= Helpers =========================
def _search_expr(start: date, end: date, drug_query: str) -> str:
    """
    Use ESPAÇOS (não '+') para AND/TO; requests faz o encode certo.
    """
    s = start.strftime("%Y%m%d")
    e = end.strftime("%Y%m%d")
    return f'patient.drug.openfda.generic_name:"{drug_query}" AND receivedate:[{s} TO {e}]'

def _openfda_get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    """
    GET simples com poucos retries; 404 => {'results': []}
    """
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=TIMEOUT_S)
        if r.status_code == 404:
            return {"results": []}
        if 200 <= r.status_code < 300:
            return r.json()
        if attempt < MAX_RETRIES and r.status_code in (429, 500, 502, 503, 504):
            import time; time.sleep(1 * attempt)
            continue
        r.raise_for_status()

def _to_flat(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Achata só o essencial (didático).
    Campos:
      safetyreportid, receivedate (DATE), patientsex, primarysourcecountry,
      serious, reaction_pt (1a reação), drug_product (1o medicamento).
    """
    flat: List[Dict[str, Any]] = []
    for ev in rows:
        patient   = (ev or {}).get("patient", {}) or {}
        reactions = patient.get("reaction", []) or []
        drugs     = patient.get("drug", []) or []
        flat.append({
            "safetyreportid":        ev.get("safetyreportid"),
            "receivedate":           ev.get("receivedate"),
            "patientsex":            patient.get("patientsex"),
            "primarysourcecountry":  ev.get("primarysourcecountry"),
            "serious":               ev.get("serious"),
            "reaction_pt":           (reactions[0].get("reactionmeddrapt") if reactions else None),
            "drug_product":          (drugs[0].get("medicinalproduct") if drugs else None),
        })

    df = pd.DataFrame(flat)
    if df.empty:
        return df

    # Converte datas e tipos simples
    df["receivedate"] = pd.to_datetime(df["receivedate"], format="%Y%m%d", errors="coerce").dt.date
    for col in ["patientsex", "serious"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Dedupe por safetyreportid
    df = df.drop_duplicates(subset=["safetyreportid"], keep="first")
    return df

# ========================= DAG =========================
@dag(
    dag_id="openfda_semaglutina_stage_simple",
    description="Exemplo didático: busca pequena da openFDA (semaglutide) e grava flat no BigQuery.",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    tags=["didatico", "openfda", "faers", "bigquery"],
)
def openfda_semaglutina_stage_simple():

    @task(retries=0)
    def run_all():
        # 1) Chama a openFDA (uma requisição pequena)
        base_url = "https://api.fda.gov/drug/event.json"
        params = {
            "search": _search_expr(TEST_START, TEST_END, DRUG_QUERY),
            "limit":  str(API_LIMIT),
        }
        print("[openFDA] Params:", params)
        payload = _openfda_get(base_url, params)
        rows = payload.get("results", []) or []
        print(f"[openFDA] Registros recebidos: {len(rows)}")

        # 2) Normaliza (flat mínimo)
        df = _to_flat(rows)
        print(f"[normalize] Linhas pós-normalização (dedupe): {len(df)}")
        if not df.empty:
            print("[normalize] Prévia:\n", df.head(10).to_string(index=False))
        else:
            print("[normalize] DataFrame vazio (nada a carregar).")

        # 3) Grava no BigQuery (WRITE_APPEND)
        if not df.empty:
            schema = [
                {"name": "safetyreportid",       "type": "STRING"},
                {"name": "receivedate",          "type": "DATE"},
                {"name": "patientsex",           "type": "INTEGER"},
                {"name": "primarysourcecountry", "type": "STRING"},
                {"name": "serious",              "type": "INTEGER"},
                {"name": "reaction_pt",          "type": "STRING"},
                {"name": "drug_product",         "type": "STRING"},
            ]
            bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
            credentials = bq_hook.get_credentials()
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
        else:
            print("[BQ] Nada a gravar; mantendo execução como sucesso.")

    run_all()

dag = openfda_semaglutina_stage_simple()
