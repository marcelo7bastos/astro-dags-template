# dags/openfda_semaglutina_stage_pipeline.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import pendulum
import pandas as pd
import requests
from datetime import date
from typing import Any, Dict, List

# ========================= Config =========================
GCP_PROJECT    = "365846072239"
BQ_DATASET     = "dataset_fda"
BQ_TABLE_STAGE = "semaglutide_events_stage"   # 3º passo (salva flat)
BQ_TABLE_COUNT = "openfda_semaglutina"        # 4º passo (agrega diário)
BQ_LOCATION    = "US"
GCP_CONN_ID    = "google_cloud_default"

# Janela pequena p/ didático (uma chamada)
TEST_START = date(2025, 1, 1)
TEST_END   = date(2025, 1, 15)
DRUG_QUERY = "semaglutide"

API_LIMIT   = 200   # 1 página só para manter XCom pequeno e simples
TIMEOUT_S   = 30
MAX_RETRIES = 3

# HTTP session
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "didactic-openfda-etl/1.0 (contato: exemplo@dominio.com)"})


# ========================= Helpers =========================
def _search_expr(start: date, end: date, drug_query: str) -> str:
    s = start.strftime("%Y%m%d"); e = end.strftime("%Y%m%d")
    # Use ESPAÇOS para AND/TO (requests faz o encode)
    return f'patient.drug.openfda.generic_name:"{drug_query}" AND receivedate:[{s} TO {e}]'

def _openfda_get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=TIMEOUT_S)
        if r.status_code == 404:
            return {"results": []}
        if 200 <= r.status_code < 300:
            return r.json()
        if attempt < MAX_RETRIES and r.status_code in (429, 500, 502, 503, 504):
            import time; time.sleep(attempt)  # backoff leve
            continue
        r.raise_for_status()

def _to_flat(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """Achata campos essenciais (didático)."""
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
    df["receivedate"] = pd.to_datetime(df["receivedate"], format="%Y%m%d", errors="coerce").dt.date
    for col in ["patientsex", "serious"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df = df.drop_duplicates(subset=["safetyreportid"], keep="first")
    return df


# ========================= DAG =========================
@dag(
    dag_id="openfda_semaglutina_stage_pipeline",
    description="Consulta openFDA (semaglutide) -> trata (flat) -> salva (BQ stage) -> agrega diário (BQ).",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["didatico", "openfda", "faers", "bigquery", "etl"],
)
def openfda_semaglutina_stage_pipeline():

    # 1) CONSULTA
    @task(retries=0)
    def fetch_raw() -> List[Dict[str, Any]]:
        base_url = "https://api.fda.gov/drug/event.json"
        params = {"search": _search_expr(TEST_START, TEST_END, DRUG_QUERY), "limit": str(API_LIMIT)}
        print("[fetch] Params:", params)
        payload = _openfda_get(base_url, params)
        rows = payload.get("results", []) or []
        print(f"[fetch] Recebidos {len(rows)} registros.")
        return rows

    # 2) TRATAMENTO (NORMALIZAÇÃO MÍNIMA)
    @task(retries=0)
    def normalize_minimal(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        df = _to_flat(rows)
        print(f"[normalize] Linhas pós-normalização: {len(df)}")
        if not df.empty:
            print("[normalize] Preview:\n", df.head(10).to_string(index=False))
        return df.to_dict(orient="records")

    # 3) SALVA NO BIGQUERY (STAGE)
    @task(retries=0)
    def load_stage(rows_flat: List[Dict[str, Any]]) -> Dict[str, str]:
        if not rows_flat:
            print("[stage] Nada a gravar.")
            return {"inserted": "0"}

        df = pd.DataFrame(rows_flat)
        schema = [
            {"name": "safetyreportid",       "type": "STRING"},
            {"name": "receivedate",          "type": "DATE"},
            {"name": "patientsex",           "type": "INTEGER"},
            {"name": "primarysourcecountry", "type": "STRING"},
            {"name": "serious",              "type": "INTEGER"},
            {"name": "reaction_pt",          "type": "STRING"},
            {"name": "drug_product",         "type": "STRING"},
        ]
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        creds = bq.get_credentials()
        df.to_gbq(
            destination_table=f"{BQ_DATASET}.{BQ_TABLE_STAGE}",
            project_id=GCP_PROJECT,
            if_exists="append",
            credentials=creds,
            table_schema=schema,
            location=BQ_LOCATION,
            progress_bar=False,
        )
        print(f"[stage] Gravados {len(df)} em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}.")
        # passa janela para o passo 4
        return {
            "start": TEST_START.strftime("%Y-%m-%d"),
            "end":   TEST_END.strftime("%Y-%m-%d"),
            "drug":  DRUG_QUERY,
        }

    # 4) AGREGA DIÁRIO (compatível com Sandbox: sem DDL/DML)
    @task(retries=0)
    def build_daily_counts(meta: Dict[str, str]) -> None:
        start, end, drug = meta["start"], meta["end"], meta["drug"]

        # SELECT de contagens (apenas leitura)
        sql = f"""
        SELECT
          receivedate AS day,
          COUNT(*)    AS events,
          '{drug}'    AS drug
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}`
        WHERE receivedate BETWEEN DATE('{start}') AND DATE('{end}')
        GROUP BY day
        ORDER BY day
        """
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        client = bq.get_client()
        df_counts = client.query(sql, location=BQ_LOCATION).to_dataframe()

        if df_counts.empty:
            print("[counts] Nenhuma linha para agregar.")
            return

        # Grava a tabela de contagens via load job (sem DML)
        schema_counts = [
            {"name": "day",    "type": "DATE"},
            {"name": "events", "type": "INTEGER"},
            {"name": "drug",   "type": "STRING"},
        ]
        creds = bq.get_credentials()
        df_counts.to_gbq(
            destination_table=f"{BQ_DATASET}.{BQ_TABLE_COUNT}",
            project_id=GCP_PROJECT,
            if_exists="replace",  # recria a cada execução (Sandbox-friendly)
            credentials=creds,
            table_schema=schema_counts,
            location=BQ_LOCATION,
            progress_bar=False,
        )
        print(f"[counts] {len(df_counts)} linhas gravadas em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_COUNT}.")

    # Encadeamento: consulta → trata → salva → agrega
    build_daily_counts(load_stage(normalize_minimal(fetch_raw())))

openfda_semaglutina_stage_pipeline()
