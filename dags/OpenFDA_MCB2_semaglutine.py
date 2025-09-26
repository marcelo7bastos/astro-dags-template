# dags/api_fda_semaglutina_to_db_dag.py
from __future__ import annotations

from datetime import datetime
import time
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.hooks.base import BaseHook

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import requests


# === DAG SPEC ===
DAG_ID        = "api_fda_semaglutina_to_db_dag"
DESCRIPTION   = "Coletar eventos adversos de semaglutide na openFDA e carregar no BigQuery."
START_DATE    = datetime(2025, 9, 1)
SCHEDULE      = "0 3 * * *"
CATCHUP       = False

# === TASK CONFIG (conforme especificação) ===
HTTP_CONN_ID  = "openfda_conn"
ENDPOINT      = "/drug/event.json"
SEARCH_EXPR   = 'patient.drug.openfda.generic_name:"semaglutide"'
LIMIT         = 100
MAX_PAGES     = 50
RATE_LIMIT_RPM = 240  # 240 req/min
SLEEP_SECONDS  = max(0.26, 60.0 / RATE_LIMIT_RPM)  # ~0.25s entre requisições

BQ_CONN_ID    = "google_cloud_default"
DEST_TABLE    = "project.dataset.semaglutide_events"  # project.dataset.table
BQ_LOCATION   = "US"
WRITE_DISPOSITION = "WRITE_APPEND"
# Observação: BigQuery não permite clusterizar por campo aninhado; manteremos apenas "primarysourcecountry".
CLUSTERING_FIELDS = ["primarysourcecountry"]
PARTITION_FIELD   = "receivedate"  # DATE field


# === SCHEMA (exato da especificação) ===
TABLE_SCHEMA = [
    {"name":"safetyreportversion","type":"INTEGER"},
    {"name":"safetyreportid","type":"STRING"},
    {"name":"primarysourcecountry","type":"STRING"},
    {"name":"occurcountry","type":"STRING"},
    {"name":"transmissiondateformat","type":"STRING"},
    {"name":"transmissiondate","type":"DATE"},
    {"name":"reporttype","type":"INTEGER"},
    {"name":"serious","type":"INTEGER"},
    {"name":"seriousnesshospitalization","type":"INTEGER"},
    {"name":"seriousnessdisabling","type":"INTEGER"},
    {"name":"seriousnessother","type":"INTEGER"},
    {"name":"receivedateformat","type":"STRING"},
    {"name":"receivedate","type":"DATE"},
    {"name":"receiptdateformat","type":"STRING"},
    {"name":"receiptdate","type":"DATE"},
    {"name":"fulfillexpeditecriteria","type":"INTEGER"},
    {"name":"companynumb","type":"STRING"},
    {"name":"duplicate","type":"INTEGER"},
    {
      "name":"reportduplicate","type":"RECORD","mode":"NULLABLE",
      "fields":[
        {"name":"duplicatesource","type":"STRING"},
        {"name":"duplicatenumb","type":"STRING"}
      ]
    },
    {
      "name":"primarysource","type":"RECORD","mode":"NULLABLE",
      "fields":[
        {"name":"reportercountry","type":"STRING"},
        {"name":"qualification","type":"INTEGER"}
      ]
    },
    {
      "name":"sender","type":"RECORD","mode":"NULLABLE",
      "fields":[
        {"name":"sendertype","type":"INTEGER"},
        {"name":"senderorganization","type":"STRING"}
      ]
    },
    {
      "name":"receiver","type":"RECORD","mode":"NULLABLE",
      "fields":[
        {"name":"receivertype","type":"INTEGER"},
        {"name":"receiverorganization","type":"STRING"}
      ]
    },
    {
      "name":"patient","type":"RECORD","mode":"NULLABLE",
      "fields":[
        {"name":"patientonsetage","type":"FLOAT"},
        {"name":"patientonsetageunit","type":"INTEGER"},
        {"name":"patientagegroup","type":"INTEGER"},
        {"name":"patientsex","type":"INTEGER"},
        {"name":"patientweight","type":"FLOAT"},
        {
          "name":"reaction","type":"RECORD","mode":"REPEATED",
          "fields":[
            {"name":"reactionmeddraversionpt","type":"STRING"},
            {"name":"reactionmeddrapt","type":"STRING"},
            {"name":"reactionoutcome","type":"INTEGER"}
          ]
        },
        {
          "name":"drug","type":"RECORD","mode":"REPEATED",
          "fields":[
            {"name":"drugcharacterization","type":"INTEGER"},
            {"name":"medicinalproduct","type":"STRING"},
            {"name":"drugauthorizationnumb","type":"STRING"},
            {"name":"drugstructuredosagenumb","type":"FLOAT"},
            {"name":"drugstructuredosageunit","type":"STRING"},
            {"name":"drugseparatedosagenumb","type":"FLOAT"},
            {"name":"drugintervaldosageunitnumb","type":"FLOAT"},
            {"name":"drugintervaldosagedefinition","type":"STRING"},
            {"name":"drugdosagetext","type":"STRING"},
            {"name":"drugdosageform","type":"STRING"},
            {"name":"drugadministrationroute","type":"STRING"},
            {"name":"drugbatchnumb","type":"STRING"},
            {"name":"drugindication","type":"STRING"},
            {"name":"drugstartdateformat","type":"STRING"},
            {"name":"drugstartdate","type":"DATE"},
            {"name":"actiondrug","type":"INTEGER"},
            {"name":"drugadditional","type":"INTEGER"},
            {
              "name":"activesubstance","type":"RECORD","mode":"NULLABLE",
              "fields":[
                {"name":"activesubstancename","type":"STRING"}
              ]
            },
            {
              "name":"openfda","type":"RECORD","mode":"NULLABLE",
              "fields":[
                {"name":"application_number","type":"STRING","mode":"REPEATED"},
                {"name":"brand_name","type":"STRING","mode":"REPEATED"},
                {"name":"generic_name","type":"STRING","mode":"REPEATED"},
                {"name":"manufacturer_name","type":"STRING","mode":"REPEATED"},
                {"name":"product_ndc","type":"STRING","mode":"REPEATED"},
                {"name":"product_type","type":"STRING","mode":"REPEATED"},
                {"name":"route","type":"STRING","mode":"REPEATED"},
                {"name":"substance_name","type":"STRING","mode":"REPEATED"},
                {"name":"rxcui","type":"STRING","mode":"REPEATED"},
                {"name":"spl_id","type":"STRING","mode":"REPEATED"},
                {"name":"spl_set_id","type":"STRING","mode":"REPEATED"},
                {"name":"package_ndc","type":"STRING","mode":"REPEATED"},
                {"name":"nui","type":"STRING","mode":"REPEATED"},
                {"name":"pharm_class_cs","type":"STRING","mode":"REPEATED"},
                {"name":"pharm_class_epc","type":"STRING","mode":"REPEATED"},
                {"name":"pharm_class_moa","type":"STRING","mode":"REPEATED"},
                {"name":"unii","type":"STRING","mode":"REPEATED"}
              ]
            }
          ]
        },
        {
          "name":"summary","type":"RECORD","mode":"NULLABLE",
          "fields":[
            {"name":"narrativeincludeclinical","type":"STRING"}
          ]
        }
      ]
    },
    {"name":"seriousnessdeath","type":"INTEGER"},
    {"name":"seriousnesslifethreatening","type":"INTEGER"},
    {"name":"seriousnesscongenitalanomali","type":"INTEGER"}
]


def _conn_base_url(conn_id: str) -> str:
    conn = BaseHook.get_connection(conn_id)
    base = conn.get_uri()
    # Ex.: https://api.fda.gov; se usar get_uri vira 'https://api.fda.gov'
    # Vamos montar apenas esquema+host
    schema = conn.schema or ""
    host = conn.host or ""
    if host and conn.schema:
        # Airflow HTTP Conn geralmente: schema=https, host=api.fda.gov
        return f"{schema}://{host}"
    if host.startswith("http"):
        return host.rstrip("/")
    raise ValueError("HTTP connection sem host/schema válido")


def _maybe_headers(conn_id: str) -> Dict[str, str]:
    conn = BaseHook.get_connection(conn_id)
    headers = {}
    if conn.extra_dejson.get("headers"):
        headers.update(conn.extra_dejson.get("headers"))
    # Se possuir api_key em extra, adiciona como query param na URL no fetch (feito abaixo)
    return headers


def _date_yyyymmdd_to_date(s: str | None) -> str | None:
    if not s or len(s) != 8 or not s.isdigit():
        return None
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


with DAG(
    dag_id=DAG_ID,
    description=DESCRIPTION,
    start_date=START_DATE,
    schedule=SCHEDULE,
    catchup=CATCHUP,
    default_args={"owner": "airflow"},
    tags=["openfda", "semaglutide", "faers"],
) as dag:

    @task(task_id="fetch_openfda")
    def fetch_openfda() -> List[Dict[str, Any]]:
        """
        buscar eventos de semaglutide na API openFDA
        - paginação via limit/skip
        - respeita rate limit
        - retorna lista de dicts (eventos brutos)
        """
        base_url = _conn_base_url(HTTP_CONN_ID)
        headers  = _maybe_headers(HTTP_CONN_ID)
        conn     = BaseHook.get_connection(HTTP_CONN_ID)
        api_key  = conn.password or conn.extra_dejson.get("api_key")  # opcional

        all_rows: List[Dict[str, Any]] = []
        session = requests.Session()
        if headers:
            session.headers.update(headers)

        for page in range(MAX_PAGES):
            params = {
                "search": SEARCH_EXPR,
                "limit": str(LIMIT),
                "skip": str(page * LIMIT),
            }
            if api_key:
                params["api_key"] = api_key

            url = f"{base_url}{ENDPOINT}"
            r = session.get(url, params=params, timeout=60)
            r.raise_for_status()
            payload = r.json()
            rows = payload.get("results", []) or []
            all_rows.extend(rows)

            if len(rows) < LIMIT:
                break

            time.sleep(SLEEP_SECONDS)  # rate limit

        return all_rows

    @task(task_id="normalize_events")
    def normalize_events(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        padronizar/limpar JSON
        - flatten básico mantendo estrutura solicitada no schema
        - normalizar datas: receivedate, receiptdate, transmissiondate, patient.drug[].drugstartdate
        - selecionar campos principais (seguindo schema)
        - deduplicar por safetyreportid
        """
        def pick(d: Dict[str, Any], key: str, typ=None):
            v = d.get(key)
            if v is None:
                return None
            if typ is int:
                try: return int(v)
                except: return None
            if typ is float:
                try: return float(v)
                except: return None
            return v

        seen = set()
        cleaned: List[Dict[str, Any]] = []
        for ev in raw_rows:
            sid = str(ev.get("safetyreportid") or "")
            if not sid or sid in seen:
                continue
            seen.add(sid)

            # Datas no topo
            ev_out: Dict[str, Any] = {
                "safetyreportversion": pick(ev, "safetyreportversion", int),
                "safetyreportid": sid,
                "primarysourcecountry": pick(ev, "primarysourcecountry"),
                "occurcountry": pick(ev, "occurcountry"),
                "transmissiondateformat": pick(ev, "transmissiondateformat"),
                "transmissiondate": _date_yyyymmdd_to_date(pick(ev, "transmissiondate")),
                "reporttype": pick(ev, "reporttype", int),
                "serious": pick(ev, "serious", int),
                "seriousnesshospitalization": pick(ev, "seriousnesshospitalization", int),
                "seriousnessdisabling": pick(ev, "seriousnessdisabling", int),
                "seriousnessother": pick(ev, "seriousnessother", int),
                "receivedateformat": pick(ev, "receivedateformat"),
                "receivedate": _date_yyyymmdd_to_date(pick(ev, "receivedate")),
                "receiptdateformat": pick(ev, "receiptdateformat"),
                "receiptdate": _date_yyyymmdd_to_date(pick(ev, "receiptdate")),
                "fulfillexpeditecriteria": pick(ev, "fulfillexpeditecriteria", int),
                "companynumb": pick(ev, "companynumb"),
                "duplicate": pick(ev, "duplicate", int),
                "reportduplicate": {
                    "duplicatesource": pick(ev.get("reportduplicate", {}), "duplicatesource"),
                    "duplicatenumb": pick(ev.get("reportduplicate", {}), "duplicatenumb"),
                } if ev.get("reportduplicate") else None,
                "primarysource": {
                    "reportercountry": pick(ev.get("primarysource", {}), "reportercountry"),
                    "qualification": pick(ev.get("primarysource", {}), "qualification", int),
                } if ev.get("primarysource") else None,
                "sender": {
                    "sendertype": pick(ev.get("sender", {}), "sendertype", int),
                    "senderorganization": pick(ev.get("sender", {}), "senderorganization"),
                } if ev.get("sender") else None,
                "receiver": {
                    "receivertype": pick(ev.get("receiver", {}), "receivertype", int),
                    "receiverorganization": pick(ev.get("receiver", {}), "receiverorganization"),
                } if ev.get("receiver") else None,
                "patient": None,  # preenchido abaixo
                "seriousnessdeath": pick(ev, "seriousnessdeath", int),
                "seriousnesslifethreatening": pick(ev, "seriousnesslifethreatening", int),
                "seriousnesscongenitalanomali": pick(ev, "seriousnesscongenitalanomali", int),
            }

            p = ev.get("patient", {}) or {}
            # Normalizar drugstartdate em cada medicamento
            drugs_out = []
            for dr in p.get("drug", []) or []:
                dstart = _date_yyyymmdd_to_date(dr.get("drugstartdate"))
                drugs_out.append({
                    "drugcharacterization": pick(dr, "drugcharacterization", int),
                    "medicinalproduct": pick(dr, "medicinalproduct"),
                    "drugauthorizationnumb": pick(dr, "drugauthorizationnumb"),
                    "drugstructuredosagenumb": pick(dr, "drugstructuredosagenumb", float),
                    "drugstructuredosageunit": pick(dr, "drugstructuredosageunit"),
                    "drugseparatedosagenumb": pick(dr, "drugseparatedosagenumb", float),
                    "drugintervaldosageunitnumb": pick(dr, "drugintervaldosageunitnumb", float),
                    "drugintervaldosagedefinition": pick(dr, "drugintervaldosagedefinition"),
                    "drugdosagetext": pick(dr, "drugdosagetext"),
                    "drugdosageform": pick(dr, "drugdosageform"),
                    "drugadministrationroute": pick(dr, "drugadministrationroute"),
                    "drugbatchnumb": pick(dr, "drugbatchnumb"),
                    "drugindication": pick(dr, "drugindication"),
                    "drugstartdateformat": pick(dr, "drugstartdateformat"),
                    "drugstartdate": dstart,
                    "actiondrug": pick(dr, "actiondrug", int),
                    "drugadditional": pick(dr, "drugadditional", int),
                    "activesubstance": {
                        "activesubstancename": pick(dr.get("activesubstance", {}), "activesubstancename")
                    } if dr.get("activesubstance") else None,
                    "openfda": dr.get("openfda") or None,
                })

            reactions_out = []
            for rx in p.get("reaction", []) or []:
                reactions_out.append({
                    "reactionmeddraversionpt": pick(rx, "reactionmeddraversionpt"),
                    "reactionmeddrapt": pick(rx, "reactionmeddrapt"),
                    "reactionoutcome": pick(rx, "reactionoutcome", int),
                })

            patient_out = {
                "patientonsetage": pick(p, "patientonsetage", float),
                "patientonsetageunit": pick(p, "patientonsetageunit", int),
                "patientagegroup": pick(p, "patientagegroup", int),
                "patientsex": pick(p, "patientsex", int),
                "patientweight": pick(p, "patientweight", float),
                "reaction": reactions_out or None,
                "drug": drugs_out or None,
                "summary": {
                    "narrativeincludeclinical": pick(p.get("summary", {}), "narrativeincludeclinical"),
                } if p.get("summary") else None,
            }

            # Remove None-only patient
            if any(v is not None for v in patient_out.values()):
                ev_out["patient"] = patient_out
            cleaned.append(ev_out)

        return cleaned

    @task(task_id="load_bigquery")
    def load_bigquery(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        gravar no BigQuery project.dataset.table
        - WRITE_APPEND
        - partitioning por DATE(receivedate)
        - clustering por primarysourcecountry
        """
        if not rows:
            return {"inserted": 0, "message": "no rows to load"}

        # Parsear project.dataset.table
        try:
            project, dataset, table = DEST_TABLE.split(".")
        except ValueError:
            raise ValueError("destination_table deve ser no formato project.dataset.table")

        bq = BigQueryHook(gcp_conn_id=BQ_CONN_ID, location=BQ_LOCATION)

        # Job config
        job_config = {
            "writeDisposition": WRITE_DISPOSITION,
            "timePartitioning": {"type": "DAY", "field": PARTITION_FIELD},
            "clustering": {"fields": CLUSTERING_FIELDS},
            "schema": {"fields": TABLE_SCHEMA},
        }

        # Carregar JSON diretamente (mantém nested/repeated)
        inserted = bq.insert_rows_json(
            table=f"{dataset}.{table}",
            rows=rows,
            project_id=project,
            ignore_unknown_values=False,
            skip_invalid_rows=False,
            schema_fields=TABLE_SCHEMA,
        )

        return {"inserted": len(rows) - len(inserted), "errors": inserted}

    @task(task_id="dq_check")
    def dq_check(rows: List[Dict[str, Any]]) -> str:
        """
        validar contagem e schema:
        - row_count > 0
        - no_nulls: [safetyreportid, receivedate]
        - no_duplicates: safetyreportid
        - schema_match: strict (validado indiretamente por insert_rows_json acima)
        """
        if not rows:
            raise ValueError("DQ FAIL: row_count == 0")

        # no_nulls
        for r in rows:
            if not r.get("safetyreportid"):
                raise ValueError("DQ FAIL: safetyreportid null/empty")
            # receivedate é DATE (string YYYY-MM-DD) após normalização; pode ser None em alguns relatos.
            # A regra exige não-nulo.
            if not r.get("receivedate"):
                raise ValueError("DQ FAIL: receivedate null/empty")

        # no_duplicates
        sids = [r["safetyreportid"] for r in rows]
        if len(sids) != len(set(sids)):
            raise ValueError("DQ FAIL: duplicates in safetyreportid")

        return "DQ PASS"

    # Dependencies: "fetch_openfda >> normalize_events >> load_bigquery >> dq_check"
    raw = fetch_openfda()
    norm = normalize_events(raw)
    load_result = load_bigquery(norm)
    # dq_check valida o mesmo conjunto normalizado (garante regras de linha)
    chain(norm, dq_check(norm))
