
"""Airflow DAG to fetch semaglutide adverse events from openFDA and load into BigQuery."""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.http.hooks.http import HttpHook

DAG_ID = "api_fda_semaglutina_to_db_dag"
DESTINATION_TABLE = "project.dataset.semaglutide_events"
TABLE_SCHEMA: List[Dict[str, Any]] = [
    {"name": "safetyreportversion", "type": "INTEGER"},
    {"name": "safetyreportid", "type": "STRING"},
    {"name": "primarysourcecountry", "type": "STRING"},
    {"name": "occurcountry", "type": "STRING"},
    {"name": "transmissiondateformat", "type": "STRING"},
    {"name": "transmissiondate", "type": "DATE"},
    {"name": "reporttype", "type": "INTEGER"},
    {"name": "serious", "type": "INTEGER"},
    {"name": "seriousnesshospitalization", "type": "INTEGER"},
    {"name": "seriousnessdisabling", "type": "INTEGER"},
    {"name": "seriousnessother", "type": "INTEGER"},
    {"name": "receivedateformat", "type": "STRING"},
    {"name": "receivedate", "type": "DATE"},
    {"name": "receiptdateformat", "type": "STRING"},
    {"name": "receiptdate", "type": "DATE"},
    {"name": "fulfillexpeditecriteria", "type": "INTEGER"},
    {"name": "companynumb", "type": "STRING"},
    {"name": "duplicate", "type": "INTEGER"},
    {
        "name": "reportduplicate",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "duplicatesource", "type": "STRING"},
            {"name": "duplicatenumb", "type": "STRING"},
        ],
    },
    {
        "name": "primarysource",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "reportercountry", "type": "STRING"},
            {"name": "qualification", "type": "INTEGER"},
        ],
    },
    {
        "name": "sender",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "sendertype", "type": "INTEGER"},
            {"name": "senderorganization", "type": "STRING"},
        ],
    },
    {
        "name": "receiver",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "receivertype", "type": "INTEGER"},
            {"name": "receiverorganization", "type": "STRING"},
        ],
    },
    {
        "name": "patient",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "patientonsetage", "type": "FLOAT"},
            {"name": "patientonsetageunit", "type": "INTEGER"},
            {"name": "patientagegroup", "type": "INTEGER"},
            {"name": "patientsex", "type": "INTEGER"},
            {"name": "patientweight", "type": "FLOAT"},
            {
                "name": "reaction",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "reactionmeddraversionpt", "type": "STRING"},
                    {"name": "reactionmeddrapt", "type": "STRING"},
                    {"name": "reactionoutcome", "type": "INTEGER"},
                ],
            },
            {
                "name": "drug",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "drugcharacterization", "type": "INTEGER"},
                    {"name": "medicinalproduct", "type": "STRING"},
                    {"name": "drugauthorizationnumb", "type": "STRING"},
                    {"name": "drugstructuredosagenumb", "type": "FLOAT"},
                    {"name": "drugstructuredosageunit", "type": "STRING"},
                    {"name": "drugseparatedosagenumb", "type": "FLOAT"},
                    {"name": "drugintervaldosageunitnumb", "type": "FLOAT"},
                    {"name": "drugintervaldosagedefinition", "type": "STRING"},
                    {"name": "drugdosagetext", "type": "STRING"},
                    {"name": "drugdosageform", "type": "STRING"},
                    {"name": "drugadministrationroute", "type": "STRING"},
                    {"name": "drugbatchnumb", "type": "STRING"},
                    {"name": "drugindication", "type": "STRING"},
                    {"name": "drugstartdateformat", "type": "STRING"},
                    {"name": "drugstartdate", "type": "DATE"},
                    {"name": "actiondrug", "type": "INTEGER"},
                    {"name": "drugadditional", "type": "INTEGER"},
                    {
                        "name": "activesubstance",
                        "type": "RECORD",
                        "mode": "NULLABLE",
                        "fields": [
                            {"name": "activesubstancename", "type": "STRING"},
                        ],
                    },
                    {
                        "name": "openfda",
                        "type": "RECORD",
                        "mode": "NULLABLE",
                        "fields": [
                            {"name": "application_number", "type": "STRING", "mode": "REPEATED"},
                            {"name": "brand_name", "type": "STRING", "mode": "REPEATED"},
                            {"name": "generic_name", "type": "STRING", "mode": "REPEATED"},
                            {"name": "manufacturer_name", "type": "STRING", "mode": "REPEATED"},
                            {"name": "product_ndc", "type": "STRING", "mode": "REPEATED"},
                            {"name": "product_type", "type": "STRING", "mode": "REPEATED"},
                            {"name": "route", "type": "STRING", "mode": "REPEATED"},
                            {"name": "substance_name", "type": "STRING", "mode": "REPEATED"},
                            {"name": "rxcui", "type": "STRING", "mode": "REPEATED"},
                            {"name": "spl_id", "type": "STRING", "mode": "REPEATED"},
                            {"name": "spl_set_id", "type": "STRING", "mode": "REPEATED"},
                            {"name": "package_ndc", "type": "STRING", "mode": "REPEATED"},
                            {"name": "nui", "type": "STRING", "mode": "REPEATED"},
                            {"name": "pharm_class_cs", "type": "STRING", "mode": "REPEATED"},
                            {"name": "pharm_class_epc", "type": "STRING", "mode": "REPEATED"},
                            {"name": "pharm_class_moa", "type": "STRING", "mode": "REPEATED"},
                            {"name": "unii", "type": "STRING", "mode": "REPEATED"},
                        ],
                    },
                ],
            },
            {
                "name": "summary",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "narrativeincludeclinical", "type": "STRING"},
                ],
            },
        ],
    },
    {"name": "seriousnessdeath", "type": "INTEGER"},
    {"name": "seriousnesslifethreatening", "type": "INTEGER"},
    {"name": "seriousnesscongenitalanomali", "type": "INTEGER"},
]

FETCH_PARAMS = {
    "search": 'patient.drug.openfda.generic_name:"semaglutide"',
    "limit": 100,
}

MAX_PAGES = 50
RATE_LIMIT_SLEEP_SECONDS = 0.3


def _parse_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        parsed = datetime.strptime(value, "%Y%m%d")
        return parsed.date().isoformat()
    except (ValueError, TypeError):
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_openfda(openfda: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not openfda:
        return None
    return {
        "application_number": openfda.get("application_number"),
        "brand_name": openfda.get("brand_name"),
        "generic_name": openfda.get("generic_name"),
        "manufacturer_name": openfda.get("manufacturer_name"),
        "product_ndc": openfda.get("product_ndc"),
        "product_type": openfda.get("product_type"),
        "route": openfda.get("route"),
        "substance_name": openfda.get("substance_name"),
        "rxcui": openfda.get("rxcui"),
        "spl_id": openfda.get("spl_id"),
        "spl_set_id": openfda.get("spl_set_id"),
        "package_ndc": openfda.get("package_ndc"),
        "nui": openfda.get("nui"),
        "pharm_class_cs": openfda.get("pharm_class_cs"),
        "pharm_class_epc": openfda.get("pharm_class_epc"),
        "pharm_class_moa": openfda.get("pharm_class_moa"),
        "unii": openfda.get("unii"),
    }


def _clean_activesubstance(substance: Any) -> Optional[Dict[str, Any]]:
    if isinstance(substance, dict):
        return {"activesubstancename": substance.get("activesubstancename")}
    return None


def _clean_drug(drugs: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    if not drugs:
        return cleaned
    for drug in drugs:
        cleaned.append(
            {
                "drugcharacterization": _to_int(drug.get("drugcharacterization")),
                "medicinalproduct": drug.get("medicinalproduct"),
                "drugauthorizationnumb": drug.get("drugauthorizationnumb"),
                "drugstructuredosagenumb": _to_float(drug.get("drugstructuredosagenumb")),
                "drugstructuredosageunit": drug.get("drugstructuredosageunit"),
                "drugseparatedosagenumb": _to_float(drug.get("drugseparatedosagenumb")),
                "drugintervaldosageunitnumb": _to_float(drug.get("drugintervaldosageunitnumb")),
                "drugintervaldosagedefinition": drug.get("drugintervaldosagedefinition"),
                "drugdosagetext": drug.get("drugdosagetext"),
                "drugdosageform": drug.get("drugdosageform"),
                "drugadministrationroute": drug.get("drugadministrationroute"),
                "drugbatchnumb": drug.get("drugbatchnumb"),
                "drugindication": drug.get("drugindication"),
                "drugstartdateformat": drug.get("drugstartdateformat"),
                "drugstartdate": _parse_date(drug.get("drugstartdate")),
                "actiondrug": _to_int(drug.get("actiondrug")),
                "drugadditional": _to_int(drug.get("drugadditional")),
                "activesubstance": _clean_activesubstance(drug.get("activesubstance")),
                "openfda": _clean_openfda(drug.get("openfda")),
            }
        )
    return cleaned


def _clean_reactions(reactions: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    if not reactions:
        return cleaned
    for reaction in reactions:
        cleaned.append(
            {
                "reactionmeddraversionpt": reaction.get("reactionmeddraversionpt"),
                "reactionmeddrapt": reaction.get("reactionmeddrapt"),
                "reactionoutcome": _to_int(reaction.get("reactionoutcome")),
            }
        )
    return cleaned


def _clean_patient(patient: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(patient, dict):
        return None
    summary = patient.get("summary")
    summary_clean = None
    if isinstance(summary, dict):
        summary_clean = {
            "narrativeincludeclinical": summary.get("narrativeincludeclinical")
        }
    return {
        "patientonsetage": _to_float(patient.get("patientonsetage")),
        "patientonsetageunit": _to_int(patient.get("patientonsetageunit")),
        "patientagegroup": _to_int(patient.get("patientagegroup")),
        "patientsex": _to_int(patient.get("patientsex")),
        "patientweight": _to_float(patient.get("patientweight")),
        "reaction": _clean_reactions(patient.get("reaction")),
        "drug": _clean_drug(patient.get("drug")),
        "summary": summary_clean,
    }


def _clean_reportduplicate(data: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    return {
        "duplicatesource": data.get("duplicatesource"),
        "duplicatenumb": data.get("duplicatenumb"),
    }


def _clean_primarysource(data: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    return {
        "reportercountry": data.get("reportercountry"),
        "qualification": _to_int(data.get("qualification")),
    }


def _clean_sender(data: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    return {
        "sendertype": _to_int(data.get("sendertype")),
        "senderorganization": data.get("senderorganization"),
    }


def _clean_receiver(data: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    return {
        "receivertype": _to_int(data.get("receivertype")),
        "receiverorganization": data.get("receiverorganization"),
    }


with DAG(
    dag_id=DAG_ID,
    description="Coletar eventos adversos de semaglutide na openFDA e carregar no BigQuery.",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    catchup=False,
    tags=["openfda", "semaglutide", "bigquery"],
) as dag:

    @task(task_id="fetch_openfda")
    def fetch_openfda() -> List[Dict[str, Any]]:
        """Buscar eventos de semaglutide na API openFDA com pagina??o controlada."""
        hook = HttpHook(method="GET", http_conn_id="openfda_conn")
        all_events: List[Dict[str, Any]] = []
        for page in range(MAX_PAGES):
            params = {**FETCH_PARAMS, "skip": page * FETCH_PARAMS["limit"]}
            response = hook.run(endpoint="/drug/event.json", params=params)
            payload = response.json()
            page_results = payload.get("results", [])
            if not page_results:
                break
            all_events.extend(page_results)
            if len(page_results) < FETCH_PARAMS["limit"]:
                break
            time.sleep(RATE_LIMIT_SLEEP_SECONDS)
        return all_events

    @task(task_id="normalize_events")
    def normalize_events(raw_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Padronizar dados, normalizar datas e deduplicar pelo safetyreportid."""
        deduped: Dict[str, Dict[str, Any]] = {}
        for event in raw_events or []:
            safetyreportid = str(event.get("safetyreportid")) if event.get("safetyreportid") else None
            if not safetyreportid or safetyreportid in deduped:
                continue
            cleaned_event: Dict[str, Any] = {
                "safetyreportversion": _to_int(event.get("safetyreportversion")),
                "safetyreportid": safetyreportid,
                "primarysourcecountry": event.get("primarysourcecountry"),
                "occurcountry": event.get("occurcountry"),
                "transmissiondateformat": event.get("transmissiondateformat"),
                "transmissiondate": _parse_date(event.get("transmissiondate")),
                "reporttype": _to_int(event.get("reporttype")),
                "serious": _to_int(event.get("serious")),
                "seriousnesshospitalization": _to_int(event.get("seriousnesshospitalization")),
                "seriousnessdisabling": _to_int(event.get("seriousnessdisabling")),
                "seriousnessother": _to_int(event.get("seriousnessother")),
                "receivedateformat": event.get("receivedateformat"),
                "receivedate": _parse_date(event.get("receivedate")),
                "receiptdateformat": event.get("receiptdateformat"),
                "receiptdate": _parse_date(event.get("receiptdate")),
                "fulfillexpeditecriteria": _to_int(event.get("fulfillexpeditecriteria")),
                "companynumb": event.get("companynumb"),
                "duplicate": _to_int(event.get("duplicate")),
                "reportduplicate": _clean_reportduplicate(event.get("reportduplicate")),
                "primarysource": _clean_primarysource(event.get("primarysource")),
                "sender": _clean_sender(event.get("sender")),
                "receiver": _clean_receiver(event.get("receiver")),
                "patient": _clean_patient(event.get("patient")),
                "seriousnessdeath": _to_int(event.get("seriousnessdeath")),
                "seriousnesslifethreatening": _to_int(event.get("seriousnesslifethreatening")),
                "seriousnesscongenitalanomali": _to_int(event.get("seriousnesscongenitalanomali")),
            }
            deduped[safetyreportid] = cleaned_event
        return list(deduped.values())

    @task(task_id="load_bigquery")
    def load_bigquery(events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Carregar dados normalizados no BigQuery com schema declarado."""
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", location="US")
        project_id, dataset_id, table_id = DESTINATION_TABLE.split(".")
        hook.create_empty_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            exists_ok=True,
            table_resource={
                "schema": {"fields": TABLE_SCHEMA},
                "timePartitioning": {"type": "DAY", "field": "receivedate"},
                "clustering": {"fields": ["reaction", "primarysourcecountry"]},
            },
        )
        if not events:
            return {"rows_loaded": 0, "table": DESTINATION_TABLE}
        hook.insert_rows_json(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            json_rows=events,
            row_ids=[event.get("safetyreportid") for event in events],
            ignore_unknown_values=False,
        )
        return {"rows_loaded": len(events), "table": DESTINATION_TABLE}

    @task(task_id="dq_check")
    def dq_check(load_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Validar contagem, nulidade e schema do conjunto carregado."""
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", location="US")
        project_id, dataset_id, table_id = DESTINATION_TABLE.split(".")
        table = hook.get_table(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
        table_schema = table.to_api_repr().get("schema", {}).get("fields", [])
        if table_schema != TABLE_SCHEMA:
            raise ValueError("schema_match failed: schema does not match specification")
        client = hook.get_client(project_id=project_id)
        query = f"""
            SELECT
                COUNT(*) AS row_count,
                COUNTIF(safetyreportid IS NULL) AS null_safetyreportid,
                COUNTIF(receivedate IS NULL) AS null_receivedate,
                COUNT(*) - COUNT(DISTINCT safetyreportid) AS duplicate_count
            FROM `{project_id}.{dataset_id}.{table_id}`
        """
        result = client.query(query).result()
        row = next(result)
        if row.row_count <= 0:
            raise ValueError("row_count check failed: table is empty")
        if row.null_safetyreportid > 0:
            raise ValueError("no_nulls check failed: safetyreportid contains NULLs")
        if row.null_receivedate > 0:
            raise ValueError("no_nulls check failed: receivedate contains NULLs")
        if row.duplicate_count > 0:
            raise ValueError("no_duplicates check failed: duplicate safetyreportid detected")
        return {
            "row_count": row.row_count,
            "rows_loaded": load_stats.get("rows_loaded", 0),
            "schema_valid": True,
        }

    dq_check(load_bigquery(normalize_events(fetch_openfda())))

__all__ = ["dag"]
