"""
Daily SOLV loader (scheduled Lambda, triggered by an EventBridge cron).

For each enabled report in REPORTS:
  1. Find the newest CSV sitting directly in the report's S3 folder that
     landed within the lookback window (subfolders are ignored).
  2. Skip it if that exact file was already merged successfully.
  3. TRUNCATE the staging table, COPY the file into it, and stamp every row
     with source_file / loaded_at.
  4. CALL the merge proc (no arguments), which pushes staging rows into the
     main table.
Each step gets a row in logging.eb_log (phase 's_load' and 's_merge').

Manual run / backfill (Lambda console "Test" or CLI invoke):
  {"report": "patient_report"}                                  -> run one report
  {"report": "patient_report",
   "key": "SOLV/SOLV/patient_report/some_file.csv",
   "force": true}                                               -> load a specific file,
                                                                   even if already loaded
  {"report": "patient_report", "backfill": true}                -> load every file in the
                                                                   folder not yet merged
                                                                   (safe to re-run)
"""

import csv
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from io import StringIO

import boto3
import psycopg2
from psycopg2 import sql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

# ---------------------------------------------------------------------------
# Settings (override with Lambda environment variables)
# ---------------------------------------------------------------------------
BUCKET = os.getenv("SOURCE_BUCKET", "uc4k-data")
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))        # how far back to look for "today's" file
STRICT_HEADERS = os.getenv("STRICT_HEADERS", "false").lower() == "true"  # fail on header name mismatch

LOG_TABLE = "logging.eb_log"
CHANNEL = "solv"

# ---------------------------------------------------------------------------
# Report definitions.
# columns = (CSV header, stg column) pairs, IN CSV ORDER. Data is loaded by
# position; the headers are only used to warn you if Solv changes the file.
# ---------------------------------------------------------------------------
PATIENT_REPORT_COLUMNS = [
    ("Booking ID Hash Solv", "booking_id_hash_solv"),
    ("Practice Name", "practice_name"),
    ("Clinic Name", "clinic_name"),
    ("EMR Clinic Name", "emr_clinic_name"),
    ("EMR Clinic ID", "emr_clinic_id"),
    ("Transfer In From Location Name", "transfer_in_from_location_name"),
    ("Booking ID EMR", "booking_id_emr"),
    ("Booking Created Time", "booking_created_time"),
    ("Booking Status", "booking_status"),
    ("Visit Type Name", "visit_type_name"),
    ("Booking Reason For Visit", "booking_reason_for_visit"),
    ("Booking Source", "booking_source"),
    ("Booking Method", "booking_method"),
    ("Booking Type of Online", "booking_type_of_online"),
    ("Booking Is Solv Connect (Yes / No)", "booking_is_solv_connect"),
    ("Booking Is Google GMB (Yes / No)", "booking_is_google_gmb"),
    ("Booking Cancellation Time", "booking_cancellation_time"),
    ("Booking Cancellation Reason", "booking_cancellation_reason"),
    ("Booking Canceled By", "booking_canceled_by"),
    ("Booking Is Left Without Being Seen (Yes / No)", "booking_is_left_without_being_seen"),
    ("Original Appointment Date", "original_appointment_date"),
    ("Original Appointment Time", "original_appointment_time"),
    ("Arrived Time", "arrived_time"),
    ("Checked In Time", "checked_in_time"),
    ("In Exam Time", "in_exam_time"),
    ("Discharged Time", "discharged_time"),
    ("Telemed Patient Joined Call Time", "telemed_patient_joined_call_time"),
    ("Minutes Here to Ready", "minutes_here_to_ready"),
    ("Minutes Ready to Exam", "minutes_ready_to_exam"),
    ("Minutes Exam to Done", "minutes_exam_to_done"),
    ("Wait Time From Arrival To Exam", "wait_time_from_arrival_to_exam"),
    ("Wait Time From Wait Start To Exam", "wait_time_from_wait_start_to_exam"),
    ("Door To Door Time (Raw)", "door_to_door_time_raw"),
    ("Door To Door Time (Wait Start to Discharge)", "door_to_door_time_wait_start_to_discharge"),
    ("Patient Solv Account ID Hash", "patient_solv_account_id_hash"),
    ("Patient ID EMR", "patient_id_emr"),
    ("Patient Mobile Phone Number", "patient_mobile_phone_number"),
    ("Is Transactional SMS Unsubscribed? (Yes / No)", "is_transactional_sms_unsubscribed"),
    ("Is Marketing SMS Unsubscribed? (Yes / No)", "is_marketing_sms_unsubscribed"),
    ("Patient Type New or Returning", "patient_type_new_or_returning"),
    ("Patient First Name", "patient_first_name"),
    ("Patient Last Name", "patient_last_name"),
    ("Patient Birth Date", "patient_birth_date"),
    ("Patient Birth Sex", "patient_birth_sex"),
    ("Patient Race", "patient_race"),
    ("Patient Ethnicity", "patient_ethnicity"),
    ("Patient Email", "patient_email"),
    ("Patient Address Street", "patient_address_street"),
    ("Patient Address Secondary", "patient_address_secondary"),
    ("Patient Address City", "patient_address_city"),
    ("Patient Address State", "patient_address_state"),
    ("Patient Address Zipcode", "patient_address_zipcode"),
    ("Patient Notes", "patient_notes"),
    ("Paperwork Status", "paperwork_status"),
    ("Paperwork Start Time", "paperwork_start_time"),
    ("Paperwork Complete Time", "paperwork_complete_time"),
    ("Paperwork Last Edited By", "paperwork_last_edited_by"),
    ("Staff Booked By First Name", "staff_booked_by_first_name"),
    ("Staff Booked By Last Name", "staff_booked_by_last_name"),
    ("Staff Front Office Name", "staff_front_office_name"),
    ("Provider Name", "provider_name"),
    ("Insurance Payer Name From Card/Paperwork", "insurance_payer_name_from_cardpaperwork"),
    ("Insurance Member Code", "insurance_member_code"),
    ("Is Government Insurance? (Yes / No)", "is_government_insurance"),
    ("Insurer Type", "insurer_type"),
]

REPORTS = {
    "patient_report": {
        "enabled": True,
        "prefix": "SOLV/SOLV/patient_report/",
        "key_regex": r"\.csv$",
        "staging_table": ("stg", "s_solv_patient_report_staging"),
        "merge_proc": ("stg", "s_solv_patient_report_staging"),
        "columns": PATIENT_REPORT_COLUMNS,
    },
    # Placeholder for the upcoming report that will land in SOLV/SOLV/ itself.
    # Only files directly in that folder are considered, so patient_report/
    # files are never picked up here. Tighten key_regex once you know the
    # file name (e.g. r"^SOLV/SOLV/visits_.*\.csv$").
    "second_report": {
        "enabled": False,
        "prefix": "SOLV/SOLV/",
        "key_regex": r"\.csv$",
        "staging_table": ("stg", "s_solv_second_report_staging"),  # needs source_file + loaded_at columns
        "merge_proc": ("stg", "s_solv_second_report_staging"),
        "columns": [],  # (CSV header, stg column) pairs, in CSV order
    },
}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_db_connection():
    creds = json.loads(os.environ["DB_CREDENTIALS"])
    return psycopg2.connect(
        dbname=creds["database"],
        user=creds["user"],
        password=creds["password"],
        host=creds["host"],
        port=creds["port"],
        connect_timeout=10,
    )


def log_start(conn, phase, source, target):
    run_id = int(time.time())
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {LOG_TABLE}
                    (run_id, channel, phase, run_source, run_target, run_status, start_ts)
                VALUES (%s, %s, %s, %s, %s, 'running', CURRENT_TIMESTAMP)""",
            (run_id, CHANNEL, phase, source, target),
        )
    conn.commit()
    return run_id


def log_end(conn, run_id, phase, source, status, error=None):
    with conn.cursor() as cur:
        cur.execute(
            f"""UPDATE {LOG_TABLE}
                SET run_status = %s, error_desc = %s, end_ts = CURRENT_TIMESTAMP
                WHERE run_id = %s AND channel = %s AND phase = %s AND run_source = %s""",
            (status, error[:255] if error else None, run_id, CHANNEL, phase, source),
        )
    conn.commit()


def already_merged(conn, s3_uri):
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT 1 FROM {LOG_TABLE}
                WHERE channel = %s AND phase = 's_merge'
                  AND run_source = %s AND run_status = 'success'
                LIMIT 1""",
            (CHANNEL, s3_uri),
        )
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def list_files(report):
    """All matching objects directly under the report's prefix (subfolders ignored)."""
    pattern = re.compile(report["key_regex"], re.IGNORECASE)
    files = []
    paginator = s3.get_paginator("list_objects_v2")
    # Delimiter="/" returns only objects directly in this folder, not in subfolders.
    for page in paginator.paginate(Bucket=BUCKET, Prefix=report["prefix"], Delimiter="/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/") and pattern.search(key):
                files.append(obj)
    return files


def find_latest_file(report):
    """Newest matching object modified within the lookback window."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    candidates = [o for o in list_files(report) if o["LastModified"] >= cutoff]

    if not candidates:
        return None

    candidates.sort(key=lambda o: o["LastModified"])
    if len(candidates) > 1:
        logger.warning(
            "Found %d files in the last %dh under %s; using the newest. Others: %s",
            len(candidates), LOOKBACK_HOURS, report["prefix"],
            [c["Key"] for c in candidates[:-1]],
        )
    return candidates[-1]["Key"]


def read_csv_from_s3(key):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return body.decode("utf-8-sig")  # utf-8-sig strips a BOM if the export has one


# ---------------------------------------------------------------------------
# Load steps
# ---------------------------------------------------------------------------
def normalize(name):
    """Case/whitespace-insensitive comparison of header text."""
    return re.sub(r"\s+", " ", name.strip().lower())


def check_header(csv_text, columns, report_name):
    """Columns load by position, so make sure the file still has the shape we expect."""
    expected = [csv_name for csv_name, _ in columns]
    header = next(csv.reader(StringIO(csv_text)), None)
    if not header:
        raise ValueError(f"{report_name}: file is empty")
    if len(header) != len(expected):
        raise ValueError(
            f"{report_name}: expected {len(expected)} columns, file has {len(header)}. "
            f"Header: {header}"
        )
    mismatches = [
        f"#{i + 1} got '{h}', expected '{e}'"
        for i, (h, e) in enumerate(zip(header, expected))
        if normalize(h) != normalize(e)
    ]
    if mismatches:
        msg = f"{report_name}: header names differ from expected columns: {mismatches}"
        if STRICT_HEADERS:
            raise ValueError(msg)
        logger.warning(msg)


def load_to_staging(conn, report, csv_text, s3_uri):
    """TRUNCATE + COPY + stamp in one transaction, so staging is never left half-loaded.

    Every staging row gets source_file / loaded_at, so the no-argument merge
    proc can tell which file it is merging.
    """
    target = sql.Identifier(*report["staging_table"])
    cols = sql.SQL(", ").join(sql.Identifier(col) for _, col in report["columns"])
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(target, cols)

    with conn.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE TABLE {}").format(target))
        cur.copy_expert(copy_sql.as_string(conn), StringIO(csv_text))
        cur.execute(
            sql.SQL("UPDATE {} SET source_file = %s, loaded_at = now()").format(target),
            (s3_uri,),
        )
        rows = cur.rowcount
    conn.commit()
    return rows


def run_merge(conn, report):
    """CALL the merge proc. Autocommit lets the proc COMMIT internally if it wants to."""
    proc = sql.Identifier(*report["merge_proc"])
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CALL {}()").format(proc))
    finally:
        conn.autocommit = False


def process_report(conn, name, report, key=None, force=False):
    key = key or find_latest_file(report)
    if key is None:
        logger.error("%s: no file found under s3://%s/%s in the last %dh",
                     name, BUCKET, report["prefix"], LOOKBACK_HOURS)
        return {"report": name, "status": "no_file"}

    s3_uri = f"s3://{BUCKET}/{key}"
    if not force and already_merged(conn, s3_uri):
        logger.info("%s: %s already merged, skipping", name, s3_uri)
        return {"report": name, "status": "already_loaded", "file": s3_uri}

    staging_name = ".".join(report["staging_table"])
    proc_name = ".".join(report["merge_proc"])

    # Step 1: file -> staging
    run_id = log_start(conn, "s_load", s3_uri, staging_name)
    try:
        csv_text = read_csv_from_s3(key)
        check_header(csv_text, report["columns"], name)
        rows = load_to_staging(conn, report, csv_text, s3_uri)
        log_end(conn, run_id, "s_load", s3_uri, "success")
        logger.info("%s: loaded %d rows from %s into %s", name, rows, s3_uri, staging_name)
    except Exception as e:
        conn.rollback()
        log_end(conn, run_id, "s_load", s3_uri, "failed", str(e))
        raise

    # Step 2: staging -> main table
    run_id = log_start(conn, "s_merge", s3_uri, proc_name)
    try:
        run_merge(conn, report)
        log_end(conn, run_id, "s_merge", s3_uri, "success")
        logger.info("%s: merge proc %s finished", name, proc_name)
    except Exception as e:
        conn.rollback()
        log_end(conn, run_id, "s_merge", s3_uri, "failed", str(e))
        raise

    return {"report": name, "status": "success", "file": s3_uri, "rows": rows}


# ---------------------------------------------------------------------------
# Backfill: load every file in the folder that hasn't been merged yet
# ---------------------------------------------------------------------------
BACKFILL_TIME_BUFFER_MS = 90_000  # stop starting new files with < 90s left


def backfill_report(conn, name, report, context):
    """Oldest-to-newest; already-merged files are skipped, so it is safe to re-run.

    Stops cleanly before the Lambda times out and reports how many files are
    left; invoke it again and it picks up where it stopped.
    """
    files = sorted(list_files(report), key=lambda o: (o["LastModified"], o["Key"]))
    logger.info("%s backfill: %d files found under %s", name, len(files), report["prefix"])

    summary = {"report": name, "found": len(files), "loaded": 0, "skipped": 0,
               "failed": [], "remaining": 0}

    for i, obj in enumerate(files):
        if context and context.get_remaining_time_in_millis() < BACKFILL_TIME_BUFFER_MS:
            summary["remaining"] = len(files) - i
            logger.warning("%s backfill: stopping for time, %d files left; invoke again to continue",
                           name, summary["remaining"])
            break
        try:
            result = process_report(conn, name, report, key=obj["Key"])
            if result["status"] == "success":
                summary["loaded"] += 1
            else:
                summary["skipped"] += 1
        except Exception as e:
            # Keep going; failed files are logged in eb_log and listed in the summary.
            logger.exception("%s backfill: %s failed", name, obj["Key"])
            summary["failed"].append({"file": obj["Key"], "error": str(e)[:300]})

    return summary


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------
LOCK_SQL = "SELECT pg_try_advisory_lock(hashtext('solv_daily_loader'))"


def handler(event, context):
    event = event if isinstance(event, dict) else {}
    only = event.get("report")
    key = event.get("key")
    force = bool(event.get("force", False))
    backfill = bool(event.get("backfill", False))

    if (key or backfill) and not only:
        raise ValueError("'key' and 'backfill' require 'report' so we know which table it belongs to")
    if key and backfill:
        raise ValueError("Use either 'key' or 'backfill', not both")

    if only:
        if only not in REPORTS:
            raise ValueError(f"Unknown report '{only}'. Options: {list(REPORTS)}")
        targets = {only: REPORTS[only]}
    else:
        targets = {n: r for n, r in REPORTS.items() if r["enabled"]}

    results = []
    conn = get_db_connection()
    try:
        # Only one loader at a time: runs share the staging table, so a backfill
        # and the daily cron must never overlap.
        with conn.cursor() as cur:
            cur.execute(LOCK_SQL)
            got_lock = cur.fetchone()[0]
        conn.commit()
        if not got_lock:
            raise RuntimeError("Another SOLV load is already running; try again when it finishes")

        if backfill:
            summary = backfill_report(conn, only, targets[only], context)
            logger.info("Backfill summary: %s", json.dumps(summary))
            return summary

        for name, report in targets.items():
            try:
                results.append(process_report(conn, name, report, key, force))
            except Exception as e:
                logger.exception("%s failed", name)
                results.append({"report": name, "status": "failed", "error": str(e)})
    finally:
        conn.close()  # also releases the advisory lock

    logger.info("Run summary: %s", json.dumps(results))

    # Fail the invocation on errors or missing files so CloudWatch alarms can catch it.
    problems = [r for r in results if r["status"] in ("failed", "no_file")]
    if problems:
        raise RuntimeError(f"SOLV load had problems: {json.dumps(problems)}")
    return results