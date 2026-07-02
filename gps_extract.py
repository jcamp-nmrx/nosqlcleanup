#!/usr/bin/env python3

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from borneo import NoSQLHandle, NoSQLHandleConfig, QueryRequest
from borneo.iam import SignatureProvider
import oci

# ── Config ────────────────────────────────────────────────────────────────────
NOSQL_ENDPOINT    = 'us-langley-1'
NOSQL_COMPARTMENT = 'ocid1.compartment.oc2..aaaaaaaafjvmapmuq65mzhzy4wveapr4trmvgvepjg7nqpnksjjlknmoohma'
TABLE             = 'SHIELDING_GPS_DATA_TBL'

OCI_NAMESPACE     = 'focalpointcloud'
BUCKET            = 'ShieldingData'
STATE_OBJECT      = '_state/last_run.json'

WORKERS           = 4

# Throttle control: rows per page and delay between pages (seconds)
PAGE_SIZE         = 50
PAGE_DELAY_SEC    = 0.5

# How many pages to accumulate before flushing completed buckets to object store
FLUSH_EVERY_PAGES = 20

# ── OCI Object Store client ───────────────────────────────────────────────────
def make_oci_client():
    config = oci.config.from_file()  # reads ~/.oci/config
    return oci.object_storage.ObjectStorageClient(config)

# ── NoSQL handle ──────────────────────────────────────────────────────────────
def make_nosql_handle():
    provider = SignatureProvider()
    config   = NoSQLHandleConfig(NOSQL_ENDPOINT, provider).set_default_compartment(NOSQL_COMPARTMENT)
    return NoSQLHandle(config)

# ── State helpers ─────────────────────────────────────────────────────────────
def read_last_run(oci_client):
    try:
        resp = oci_client.get_object(OCI_NAMESPACE, BUCKET, STATE_OBJECT)
        data = json.loads(resp.data.content.decode('utf-8'))
        ts   = datetime.fromisoformat(data['last_successful_run'])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        print(f"Last successful run: {ts.isoformat()}")
        return ts
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            # First ever run — start from the beginning of the table
            ts = datetime(2025, 8, 1, 14, 22, 34, tzinfo=timezone.utc)
            print(f"No state file found. Defaulting to: {ts.isoformat()}")
            return ts
        raise

def write_last_run(oci_client, ts):
    data = json.dumps({'last_successful_run': ts.isoformat()}).encode('utf-8')
    oci_client.put_object(OCI_NAMESPACE, BUCKET, STATE_OBJECT, data)
    print(f"State updated: {ts.isoformat()}")

# ── Object store path builder ─────────────────────────────────────────────────
def build_object_path(sample_time, te_id, run_ts):
    """
    gps-data/YYYY/MM/te_id=<te_id>/DD/HH/<run_ts>.json
    """
    if sample_time.tzinfo is None:
        sample_time = sample_time.replace(tzinfo=timezone.utc)
    return (
        f"gps-data/"
        f"{sample_time.year:04d}/"
        f"{sample_time.month:02d}/"
        f"te_id={te_id}/"
        f"{sample_time.day:02d}/"
        f"{sample_time.hour:02d}/"
        f"{run_ts.strftime('%Y-%m-%dT%H:%M:%SZ')}.json"
    )

# ── Row serializer ────────────────────────────────────────────────────────────
def serialize_row(row):
    """Convert NoSQL row to JSON-serializable dict."""
    out = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out

# ── Write one group to object store ──────────────────────────────────────────
def write_group(key, rows, run_ts, oci_client):
    year, month, day, hour, te_id = key
    sample_time = datetime(year, month, day, hour, tzinfo=timezone.utc)
    obj_path    = build_object_path(sample_time, te_id, run_ts)
    payload     = json.dumps(rows, indent=2).encode('utf-8')
    oci_client.put_object(OCI_NAMESPACE, BUCKET, obj_path, payload,
                          content_type='application/json')
    return obj_path

# ── Flush completed buckets ───────────────────────────────────────────────────
def flush_buckets(groups, active_keys, run_ts, oci_client, executor, pending_futures, written, errors):
    """
    Submit write jobs for any bucket key that is NOT in active_keys
    (i.e. we haven't seen a new row for that bucket recently, so it's safe to flush).
    Removes flushed keys from groups.
    """
    flush_keys = [k for k in list(groups.keys()) if k not in active_keys]
    for key in flush_keys:
        rows = groups.pop(key)
        f = executor.submit(write_group, key, rows, run_ts, oci_client)
        pending_futures[f] = key

    # Collect any completed futures
    done = [f for f in list(pending_futures) if f.done()]
    for f in done:
        key = pending_futures.pop(f)
        try:
            f.result()
            written[0] += 1
        except Exception as e:
            print(f"  ERROR writing {key}: {e}")
            errors[0] += 1

    return len(flush_keys)

# ── Core extract logic ────────────────────────────────────────────────────────
def extract_and_write(since_ts, run_ts, nosql_handle, oci_client):
    """
    Query all rows with create_date >= since_ts.
    Group by (sample_time_hour, te_id) and write one JSON file per group.
    Flushes completed buckets incrementally to avoid holding all rows in memory.
    """
    since_str = since_ts.strftime('%Y-%m-%dT%H:%M:%S')
    sql = f"SELECT * FROM {TABLE} WHERE create_date >= '{since_str}'"
    print(f"Query: {sql}")

    req        = (QueryRequest()
                  .set_statement(sql)
                  .set_limit(PAGE_SIZE))
    groups     = {}   # key: (year, month, day, hour, te_id) -> list of rows
    total_rows = 0
    page_num   = 0
    written    = [0]  # mutable int via list
    errors     = [0]

    pending_futures = {}

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:

        while True:
            result    = nosql_handle.query(req)
            page_rows = result.get_results()
            page_num  += 1
            total_rows += len(page_rows)

            # Track which bucket keys appear in THIS page (still "active")
            active_keys = set()
            for row in page_rows:
                sample_time = row.get('sample_time')
                te_id       = row.get('te_id')

                if sample_time is None or te_id is None:
                    continue

                if sample_time.tzinfo is None:
                    sample_time = sample_time.replace(tzinfo=timezone.utc)

                key = (sample_time.year, sample_time.month, sample_time.day,
                       sample_time.hour, te_id)
                groups.setdefault(key, []).append(serialize_row(row))
                active_keys.add(key)

            if page_num % 10 == 0:
                print(f"  ... page {page_num}, {total_rows:,} rows fetched, "
                      f"{written[0]:,} buckets written so far")

            # Periodically flush buckets that weren't touched this page
            if page_num % FLUSH_EVERY_PAGES == 0:
                flushed = flush_buckets(groups, active_keys, run_ts, oci_client,
                                        executor, pending_futures, written, errors)
                if flushed:
                    print(f"  [flush] submitted {flushed} buckets "
                          f"({len(groups)} still in memory)")

            if req.is_done():
                break

            time.sleep(PAGE_DELAY_SEC)

        # ── Final flush: write everything remaining ───────────────────────────
        print(f"Fetch complete: {total_rows:,} rows, flushing {len(groups)} remaining buckets...")
        for key, rows in groups.items():
            f = executor.submit(write_group, key, rows, run_ts, oci_client)
            pending_futures[f] = key

        for f in as_completed(pending_futures):
            key = pending_futures[f]
            try:
                f.result()
                written[0] += 1
            except Exception as e:
                print(f"  ERROR writing {key}: {e}")
                errors[0] += 1

    print(f"Done -- written={written[0]:,}  errors={errors[0]}")
    return errors[0] == 0

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    oci_client   = make_oci_client()
    nosql_handle = make_nosql_handle()

    run_ts   = datetime.now(timezone.utc)
    since_ts = read_last_run(oci_client)

    success = extract_and_write(since_ts, run_ts, nosql_handle, oci_client)

    if success:
        write_last_run(oci_client, run_ts)
    else:
        print("Errors occurred -- last_run.json NOT updated. Will retry from same timestamp next run.")

    nosql_handle.close()

if __name__ == '__main__':
    main()
