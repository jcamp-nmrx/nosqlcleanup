#!/usr/bin/env python3

import json
import threading
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
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
            # First ever run — go back 1 hour
            ts = datetime.now(timezone.utc) - relativedelta(hours=1)
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

    Example: gps-data/2025/04/te_id=123/25/08/2025-04-25T09:00:00Z.json

    Query patterns:
      All of April 2025 for te_id=123  -> prefix: gps-data/2025/04/te_id=123/
      April 25-27 for te_id=123        -> prefix per day: gps-data/2025/04/te_id=123/25/
      April 25 8am-5pm for te_id=123   -> prefix per hour: gps-data/2025/04/te_id=123/25/08/
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

# ── Core extract logic ────────────────────────────────────────────────────────
def extract_and_write(since_ts, run_ts, nosql_handle, oci_client):
    """
    Query all rows with create_date >= since_ts.
    Group by (sample_time_hour, te_id) and write one JSON file per group.
    """
    since_str = since_ts.strftime('%Y-%m-%dT%H:%M:%S')
    sql = f"SELECT * FROM {TABLE} WHERE create_date >= '{since_str}'"
    print(f"Query: {sql}")

    req    = QueryRequest().set_statement(sql)
    groups = {}   # key: (year, month, day, hour, te_id) -> list of rows

    # ── Fetch all pages ───────────────────────────────────────────────────────
    while True:
        result = nosql_handle.query(req)
        for row in result.get_results():
            sample_time = row.get('sample_time')
            te_id       = row.get('te_id')

            if sample_time is None or te_id is None:
                continue

            if sample_time.tzinfo is None:
                sample_time = sample_time.replace(tzinfo=timezone.utc)

            key = (sample_time.year, sample_time.month, sample_time.day,
                   sample_time.hour, te_id)
            groups.setdefault(key, []).append(serialize_row(row))

        if req.is_done():
            break

    print(f"Fetched rows grouped into {len(groups):,} buckets")

    # ── Write each group to object store ─────────────────────────────────────
    written = 0
    errors  = 0
    lock    = threading.Lock()

    def write_group(key, rows):
        nonlocal written, errors
        year, month, day, hour, te_id = key
        sample_time = datetime(year, month, day, hour, tzinfo=timezone.utc)
        obj_path    = build_object_path(sample_time, te_id, run_ts)
        payload     = json.dumps(rows, indent=2).encode('utf-8')
        try:
            oci_client.put_object(OCI_NAMESPACE, BUCKET, obj_path, payload,
                                  content_type='application/json')
            with lock:
                written += 1
        except Exception as e:
            print(f"  ERROR writing {obj_path}: {e}")
            with lock:
                errors += 1

    threads = []
    for key, rows in groups.items():
        t = threading.Thread(target=write_group, args=(key, rows))
        threads.append(t)
        t.start()
        # cap concurrency
        if len([x for x in threads if x.is_alive()]) >= WORKERS:
            for x in threads:
                x.join(timeout=0.1)

    for t in threads:
        t.join()

    print(f"Done -- written={written:,}  errors={errors}")
    return errors == 0

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

if __name__ == '__main__':
    main()
