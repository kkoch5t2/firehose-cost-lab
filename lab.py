#!/usr/bin/env python3
"""Finite Firehose experiment. No AWS calls occur with --dry-run."""
import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

UNIT = 5120
LINE_BYTES = 200


def line(run_id, seq):
    # ASCII JSON + padding + newline: exactly 200 bytes including newline.
    raw = json.dumps({"run": run_id, "seq": seq, "status": 200},
                     separators=(",", ":")).encode("ascii")
    if len(raw) >= LINE_BYTES:
        raise ValueError("line too long")
    return raw + b" " * (LINE_BYTES - len(raw) - 1) + b"\n"


def records(run_id, count, group):
    for start in range(0, count, group):
        yield b"".join(line(run_id, i) for i in range(start, min(start + group, count)))


def batches(source):
    batch, size = [], 0
    for item in source:
        if batch and (len(batch) >= 500 or size + len(item) > 250000):
            yield batch
            batch, size = [], 0
        batch.append(item)
        size += len(item)
    if batch:
        yield batch


def send_batch(client, stream, payloads, stats):
    pending = [{"Data": p} for p in payloads]
    for attempt in range(5):
        stats["api_calls"] += 1
        response = client.put_record_batch(DeliveryStreamName=stream, Records=pending)
        outcomes = response["RequestResponses"]
        if len(outcomes) != len(pending):
            raise RuntimeError("unexpected response length")
        failed = []
        for record, outcome in zip(pending, outcomes):
            if outcome.get("ErrorCode"):
                failed.append(record)
            else:
                n = len(record["Data"])
                stats["accepted_records"] += 1
                stats["accepted_bytes"] += n
                stats["estimated_billable_bytes"] += ((n + UNIT - 1) // UNIT) * UNIT
        if not failed:
            return
        stats["partial_failure_records"] += len(failed)
        pending = failed
        time.sleep(min(2 ** attempt, 8))
    raise RuntimeError(f"{len(pending)} records still failed; inspect manifest before retrying")


def verify(s3, bucket, scenario, run_id, count):
    # Stream objects instead of downloading the whole experiment to CloudShell.
    seen, duplicates, invalid, objects = set(), 0, 0, 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=scenario + "/"):
        for obj in page.get("Contents", []):
            objects += 1
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"]
            try:
                for raw in body.iter_lines():
                    data = json.loads(raw)
                    if data.get("run") != run_id:
                        continue
                    seq = data.get("seq")
                    if not isinstance(seq, int) or not 0 <= seq < count:
                        invalid += 1
                    elif seq in seen:
                        duplicates += 1
                    else:
                        seen.add(seq)
            finally:
                body.close()
    result = {"run_id": run_id, "expected_lines": count, "unique_lines": len(seen),
              "missing_lines": count - len(seen), "duplicate_lines": duplicates,
              "invalid_lines": invalid, "objects_scanned": objects}
    print(json.dumps(result, indent=2))
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", choices=["a", "b"], required=True)
    p.add_argument("--kind", choices=["live", "bulk"], default="live")
    p.add_argument("--lines", type=int, default=1000000)
    p.add_argument("--region", default="ap-southeast-2")
    p.add_argument("--price-usd-per-gb", type=float, default=None)
    p.add_argument("--stack", default="firehose-cost-lab")
    p.add_argument("--run-id", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verify", metavar="MANIFEST")
    args = p.parse_args()
    if args.lines < 1 or args.lines > 1000000:
        p.error("--lines must be between 1 and 1000000")
    if args.verify:
        import boto3
        m = json.loads(Path(args.verify).read_text())
        result = verify(boto3.client("s3", region_name=m["region"]), m["bucket"],
                        m["scenario"], m["run_id"], m["source_lines"])
        Path(args.verify + ".verification.json").write_text(json.dumps(result, indent=2))
        if result["missing_lines"] or result["duplicate_lines"] or result["invalid_lines"]:
            raise SystemExit("Verification incomplete: wait and retry; duplicates require investigation")
        return
    count = 60 if args.kind == "live" else args.lines
    price = args.price_usd_per_gb
    if price is None:
        price = {"ap-southeast-2": .037, "ap-northeast-1": .036}.get(args.region)
    if price is None or price <= 0:
        p.error("Specify a positive --price-usd-per-gb for this region")
    run_id = args.run_id or uuid.uuid4().hex[:12]
    if not run_id.isalnum() or len(run_id) > 16:
        p.error("--run-id must be 1-16 ASCII alphanumeric characters")
    run_id.encode("ascii")
    group = 1 if args.scenario == "a" else 60
    expected_records = (count + group - 1) // group
    expected_billable = sum(((len(r) + UNIT - 1) // UNIT) * UNIT
                            for r in records(run_id, count, group))
    stats = {"api_calls": 0, "accepted_records": 0, "accepted_bytes": 0,
             "estimated_billable_bytes": 0, "partial_failure_records": 0}
    manifest = {"run_id": run_id, "scenario": args.scenario, "kind": args.kind,
                "region": args.region, "source_lines": count, "line_bytes": LINE_BYTES,
                "expected_records": expected_records,
                "expected_billable_bytes": expected_billable,
                "price_usd_per_gb": price,
                "estimated_usd": expected_billable / 2**30 * price,
                "started_utc": datetime.now(timezone.utc).isoformat(),
                "dry_run": args.dry_run, "stats": stats, "status": "planned"}
    print(json.dumps(manifest, indent=2), flush=True)
    if args.dry_run:
        return
    import boto3
    from botocore.config import Config
    config = Config(retries={"mode": "standard", "total_max_attempts": 3})
    cf = boto3.client("cloudformation", region_name=args.region, config=config)
    outputs = cf.describe_stacks(StackName=args.stack)["Stacks"][0]["Outputs"]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in outputs}
    stream = outputs["StreamA" if args.scenario == "a" else "StreamB"]
    manifest.update(bucket=outputs["BucketName"], stream=stream)
    client = boto3.client("firehose", region_name=args.region, config=config)
    manifest_path = Path(f"result-{run_id}-{args.scenario}.json")
    started = time.monotonic()
    try:
        manifest["status"] = "running"
        if args.kind == "live":
            buffer = []
            for seq in range(60):
                time.sleep(max(0, started + seq - time.monotonic()))
                payload = line(run_id, seq)
                if args.scenario == "a":
                    stats["api_calls"] += 1
                    client.put_record(DeliveryStreamName=stream, Record={"Data": payload})
                    stats["accepted_records"] += 1
                    stats["accepted_bytes"] += len(payload)
                    stats["estimated_billable_bytes"] += UNIT
                else:
                    buffer.append(payload)
            time.sleep(max(0, started + 60 - time.monotonic()))
            if args.scenario == "b":
                payload = b"".join(buffer)
                stats["api_calls"] += 1
                client.put_record(DeliveryStreamName=stream, Record={"Data": payload})
                stats["accepted_records"] = 1
                stats["accepted_bytes"] = len(payload)
                stats["estimated_billable_bytes"] = ((len(payload) + UNIT - 1) // UNIT) * UNIT
        else:
            # Both bulk cases use PutRecordBatch; only the record boundaries differ.
            # 0.5 MiB/s target stays below Sydney's documented default 1 MiB/s.
            for batch in batches(records(run_id, count, group)):
                send_batch(client, stream, batch, stats)
                time.sleep(max(0, started + stats["accepted_bytes"] / (512 * 1024)
                               - time.monotonic()))
        manifest["status"] = "completed"
    except BaseException as exc:
        manifest["status"] = "interrupted_or_failed"
        manifest["error"] = type(exc).__name__ + ": " + str(exc)
        raise
    finally:
        manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
        manifest["elapsed_seconds"] = round(time.monotonic() - started, 3)
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"Saved {manifest_path}", flush=True)
        print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    main()
