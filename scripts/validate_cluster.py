#!/usr/bin/env python3
"""Recette réelle Kafka/Spark/Cassandra. Aucune suppression de données ou de checkpoint."""
import argparse
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from fraud_core import detect, iso, parse_time, validate_transaction
from storage import decode_cqlsh_json

UTC = timezone.utc
BASE = ROOT / ".runtime" / "cluster-validation"


def docker(*args, input=None, timeout=60, check=True):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, input=input,
                          text=True, encoding="utf-8", capture_output=True,
                          timeout=timeout, check=check)


def cql(query):
    output = docker("exec", "-T", "cassandra", "cqlsh", "-e", query).stdout
    return [decode_cqlsh_json(line) for line in output.splitlines() if line.strip().startswith("{")]


def publish(events):
    content = "\n".join(str(event.get("card_holder_id", "invalide")) + "|" +
                        json.dumps(event, ensure_ascii=False) for event in events) + "\n"
    docker("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
           "--bootstrap-server", "kafka:19092", "--topic", "financial-transactions",
           "--property", "parse.key=true", "--property", "key.separator=|",
           "--producer-property", "acks=all", "--producer-property", "enable.idempotence=true",
           input=content, timeout=90)


def expected(events):
    history, alerts = [], {}
    for raw in events:
        tx = validate_transaction(raw)
        if any(old["transaction_id"] == tx["transaction_id"] for old in history):
            continue
        for alert in detect(tx, history):
            alerts[alert["alert_id"]] = alert
        history.append(tx)
    return history, alerts


def load(run):
    folder = Path(run) if run else Path((BASE / "latest.txt").read_text(encoding="utf-8"))
    return folder, json.loads((folder / "manifest.json").read_text(encoding="utf-8"))


def save(folder, manifest):
    (folder / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def start():
    prefix = "QA" + uuid.uuid4().hex[:10].upper()
    folder = BASE / prefix
    folder.mkdir(parents=True)
    anchor = datetime.fromtimestamp(int(datetime.now(UTC).timestamp()) // 120 * 120, UTC) - timedelta(minutes=4)
    def tx(name, client, seconds=30, **fields):
        return {"transaction_id": prefix + "-" + name, "card_holder_id": prefix + "-" + client,
                "amount": 125.25, "location": "Casablanca", "merchant_type": "Électronique",
                "timestamp": iso(anchor + timedelta(seconds=seconds)), **fields}
    cases = [
        tx("amount-equal", "amount-equal", amount=10000),
        tx("amount-above", "amount-above", amount=10000.01),
        tx("travel120-b", "travel120", 120, location="Tanger"),
        tx("travel120-a", "travel120", 0),
        tx("travel-over-a", "travel-over", 0),
        tx("travel-over-b", "travel-over", 120.001, location="Tanger"),
        tx("same-a", "same", 10), tx("same-b", "same", 50),
        tx("simultaneous-b", "simultaneous", 30, location="Tanger"),
        tx("simultaneous-a", "simultaneous", 30),
        tx("independent-a", "client-a", 10), tx("independent-b", "client-b", 20, location="Tanger"),
        tx("frequency-c", "frequency", 50), tx("frequency-a", "frequency", 10),
        tx("frequency-b", "frequency", 20),
        tx("boundary-a", "boundary", 118), tx("boundary-b", "boundary", 119),
        tx("boundary-c", "boundary", 120),
        tx("accent", "accent", location="Fès"),
    ]
    invalid = [
        tx("bad-city", "invalid", location="Inconnue"),
        tx("bad-amount", "invalid", amount=-1),
        tx("bad-decimal", "invalid", amount=12.345),
        tx("bad-bool", "invalid", amount=True),
        tx("bad-time", "invalid", timestamp="2026-11-17T10:00:00"),
        tx("bad-future", "invalid", timestamp=iso(datetime.now(UTC) + timedelta(hours=1))),
        tx("bad-merchant", "invalid", merchant_type=" "),
    ]
    missing = tx("bad-missing", "invalid")
    del missing["location"]
    invalid.append(missing)
    manifest = {"prefix": prefix, "events": cases, "invalid": invalid,
                "update": tx("frequency-d", "frequency", 55),
                "created_at": iso(datetime.now(UTC)), "checks": []}
    save(folder, manifest)
    BASE.mkdir(exist_ok=True)
    (BASE / "latest.txt").write_text(str(folder), encoding="utf-8")
    begin = time.monotonic()
    publish(cases + invalid + [cases[2], cases[12]])
    manifest["published_at"] = iso(datetime.now(UTC))
    manifest["producer_seconds"] = round(time.monotonic() - begin, 3)
    save(folder, manifest)
    print(json.dumps({"run": str(folder), "valid": len(cases), "invalid": len(invalid), "duplicates": 2}, ensure_ascii=False), flush=True)


def read_data(manifest):
    events, wanted = expected(manifest["events"])
    days = sorted({parse_time(event["timestamp"]).date().isoformat() for event in events})
    queries = []
    for day in days:
        queries.extend([f"SELECT JSON * FROM fraud_monitoring.{table} WHERE event_date='{day}';"
                        for table in ("alerts_by_day", "transactions_by_day")])
    rows = cql(" ".join(queries))
    prefix = manifest["prefix"]
    payloads = [json.loads(row["payload"]) for row in rows]
    actual_tx = {r["transaction_id"]: r for r in payloads if "alert_id" not in r and r["transaction_id"].startswith(prefix)}
    actual_alerts = {r["alert_id"]: r for r in payloads if "alert_id" in r and r["card_holder_id"].startswith(prefix)}
    return events, wanted, actual_tx, actual_alerts


def comparable(alert):
    return {key: alert.get(key) for key in ("alert_id", "transaction_id", "card_holder_id", "rule", "severity",
            "timestamp", "amount", "location", "transaction_count", "related_transaction_ids", "detail")}


def check(folder, manifest, timeout):
    started = time.monotonic()
    while True:
        events, wanted, actual_tx, actual = read_data(manifest)
        missing_tx = {r["transaction_id"] for r in events} - actual_tx.keys()
        extra_tx = actual_tx.keys() - {r["transaction_id"] for r in events}
        mismatch = {key for key in wanted if key not in actual or comparable(actual[key]) != comparable(wanted[key])}
        unexpected = actual.keys() - wanted.keys()
        print(json.dumps({"transactions": len(actual_tx), "expected_transactions": len(events),
                          "alerts": len(actual), "expected_alerts": len(wanted),
                          "missing_transactions": len(missing_tx), "mismatched_alerts": len(mismatch),
                          "unexpected_transactions": list(extra_tx),
                          "unexpected_alerts": list(unexpected)}, ensure_ascii=False), flush=True)
        if not missing_tx and not extra_tx and not mismatch and not unexpected:
            break
        if time.monotonic() - started > timeout:
            (folder / "failure.json").write_text(json.dumps({"expected": wanted, "actual": actual}, ensure_ascii=False, indent=2), encoding="utf-8")
            raise AssertionError("La recette Cassandra ne correspond pas aux règles attendues.")
        time.sleep(10)
    # Vérifier aussi la table principale et ses clés stables.
    queries = []
    for day, client in sorted({(parse_time(a["timestamp"]).date().isoformat(), a["card_holder_id"]) for a in wanted.values()}):
        queries.append(f"SELECT JSON * FROM fraud_monitoring.detected_alerts WHERE alert_date='{day}' AND card_holder_id='{client}';")
    primary = cql(" ".join(queries)) if queries else []
    assert len(primary) == len(wanted), "Doublons ou lignes manquantes dans detected_alerts"
    assert {row["alert_id"] for row in primary} == set(wanted)
    for row in primary:
        assert comparable(json.loads(row["payload"])) == comparable(wanted[row["alert_id"]])
    # Vérifier les deux sorties Kafka : les versions successives d'une alerte sont autorisées.
    outputs = {}
    for topic in ("fraud-alerts", "invalid-transactions"):
        result = docker("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                        "--bootstrap-server", "kafka:19092", "--topic", topic,
                        "--from-beginning", "--timeout-ms", "6000", timeout=40, check=False)
        records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
        outputs[topic] = records
        (folder / (topic + ".json")).write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    kafka_alerts = {row["alert_id"]: row for row in outputs["fraud-alerts"] if row["card_holder_id"].startswith(manifest["prefix"])}
    assert set(kafka_alerts) == set(wanted), "Les identités Kafka et Cassandra diffèrent"
    for key in wanted:
        assert comparable(kafka_alerts[key]) == comparable(wanted[key]), "Payload Kafka différent"
    rejected = {json.loads(row["raw"])["transaction_id"] for row in outputs["invalid-transactions"] if manifest["prefix"] in row["raw"]}
    assert rejected == {row["transaction_id"] for row in manifest["invalid"]}, "Rejets invalides manquants ou incorrects"
    record = {"at": iso(datetime.now(UTC)), "transactions": len(actual_tx), "alerts": len(actual),
              "invalid": len(rejected), "verification_seconds": round(time.monotonic()-started, 3),
              "phase": manifest.get("phase", "initial"), "result": "PASS"}
    manifest["checks"].append(record)
    save(folder, manifest)
    print(json.dumps(record, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "check", "update", "replay", "probe"))
    parser.add_argument("--run")
    parser.add_argument("--timeout", type=int, default=240)
    args = parser.parse_args()
    if args.action == "start":
        start()
        return
    folder, manifest = load(args.run)
    if args.action == "check":
        check(folder, manifest, args.timeout)
    elif args.action == "update":
        if manifest["update"] not in manifest["events"]:
            manifest["events"].append(manifest["update"])
        manifest["phase"] = "frequency-update"
        save(folder, manifest)
        publish([manifest["update"]])
        print("Mise à jour de fréquence publiée.", flush=True)
    elif args.action == "replay":
        manifest["phase"] = "replay"
        save(folder, manifest)
        publish(manifest["events"])
        print("Messages rejoués avec les mêmes identifiants.", flush=True)
    elif args.action == "probe":
        name = "probe-" + uuid.uuid4().hex[:8]
        tx = {"transaction_id": manifest["prefix"] + "-" + name, "card_holder_id": manifest["prefix"] + "-" + name,
              "amount": 14500, "location": "Fès", "merchant_type": "Bijouterie", "timestamp": iso(datetime.now(UTC))}
        manifest["events"].append(tx)
        manifest["phase"] = "recovery"
        save(folder, manifest)
        publish([tx])
        print("Transaction de reprise publiée.", flush=True)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()

