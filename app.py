#!/usr/bin/env python3
"""Console de supervision française, uniquement avec la bibliothèque standard."""

import argparse
import ipaddress
import json
import logging
import math
import re
import sys
import threading
from collections import Counter
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse, urlsplit

from fraud_core import CITIES, RULES, UTC, iso, parse_time
from storage import ClusterSource, DemoSource, Store

ROOT = Path(__file__).resolve().parent
PERIODS = {"15m": 900, "1h": 3600, "24h": 86400}
# Noms acceptés dans l’en-tête Host, en plus des adresses IP : protège contre le DNS rebinding.
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def snapshot(source, store, period):
    now = datetime.now(UTC)
    seconds = PERIODS[period]
    since = now - timedelta(seconds=seconds)
    transactions, alerts = source.read(iso(since), iso(now))
    truncated = getattr(source, "truncated", False) or len(transactions) > 10000 or len(alerts) > 10000
    transactions = sorted(transactions, key=lambda r: r["timestamp"], reverse=True)[:10000]
    alerts = sorted({a["alert_id"]: a for a in alerts}.values(), key=lambda r: r["timestamp"], reverse=True)[:10000]
    store.remember_alerts(alerts)
    reviews = store.reviews()
    alerts = [{**alert, "status": reviews.get(alert["alert_id"], "nouvelle")} for alert in alerts]
    suspicious_ids = {identity for a in alerts for identity in a.get("related_transaction_ids", [a["transaction_id"]])}
    pending_transactions = len(suspicious_ids - {t["transaction_id"] for t in transactions})
    flagged = [t for t in transactions if t["transaction_id"] in suspicious_ids]
    volume = sum(round(t["amount"] * 100) for t in transactions) / 100
    buckets = 30
    step = seconds / buckets
    series = [{"timestamp": iso(since + timedelta(seconds=i * step)), "transactions": 0, "alerts": 0} for i in range(buckets)]
    for rows, key in ((transactions, "transactions"), (alerts, "alerts")):
        for row in rows:
            index = min(buckets - 1, max(0, math.floor((parse_time(row["timestamp"]) - since).total_seconds() / step)))
            series[index][key] += 1
    city_counts = Counter(t["location"] for t in transactions)
    city_alerts = Counter(a["location"] for a in alerts)
    distribution = Counter(a["rule"] for a in alerts)
    return {
        "mode": source.mode, "period": period, "generated_at": iso(now), "paused": source.paused.is_set(),
        "interval": getattr(source, "interval", None),
        "truncated": truncated, "summary": {
            "legacy_alerts": sum(a["rule"] != "MONTANT_ELEVE" and "related_transaction_ids" not in a for a in alerts),
            "transactions": len(transactions), "alerts": len(alerts),
            "open_alerts": sum(a["status"] in {"nouvelle", "en_cours"} for a in alerts),
            "critical_alerts": sum(a["severity"] == "critique" for a in alerts),
            "flagged_transactions": len(flagged),
            "pending_transactions": pending_transactions,
            "risk_rate": round(len(flagged) / len(transactions) * 100, 2) if transactions else 0,
            "volume": volume, "exposure": sum(round(t["amount"] * 100) for t in flagged) / 100,
            "clients": len({t["card_holder_id"] for t in transactions}),
        },
        "series": series, "transactions": transactions, "alerts": alerts,
        "distribution": [{"rule": key, "label": rule["label"], "count": distribution[key]} for key, rule in RULES.items()],
        "cities": [{"name": name, "latitude": point[0], "longitude": point[1], "count": city_counts[name], "alerts": city_alerts[name]} for name, point in CITIES.items()],
    }


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, source, store):
        super().__init__(address, Handler)
        self.source, self.store = source, store
        self.requested_host = str(address[0]).lower()
        self.cache = {}
        self.cache_lock = threading.RLock()

    def get_snapshot(self, period):
        with self.cache_lock:
            cached = self.cache.get(period)
            ttl = 8 if self.source.mode == "cluster" else 1
            if cached and (datetime.now(UTC) - cached[0]).total_seconds() < ttl:
                return cached[1]
            value = snapshot(self.source, self.store, period)
            self.cache[period] = (datetime.now(UTC), value)
            return value


class Handler(BaseHTTPRequestHandler):
    server_version = "Vigie/1.0"

    def log_message(self, fmt, *args):
        if args and str(args[1] if len(args) > 1 else "") not in {"200", "304"}:
            logging.info(fmt, *args)

    def host_allowed(self):
        name = urlsplit(f"//{self.headers.get('Host', '')}").hostname
        if not name:
            return False
        if name in LOCAL_HOSTS or name.endswith(".localhost") or name == self.server.requested_host:
            return True
        try:
            ipaddress.ip_address(name)
            return True
        except ValueError:
            return False

    def reject_host(self):
        return self.respond(421, {"error": "Hôte non autorisé : ouvrez la console avec 127.0.0.1, localhost ou une adresse IP."})

    def respond(self, status, body, content_type="application/json; charset=utf-8"):
        if not isinstance(body, bytes):
            body = json.dumps(body, ensure_ascii=False, allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "same-origin")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; font-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
        self.end_headers()
        if self.command == "HEAD":
            return
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        if not self.host_allowed():
            return self.reject_host()
        url = urlparse(self.path)
        if url.path == "/api/snapshot":
            period = parse_qs(url.query).get("period", ["1h"])[0]
            if period not in PERIODS:
                return self.respond(400, {"error": "Période inconnue."})
            try:
                return self.respond(200, self.server.get_snapshot(period))
            except RuntimeError as exc:
                return self.respond(503, {"error": str(exc), "mode": self.server.source.mode})
        if url.path == "/api/health":
            return self.respond(200, {"mode": self.server.source.mode, "services": self.server.source.health()})
        assets = {"/": ("index.html", "text/html; charset=utf-8"), "/styles.css": ("styles.css", "text/css; charset=utf-8"),
                  "/app.js": ("app.js", "text/javascript; charset=utf-8"), "/favicon.svg": ("favicon.svg", "image/svg+xml")}
        if url.path not in assets:
            return self.respond(404, {"error": "Ressource introuvable."})
        name, mime = assets[url.path]
        self.respond(200, (ROOT / "web" / name).read_bytes(), mime)

    def do_POST(self):
        if not self.host_allowed():
            return self.reject_host()
        origin = self.headers.get("Origin")
        if (origin and origin != f"http://{self.headers.get('Host')}") or self.headers.get("Sec-Fetch-Site") == "cross-site":
            return self.respond(403, {"error": "Origine non autorisée."})
        if self.headers.get_content_type() != "application/json":
            return self.respond(415, {"error": "Un corps JSON est requis."})
        try:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                return self.respond(400, {"error": "En-tête Content-Length invalide."})
            if not 0 < length <= 16384:
                return self.respond(413, {"error": "Requête vide ou trop volumineuse."})
            try:
                body = json.loads(self.rfile.read(length))
            except UnicodeDecodeError:
                return self.respond(400, {"error": "Le corps de la requête doit être encodé en UTF-8."})
            except json.JSONDecodeError as exc:
                return self.respond(400, {"error": f"JSON invalide (ligne {exc.lineno}, colonne {exc.colno})."})
            if not isinstance(body, dict):
                raise ValueError("Un objet JSON est requis.")
            path = urlparse(self.path).path
            if path == "/api/control":
                if self.server.source.mode != "demo":
                    return self.respond(409, {"error": "La simulation est réservée au mode démonstration."})
                if body.get("action") == "pause":
                    self.server.source.paused.set()
                elif body.get("action") == "resume":
                    self.server.source.paused.clear()
                elif body.get("action") == "speed":
                    interval = body.get("interval")
                    if isinstance(interval, bool) or interval not in (1, 3, 5):
                        raise ValueError("Choisissez un intervalle de 1, 3 ou 5 secondes.")
                    self.server.source.interval = interval
                else:
                    raise ValueError("Action inconnue.")
                result = {"paused": self.server.source.paused.is_set(), "interval": self.server.source.interval}
            elif path == "/api/scenario":
                if self.server.source.mode != "demo":
                    return self.respond(409, {"error": "Utilisez producer.py pour alimenter Kafka en mode cluster."})
                events = self.server.source.generator.scenario(body.get("scenario"), options=body.get("options"))
                alerts = [a for tx in events for a in self.server.store.ingest(tx)]
                alerts = list({a["alert_id"]: a for a in alerts}.values())
                result = {"transactions": len(events), "alerts": len(alerts), "events": events,
                          "detected_alerts": alerts, "card_holder_id": events[0]["card_holder_id"],
                          "scenario": body["scenario"], "executed_at": iso(datetime.now(UTC))}
            elif re.fullmatch(r"/api/alerts/[a-f0-9]{64}/status", path):
                identity = path.split("/")[3]
                if not self.server.store.has_alert(identity):
                    return self.respond(404, {"error": "Alerte inconnue de la console."})
                self.server.store.review(identity, body.get("status"))
                result = {"status": body["status"]}
            else:
                return self.respond(404, {"error": "Ressource introuvable."})
            with self.server.cache_lock:
                self.server.cache.clear()
            self.respond(200, result)
        except ValueError as exc:
            self.respond(400, {"error": str(exc)})
        except (TypeError, KeyError):
            self.respond(400, {"error": "Paramètres de requête invalides."})


def main():
    parser = argparse.ArgumentParser(description="Vigie — console de surveillance des transactions financières")
    parser.add_argument("--mode", choices=("demo", "cluster"), default="demo", help="demo : Python seul ; cluster : lecture de Cassandra via Docker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--database", type=Path, default=ROOT / ".runtime" / "vigie.sqlite3")
    args = parser.parse_args()
    # Messages français lisibles dans la console Windows et dans les journaux redirigés.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    store = Store(args.database)
    source = DemoSource(store) if args.mode == "demo" else ClusterSource(store)
    if args.mode == "demo":
        source.start()
    server = Server((args.host, args.port), source, store)
    print(f"Vigie · {args.mode} · http://{args.host}:{server.server_port}", flush=True)
    print("Ctrl+C pour arrêter.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        source.close()


if __name__ == "__main__":
    main()
