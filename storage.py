"""Persistance locale de démonstration et lecture du cluster sans pilote Python."""

import json
import socket
import sqlite3
import subprocess
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen

from fraud_core import HISTORY_SECONDS, UTC, Generator, detect, iso, parse_time, validate_transaction

ROOT = Path(__file__).resolve().parent


def decode_cqlsh_json(line):
    """Annule l’échappement d’affichage de cqlsh, puis décode SELECT JSON.

    cqlsh 4.1 double chaque antislash dans format_value_text, y compris
    ceux qui protègent les guillemets du payload JSON imbriqué.
    """
    return json.loads(line.strip().replace("\\\\", "\\"))


class Store:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        with self.connect() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS transactions (id TEXT PRIMARY KEY, client TEXT NOT NULL, timestamp TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS transactions_time ON transactions(timestamp);
                CREATE INDEX IF NOT EXISTS transactions_client_time ON transactions(client, timestamp);
                CREATE TABLE IF NOT EXISTS alerts (id TEXT PRIMARY KEY, timestamp TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS alerts_time ON alerts(timestamp);
                CREATE TABLE IF NOT EXISTS reviews (id TEXT PRIMARY KEY, status TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS observed_alerts (id TEXT PRIMARY KEY);
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            with db:
                yield db
        finally:
            db.close()

    def ingest(self, raw):
        tx = validate_transaction(raw)
        event_time = parse_time(tx["timestamp"])
        with self.lock, self.connect() as db:
            if db.execute("SELECT 1 FROM transactions WHERE id=?", (tx["transaction_id"],)).fetchone():
                return []
            rows = db.execute("SELECT payload FROM transactions WHERE client=? AND timestamp BETWEEN ? AND ?",
                              (tx["card_holder_id"], iso(event_time - timedelta(seconds=HISTORY_SECONDS)),
                               iso(event_time + timedelta(seconds=HISTORY_SECONDS)))).fetchall()
            alerts = detect(tx, [json.loads(r[0]) for r in rows])
            db.execute("INSERT INTO transactions VALUES (?,?,?,?)", (tx["transaction_id"], tx["card_holder_id"], tx["timestamp"], json.dumps(tx, ensure_ascii=False)))
            for alert in alerts:
                db.execute("INSERT INTO alerts VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET timestamp=excluded.timestamp, payload=excluded.payload",
                           (alert["alert_id"], alert["timestamp"], json.dumps(alert, ensure_ascii=False)))
            return alerts

    def rows(self, table, since, until):
        if table not in {"transactions", "alerts"}:
            raise ValueError("Table inconnue.")
        with self.connect() as db:
            rows = db.execute(f"SELECT payload FROM {table} WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp DESC LIMIT 10001", (since, until)).fetchall()
        return [json.loads(r[0]) for r in rows]

    def reviews(self):
        with self.connect() as db:
            return dict(db.execute("SELECT id,status FROM reviews"))

    def remember_alerts(self, alerts):
        with self.connect() as db:
            db.executemany("INSERT OR IGNORE INTO observed_alerts VALUES (?)", [(a["alert_id"],) for a in alerts])

    def has_alert(self, identity):
        with self.connect() as db:
            return bool(db.execute("SELECT 1 FROM alerts WHERE id=? UNION ALL SELECT 1 FROM observed_alerts WHERE id=? LIMIT 1", (identity, identity)).fetchone())

    def review(self, identity, status):
        if status not in {"nouvelle", "en_cours", "confirmee", "ecartee"}:
            raise ValueError("Statut inconnu.")
        with self.connect() as db:
            db.execute("INSERT INTO reviews VALUES (?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status", (identity, status))

    def seed(self):
        now = datetime.now(UTC)
        with self.connect() as db:
            latest = db.execute("SELECT MAX(timestamp) FROM transactions").fetchone()[0]
            if latest and parse_time(latest) >= now - timedelta(minutes=15):
                return
        generator = Generator(42)
        # Ajouter un historique récent sans effacer les données ni le suivi existants.
        ages = sorted((generator.random.uniform(0, 3500) for _ in range(360)), reverse=True)
        for i in range(360):
            when = now - timedelta(seconds=ages[i])
            tx = generator.transaction(when)
            if i % 47 == 0:
                tx.update(amount=round(generator.random.uniform(11000, 24500), 2), merchant_type="Bijouterie")
            self.ingest(tx)
            if i in (100, 190, 300):
                for event in generator.scenario("voyage", when):
                    self.ingest(event)
            if i in (140, 250, 330):
                for event in generator.scenario("frequence", when):
                    self.ingest(event)


class DemoSource:
    mode = "demo"

    def __init__(self, store):
        self.store = store
        self.generator = Generator()
        self.paused = threading.Event()
        self.stop = threading.Event()
        self.thread = None
        self.interval = 3

    def start(self):
        self.store.seed()
        self.thread = threading.Thread(target=self.run, name="simulation", daemon=True)
        self.thread.start()

    def run(self):
        count = 0
        while not self.stop.wait(self.interval):
            if not self.paused.is_set():
                count += 1
                tx = self.generator.transaction()
                if count % 23 == 0:
                    tx["amount"] = 14500
                self.store.ingest(tx)

    def read(self, since, until):
        return self.store.rows("transactions", since, until), self.store.rows("alerts", since, until)

    def health(self):
        return [{"name": name, "state": "simulation", "detail": detail} for name, detail in
                [("Kafka", "Flux local simulé"), ("Spark", "Règles exécutées en Python"), ("Cassandra", "Stockage local SQLite")]]

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=5)


class ClusterSource:
    """cqlsh est fourni par le conteneur Cassandra ; aucun paquet pip n’est requis."""
    mode = "cluster"

    def __init__(self, store):
        self.store = store
        self.paused = threading.Event()

    def query(self, table, day):
        # table et date ne proviennent jamais d’une saisie utilisateur.
        if table not in {"transactions_by_day", "alerts_by_day"}:
            raise ValueError("Table inconnue.")
        query = f"SELECT JSON * FROM fraud_monitoring.{table} WHERE event_date = '{day.isoformat()}' LIMIT 10001;"
        result = subprocess.run(["docker", "compose", "exec", "-T", "cassandra", "cqlsh", "-e", query],
                                cwd=ROOT, text=True, encoding="utf-8", capture_output=True, timeout=20, check=True)
        rows = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("{"):
                record = decode_cqlsh_json(line)
                # Les vues CQL conservent le contrat JSON d’origine.
                rows.append(json.loads(record["payload"]))
        return rows

    def read(self, since, until):
        first, last = parse_time(since).date(), parse_time(until).date()
        transactions, alerts = [], []
        self.truncated = False
        try:
            day = first
            while day <= last:
                day_transactions = self.query("transactions_by_day", day)
                day_alerts = self.query("alerts_by_day", day)
                # Conserver le signal de limite avant le filtrage de la période.
                self.truncated |= len(day_transactions) >= 10001 or len(day_alerts) >= 10001
                transactions.extend(day_transactions)
                alerts.extend(day_alerts)
                day += timedelta(days=1)
        except (OSError, subprocess.SubprocessError, ValueError, KeyError) as exc:
            raise RuntimeError("Lecture Cassandra indisponible. Vérifiez Docker, le schéma et le job Spark.") from exc
        return ([r for r in transactions if since <= r["timestamp"] <= until],
                [r for r in alerts if since <= r["timestamp"] <= until])

    def cassandra_version(self):
        """Requête CQL réelle : un port ouvert ne garantit pas que Cassandra réponde."""
        result = subprocess.run(["docker", "compose", "exec", "-T", "cassandra", "cqlsh", "-e",
                                 "SELECT JSON release_version FROM system.local;"],
                                cwd=ROOT, text=True, encoding="utf-8", capture_output=True, timeout=20, check=True)
        for line in result.stdout.splitlines():
            if line.strip().startswith("{"):
                return decode_cqlsh_json(line)["release_version"]
        raise ValueError("Réponse CQL inattendue.")

    def health(self):
        result = []
        for name, port in (("Kafka", 9092), ("Cassandra", 9042)):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.6):
                    pass
                state, detail = "joignable", f"Port {port} accessible ; disponibilité applicative non vérifiée"
            except OSError:
                state, detail = "indisponible", f"Port {port} inaccessible"
            if name == "Cassandra" and state == "joignable":
                try:
                    state, detail = "operationnel", f"Requête CQL réussie · Cassandra {self.cassandra_version()}"
                except (OSError, subprocess.SubprocessError, ValueError, KeyError):
                    state, detail = "indisponible", f"Port {port} ouvert, mais la requête CQL a échoué"
            result.append({"name": name, "state": state, "detail": detail})
        try:
            with urlopen("http://127.0.0.1:8080/json/", timeout=1) as response:
                master = json.load(response)
            active = len(master.get("activeapps", []))
            state, detail = ("actif", f"{active} application(s) enregistrée(s)") if active else ("en_attente", "Master joignable, aucune application active")
        except (OSError, ValueError):
            state, detail = "indisponible", "Master Spark inaccessible"
        result.insert(1, {"name": "Spark", "state": state, "detail": detail})
        return result

    def close(self):
        pass
