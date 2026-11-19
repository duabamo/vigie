import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import fraud_core
from app import Server, snapshot
from fraud_core import CITIES, UTC, Generator, alert_id, detect, distance_km, iso, parse_time, validate_transaction
from storage import ClusterSource, DemoSource, Store

ROOT = Path(__file__).resolve().parents[1]


class RuleTests(unittest.TestCase):
    def setUp(self):
        self.time = datetime(2026, 10, 6, 8, 54, 30, tzinfo=UTC)
        self.generator = Generator(42)

    def tx(self, **overrides):
        return self.generator.transaction(self.time, card_holder_id="USR-401", **overrides)

    def test_exact_amount_boundary(self):
        self.assertEqual(detect(self.tx(amount=10000), []), [])
        result = detect(self.tx(amount=10000.01), [])
        self.assertEqual([a["rule"] for a in result], ["MONTANT_ELEVE"])

    def test_validation_rejects_bad_amounts(self):
        for value in (True, None, "NaN", "Infinity", -1, 0, 12.345, "1000000000"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_transaction(self.tx(amount=value))

    def test_validation_requires_contract(self):
        tx = self.tx()
        for key in tuple(tx):
            bad = dict(tx)
            del bad[key]
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_transaction(bad)

    def test_invalid_identifiers_and_city(self):
        for override in ({"transaction_id": "<script>"}, {"card_holder_id": "' OR 1=1"}, {"location": "Inconnue"}, {"merchant_type": " "}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                validate_transaction({**self.tx(), **override})

    def test_timezone_required_and_normalized(self):
        with self.assertRaises(ValueError):
            parse_time("2026-10-06T09:00:00")
        self.assertEqual(iso(parse_time("2026-10-06T09:00:00+01:00")), "2026-10-06T08:00:00.000Z")

    def test_travel_symmetric_under_arrival_order(self):
        a, b = self.generator.scenario("voyage", self.time)
        forward = [x for x in detect(b, [a]) if x["rule"] == "VOYAGE_IMPOSSIBLE"]
        reverse = [x for x in detect(a, [b]) if x["rule"] == "VOYAGE_IMPOSSIBLE"]
        self.assertEqual(len(forward), 1)
        self.assertEqual(forward, reverse)
        self.assertGreater(forward[0]["distance_km"], 200)

    def test_travel_two_minute_boundary(self):
        a, b = self.generator.scenario("voyage", self.time)
        a["timestamp"] = iso(self.time - timedelta(seconds=120))
        self.assertTrue(any(x["rule"] == "VOYAGE_IMPOSSIBLE" for x in detect(b, [a])))
        a["timestamp"] = iso(self.time - timedelta(seconds=120.001))
        self.assertEqual(detect(b, [a]), [])

    def test_same_location_does_not_trigger_travel(self):
        a, b = self.generator.scenario("voyage", self.time)
        b["location"] = a["location"]
        self.assertEqual(detect(b, [a]), [])

    def test_same_timestamp_different_cities(self):
        a, b = self.generator.scenario("voyage", self.time)
        a["timestamp"] = b["timestamp"]
        self.assertEqual(len(detect(b, [a])), 1)
        self.assertTrue(detect(b, [a])[0]["speed_kmh"] > 900)

    def test_different_clients_are_independent(self):
        a, b = self.generator.scenario("voyage", self.time)
        a["card_holder_id"] = "USR-OTHER"
        self.assertEqual(detect(b, [a]), [])

    def test_three_distinct_transactions_trigger_frequency(self):
        a, b, c = self.generator.scenario("frequence", self.time)
        self.assertEqual(detect(b, [a]), [])
        result = detect(c, [a, b])
        self.assertEqual([a["rule"] for a in result], ["FREQUENCE_ELEVEE"])
        self.assertEqual(result[0]["transaction_count"], 3)

    def test_frequency_does_not_cross_fixed_window(self):
        a, b, c = self.generator.scenario("frequence", self.time)
        c["timestamp"] = iso(parse_time(a["timestamp"]) + timedelta(seconds=120))
        self.assertEqual(detect(c, [a, b]), [])

    def test_distance_sanity(self):
        self.assertEqual(distance_km("Rabat", "Rabat"), 0)
        self.assertAlmostEqual(distance_km("Rabat", "Casablanca"), distance_km("Casablanca", "Rabat"))
        self.assertTrue(80 < distance_km("Rabat", "Casablanca") < 100)

    def test_alert_identity_is_stable(self):
        tx = self.tx(amount=14500)
        self.assertEqual(detect(tx, [])[0]["alert_id"], alert_id("MONTANT_ELEVE", tx["transaction_id"]))

    def test_frequency_scenario_has_no_future_event_at_window_boundary(self):
        boundary = datetime(2026, 10, 6, 8, 54, tzinfo=UTC)
        events = self.generator.scenario("frequence", boundary, {"count": 8})
        self.assertTrue(all(parse_time(tx["timestamp"]) <= boundary for tx in events))
        self.assertEqual(len({int(parse_time(tx["timestamp"]).timestamp()) // 120 for tx in events}), 1)
        self.assertEqual(len({tx["transaction_id"] for tx in events}), 8)

    def test_custom_travel_above_interval_limit_does_not_trigger(self):
        a, b = self.generator.scenario("voyage", self.time, {"elapsed_seconds": 121})
        self.assertEqual(detect(b, [a]), [])

    def test_custom_amount_keeps_strict_threshold(self):
        events = self.generator.scenario("montant", self.time, {"amount": 10000})
        self.assertEqual(detect(events[0], []), [])

    def test_scenario_rejects_invalid_options(self):
        cases = [("voyage", {"elapsed_seconds": True}), ("voyage", {"elapsed_seconds": float("nan")}),
                 ("voyage", {"from_city": "Inconnue"}), ("frequence", {"count": 2.5}),
                 ("frequence", {"count": 9}), ("montant", {"amount": -1}),
                 ("normale", {"amount": 14500}), ("montant", [])]
        for scenario, options in cases:
            with self.subTest(scenario=scenario, options=options), self.assertRaises(ValueError):
                self.generator.scenario(scenario, self.time, options)

    def test_scenario_clients_are_independent(self):
        first = self.generator.scenario("frequence", self.time, {"count": 2})
        second = self.generator.scenario("normale", self.time)
        self.assertNotEqual(first[0]["card_holder_id"], second[0]["card_holder_id"])
        self.assertEqual(detect(second[0], first), [])

    def test_scenario_rejects_non_text_name(self):
        for name in ({}, [], None):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "Scénario"):
                self.generator.scenario(name, self.time)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / "test.sqlite3")
        self.generator = Generator(42)
        self.now = datetime.now(UTC) - timedelta(seconds=3)

    def rows(self, table):
        return self.store.rows(table, iso(self.now - timedelta(hours=1)), iso(self.now + timedelta(minutes=1)))

    def test_duplicate_transaction_is_not_counted(self):
        tx = self.generator.transaction(self.now, amount=14500)
        self.store.ingest(tx)
        self.assertEqual(self.store.ingest(tx), [])
        self.assertEqual(len(self.rows("transactions")), 1)
        self.assertEqual(len(self.rows("alerts")), 1)

    def test_alert_upsert_and_review_survive_reopen(self):
        a, b, c = self.generator.scenario("frequence", self.now)
        for tx in (a, b, c):
            self.store.ingest(tx)
        alert = self.rows("alerts")[0]
        self.store.review(alert["alert_id"], "confirmee")
        later = {**c, "transaction_id": "TXN-FOURTH", "timestamp": iso(parse_time(c["timestamp"]) + timedelta(seconds=.1))}
        self.store.ingest(later)
        reopened = Store(self.store.path)
        self.assertEqual(reopened.reviews()[alert["alert_id"]], "confirmee")
        self.assertEqual(len(self.rows("alerts")), 1)
        self.assertEqual(self.rows("alerts")[0]["transaction_count"], 4)

    def test_observed_cluster_alert_is_durable(self):
        alert = detect(self.generator.transaction(self.now, amount=14500), [])[0]
        self.store.remember_alerts([alert])
        self.assertTrue(Store(self.store.path).has_alert(alert["alert_id"]))
        self.assertFalse(self.store.has_alert("f" * 64))

    def test_out_of_order_persistence_finds_travel(self):
        a, b = self.generator.scenario("voyage", self.now)
        self.store.ingest(b)
        self.store.ingest(a)
        self.assertEqual(len(self.rows("alerts")), 1)

    def test_invalid_transaction_leaves_database_unchanged(self):
        with self.assertRaises(ValueError):
            self.store.ingest(self.generator.transaction(self.now, amount=-1))
        self.assertEqual(self.rows("transactions"), [])

    def test_summary_does_not_double_count_multi_rule_transaction(self):
        a, b = self.generator.scenario("voyage", self.now)
        b["amount"] = 14500
        for tx in (a, b):
            self.store.ingest(tx)
        source = DemoSource(self.store)
        result = snapshot(source, self.store, "1h")
        self.assertEqual(result["summary"]["alerts"], 2)
        self.assertEqual(result["summary"]["flagged_transactions"], 2)
        self.assertEqual(result["summary"]["risk_rate"], 100)
        self.assertAlmostEqual(result["summary"]["exposure"], a["amount"] + b["amount"])

    def test_frequency_tracks_all_members_after_updates_and_replay(self):
        events = self.generator.scenario("frequence", self.now, {"count": 8})
        for tx in reversed(events):
            self.store.ingest(tx)
        self.store.ingest(events[0])
        result = snapshot(DemoSource(self.store), self.store, "1h")
        self.assertEqual(result["summary"]["alerts"], 1)
        self.assertEqual(result["summary"]["flagged_transactions"], 8)
        self.assertEqual(set(result["alerts"][0]["related_transaction_ids"]), {tx["transaction_id"] for tx in events})

    def test_historical_alerts_remain_readable_and_report_partial_evidence(self):
        a, b = self.generator.scenario("voyage", self.now)
        alert = detect(b, [a])[0]
        del alert["related_transaction_ids"]
        source = DemoSource(self.store)
        with patch.object(source, "read", return_value=([a, b], [alert])):
            result = snapshot(source, self.store, "1h")
        self.assertEqual(result["summary"]["legacy_alerts"], 1)
        self.assertEqual(result["summary"]["flagged_transactions"], 1)

    def test_cluster_limit_survives_period_filtering(self):
        source = ClusterSource(self.store)
        outside = self.generator.transaction(self.now - timedelta(hours=2))
        with patch.object(source, "query", side_effect=lambda table, day: [outside] * 10001 if table == "transactions_by_day" else []):
            result = snapshot(source, self.store, "1h")
        self.assertTrue(result["truncated"])
        self.assertEqual(result["transactions"], [])

    def test_snapshot_reports_missing_transactions(self):
        alert = detect(self.generator.transaction(self.now, amount=14500), [])[0]
        source = DemoSource(self.store)
        with patch.object(source, "read", return_value=([], [alert])):
            result = snapshot(source, self.store, "1h")
        self.assertEqual(result["summary"]["pending_transactions"], 1)

    def test_cluster_read_does_not_fall_back_to_demo(self):
        source = ClusterSource(self.store)
        with patch("storage.subprocess.run", side_effect=FileNotFoundError), self.assertRaisesRegex(RuntimeError, "Cassandra"):
            source.read(iso(self.now - timedelta(minutes=15)), iso(self.now))

    def test_cluster_reads_json_payload(self):
        source = ClusterSource(self.store)
        tx = self.generator.transaction(self.now)
        cql_json = json.dumps({"event_date": "2026-10-06", "payload": json.dumps(tx)})
        output = subprocess.CompletedProcess([], 0, " [json]\n---------\n " + cql_json + "\n\n(1 rows)\n", "")
        with patch("storage.subprocess.run", return_value=output):
            self.assertEqual(source.query("transactions_by_day", self.now.date()), [tx])

    def test_cluster_decodes_cqlsh_display_escaping(self):
        source = ClusterSource(self.store)
        tx = self.generator.transaction(self.now, location="Fès", merchant_type='Réseau\\A "B"\nÉtage')
        record = json.dumps({"event_date": "2026-11-18", "payload": json.dumps(tx, ensure_ascii=False)}, ensure_ascii=False)
        rendered = record.replace("\\", "\\\\")
        output = subprocess.CompletedProcess([], 0, " [json]\n-----\n " + rendered + "\n(1 rows)\n", "")
        with patch("storage.subprocess.run", return_value=output):
            self.assertEqual(source.query("transactions_by_day", self.now.date()), [tx])

    def test_seed_refreshes_old_history_without_losing_review(self):
        old = self.generator.transaction(self.now - timedelta(days=2), amount=14500)
        alert = self.store.ingest(old)[0]
        self.store.review(alert["alert_id"], "confirmee")
        self.store.seed()
        current = snapshot(DemoSource(self.store), self.store, "1h")
        self.assertGreaterEqual(current["summary"]["transactions"], 375)
        self.assertEqual({a["rule"] for a in current["alerts"]}, {"MONTANT_ELEVE", "VOYAGE_IMPOSSIBLE", "FREQUENCE_ELEVEE"})
        self.assertTrue(self.store.has_alert(alert["alert_id"]))
        self.assertEqual(self.store.reviews()[alert["alert_id"]], "confirmee")

    def test_seed_does_not_duplicate_recent_history(self):
        self.store.seed()
        before = len(self.rows("transactions"))
        self.store.seed()
        self.assertEqual(len(self.rows("transactions")), before)


class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.store = Store(Path(cls.temp.name) / "api.sqlite3")
        cls.source = DemoSource(cls.store)
        cls.server = Server(("127.0.0.1", 0), cls.source, cls.store)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=3)
        cls.temp.cleanup()

    def request(self, path, body=None, headers=None):
        payload = json.dumps(body).encode() if body is not None else None
        request = Request(self.base + path, data=payload, headers={"Content-Type": "application/json", **(headers or {})})
        try:
            response = urlopen(request, timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            raw = response.read()
            return response.status, response.headers, json.loads(raw) if response.headers.get_content_type() == "application/json" else raw

    def test_static_assets_and_security_headers(self):
        for path in ("/", "/styles.css", "/app.js", "/favicon.svg"):
            with self.subTest(path=path):
                code, headers, body = self.request(path)
                self.assertEqual(code, 200)
                self.assertTrue(body)
                self.assertEqual(headers["X-Content-Type-Options"], "nosniff")

    def test_path_traversal_rejected(self):
        self.assertEqual(self.request("/../schema.cql")[0], 404)

    def test_unknown_period_rejected(self):
        self.assertEqual(self.request("/api/snapshot?period=invalid")[0], 400)

    def raw_request(self, path, data=None, method=None, headers=None):
        request = Request(self.base + path, data=data, method=method, headers=headers or {})
        try:
            response = urlopen(request, timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            return response.status, response.headers, response.read()

    def test_invalid_json_reports_french_error(self):
        code, _, raw = self.raw_request("/api/control", b"{pause", headers={"Content-Type": "application/json"})
        self.assertEqual(code, 400)
        self.assertEqual(json.loads(raw)["error"], "JSON invalide (ligne 1, colonne 2).")
        code, _, raw = self.raw_request("/api/control", b'{"action": "\xff"}', headers={"Content-Type": "application/json"})
        self.assertEqual((code, json.loads(raw)["error"]), (400, "Le corps de la requête doit être encodé en UTF-8."))
        code, _, raw = self.raw_request("/api/alerts/" + "a" * 64 + "/status", b'{"status": []}', headers={"Content-Type": "application/json"})
        self.assertIn(code, (400, 404))
        self.assertNotIn("unhashable", json.loads(raw)["error"])

    def test_head_returns_headers_without_body(self):
        for path in ("/", "/api/health"):
            with self.subTest(path=path):
                code, headers, raw = self.raw_request(path, method="HEAD")
                self.assertEqual((code, raw), (200, b""))
                self.assertGreater(int(headers["Content-Length"]), 0)

    def test_unknown_host_header_rejected(self):
        port = self.server.server_port
        for host, expected in ((f"evil.example:{port}", 421), (f"localhost:{port}", 200), (f"127.0.0.1:{port}", 200),
                               (f"[::1]:{port}", 200), (f"192.168.1.20:{port}", 200)):
            with self.subTest(host=host):
                self.assertEqual(self.raw_request("/api/health", headers={"Host": host})[0], expected)
        self.assertEqual(self.raw_request("/api/control", b'{"action": "pause"}',
                                          headers={"Host": "evil.example", "Content-Type": "application/json"})[0], 421)

    def test_pause_and_resume(self):
        self.assertTrue(self.request("/api/control", {"action": "pause"})[2]["paused"])
        self.assertFalse(self.request("/api/control", {"action": "resume"})[2]["paused"])

    def test_cross_origin_mutation_rejected(self):
        self.assertEqual(self.request("/api/control", {"action": "pause"}, {"Origin": "https://example.com"})[0], 403)

    def test_invalid_scenario_is_french_error(self):
        code, _, body = self.request("/api/scenario", {"scenario": "invalid"})
        self.assertEqual(code, 400)
        self.assertIn("Scénario", body["error"])

    def test_two_status_updates_without_refresh(self):
        code, _, result = self.request("/api/scenario", {"scenario": "montant"})
        self.assertEqual(code, 200)
        self.assertEqual(result["alerts"], 1)
        data = self.request("/api/snapshot?period=1h")[2]
        identity = data["alerts"][0]["alert_id"]
        path = f"/api/alerts/{identity}/status"
        self.assertEqual(self.request(path, {"status": "en_cours"})[0], 200)
        self.assertEqual(self.request(path, {"status": "confirmee"})[0], 200)
        self.assertEqual(self.request("/api/snapshot?period=1h")[2]["alerts"][0]["status"], "confirmee")

    def test_unknown_alert_rejected(self):
        self.assertEqual(self.request(f"/api/alerts/{'f' * 64}/status", {"status": "confirmee"})[0], 404)

    def test_scenario_returns_traceable_unique_alerts(self):
        code, _, result = self.request("/api/scenario", {"scenario": "frequence", "options": {"count": 8}})
        self.assertEqual(code, 200)
        self.assertEqual(result["transactions"], 8)
        self.assertEqual(result["alerts"], 1)
        self.assertEqual(result["detected_alerts"][0]["transaction_count"], 8)
        self.assertTrue(all(tx["card_holder_id"] == result["card_holder_id"] for tx in result["events"]))

    def test_scenario_amount_under_threshold_returns_no_alert(self):
        code, _, result = self.request("/api/scenario", {"scenario": "montant", "options": {"amount": 10000}})
        self.assertEqual(code, 200)
        self.assertEqual(result["alerts"], 0)
        self.assertEqual(result["events"][0]["amount"], 10000)

    def test_invalid_scenario_does_not_write_partial_events(self):
        with self.store.connect() as db:
            before = db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        code, _, _ = self.request("/api/scenario", {"scenario": "voyage", "options": {"to_city": "Inconnue"}})
        self.assertEqual(code, 400)
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0], before)

    def test_demo_cadence_can_change_and_is_reported(self):
        code, _, result = self.request("/api/control", {"action": "speed", "interval": 1})
        self.assertEqual(code, 200)
        self.assertEqual(result["interval"], 1)
        self.assertEqual(self.request("/api/snapshot")[2]["interval"], 1)
        self.request("/api/control", {"action": "speed", "interval": 3})

    def test_demo_rejects_unknown_cadence(self):
        for interval in (0, -1, 2, True, "3"):
            with self.subTest(interval=interval):
                self.assertEqual(self.request("/api/control", {"action": "speed", "interval": interval})[0], 400)

    def test_cluster_mode_disables_simulation(self):
        with patch.object(self.source, "mode", "cluster"):
            self.assertEqual(self.request("/api/scenario", {"scenario": "montant"})[0], 409)

    def test_cluster_failure_returns_503_with_mode(self):
        self.server.cache.clear()
        with patch.object(self.source, "mode", "cluster"), patch.object(self.source, "read", side_effect=RuntimeError("Cassandra indisponible")):
            code, _, body = self.request("/api/snapshot")
        self.assertEqual(code, 503)
        self.assertEqual(body["mode"], "cluster")


class MarkupTests(unittest.TestCase):
    def test_ids_are_unique_and_inputs_have_accessible_names(self):
        class Parser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.ids, self.labels, self.inputs = [], set(), []
                self.label_depth = 0

            def handle_starttag(self, tag, attrs):
                attrs = dict(attrs)
                if "id" in attrs:
                    self.ids.append(attrs["id"])
                if tag == "label" and "for" in attrs:
                    self.labels.add(attrs["for"])
                if tag == "label":
                    self.label_depth += 1
                if tag in {"input", "select"}:
                    self.inputs.append({**attrs, "implicit_label": self.label_depth > 0})

            def handle_endtag(self, tag):
                if tag == "label":
                    self.label_depth -= 1
        parser = Parser()
        parser.feed((ROOT / "web/index.html").read_text(encoding="utf-8"))
        self.assertEqual(len(parser.ids), len(set(parser.ids)))
        for attrs in parser.inputs:
            self.assertTrue(attrs.get("aria-label") or attrs.get("id") in parser.labels or attrs["implicit_label"], attrs)

    def test_producer_output_matches_transaction_contract(self):
        output = subprocess.run([sys.executable, "producer.py", "--count", "2", "--interval", "0", "--scenario", "voyage"],
                                cwd=ROOT, check=True, text=True, encoding="utf-8", capture_output=True)
        records = [validate_transaction(json.loads(line)) for line in output.stdout.splitlines()]
        self.assertEqual(len(records), 4)
        self.assertEqual(len({r["transaction_id"] for r in records}), 4)

    def test_producer_uses_utf8_even_with_ascii_console_default(self):
        output = subprocess.run([sys.executable, "producer.py", "--count", "30", "--interval", "0", "--scenario", "normale"],
                                cwd=ROOT, check=True, capture_output=True,
                                env={**os.environ, "PYTHONIOENCODING": "ascii"})
        text = output.stdout.decode("utf-8")
        self.assertIn("Fès", text)
        self.assertEqual(len([validate_transaction(json.loads(line)) for line in text.splitlines()]), 30)


class SharedThresholdTests(unittest.TestCase):
    """Les seuils Spark proviennent de fraud_core ; les textes produits restent ceux d’origine."""

    def test_spark_job_has_no_hardcoded_threshold(self):
        source = (ROOT / "streaming_cassandra.py").read_text(encoding="utf-8")
        for literal in ("> 10000", "INTERVAL 2 MINUTES", '"2 minutes"', ">= 50", "> 900", ">= 3", "/ 120", "10 000 DH",
                        "INTERVAL 5 MINUTES", "plus de 5 minutes", r"\d{4}-\d{2}"):
            self.assertNotIn(literal, source)

    def test_generated_spark_expressions_keep_original_text(self):
        import streaming_cassandra
        self.assertEqual(streaming_cassandra.spark_duration(fraud_core.WINDOW_SECONDS), "2 minutes")
        self.assertEqual(streaming_cassandra.sql_interval(fraud_core.TRAVEL_MAX_SECONDS), "INTERVAL 2 MINUTES")
        self.assertEqual(streaming_cassandra.sql_interval(90), "INTERVAL 90 SECONDS")
        self.assertEqual(fraud_core.AMOUNT_DETAIL, "Le montant dépasse le seuil de 10 000 DH.")
        self.assertEqual(fraud_core.frequency_detail(3), "3 transactions dans une fenêtre fixe de 2 minutes.")
        self.assertEqual(streaming_cassandra.sql_interval(fraud_core.FUTURE_TOLERANCE_SECONDS), "INTERVAL 5 MINUTES")
        self.assertEqual(fraud_core.window_label(fraud_core.FUTURE_TOLERANCE_SECONDS), "5 minutes")
        self.assertEqual(fraud_core.TIMESTAMP_PATTERN, r"^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$")

    def test_default_thresholds_are_unchanged(self):
        self.assertEqual((fraud_core.AMOUNT_THRESHOLD, fraud_core.WINDOW_SECONDS, fraud_core.TRAVEL_MAX_SECONDS,
                          fraud_core.MIN_DISTANCE_KM, fraud_core.SPEED_THRESHOLD, fraud_core.VELOCITY_THRESHOLD),
                         (10_000, 120, 120, 50, 900, 3))

    def test_travel_horizon_is_read_from_shared_constant(self):
        now = datetime(2026, 10, 6, 8, 54, 30, tzinfo=UTC)
        gen = Generator(1)
        a = gen.transaction(now - timedelta(seconds=200), card_holder_id="USR-9", location="Casablanca")
        b = gen.transaction(now, card_holder_id="USR-9", location="Oujda")
        rules = lambda: {alert["rule"] for alert in detect(validate_transaction(b), [validate_transaction(a)])}
        self.assertNotIn("VOYAGE_IMPOSSIBLE", rules())
        with patch.object(fraud_core, "TRAVEL_MAX_SECONDS", 3600):
            self.assertIn("VOYAGE_IMPOSSIBLE", rules())


class MinorFixesTests(unittest.TestCase):
    """Corrections P5 : texte du déplacement commun aux deux moteurs, sorties UTF-8."""

    def test_travel_detail_matches_spark_wording(self):
        now = datetime(2026, 11, 18, 14, 30, tzinfo=UTC)
        events = Generator(5).scenario("voyage", now, {"from_city": "Casablanca", "to_city": "Tanger", "elapsed_seconds": 40})
        alerts = detect(events[1], [events[0]])
        detail = next(a["detail"] for a in alerts if a["rule"] == "VOYAGE_IMPOSSIBLE")
        # Valeurs relevées dans Cassandra pour ce trajet : 291 km, 40 s, 26 221 km/h.
        self.assertEqual(detail, "Casablanca → Tanger : 291 km en 40 s ; vitesse estimée 26 221 km/h.")
        self.assertEqual(fraud_core.travel_detail("Fès", "Meknès", 53.2, 0, 191_520.4),
                         "Fès → Meknès : 53 km en 0 s ; vitesse estimée 191 520 km/h.")

    def test_spark_uses_same_travel_wording(self):
        source = (ROOT / "streaming_cassandra.py").read_text(encoding="utf-8")
        self.assertNotIn("soit", source)
        self.assertEqual(source.count('spark_number("'), 3)
        self.assertIn('F.format_number(F.col(column), 0), ",", " ")', source)

    def test_producer_stderr_is_utf8(self):
        output = subprocess.run([sys.executable, "producer.py", "--count", "1", "--interval", "0"], cwd=ROOT, check=True,
                                capture_output=True, env={**os.environ, "PYTHONIOENCODING": "ascii"})
        self.assertIn("générée(s)", output.stderr.decode("utf-8"))


class TimestampContractTests(unittest.TestCase):
    """Verdicts relevés sur le cluster le 18 novembre 2026 : Python décide comme le filtre Spark."""

    NOW = datetime(2026, 11, 18, 14, 30, tzinfo=UTC)

    def tx(self, timestamp):
        return {**Generator(3).transaction(self.NOW, card_holder_id="USR-P2"), "timestamp": timestamp}

    def test_formats_accepted_by_spark(self):
        for value in ("2026-11-18T14:30:00Z", "2026-11-18T15:30:00+01:00", "2026-11-18T09:30:00-05:00",
                      "2026-11-18T14:30:00+00:00", "2026-11-18T14:30:00.5Z", "2026-11-18T14:30:00.123Z",
                      "2026-11-18T14:30:00.123456Z", "2026-10-06T08:55:00Z"):
            with self.subTest(value=value):
                validate_transaction(self.tx(value), now=self.NOW)

    def test_formats_rejected_by_spark(self):
        for value in ("2026-11-18 14:30:00Z", "20261118T143000Z", "2026-11-18T15:30:00+0100", "2026-11-18T15:30:00+01",
                      "2026-11-18T14:30Z", "2026-11-18T14:30:00z", "2026-11-18T14:30:00", "2026-11-17T24:00:00Z",
                      "2026-02-30T10:00:00Z", "2026-11-18T10:00:60Z", "٢٠٢٦-11-18T14:30:00Z", 1763470000):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_transaction(self.tx(value), now=self.NOW)

    def test_future_tolerance_is_five_minutes(self):
        for delta, accepted in ((timedelta(minutes=4), True), (timedelta(minutes=5), True),
                                (timedelta(minutes=5, seconds=1), False), (timedelta(minutes=6), False), (timedelta(days=30), False)):
            tx = self.tx(iso(self.NOW + delta))
            with self.subTest(delta=delta):
                if accepted:
                    validate_transaction(tx, now=self.NOW)
                else:
                    with self.assertRaisesRegex(ValueError, "plus de 5 minutes"):
                        validate_transaction(tx, now=self.NOW)

    def test_demo_store_rejects_future_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "p2.sqlite3")
            future = iso(datetime.now(UTC) + timedelta(days=30))
            with self.assertRaises(ValueError):
                store.ingest(self.tx(future))
            with store.connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0], 0)
            store.ingest(self.tx(iso(datetime.now(UTC))))
            with store.connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0], 1)


class ResilienceTests(unittest.TestCase):
    """Indisponibilité de Cassandra : relances bornées, erreurs jamais masquées."""

    def test_write_succeeds_after_transient_failures(self):
        import streaming_cassandra
        attempts, pauses = [], []

        def write():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("Unknown host 'cassandra'")
            return "écrit"
        with self.assertLogs("vigie.streaming", "WARNING") as logs:
            self.assertEqual(streaming_cassandra.with_retries(write, "test", delays=(2, 4, 8), sleep=pauses.append), "écrit")
        self.assertEqual((len(attempts), pauses), (3, [2, 4]))
        self.assertEqual(len(logs.records), 2)

    def test_write_failure_is_raised_after_last_attempt(self):
        import streaming_cassandra
        pauses = []
        error = ConnectionError("Cassandra arrêté")

        def write():
            raise error
        with self.assertRaises(ConnectionError) as raised, self.assertLogs("vigie.streaming", "WARNING") as logs:
            streaming_cassandra.with_retries(write, "test", delays=(2, 4), sleep=pauses.append)
        self.assertIs(raised.exception, error)
        self.assertEqual(logs.records[-1].levelname, "ERROR")
        self.assertEqual(pauses, [2, 4])

    def test_logged_reason_is_the_jvm_cause(self):
        import streaming_cassandra
        error = Exception("An error occurred while calling o823.save.\n"
                          ": java.io.IOException: Failed to open native connection to Cassandra at {}\n\tat com.datastax…")
        self.assertEqual(streaming_cassandra.first_line(error),
                         "java.io.IOException: Failed to open native connection to Cassandra at {}")
        self.assertEqual(streaming_cassandra.first_line(ConnectionError("Refus")), "Refus")
        self.assertEqual(streaming_cassandra.first_line(ValueError()), "ValueError")

    def test_default_write_retries_last_about_ninety_seconds(self):
        import streaming_cassandra
        self.assertEqual(sum(streaming_cassandra.CASSANDRA_RETRY_DELAYS), 90)

    def test_query_restart_delays_grow_then_give_up(self):
        import streaming_cassandra
        now = [0.0]
        policy = streaming_cassandra.RestartPolicy(delays=(5, 15, 30), limit=4, window=600, clock=lambda: now[0])
        delays = []
        for _ in range(5):
            delays.append(policy.next_delay("montants"))
            now[0] += 10
        self.assertEqual(delays, [5, 15, 30, 30, None])
        self.assertEqual(policy.next_delay("frequences"), 5)

    def test_query_restart_budget_recovers_after_window(self):
        import streaming_cassandra
        now = [0.0]
        policy = streaming_cassandra.RestartPolicy(delays=(5, 15), limit=2, window=600, clock=lambda: now[0])
        policy.next_delay("montants")
        policy.next_delay("montants")
        now[0] = 601
        self.assertEqual(policy.next_delay("montants"), 5)

    def health(self, cql):
        source = ClusterSource(Store(Path(tempfile.mkdtemp()) / "health.sqlite3"))
        with patch("storage.socket.create_connection"), \
                patch("storage.urlopen", side_effect=OSError), \
                patch("storage.subprocess.run", **cql):
            return {service["name"]: service for service in source.health()}

    def test_cassandra_health_runs_a_real_query(self):
        output = subprocess.CompletedProcess([], 0, stdout='\n {"release_version": "4.1.7"}\n\n(1 rows)\n', stderr="")
        cassandra = self.health({"return_value": output})["Cassandra"]
        self.assertEqual(cassandra["state"], "operationnel")
        self.assertIn("4.1.7", cassandra["detail"])

    def test_open_port_without_cql_answer_is_unavailable(self):
        failure = subprocess.CalledProcessError(1, "cqlsh")
        services = self.health({"side_effect": failure})
        self.assertEqual(services["Cassandra"]["state"], "indisponible")
        self.assertIn("CQL", services["Cassandra"]["detail"])
        self.assertEqual(services["Kafka"]["state"], "joignable")

    def test_submit_script_guards_against_duplicate_jobs(self):
        script = (ROOT / "submit.sh").read_text(encoding="utf-8")
        self.assertNotIn("\r", script)
        for marker in ("flock -n 9", "--inside-stop", "RESTART_LIMIT", "trap stop_job INT TERM HUP"):
            self.assertIn(marker, script)


if __name__ == "__main__":
    unittest.main()
