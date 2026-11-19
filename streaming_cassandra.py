"""Détection en temps événement ; connecteurs Kafka et Cassandra côté JVM.

Exécution : bash submit.sh. Aucune bibliothèque Python tierce hors PySpark.
"""

import logging
import json
import os
import time

from fraud_core import (AMOUNT_DETAIL, AMOUNT_THRESHOLD, CITIES, FUTURE_TOLERANCE_SECONDS, MIN_DISTANCE_KM, RULES,
                        SPEED_THRESHOLD, TIMESTAMP_PATTERN, TRAVEL_MAX_SECONDS, VELOCITY_THRESHOLD, WINDOW_SECONDS,
                        window_label)

LOG = logging.getLogger("vigie.streaming")
TRANSACTION_FIELDS = ("transaction_id", "card_holder_id", "amount", "location", "merchant_type", "timestamp")


def spark_duration(seconds):
    """Durée Spark ; 120 s s'écrit « 2 minutes », comme avant la centralisation des seuils."""
    return f"{seconds // 60} minutes" if seconds % 60 == 0 else f"{seconds} seconds"


def spark_number(column):
    """Même rendu que fraud_core.french_number(round(x)) : « 26 221 » et non « 26221.0 »."""
    from pyspark.sql import functions as F
    return F.regexp_replace(F.format_number(F.col(column), 0), ",", " ")


def sql_interval(seconds):
    return f"INTERVAL {spark_duration(seconds).upper()}"


# Une coupure brève de Cassandra est absorbée dans le micro-lot (≈ 90 s cumulées).
CASSANDRA_RETRY_DELAYS = (2, 4, 8, 16, 30, 30)
# Au-delà, seule la requête en échec est relancée depuis son checkpoint.
QUERY_RESTART_DELAYS = (5, 15, 30, 60)
QUERY_RESTART_LIMIT = 5
QUERY_RESTART_WINDOW = 600


def first_line(error):
    """Motif lisible ; Py4J place la cause JVM sur une ligne préfixée par « : »."""
    lines = [line.strip() for line in str(error).splitlines() if line.strip()]
    cause = next((line[1:].strip() for line in lines if line.startswith(": ")), None)
    return (cause or (lines[0] if lines else type(error).__name__))[:300]


def with_retries(action, label, delays=CASSANDRA_RETRY_DELAYS, sleep=time.sleep):
    """Exécute action ; après le dernier délai, l’erreur d’origine remonte sans être masquée."""
    for attempt, delay in enumerate((*delays, None), start=1):
        try:
            return action()
        except Exception as error:  # Les erreurs JVM arrivent via Py4J sans type Python précis.
            if delay is None:
                LOG.error("%s : échec après %s tentatives : %s", label, attempt, first_line(error))
                raise
            LOG.warning("%s : tentative %s en échec (%s) ; nouvel essai dans %s s", label, attempt, first_line(error), delay)
            sleep(delay)


class RestartPolicy:
    """Délai avant relance d’une requête ; None quand les échecs récents dépassent la limite."""

    def __init__(self, delays=QUERY_RESTART_DELAYS, limit=QUERY_RESTART_LIMIT, window=QUERY_RESTART_WINDOW, clock=time.monotonic):
        self.delays, self.limit, self.window, self.clock = delays, limit, window, clock
        self.failures = {}

    def next_delay(self, name):
        now = self.clock()
        recent = [moment for moment in self.failures.get(name, []) if now - moment < self.window] + [now]
        self.failures[name] = recent
        if len(recent) > self.limit:
            return None
        return self.delays[min(len(recent), len(self.delays)) - 1]


def run():
    from pyspark.sql import SparkSession, functions as F, types as T

    kafka = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    cassandra = os.environ.get("CASSANDRA_HOST", "cassandra")
    checkpoint = os.environ.get("CHECKPOINT_DIR", "/opt/spark/checkpoints")
    keyspace = "fraud_monitoring"
    spark = (SparkSession.builder.appName("Vigie-Fraud-Monitoring")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.cassandra.connection.host", cassandra)
             .config("spark.sql.streaming.multipleWatermarkPolicy", "min")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    schema = T.StructType([
        T.StructField("transaction_id", T.StringType()),
        T.StructField("card_holder_id", T.StringType()),
        T.StructField("amount", T.DecimalType(12, 2)),
        T.StructField("location", T.StringType()),
        T.StructField("merchant_type", T.StringType()),
        T.StructField("timestamp", T.StringType()),
    ])

    def read_input():
        messages = (spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", kafka)
                    .option("subscribe", "financial-transactions")
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "true")
                    .option("maxOffsetsPerTrigger", "10000").load())
        parsed = messages.select(F.col("value").cast("string").alias("raw"),
                                 F.from_json(F.col("value").cast("string"), schema).alias("record"))
        flat = parsed.select("raw", "record.*").withColumnRenamed("timestamp", "timestamp_text")
        flat = flat.withColumn("timestamp", F.to_timestamp("timestamp_text"))
        valid = (F.col("transaction_id").rlike(r"^[A-Za-z0-9_-]{1,64}$") &
                 F.col("card_holder_id").rlike(r"^[A-Za-z0-9_-]{1,64}$") &
                 F.col("amount").isNotNull() & (F.col("amount") > 0) & (F.col("amount") <= F.lit(999999999.99)) &
                 F.get_json_object("raw", "$.amount").rlike(r"^[0-9]+(?:\.[0-9]{1,2}0*)?$") &
                 F.col("location").isin(list(CITIES)) &
                 (F.length(F.trim("merchant_type")) > 0) & (F.length("merchant_type") <= 64) &
                 F.col("timestamp_text").rlike(TIMESTAMP_PATTERN) &
                 F.col("timestamp").isNotNull() &
                 (F.col("timestamp") <= F.current_timestamp() + F.expr(sql_interval(FUTURE_TOLERANCE_SECONDS))))
        return flat.withColumn("valid", F.coalesce(valid, F.lit(False)))

    parsed = read_input()
    transactions = parsed.filter("valid").select(*TRANSACTION_FIELDS)

    def save_cassandra(frame, table):
        # Upserts par clé primaire : une tentative répétée réécrit les mêmes lignes.
        with_retries(lambda: (frame.write.format("org.apache.spark.sql.cassandra")
                              .options(keyspace=keyspace, table=table).mode("append").save()),
                     f"Écriture Cassandra {table}")

    def json_payload(frame, columns):
        fields = [F.date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp") if name == "timestamp"
                  else F.col(name) for name in columns]
        return frame.withColumn("payload", F.to_json(F.struct(*fields)))

    def persist_transactions(batch, batch_id):
        batch.persist()
        try:
            if not batch.isEmpty():
                frame = json_payload(batch, TRANSACTION_FIELDS)
                frame = frame.withColumn("event_date", F.to_date("timestamp"))
                save_cassandra(frame.select("event_date", "timestamp", "transaction_id", "payload"), "transactions_by_day")
        finally:
            batch.unpersist()

    def prepare_alerts(frame, rule, identity, detail):
        return (frame.withColumn("alert_id", F.sha2(identity, 256))
                .withColumn("rule", F.lit(rule))
                .withColumn("rule_label", F.lit(RULES[rule]["label"]))
                .withColumn("severity", F.lit(RULES[rule]["severity"]))
                .withColumn("detail", detail)
                .withColumn("status", F.lit("nouvelle"))
                .withColumn("related_transaction_ids", F.col("related_transaction_ids") if "related_transaction_ids" in frame.columns
                            else F.array("transaction_id"))
                .withColumn("storage_timestamp", F.col("timestamp")))

    def persist_alerts(batch, batch_id):
        batch.persist()
        try:
            if batch.isEmpty():
                return
            columns = [name for name in batch.columns if name not in {"latitude", "longitude", "storage_timestamp"}]
            frame = json_payload(batch, columns)
            stored = frame.withColumn("timestamp", F.col("storage_timestamp"))
            save_cassandra(stored.withColumn("alert_date", F.to_date("timestamp")).select(
                "alert_date", "card_holder_id", "timestamp", "alert_id", "transaction_id", "amount", "location", "merchant_type", "rule", "severity", "detail", "payload"), "detected_alerts")
            save_cassandra(stored.withColumn("event_date", F.to_date("timestamp")).select(
                "event_date", "timestamp", "alert_id", "payload"), "alerts_by_day")
            (frame.select(F.col("alert_id").alias("key"), F.col("payload").alias("value"))
             .write.format("kafka").option("kafka.bootstrap.servers", kafka).option("topic", "fraud-alerts").save())
            LOG.info("Lot %s persisté dans Cassandra et publié dans Kafka", batch_id)
        finally:
            batch.unpersist()

    high_amount = prepare_alerts(
        transactions.filter(F.col("amount") > AMOUNT_THRESHOLD), "MONTANT_ELEVE",
        F.concat_ws(":", F.lit("MONTANT_ELEVE"), F.col("transaction_id")),
        F.lit(AMOUNT_DETAIL))

    # Deux sources indépendantes évitent les ambiguïtés d’attribut d’une auto-jointure.
    geo = spark.createDataFrame([(name, *point) for name, point in CITIES.items()], ["location", "latitude", "longitude"])
    left = transactions.join(F.broadcast(geo), "location").withWatermark("timestamp", "10 minutes").alias("a")
    right_tx = read_input().filter("valid").select(*TRANSACTION_FIELDS)
    right = right_tx.join(F.broadcast(geo), "location").withWatermark("timestamp", "10 minutes").alias("b")
    pairs = left.join(right, F.expr(f"""
        a.card_holder_id = b.card_holder_id
        AND a.timestamp >= b.timestamp
        AND a.timestamp <= b.timestamp + {sql_interval(TRAVEL_MAX_SECONDS)}
        AND (a.timestamp > b.timestamp OR a.transaction_id > b.transaction_id)
        AND a.transaction_id <> b.transaction_id
    """), "inner")
    radians = F.lit(3.141592653589793 / 180)
    haversine = (F.pow(F.sin((F.col("a.latitude") - F.col("b.latitude")) * radians / 2), 2) +
                 F.cos(F.col("a.latitude") * radians) * F.cos(F.col("b.latitude") * radians) *
                 F.pow(F.sin((F.col("a.longitude") - F.col("b.longitude")) * radians / 2), 2))
    distance = F.lit(6371.0088 * 2) * F.asin(F.sqrt(F.least(F.lit(1.0), haversine)))
    seconds = F.col("a.timestamp").cast("double") - F.col("b.timestamp").cast("double")
    travel = pairs.select(*[F.col(f"a.{name}").alias(name) for name in TRANSACTION_FIELDS],
                          F.col("b.transaction_id").alias("previous_id"), F.col("b.location").alias("previous_location"),
                          F.date_format("b.timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("previous_timestamp"),
                          distance.alias("distance_km"), seconds.alias("elapsed_seconds"))
    travel = travel.withColumn("speed_kmh", F.col("distance_km") * 3600 / F.greatest(F.col("elapsed_seconds"), F.lit(1)))
    travel = travel.filter((F.col("distance_km") >= MIN_DISTANCE_KM) & (F.col("speed_kmh") > SPEED_THRESHOLD))
    sorted_ids = F.array_sort(F.array("transaction_id", "previous_id"))
    travel = travel.withColumn("related_transaction_ids", sorted_ids)
    travel = prepare_alerts(travel, "VOYAGE_IMPOSSIBLE",
                            F.concat_ws(":", F.lit("VOYAGE_IMPOSSIBLE"), sorted_ids[0], sorted_ids[1]),
                            F.concat(F.col("previous_location"), F.lit(" → "), F.col("location"), F.lit(" : "),
                                     spark_number("distance_km"), F.lit(" km en "),
                                     spark_number("elapsed_seconds"), F.lit(" s ; vitesse estimée "),
                                     spark_number("speed_kmh"), F.lit(" km/h."))).drop("previous_id", "elapsed_seconds")

    # Une seule opération avec état par requête ; mise à jour dès le troisième événement.
    latest = F.struct(*[F.col(name) for name in ("timestamp", "transaction_id", "card_holder_id", "amount", "location", "merchant_type")])
    frequency = (transactions.withWatermark("timestamp", "10 minutes")
                 .groupBy(F.window("timestamp", spark_duration(WINDOW_SECONDS)), "card_holder_id")
                 .agg(F.collect_set("transaction_id").alias("identities"), F.max(latest).alias("latest"))
                 .withColumn("transaction_count", F.size("identities"))
                 .filter(F.col("transaction_count") >= VELOCITY_THRESHOLD))
    frequency = frequency.select("window", "transaction_count", F.array_sort("identities").alias("related_transaction_ids"), "latest.*")
    frequency = prepare_alerts(frequency, "FREQUENCE_ELEVEE",
                               F.concat_ws(":", F.lit("FREQUENCE_ELEVEE"), F.col("card_holder_id"),
                                           F.floor(F.col("window.start").cast("long") / WINDOW_SECONDS).cast("string")),
                               F.concat(F.col("transaction_count"), F.lit(f" transactions dans une fenêtre fixe de {window_label()}.")))
    # La clé Cassandra reste fixe même lorsque la dernière transaction change.
    frequency = frequency.withColumn("storage_timestamp", F.col("window.start")).drop("window")

    rejected = parsed.filter("NOT valid").select(
        F.lit("invalide").alias("key"),
        F.to_json(F.struct("raw", F.lit(f"Contrat invalide, ville inconnue ou horodatage futur de plus de {window_label(FUTURE_TOLERANCE_SECONDS)}.").alias("reason"))).alias("value"))
    def start_rejected():
        return (rejected.writeStream.queryName("messages_invalides").format("kafka")
                .option("kafka.bootstrap.servers", kafka).option("topic", "invalid-transactions")
                .option("checkpointLocation", f"{checkpoint}/invalides").trigger(processingTime="5 seconds").start())

    def batch_query(name, frame, sink):
        # Même nom, même plan et même checkpoint à chaque relance : reprise sans perte.
        return lambda: (frame.writeStream.queryName(name).outputMode("update" if name == "frequences" else "append")
                        .option("checkpointLocation", f"{checkpoint}/{name}")
                        .foreachBatch(sink).trigger(processingTime="5 seconds").start())

    starters = {"messages_invalides": start_rejected}
    for name, frame, sink in (("transactions", transactions, persist_transactions),
                              ("montants", high_amount, persist_alerts),
                              ("deplacements", travel, persist_alerts),
                              ("frequences", frequency, persist_alerts)):
        starters[name] = batch_query(name, frame, sink)
    queries = {}
    policy = RestartPolicy()
    try:
        for name, start in starters.items():
            queries[name] = start()
        LOG.info("Requêtes actives : %s", ", ".join(queries))
        progress_batches = {}
        while True:
            try:
                spark.streams.awaitAnyTermination(15)
            except Exception:  # L’échec est relu ci-dessous sur la requête concernée.
                pass
            spark.streams.resetTerminated()
            for name, query in list(queries.items()):
                if query.isActive:
                    progress = query.lastProgress
                    if progress and progress_batches.get(name) != progress["batchId"]:
                        progress_batches[name] = progress["batchId"]
                        fields = ("name", "batchId", "timestamp", "numInputRows", "inputRowsPerSecond",
                                  "processedRowsPerSecond", "durationMs", "eventTime", "stateOperators")
                        LOG.info("PROGRESS %s", json.dumps({key: progress.get(key) for key in fields}, ensure_ascii=False))
                    continue
                error = query.exception()
                if error is None:
                    LOG.info("Requête %s arrêtée sans erreur ; arrêt du job.", name)
                    return
                delay = policy.next_delay(name)
                if delay is None:
                    raise RuntimeError(f"Requête {name} : plus de {QUERY_RESTART_LIMIT} échecs en {QUERY_RESTART_WINDOW} s ; "
                                       f"dernier motif : {first_line(error)}")
                LOG.error("Requête %s en échec : %s ; relance depuis son checkpoint dans %s s", name, first_line(error), delay)
                time.sleep(delay)
                queries[name] = starters[name]()
                LOG.info("Requête %s relancée ; les autres requêtes n’ont pas été interrompues.", name)
    finally:
        for query in queries.values():
            try:
                query.stop()
            except Exception as error:
                LOG.warning("Arrêt de la requête %s : %s", query.name, first_line(error))
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
