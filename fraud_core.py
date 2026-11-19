"""Règles métier partagées par la démonstration et le générateur de flux."""

import hashlib
import math
import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

UTC = timezone.utc
# Seuils uniques, importés par la démonstration et par le job Spark.
AMOUNT_THRESHOLD = 10_000
WINDOW_SECONDS = 120
TRAVEL_MAX_SECONDS = WINDOW_SECONDS
SPEED_THRESHOLD = 900
MIN_DISTANCE_KM = 50
VELOCITY_THRESHOLD = 3
# Historique nécessaire à la démonstration pour évaluer les deux règles avec état.
HISTORY_SECONDS = max(WINDOW_SECONDS, TRAVEL_MAX_SECONDS)
# Contrat d'horodatage commun au job Spark et à la démonstration.
FUTURE_TOLERANCE_SECONDS = 300
TIMESTAMP_PATTERN = (
    r"(?!0000)[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
    r"(?:\.([0-9]{1,6}))?(?:Z|[+-](?:(?:0[0-9]|1[0-7]):[0-5][0-9]|18:00))"
)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
WINDOW_DURATION = timedelta(seconds=WINDOW_SECONDS)
TRANSACTION_FIELDS = ("transaction_id", "card_holder_id", "amount", "location", "merchant_type", "timestamp")
CITIES = {
    "Casablanca": (33.5731, -7.5898),
    "Rabat": (34.0209, -6.8416),
    "Marrakech": (31.6295, -7.9811),
    "Tanger": (35.7595, -5.8340),
    "Fès": (34.0331, -5.0003),
    "Agadir": (30.4278, -9.5981),
    "Oujda": (34.6814, -1.9086),
    "Meknès": (33.8935, -5.5473),
}
RULES = {
    "MONTANT_ELEVE": {"label": "Montant élevé", "severity": "élevée"},
    "VOYAGE_IMPOSSIBLE": {"label": "Déplacement impossible", "severity": "critique"},
    "FREQUENCE_ELEVEE": {"label": "Fréquence inhabituelle", "severity": "modérée"},
}
MERCHANTS = ("Alimentation", "Restaurant", "Transport", "Mode", "Électronique", "Bijouterie")


def french_number(value):
    return f"{value:,}".replace(",", " ")


AMOUNT_DETAIL = f"Le montant dépasse le seuil de {french_number(AMOUNT_THRESHOLD)} DH."


def window_label(seconds=WINDOW_SECONDS):
    return f"{seconds // 60} minutes" if seconds % 60 == 0 else f"{seconds} secondes"


def frequency_detail(count):
    return f"{count} transactions dans une fenêtre fixe de {window_label()}."


def travel_detail(origin, destination, distance, seconds, speed):
    """Texte commun à la démonstration et au job Spark : nombres arrondis à l'unité, espace pour les milliers."""
    numbers = [french_number(round(value)) for value in (distance, seconds, speed)]
    return f"{origin} → {destination} : {numbers[0]} km en {numbers[1]} s ; vitesse estimée {numbers[2]} km/h."


def iso(value):
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_time(value):
    if not isinstance(value, str):
        raise ValueError("L’horodatage doit être une chaîne ISO 8601.")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Horodatage ISO 8601 invalide.") from exc
    if result.tzinfo is None:
        raise ValueError("Le fuseau horaire est obligatoire.")
    try:
        return result.astimezone(UTC)
    except OverflowError as exc:
        raise ValueError("Horodatage hors de la plage UTC prise en charge.") from exc


def validate_timestamp(value, now=None):
    """Valide le format et la tolérance par rapport à la réception."""
    match = re.fullmatch(TIMESTAMP_PATTERN, value) if isinstance(value, str) else None
    if match is None:
        raise ValueError("L’horodatage doit suivre le format AAAA-MM-JJTHH:MM:SS avec un fuseau Z ou ±HH:MM.")
    if match.group(1) is not None:
        start, end = match.span(1)
        # Python 3.10 attend trois ou six décimales dans fromisoformat.
        value = value[:start] + value[start:end].ljust(6, "0") + value[end:]
    result = parse_time(value)
    if result > (now or datetime.now(UTC)) + timedelta(seconds=FUTURE_TOLERANCE_SECONDS):
        raise ValueError(f"L’horodatage dépasse de plus de {window_label(FUTURE_TOLERANCE_SECONDS)} l’heure actuelle.")
    return result


def validate_transaction(raw, now=None):
    if not isinstance(raw, dict):
        raise ValueError("Une transaction doit être un objet JSON.")
    if any(key not in raw for key in TRANSACTION_FIELDS):
        raise ValueError("Les six champs du contrat de transaction sont obligatoires.")
    for key in ("transaction_id", "card_holder_id"):
        if not isinstance(raw[key], str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", raw[key]):
            raise ValueError(f"Identifiant invalide : {key}.")
    try:
        if isinstance(raw["amount"], bool):
            raise ValueError("Montant invalide.")
        amount = Decimal(str(raw["amount"]))
        if not amount.is_finite() or amount <= 0 or amount > Decimal("999999999.99"):
            raise ValueError("Le montant doit être positif, fini et inférieur à un milliard de DH.")
        if amount != amount.quantize(Decimal("0.01")):
            raise ValueError("Le montant accepte au plus deux décimales.")
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("Montant invalide.") from exc
    if not isinstance(raw["location"], str) or raw["location"] not in CITIES:
        raise ValueError("La ville doit appartenir au référentiel géographique marocain.")
    if not isinstance(raw["merchant_type"], str) or not 1 <= len(raw["merchant_type"].strip()) <= 64:
        raise ValueError("Catégorie de commerce invalide.")
    return {**{key: raw[key] for key in TRANSACTION_FIELDS},
            "amount": float(amount), "timestamp": iso(validate_timestamp(raw["timestamp"], now))}


def distance_km(city_a, city_b):
    lat1, lon1 = map(math.radians, CITIES[city_a])
    lat2, lon2 = map(math.radians, CITIES[city_b])
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6371.0088 * 2 * math.asin(min(1, math.sqrt(h)))


def alert_id(*parts):
    return hashlib.sha256(":".join(map(str, parts)).encode()).hexdigest()


def make_alert(tx, rule, detail, identity, **extra):
    return {
        **tx, "alert_id": alert_id(rule, *identity), "rule": rule,
        "rule_label": RULES[rule]["label"], "severity": RULES[rule]["severity"],
        "detail": detail, "status": "nouvelle", "related_transaction_ids": [tx["transaction_id"]], **extra,
    }


def detect(tx, history):
    """Accepte les arrivées désordonnées ; l’historique fourni est borné par client."""
    alerts = []
    event_time = parse_time(tx["timestamp"])
    if tx["amount"] > AMOUNT_THRESHOLD:
        alerts.append(make_alert(tx, "MONTANT_ELEVE", AMOUNT_DETAIL, [tx["transaction_id"]]))
    relevant = [p for p in history if p["card_holder_id"] == tx["card_holder_id"] and p["transaction_id"] != tx["transaction_id"]]
    for previous in relevant:
        previous_time = parse_time(previous["timestamp"])
        delta = abs((event_time - previous_time).total_seconds())
        if delta > TRAVEL_MAX_SECONDS:
            continue
        distance = distance_km(tx["location"], previous["location"])
        speed = distance * 3600 / max(delta, 1)
        if distance >= MIN_DISTANCE_KM and speed > SPEED_THRESHOLD:
            previous_first = (previous_time, previous["transaction_id"]) < (event_time, tx["transaction_id"])
            early, late = (previous, tx) if previous_first else (tx, previous)
            alerts.append(make_alert(
                late, "VOYAGE_IMPOSSIBLE",
                travel_detail(early["location"], late["location"], distance, delta, speed),
                sorted([tx["transaction_id"], previous["transaction_id"]]),
                previous_location=early["location"], previous_timestamp=early["timestamp"],
                related_transaction_ids=sorted([tx["transaction_id"], previous["transaction_id"]]),
                distance_km=round(distance, 1), speed_kmh=round(speed),
            ))
    bucket = (event_time - EPOCH) // WINDOW_DURATION
    window = [p for p in relevant if (parse_time(p["timestamp"]) - EPOCH) // WINDOW_DURATION == bucket] + [tx]
    if len(window) >= VELOCITY_THRESHOLD:
        latest = max(window, key=lambda item: (parse_time(item["timestamp"]), item["transaction_id"]))
        alerts.append(make_alert(latest, "FREQUENCE_ELEVEE", frequency_detail(len(window)),
                                 [tx["card_holder_id"], bucket], transaction_count=len(window),
                                 related_transaction_ids=sorted(p["transaction_id"] for p in window)))
    return alerts


class Generator:
    def __init__(self, seed=None):
        self.random = random.Random(seed)

    def transaction(self, when=None, **overrides):
        holder = overrides.get("card_holder_id", f"USR-{self.random.randint(101, 180)}")
        city = list(CITIES)[int(holder.split("-")[-1]) % len(CITIES)] if holder.split("-")[-1].isdigit() else "Casablanca"
        return {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "card_holder_id": holder,
            "amount": round(self.random.uniform(45, 2800), 2),
            "location": city,
            "merchant_type": self.random.choice(MERCHANTS),
            "timestamp": iso(when or datetime.now(UTC)), **overrides,
        }

    def scenario(self, kind, when=None, options=None):
        when = when or datetime.now(UTC)
        options = {} if options is None else options
        if not isinstance(options, dict):
            raise ValueError("Les paramètres du scénario doivent être un objet.")
        allowed = {"normale": set(), "montant": {"amount"},
                   "voyage": {"from_city", "to_city", "elapsed_seconds"}, "frequence": {"count"}}
        if not isinstance(kind, str) or kind not in allowed:
            raise ValueError("Scénario inconnu.")
        if set(options) - allowed[kind]:
            raise ValueError("Paramètre non reconnu pour ce scénario.")
        holder = f"USR-{uuid.uuid4().hex.upper()}"
        if kind == "montant":
            events = [self.transaction(when, card_holder_id=holder, amount=options.get("amount", 14500), location="Casablanca", merchant_type="Bijouterie")]
        if kind == "voyage":
            elapsed = options.get("elapsed_seconds", 40)
            if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or not 1 <= elapsed <= 300:
                raise ValueError("L’intervalle doit être compris entre 1 et 300 secondes.")
            events = [self.transaction(when - timedelta(seconds=elapsed), card_holder_id=holder, location=options.get("from_city", "Casablanca")),
                      self.transaction(when, card_holder_id=holder, location=options.get("to_city", "Tanger"))]
        if kind == "frequence":
            count = options.get("count", 3)
            if isinstance(count, bool) or not isinstance(count, int) or not 2 <= count <= 8:
                raise ValueError("Le nombre de transactions doit être compris entre 2 et 8.")
            # Aucun événement futur, même au premier instant d’une fenêtre.
            start = when - (when - EPOCH) % WINDOW_DURATION
            events = [self.transaction(max(start, when - timedelta(seconds=count - 1 - i)), card_holder_id=holder, location="Rabat") for i in range(count)]
        if kind == "normale":
            events = [self.transaction(when, card_holder_id=holder, location=self.random.choice(list(CITIES)))]
        return [validate_transaction(event) for event in events]
