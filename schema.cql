-- Configuration locale à un nœud. Adapter le facteur de réplication au cluster cible.
CREATE KEYSPACE IF NOT EXISTS fraud_monitoring
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

-- Consultation ciblée : jour UTC + client. Identifiant déterministe en clustering.
CREATE TABLE IF NOT EXISTS fraud_monitoring.detected_alerts (
    alert_date date,
    card_holder_id text,
    timestamp timestamp,
    alert_id text,
    transaction_id text,
    amount decimal,
    location text,
    merchant_type text,
    rule text,
    severity text,
    detail text,
    payload text,
    PRIMARY KEY ((alert_date, card_holder_id), timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, alert_id ASC)
AND comment = 'Alertes par jour UTC et client ; upserts idempotents';

-- Tables de lecture adaptées à la console ; aucune requête ALLOW FILTERING.
CREATE TABLE IF NOT EXISTS fraud_monitoring.alerts_by_day (
    event_date date,
    timestamp timestamp,
    alert_id text,
    payload text,
    PRIMARY KEY ((event_date), timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, alert_id ASC);

CREATE TABLE IF NOT EXISTS fraud_monitoring.transactions_by_day (
    event_date date,
    timestamp timestamp,
    transaction_id text,
    payload text,
    PRIMARY KEY ((event_date), timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC);
