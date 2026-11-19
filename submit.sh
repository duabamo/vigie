#!/usr/bin/env bash
set -euo pipefail

# Utilisation : bash submit.sh [options spark-submit]   lance le job et le relance après un arrêt anormal
#               bash submit.sh --stop                   arrête le job en cours dans le conteneur
LOCK=/tmp/vigie-submit.lock
PIDFILE=/tmp/vigie-submit.pid
RESTART_DELAY=20     # secondes avant une nouvelle soumission
RESTART_LIMIT=5      # arrêts anormaux consécutifs tolérés
STABLE_SECONDS=600   # un job actif depuis 10 minutes remet le compteur à zéro

if [[ "${1:-}" != --inside* ]]; then
  cd "$(dirname "$0")"
  if ! command -v docker >/dev/null 2>&1; then
    printf '%s\n' 'Docker est requis pour la soumission. Pour la console locale : python3 app.py' >&2
    exit 1
  fi
  # Git Bash ne doit pas convertir le chemin Linux du conteneur en chemin Windows.
  export MSYS_NO_PATHCONV=1
  if [[ "${1:-}" == "--stop" ]]; then
    exec docker compose exec -T spark-master bash /opt/project/submit.sh --inside-stop
  fi
  # docker exec ne transmet pas les signaux : Ctrl+C ou la fermeture du terminal arrêtent explicitement le job.
  client=""
  stop_job() {
    trap - INT TERM HUP
    printf '%s\n' 'Arrêt du job Spark demandé…' >&2
    docker compose exec -T spark-master bash /opt/project/submit.sh --inside-stop || true
    [[ -n "$client" ]] && kill "$client" 2>/dev/null || true
    exit 130
  }
  trap stop_job INT TERM HUP
  docker compose exec -T spark-master bash /opt/project/submit.sh --inside "$@" &
  client=$!
  code=0
  wait "$client" || code=$?
  exit "$code"
fi

if [[ "$1" == "--inside-stop" ]]; then
  if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    supervisor=$(cat "$PIDFILE")
    kill -TERM "$supervisor"
    for _ in $(seq 60); do kill -0 "$supervisor" 2>/dev/null || break; sleep 1; done
    printf '%s\n' 'Job Vigie arrêté.'
  elif pkill -TERM -f 'org.apache.spark.deploy.SparkSubmit.*streaming_cassandra.py'; then
    printf '%s\n' 'Job Vigie sans superviseur arrêté.'
  else
    printf '%s\n' 'Aucun job Vigie actif.'
  fi
  exit 0
fi
shift

# Un seul job par conteneur : le verrou reste tenu par spark-submit tant qu’il tourne.
exec 9>"$LOCK"
if ! flock -n 9; then
  printf '%s\n' 'Un job Vigie est déjà actif dans le conteneur. Arrêtez-le avec : bash submit.sh --stop' >&2
  exit 1
fi
echo $$ > "$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT

child=""
stopping=0
on_stop() {
  stopping=1
  [[ -n "$child" ]] && kill -TERM "$child" 2>/dev/null || true
}
trap on_stop INT TERM HUP

export PYSPARK_PYTHON=python3
export PYTHONPATH="/opt/project${PYTHONPATH:+:$PYTHONPATH}"
failures=0
while :; do
  started=$(date +%s)
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name Vigie-Fraud-Monitoring \
    --driver-memory 1g \
    --executor-memory 1g \
    --total-executor-cores 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    --conf spark.jars.ivy=/opt/spark/ivy \
    --conf spark.driver.host=spark-master \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.driver.port=39000 \
    --conf spark.blockManager.port=39001 \
    --conf spark.sql.shuffle.partitions=2 \
    --py-files /opt/project/fraud_core.py \
    "$@" \
    /opt/project/streaming_cassandra.py &
  child=$!
  code=0
  wait "$child" || code=$?
  # Un signal interrompt wait : attendre la fin réelle du job avant de décider.
  while kill -0 "$child" 2>/dev/null; do wait "$child" || code=$?; done
  child=""
  if (( stopping )); then
    printf '%s\n' 'Job arrêté à la demande.'
    exit 0
  fi
  if (( code == 0 )); then
    printf '%s\n' 'Job terminé normalement.'
    exit 0
  fi
  if (( $(date +%s) - started >= STABLE_SECONDS )); then
    failures=0
  fi
  failures=$((failures + 1))
  if (( failures > RESTART_LIMIT )); then
    printf '%s\n' "Job arrêté anormalement ${failures} fois de suite (dernier code ${code}) ; abandon. Consulter les journaux ci-dessus." >&2
    exit "$code"
  fi
  printf '%s\n' "$(date -u +%FT%TZ) Job Spark arrêté (code ${code}) ; nouvelle soumission ${failures}/${RESTART_LIMIT} dans ${RESTART_DELAY} s, reprise depuis les checkpoints." >&2
  sleep "$RESTART_DELAY" &
  child=$!
  wait "$child" || true
  child=""
  if (( stopping )); then
    exit 0
  fi
done
