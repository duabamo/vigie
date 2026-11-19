#!/usr/bin/env python3
"""Génération NDJSON ; publication Kafka facultative via le CLI du conteneur."""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from fraud_core import Generator


def main():
    # Kafka et NDJSON utilisent UTF-8, y compris avec les pipes Windows.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Générateur de transactions financières en dirhams")
    parser.add_argument("--scenario", choices=("mix", "normale", "montant", "voyage", "frequence"), default="mix")
    parser.add_argument("--count", type=int, default=100, help="Nombre de scénarios ; un scénario peut contenir plusieurs transactions")
    parser.add_argument("--interval", type=float, default=.5, help="Délai en secondes entre scénarios")
    parser.add_argument("--seed", type=int, default=42, help="Graine des montants et catégories ; les identifiants restent uniques")
    parser.add_argument("--kafka", action="store_true", help="Publier via docker compose ; sinon écrire sur la sortie standard")
    args = parser.parse_args()
    if args.count < 1 or args.interval < 0:
        parser.error("Le nombre de scénarios doit être positif et le délai positif ou nul.")
    generator = Generator(args.seed)
    process = None
    if args.kafka:
        command = ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
                   "--bootstrap-server", "kafka:19092", "--topic", "financial-transactions",
                   "--property", "parse.key=true", "--property", "key.separator=|",
                   "--producer-property", "acks=all", "--producer-property", "enable.idempotence=true"]
        try:
            process = subprocess.Popen(command, cwd=Path(__file__).resolve().parent, stdin=subprocess.PIPE, text=True, encoding="utf-8")
        except OSError as exc:
            parser.exit(1, f"Publication impossible : {exc}\n")
    output = process.stdin if process else sys.stdout
    count = 0
    try:
        for index in range(args.count):
            scenario = args.scenario
            if scenario == "mix":
                scenario = "voyage" if index % 29 == 8 else "frequence" if index % 31 == 12 else "montant" if index % 17 == 0 else "normale"
            for tx in generator.scenario(scenario):
                payload = json.dumps(tx, ensure_ascii=False, allow_nan=False)
                output.write((tx["card_holder_id"] + "|" if process else "") + payload + "\n")
                output.flush()
                count += 1
            if index < args.count - 1:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    except (BrokenPipeError, OSError) as exc:
        print(f"Le producteur a perdu la connexion : {exc}", file=sys.stderr)
        return 1
    finally:
        if process:
            process.stdin.close()
            try:
                code = process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.terminate()
                code = process.wait(timeout=5)
            if code:
                print(f"Le CLI Kafka s’est arrêté avec le code {code}.", file=sys.stderr)
    print(f"{count} transaction(s) {'publiée(s)' if process else 'générée(s)' }.", file=sys.stderr)
    return code if process else 0


if __name__ == "__main__":
    sys.exit(main())
