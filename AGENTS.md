# Project overview

Vigie est un mini-projet français de détection de fraude. Le sujet (PDF de l’enseignant) n’est pas inclus dans le dépôt ; ses exigences sont reprises dans docs/audit.md et docs/rapport.md. Deux modes : démonstration Python/SQLite et cluster Kafka/Spark/Cassandra. Aucun paquet Python externe n’est nécessaire à la console ou aux tests locaux.

## Commands

```bash
python3 app.py --help
python3 app.py
python3 -m unittest discover -v
python3 -m compileall -q app.py fraud_core.py storage.py producer.py streaming_cassandra.py
bash -n submit.sh scripts/init-topics.sh
```

Le test HTTP ouvre un port temporaire sur 127.0.0.1. `docker compose up -d` et `bash submit.sh` nécessitent un environnement Docker autorisé ; ils téléchargent des images et des connecteurs au premier lancement. Ne pas les exécuter sans cet accord. Le relevé de la recette distribuée exécutée sur Docker Desktop se trouve dans docs/validation.md.

## Conventions

- Interface, messages métier et documentation en français ; identifiants techniques du sujet conservés.
- Horodatages UTC dans le contrat ; affichage Africa/Casablanca ; montants en DH.
- `fraud_core.py` : règles Python. `streaming_cassandra.py` : expressions Spark natives, sans UDF Python.
- `web/` : HTML/CSS/JS natifs ; aucune dépendance ou ressource distante.
- Les données de fonctionnement résident dans `.runtime/`, ignoré par Git.
- Un identifiant d’alerte déterministe pour chaque montant, paire de transactions ou fenêtre/client.

## Definition of done

Tests locaux et contrôles de syntaxe réussis. Règles cohérentes entre démonstration et cluster. Livrables du sujet conservés. Documenter séparément les contrôles réellement exécutés et les validations Docker/Spark non exécutées.

## Never do

- Ne pas installer ou ajouter de dépendance sans accord. Préparer les configurations ne vaut pas autorisation d’installation.
- Ne pas présenter la démonstration comme une mesure de Kafka, Spark ou Cassandra.
- Ne pas effacer de données ou checkpoints, supprimer de fichiers, committer, pousser ou publier sans instruction explicite.
- Ne pas promettre une transaction atomique entre Kafka et Cassandra : `foreachBatch` est au moins une fois.

## Code Review Rules

Vérifier les bornes, les événements désordonnés, les clés Cassandra stables et les rejouements. Les checkpoints doivent être partagés par driver et worker. Le suivi d’une alerte ne doit pas dépendre du cache HTTP. Signaler une lecture partielle ou indisponible au lieu de substituer des données de démonstration.
