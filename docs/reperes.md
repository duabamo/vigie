# Repères du dépôt

1. Vigie associe un pipeline de détection de fraude et une console en français.
2. Le sujet d’origine n’est pas inclus ; ses exigences sont reprises dans `docs/audit.md`.
3. `python3 app.py` lance la démonstration sans paquet supplémentaire.
4. `web/` contient l’interface native HTML, CSS et JavaScript.
5. `fraud_core.py` définit les règles et le générateur de transactions.
6. `storage.py` conserve la démonstration et le suivi dans SQLite.
7. `streaming_cassandra.py` exécute les six requêtes Spark du cluster.
8. `compose.yaml`, `schema.cql` et `submit.sh` décrivent son lancement.
9. `python3 -m unittest discover -v` vérifie les règles, la persistance et l’API.
10. Le mode cluster lit Cassandra avec `cqlsh` dans Docker, sans pilote Python.
