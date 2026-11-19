# Démonstration et recette

## Présentation locale — environ cinq minutes

Lancer `python3 app.py` et ouvrir http://127.0.0.1:8000.

1. Montrer la mention « Démonstration » et la période sélectionnée. Expliquer que les indicateurs décrivent les événements chargés, sans mesure de performance du cluster.
2. Lire les quatre indicateurs de la vue d’ensemble et la répartition des alertes. Passer de la dernière heure aux quinze dernières minutes.
3. Ouvrir « Atelier de démo » et mettre le flux en pause. Lancer « Paiement habituel » : le résultat doit afficher une transaction et aucune alerte.
4. Choisir « Montant à examiner ». Comparer 10 000 DH, sans alerte, et 10 000,01 DH, avec une alerte. Ouvrir celle-ci directement dans le résultat.
5. Choisir Casablanca → Tanger en 40 secondes : une alerte de déplacement impossible est attendue. Refaire l’essai avec un intervalle de 121 secondes : aucune alerte de déplacement n’est attendue.
6. Comparer deux et trois paiements rapprochés : le seuil de fréquence doit être visible. Un essai à huit paiements doit conserver une seule alerte, avec un compteur de huit.
7. Utiliser « Voir ces transactions » pour retrouver le client de l’essai. Dans « Alertes », marquer une alerte en cours puis confirmée. Parcourir les fiches avec « Précédente / Suivante » et recharger pour vérifier la persistance du suivi.
8. Exporter les résultats filtrés. Vérifier les colonnes françaises, le montant, l’horodatage UTC et la mention « Démonstration ».

La pause arrête uniquement la génération automatique. Les essais manuels restent disponibles. Changer la cadence entre 1, 3 et 5 secondes dans l’atelier, puis reprendre le flux.

## Tests automatisés locaux

```bash
python3 -m unittest discover -v
```

Le test HTTP utilise un port éphémère sur l’interface locale et un fichier de données temporaire. Il ne modifie pas les données de la démonstration.

## Recette du cluster

### Vérification automatisée

Après le démarrage des services et du job Spark, lancer depuis la racine :

```bash
python scripts/validate_cluster.py start
python scripts/validate_cluster.py check
python scripts/validate_cluster.py update
python scripts/validate_cluster.py check
python scripts/validate_cluster.py replay
python scripts/validate_cluster.py check
```

Le script publie des transactions identifiées par un préfixe unique. Il vérifie les bornes, les arrivées désordonnées, les doublons, les accents, les rejets, les deux tables d’alertes et le topic Kafka. Les résultats et le manifeste sont conservés dans `.runtime/cluster-validation/`. Il ne supprime aucune donnée ni aucun checkpoint. Le délai de vérification est réglable avec `--timeout`.

Après un redémarrage du job conservant ses checkpoints, lancer `python scripts/validate_cluster.py probe`, puis `python scripts/validate_cluster.py check`. Cette séquence vérifie la présence des alertes antérieures et le traitement d’une nouvelle transaction. Le script ne démarre ni n’arrête les services lui-même. Les journaux du job contiennent désormais des entrées `PROGRESS` pour les micro-lots, leur durée, le débit et l’état des opérateurs.

Cette partie nécessite l’environnement et les téléchargements décrits dans le README. Les commandes ci-dessous constituent un protocole à exécuter, pas un relevé de résultats déjà obtenus.

### A. Démarrage

```bash
docker compose up -d
docker compose ps -a
```

Attendu : Kafka, Cassandra, master et worker en fonctionnement ; initialisations terminées avec le code 0. Le master sur http://127.0.0.1:8080 doit afficher un worker avec deux cœurs.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list
docker compose exec cassandra cqlsh -e 'DESCRIBE KEYSPACE fraud_monitoring'
```

Attendu : les trois topics et les trois tables du schéma.

### B. Soumission et première alerte

Dans un terminal, lancer `bash submit.sh`. Vérifier l’application sur http://127.0.0.1:4040 et ses cinq requêtes de streaming. Dans un autre terminal :

```bash
python3 producer.py --kafka --scenario montant --count 1
docker compose exec cassandra cqlsh -e 'SELECT JSON * FROM fraud_monitoring.alerts_by_day LIMIT 5'
```

Attendu : présence d’une alerte `MONTANT_ELEVE` à 14 500 DH. Ouvrir ensuite `python3 app.py --mode cluster` et retrouver le même identifiant. Consulter également `fraud-alerts` avec le consommateur CLI.

### C. Fenêtres et déplacements

```bash
python3 producer.py --kafka --scenario voyage --count 1
python3 producer.py --kafka --scenario frequence --count 1
```

Attendu : une alerte de déplacement et une alerte de fréquence dès le traitement des micro-lots. La fréquence utilise le mode `update`, sans attendre la clôture du watermark. Vérifier aussi `related_transaction_ids` dans le payload Cassandra et dans Kafka : deux identités pour le déplacement, trois pour la fréquence. Ajouter un quatrième paiement dans la même fenêtre : la même alerte doit contenir quatre identités. Le journal de la console doit signaler chaque membre.

Pour tester les bornes, produire des messages JSON spécifiques avec les six champs du contrat, des identifiants distincts et des horodatages récents. Publier chaque ligne avec le CLI `kafka-console-producer.sh` :

| Cas | Attendu |
| --- | --- |
| Montant = 10 000 DH | Pas d’alerte de montant |
| Montant = 10 000,01 DH | Une alerte de montant |
| Deux villes éloignées, même client, intervalle = 120 s | Alerte de déplacement |
| Même paire, intervalle = 120,001 s | Pas d’alerte de déplacement |
| Trois identifiants distincts, même client, même fenêtre | Une alerte de fréquence |
| Troisième transaction dans la fenêtre suivante | Pas d’alerte pour la fenêtre incomplète |
| Même identifiant de transaction réémis | Pas de nouvelle identité d’alerte ; fréquence distincte inchangée |
| Ville inconnue, champ absent, montant négatif, fuseau absent | Message dans `invalid-transactions` |

Pour les arrivées désordonnées, publier l’événement le plus récent avant le plus ancien. Rester dans la tolérance du watermark ; vérifier la conservation de l’ordre chronologique dans l’alerte.

### D. Reprise et idempotence

1. Produire une série, attendre son apparition dans Cassandra et noter les identifiants d’alerte.
2. Arrêter le job avec `Ctrl+C`, conserver tous les volumes, puis relancer `bash submit.sh`.
3. Vérifier la reprise des cinq requêtes et l’absence d’augmentation des identités déjà présentes dans Cassandra.
4. Récréer le worker avec `docker compose up -d --force-recreate spark-worker`, puis attendre son enregistrement et vérifier la reprise de traitement. Le volume de checkpoints doit être monté sur le driver et le worker.
5. Produire une nouvelle série et vérifier sa persistance.

Pour éprouver précisément le double sink, provoquer dans un environnement de test un arrêt après la publication Kafka et avant validation du micro-lot. Une publication répétée est autorisée par la garantie au moins une fois. Vérifier qu’un consommateur appliquant des upserts par `alert_id` conserve une seule identité, avec les dernières valeurs de la fenêtre.

### E. Erreurs de supervision

Arrêter Cassandra avec `docker compose stop cassandra` après chargement de la console cluster. Au prochain chargement, une erreur doit apparaître. La console peut garder la dernière vue reçue, signalée comme telle, et ne doit jamais afficher les données de démonstration comme des données du cluster. Redémarrer Cassandra avec `docker compose start cassandra`.

Les indicateurs financiers sont signalés comme provisoires si une alerte a été chargée avant sa transaction. Le statut « Port joignable » ne vaut pas validation de la chaîne entière.

## Protocole de mesure

Les observations du 18 novembre figurent dans [validation.md](validation.md). Pour une nouvelle campagne :

| Mesure | Méthode | Valeur |
| --- | --- | --- |
| Temps de démarrage du cluster | Du `up` à l’enregistrement du worker | À mesurer |
| Latence d’une alerte de montant | Horodatage de production jusqu’à présence dans Cassandra | À mesurer |
| Débit soutenu | Nombre de transactions traitées / durée avec flux continu | À mesurer |
| Temps de reprise | Redémarrage du job jusqu’au premier nouveau lot validé | À mesurer |
| Taille de l’état | Métriques Structured Streaming après flux et progression du watermark | À mesurer |

Conserver la configuration matérielle, les versions et les journaux avec les mesures. Une moyenne sur un unique scénario ne permet pas de caractériser le débit ou la latence de la chaîne.
