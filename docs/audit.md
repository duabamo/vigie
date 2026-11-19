# Audit du mini-projet 06

Revue des 17 et 18 novembre 2026 à partir du PDF fourni, du code et des essais locaux et distribués.

## Verdict

**Les trois livrables et le déploiement demandé ont été vérifiés sur le poste cible.** Le flux Kafka a alimenté cinq requêtes Spark. Les alertes publiées dans Kafka ont été comparées aux lignes de Cassandra, puis consultées dans la console. [Résultats de la recette](validation.md).

## Correspondance avec le PDF

| Exigence | Réalisation inspectée | État de preuve |
| --- | --- | --- |
| Flux JSON avec les six champs du sujet | `producer.py`, validation Python et schéma Spark | 19 transactions valides ingérées, huit messages invalides rejetés |
| Topic d’entrée `financial-transactions` | `scripts/init-topics.sh`, producteur et source Spark | Publication et lecture sur le broker Docker |
| Montant strictement supérieur à 10 000 DH | Règles Python et filtre Spark | 10 000 et 10 000,01 DH distingués |
| Fenêtres de deux minutes | Agrégation par client, fenêtre fixe, mode update | Quatrième paiement ajouté à la même alerte |
| Déplacements impossibles | Jointure temporelle bornée, géographie et vitesse | Intervalle de 120 s inclus, cas à 120,001 s exclu, événements désordonnés vérifiés |
| Topic de sortie `fraud-alerts` | Sink Kafka dans `foreachBatch` | Identités et contenu comparés avec Cassandra après rejeu |
| Keyspace `fraud_monitoring`, table `detected_alerts` | `schema.cql` | Initialisation CQL exécutée deux fois |
| Partitionnement par date / client | `((alert_date, card_holder_id), timestamp, alert_id)` | Lectures ciblées par jour et client réussies |
| Connecteur Cassandra | Packages Maven dans `submit.sh`, sink DataFrame | Connecteurs résolus, écritures et lectures vérifiées |
| Cluster conteneurisé et listeners internes | `compose.yaml`, listener `kafka:19092`, volumes de checkpoints partagés | Worker enregistré, reprise après redémarrage et remplacement du worker |
| Trois livrables ingénieur | `submit.sh`, `streaming_cassandra.py`, `schema.cql` | Script de soumission, job et DDL exécutés |

Le sujet ne demande pas de modèle ML, d’authentification, de cloud ou de nouvelle infrastructure. L’interface est un complément utile à la présentation et à l’analyse. Les améliorations conservent HTML/CSS/JavaScript natifs, Python standard et les technologies imposées.

## Corrections apportées

### Signalement et preuves

Avant correction, une alerte de déplacement ou de fréquence ne signalait que la transaction finale dans les indicateurs et le journal. Les nouvelles alertes transportent maintenant `related_transaction_ids` dans les deux moteurs :

- montant : la transaction concernée ;
- déplacement : les deux opérations de la paire ;
- fréquence : tous les identifiants distincts de la fenêtre, mis à jour sans créer une autre identité d’alerte.

Le taux et le montant signalés reposent sur l’union de ces identités, limitée aux transactions chargées. Une opération couverte par plusieurs règles est comptée une seule fois. Le détail montre les preuves disponibles. Les anciens payloads restent compatibles et leur manque de preuves est signalé.

### Portabilité et lecture partielle

Le test du producteur utilisait une commande `python3` indisponible sur ce poste Windows. Il utilise désormais l’interpréteur du test. Les échanges NDJSON, le pipe Kafka et les sorties de cqlsh sont lus ou écrits en UTF-8.

La limite de lecture Cassandra est maintenant conservée avant le filtrage par période. Une lecture limitée ne peut plus sembler complète uniquement parce que les lignes hors période ont été retirées.

### Interface

- Typographie et contrastes renforcés, hiérarchie des indicateurs plus nette.
- File d’investigation avec raccourci vers les critiques ouvertes et tri par gravité.
- Montant signalé distinct du volume total, clairement décrit comme autre chose qu’une perte confirmée.
- Fiches avec liste des transactions ayant contribué à la détection.
- Carte récente des alertes adaptée au téléphone ; graphique redimensionné avec des axes lisibles.
- Couleurs cohérentes par règle : déplacement rouge, montant ambre, fréquence vert.
- Heure de réception visible et état de connexion interrompue explicite.
- Diagramme corrigé : Spark produit les écritures Cassandra et Kafka ; Cassandra n’est pas représenté comme un consommateur du topic d’alertes.
- Suppression d’un motif de barres décoratif qui ne correspondait à aucune donnée.

## Vérifications effectuées

53 tests Python réussis. Syntaxes JavaScript et Bash vérifiées. Cluster démarré et pipeline exécuté.

Les six pages ont été parcourues dans Edge à cinq largeurs, soit 30 combinaisons sans débordement horizontal global. Les scénarios, les preuves, la file critique, le suivi, la recherche, le CSV et la récupération réseau ont été vérifiés. Les tableaux détaillés conservent leur propre défilement horizontal lorsque nécessaire.

Voir [validation.md](validation.md) pour le relevé et [les captures](../README.md) pour l’interface.

## Portée de la validation

La recette couvre le déploiement à un broker et un nœud Cassandra prévu pour la soutenance. Le consommateur Kafka doit appliquer les mises à jour par identifiant d’alerte. Le rejeu est possible entre l’écriture des sorties et la validation du micro-lot. Les mesures du poste et les limites du test figurent dans [validation.md](validation.md).
