# Vérifications effectuées

## Mise en page et navigation au clavier

- Les six pages de démonstration ont été contrôlées dans Edge sur 17 dimensions de fenêtre, de 320 à 1 440 pixels de large, y compris deux formats paysage. Les contrôles couvrent le débordement de page, les panneaux de la barre supérieure et la lisibilité des commandes.
- Le menu mobile maintient le focus au clavier, le rend au bouton à la fermeture et libère le contenu au retour à la mise en page de bureau. L’actualisation conserve le focus sur une alerte encore présente.
- L’émulation tactile à 320 pixels a vérifié le lancement d’un scénario, l’enregistrement d’un suivi, le défilement du tableau, la remise à zéro des filtres et l’export CSV. Les 82 tests locaux ont également réussi.

## Correction de l’ingestion

- 82 tests locaux réussis sous Python 3.12 et 3.15. Ils couvrent le seuil de déplacement à la microseconde, le rejet de `24:00:00`, la lecture d’une ancienne base SQLite et la détection de lignes Cassandra altérées ou en double dans des réponses simulées.
- Syntaxe Python, JavaScript et des deux scripts Bash vérifiée. Le jeu de recette a été construit hors ligne avec 19 transactions valides distinctes, quatre alertes attendues, neuf messages invalides et trois réémissions.
- La version corrigée reste à valider avec Kafka, Spark et Cassandra. Les résultats Docker ci-dessous datent d’avant la correction de l’ingestion.

## Recette distribuée du 18 novembre 2026

Docker Engine 29.4.0 et Compose 5.1.1 ont exécuté Kafka 3.9.0, Spark 3.5.3 et Cassandra 4.1.7 sur le poste Windows. Les trois images téléchargées via le miroir Google ont le même digest que les images officielles de Docker Hub. Spark a résolu les connecteurs Maven et enregistré un worker de deux cœurs. Les trois services d’initialisation ont terminé avec le code 0.

Le jeu de recette porte le préfixe `QA30F6A359AC`. Les effectifs ci-dessous sont limités à ce jeu. Chaque contrôle lit le topic de sortie, les deux tables d’alertes, le journal des transactions et le topic des rejets.

| Contrôle | Transactions distinctes | Alertes distinctes | Rejets distincts |
| --- | ---: | ---: | ---: |
| Seuil de montant, fenêtres, voyages, deux doublons | 19 | 4 | 8 |
| Quatrième paiement dans la fenêtre de fréquence | 20 | 4 | 8 |
| Rejeu de toutes les transactions valides | 20 | 4 | 8 |
| Redémarrage du master et du worker, resoumission et nouveau paiement | 21 | 5 | 8 |
| Remplacement du worker, nouveau paiement traité par le job en cours | 22 | 6 | 8 |

La comparaison porte sur les identifiants, le client, le montant, la règle, le niveau, la date et les transactions associées. Les cas comprennent 10 000 et 10 000,01 DH, les déplacements à 120 et 120,001 secondes, les arrivées désordonnées et les transactions simultanées. Les fenêtres fixes et la mise à jour d’une alerte existante ont été observées dans les deux sorties.

Pendant l’arrêt de Cassandra, l’API a renvoyé HTTP 503 en mode cluster. La séquence arrêt, contrôle d’erreur, redémarrage et nouvelle lecture réussie a duré 31,86 s. Dans Edge, la file d’investigation a ouvert une alerte de déplacement avec ses transactions issues de Cassandra.

**Mesures sur le poste de soutenance.** Une sonde de montant a été observée dans Cassandra 7,52 s après le lancement du producteur. Cette durée inclut le démarrage du CLI Kafka et les lectures de contrôle avec cqlsh. Un flux distinct de 120 paiements, cadencé à quatre par seconde, est devenu entièrement visible dans l’API en 48,84 s, soit 2,46 paiements par seconde sur cette séquence. Ce débit inclut les sondages et le cache de la console. Un seul essai à charge imposée ne mesure ni la capacité maximale ni un percentile de latence.

Le lancement depuis Git Bash préserve désormais le chemin Linux du script. Le contrôle de santé vise le nom réseau du master Spark. Le lecteur de la console décode l’échappement des antislashs introduit par cqlsh 4.1. Un test de non-régression couvre les guillemets, accents et antislashs.

Le manifeste de la recette, les sorties Kafka et les contrôles horodatés sont conservés dans `.runtime/cluster-validation/QA30F6A359AC/`. Les digests, mesures de charge et journaux Spark sont dans `.runtime/review/`. Ces dossiers locaux, exclus par `.gitignore`, n’ont pas été livrés avec le projet. Les preuves de la recette rejouée le 18 novembre 2026 sont livrées dans [`docs/preuves/QAFCAC8F49A7/`](preuves/QAFCAC8F49A7/manifest.json). Pour répéter les essais, suivre la [recette du cluster](recette.md).

## Vérifications locales

Relevé du 17 novembre 2026. Aucun paquet, image Docker ou connecteur Maven n’a été installé pour ces contrôles.

| Contrôle | Résultat |
| --- | --- |
| Suite `unittest` | **53 tests réussis** |
| Syntaxe Python | Compilation des modules et tests réussie |
| Syntaxe JavaScript | Vérification avec le Node.js déjà présent, réussie |
| Syntaxe Bash | `bash -n submit.sh scripts/init-topics.sh` réussi |
| Configuration Compose | `docker compose config --quiet` réussi, sans démarrage de service |
| Checkpoints | Même volume et même chemin déclarés sur master et worker |
| Structure HTML | Identifiants uniques et noms accessibles pour les champs de saisie |
| Documentation | Liens locaux vérifiés |

## Parcours dans le navigateur

Les parcours ont été exécutés avec Microsoft Edge déjà installé, en mode sans fenêtre, sur un serveur local de test. Le fichier de données utilisé était distinct de celui de l’application.

Contrôles réussis :

- Chargement des indicateurs et des graphiques sur ordinateur, largeur 1 440 px.
- Navigation vers les alertes, filtre par règle et recherche sans résultat.
- Ouverture de la fiche d’alerte et enregistrement d’un statut d’investigation.
- Export CSV des résultats filtrés avec identification de l’environnement.
- Mise en pause du flux de démonstration.
- Consultation des états de simulation dans la page du pipeline.
- Injection d’un scénario et apparition de sa nouvelle alerte.
- Changement de période avec chargement des données correspondantes.
- Affichage des six pages à 320, 390 et 768 px, sans débordement horizontal de la page.
- Ouverture du menu mobile et navigation vers les transactions.
- Absence d’exception JavaScript pendant ces parcours.

## Améliorations de l’interface et de la démonstration

Les contrôles locaux supplémentaires couvrent :

- Comparaison de 10 000 et 10 000,01 DH dans l’atelier.
- Comparaison de deux et huit paiements, avec une seule alerte de fréquence actualisée.
- Comparaison d’un déplacement en 40 et en 121 secondes.
- Résultat traçable : client distinct, transactions produites et accès direct au détail d’alerte.
- Accès aux transactions du client depuis une fiche d’investigation.
- Réglage de la cadence du flux et pause.
- Recherche sans accents, tri par priorité et effacement des filtres.
- Navigation précédente / suivante dans une sélection d’alertes.
- Sauvegarde retardée d’une alerte sans fermeture d’une autre fiche ouverte ensuite.
- Cohérence entre les paramètres affichés et ceux exécutés depuis un raccourci.
- Maintien du message d’erreur et restauration du formulaire après un essai refusé.
- Mémorisation de la période dans l’onglet et accès au résultat après un essai sur téléphone.
- Ajout d’un historique récent au redémarrage d’une démo ancienne, en conservant le suivi antérieur.

Cette passe porte sur l’interface et la démonstration locale. Aucune validation du pipeline distribué n’a été engagée.

Les tableaux conservent un défilement horizontal interne lorsque leurs colonnes dépassent la largeur du téléphone.

Captures : [ordinateur](apercu.png), [mobile](apercu-mobile.png), [atelier](apercu-atelier.png), [résultat sur téléphone](apercu-atelier-mobile.png).

## Nouvelle revue : conformité, fiabilité et interface

Résultats exécutés sur le poste Windows, avec Python, Node.js, Git Bash et Edge déjà installés :

- 53 tests Python, dont les nouveaux cas de fréquence à huit membres avec ordre inversé et rejeu, les anciennes alertes, la limite Cassandra avant filtrage, et le producteur UTF-8 même avec une sortie ASCII configurée.
- Compilation Python, syntaxe JavaScript et Bash, validation de la configuration Compose.
- 30 combinaisons de navigation : six pages à 320, 390, 768, 1 024 et 1 440 px, sans débordement horizontal global.
- Accès direct aux alertes critiques ouvertes, tri par priorité, détail avec deux transactions pour un déplacement et huit pour la fréquence.
- Enregistrement du suivi, recherche vide, export des alertes filtrées.
- Scénarios exécutés dans le navigateur : montant à 10 000 et à 10 000,01 DH, fréquence à huit paiements, déplacement.
- Filtre des transactions signalées : les huit membres de l’essai de fréquence sont retrouvés.
- Menu mobile, erreur réseau simulée, maintien des données reçues et récupération après rétablissement.
- Aucune exception JavaScript pendant ces parcours.

Les captures liées ci-dessus ont été renouvelées. Le serveur de revue utilise une base distincte sous `.runtime/review/` ; la base habituelle n’a pas été modifiée.

## Corrections après audit : seuils, robustesse et contrat (18 novembre 2026)

Essais exécutés sur le cluster Docker Desktop du poste Windows, job Spark redémarré sur les checkpoints existants :

- 76 tests Python, dont les seuils partagés, la politique de relance face à Cassandra, le contrat d’horodatage et les corrections de la console.
- Recette `validate_cluster.py` (`start`, `check`, `update`, `replay`, `probe`) : **PASS** à chaque phase, après chaque correction. Depuis la dernière correction, elle compare aussi le motif des alertes : Spark et la démonstration écrivent le même texte. Preuves : [`docs/preuves/QAFCAC8F49A7/`](preuves/QAFCAC8F49A7/manifest.json).
- Seuils centralisés dans `fraud_core.py` : expressions Spark générées identiques au texte d’origine (`INTERVAL 2 MINUTES`, `2 minutes`, `INTERVAL 5 MINUTES`).
- Coupure courte de Cassandra : absorbée par les relances, aucune requête relancée.
- Coupure longue de Cassandra (219 s) : seules les requêtes en échec sont relancées depuis leur checkpoint ; les rejets continuent pendant la panne ; l’alerte arrive dans Cassandra et `fraud-alerts` 16 s après le retour du service.
- Arrêt brutal du driver (`kill -9`) : nouvelle soumission par `submit.sh`, job actif en 29 s.
- Second lancement de `submit.sh` refusé ; `bash submit.sh --stop` arrête un job supervisé ou orphelin.
- Contrat partagé : 32 messages publiés dans Kafka et validés en Python. Les deux moteurs rendent le même verdict pour 31 cas (avant correction : 24). Écart restant : une heure à un chiffre (`T9:05:00Z`) est acceptée par Spark et rejetée par la démonstration.

- Motif du déplacement impossible identique dans les deux moteurs : « Casablanca → Tanger : 291 km en 120 s ; vitesse estimée 8 740 km/h. » Avant, Spark écrivait « 291.0 km en 120.0 s ; vitesse estimée 8740.0 km/h. » et la démonstration « 291 km en 120 s  soit 8 740 km/h. ».
- Console : JSON invalide signalé en français, requêtes `HEAD` acceptées, en-tête `Host` limité à `localhost`, `127.0.0.1` et aux adresses IP (réponse 421 sinon). Sorties du serveur et du producteur en UTF-8 sous Windows.

Non vérifié : `Ctrl+C` tapé dans un terminal interactif. L’arrêt par signal et par `--stop` a été vérifié.

## Refonte de l’interface (19 novembre 2026)

Nouvelle identité visuelle : barre latérale anthracite, couleur principale orange, cartes blanches sur fond gris clair. Seuls les fichiers de `web/` ont changé ; l’API, le serveur et les règles de détection sont inchangés.

- Tableau de bord en deux colonnes : quatre indicateurs avec tendance et courbe, vue globale des risques par gravité, évolution des transactions et des alertes, dernières alertes avec gravité, alertes critiques, répartition par règle, indicateurs de risque et activité par ville. Les tendances comparent les deux moitiés de la période sur les données reçues de l’API.
- En-tête : recherche d’alertes, période, actualisation, export, notifications des alertes à traiter, paramètres et indicateur de mode.
- 76 tests Python réussis ; syntaxe JavaScript vérifiée ; aucune erreur dans la console du navigateur.
- Six pages contrôlées à 375, 768, 1 024 et 1 440 px : aucun débordement horizontal. Menu mobile, notifications, fiche d’investigation, enregistrement du suivi et scénario de déplacement vérifiés dans le navigateur. Mode cluster vérifié sur la console reliée à Cassandra.
- Captures renouvelées ; les captures « mobiles » sont prises à 500 px, largeur minimale de la fenêtre d’Edge sans interface.
