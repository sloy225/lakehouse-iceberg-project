# Architecture Modern Data Lakehouse (Apache Iceberg, Spark, Trino & S3)

Ce projet implémente de bout en bout une architecture **Modern Data Lakehouse** locale et conteneurisée via Docker Compose. Il démontre comment unifier la flexibilité et le faible coût du stockage objet S3 avec la gouvernance, les transactions ACID et les performances analytiques d'un entrepôt de données (Data Warehouse), sans verrouillage propriétaire (*vendor lock-in*).

---

## 1. Problématique & Choix d'Architecture

Les architectures Data traditionnelles souffrent souvent d'une dualité coûteuse :
* **Data Lakes classiques (Parquet brut sur S3)** : économiques et évolutifs, mais dépourvus de transactions ACID, sujets à la corruption lors d'écritures concurrentes, et complexes à mettre à jour (`UPDATE`/`DELETE`).
* **Data Warehouses propriétaires** : performants en SQL, mais coûteux et couplant fortement le stockage au moteur de calcul.

### La solution retenue : le pattern Lakehouse ouvert
Ce projet découple totalement **le stockage**, **les métadonnées** et **les moteurs de calcul** :

```text
+-----------------------------------------------------------------------------+
|                        COUCHE DE CALCUL & CONSOMMATION                      |
|                                                                             |
|   +-----------------------------------+   +-----------------------------+   |
|   |   Apache Spark 3.5 + JupyterLab   |   |          Trino 464          |   |
|   |  • Ingestion & Pipelines ETL      |   |  • Requêtage SQL analytique |   |
|   |  • Opérations ACID (UPDATE/MERGE) |   |  • Exposition BI (Power BI) |   |
|   |  • Port : 8888 (UI) / 4040 (Jobs) |   |  • Port : 8081              |   |
|   +-----------------------------------+   +-----------------------------+   |
|                     |                                     |                 |
+---------------------|-------------------------------------|-----------------+
                      | (REST API)                          | (REST API)
                      v                                     v
+-----------------------------------------------------------------------------+
|                     COUCHE DE MÉTADONNÉES & GOUVERNANCE                     |
|                                                                             |
|                +-------------------------------------------+                |
|                |     Apache Iceberg REST Catalog 1.6.0     |                |
|                |  • Point de vérité unique des snapshots   |                |
|                |  • Verrouillage optimiste multi-moteurs   |                |
|                |  • Port : 8181                            |                |
|                +-------------------------------------------+                |
+-----------------------------------------------------------------------------+
                      |                                     |
                      | (Protocole S3A)                     | (S3 Native FS)
                      v                                     v
+-----------------------------------------------------------------------------+
|                          COUCHE DE STOCKAGE OBJET                           |
|                                                                             |
|                +-------------------------------------------+                |
|                |       Stockage Compatible S3 (Silo)       |                |
|                |  • Bucket : lakehouse-warehouse           |                |
|                |  • Fichiers de données : Parquet (ZSTD)   |                |
|                |  • Métadonnées : Manifests & Snapshots    |                |
|                |  • Ports : 9000 (API S3) / 9001 (Console) |                |
|                +-------------------------------------------+                |
+-----------------------------------------------------------------------------+
```

---

## 2. Détail des Composants de la Stack

| Service Docker | Image | Rôle dans l'architecture | Accès / Ports |
| :--- | :--- | :--- | :--- |
| **`lakehouse-minio`** | `pgsty/silo` | Stockage objet compatible S3 hébergeant physiquement les fichiers Parquet compressés (ZSTD) et l'arborescence des métadonnées Iceberg. | API : `localhost:9000`<br>Console : `http://localhost:9001` |
| **`lakehouse-catalog`** | `tabulario/iceberg-rest:1.6.0` | Catalogue centralisé exposant une API REST standard. Permet à Spark et Trino de lire et modifier simultanément les mêmes tables sans conflit. | `http://localhost:8181` |
| **`lakehouse-spark`** | `jupyter/pyspark-notebook:spark-3.5.0` | Moteur de traitement distribué dédié à l'ingestion ETL, aux transformations Médaillon et à l'exploration interactive via notebooks. | JupyterLab : `http://localhost:8888?token=lakehouse`<br>Spark UI : `http://localhost:4040` |
| **`lakehouse-trino`** | `trinodb/trino:464` | Moteur SQL MPP (Massively Parallel Processing) haute performance permettant d'interroger la couche Gold et de connecter des outils BI (Power BI, DBeaver). | `http://localhost:8081` |

---

## 3. Fonctionnalités Clés Implémentées

### A. Architecture Médaillon (`bronze` → `silver` → `gold`)
Le pipeline `notebooks/medallion_pipeline.py` structure les données en trois niveaux de maturité :
1. **`lakehouse.bronze.orders`** : Zone d'atterrissage brute conservant l'historique exact des événements ingérés (incluant doublons et anomalies).
2. **`lakehouse.silver.orders_clean`** : Zone nettoyée et normalisée (dédoublonnage sur `order_id`, filtrage des montants invalides, typage fort des dates et horodatage de traitement `processed_at`).
3. **`lakehouse.gold.daily_revenue`** : Couche d'agrégation métier prête pour la consommation analytique (calcul du volume de commandes validées et du chiffre d'affaires journalier).

### B. Transactions ACID sur Stockage Objet
Contrairement au format Parquet classique où modifier une ligne oblige à réécrire toute une partition, Apache Iceberg gère des opérations **`UPDATE`**, **`DELETE`** et **`MERGE INTO`** atomiques sur S3 grâce à un système de snapshots immuables et de fichiers de suppression (*delete files*).

### C. Time Travel (Audit & Reproductibilité)
Chaque transaction génère un nouvel identifiant de snapshot (`snapshot_id`). Le notebook `01_exploration_iceberg.ipynb` démontre la capacité à :
* Lister l'historique complet des commits via la table système `.snapshots`.
* Interroger l'état exact d'une table avant une mise à jour (`option("snapshot-id", ...)`), garantissant la reproductibilité des analyses et l'auditabilité des données.

### D. Schema Evolution (Évolution de schéma à chaud)
L'ajout de nouvelles colonnes (`ALTER TABLE lakehouse.silver.orders_clean ADD COLUMN payment_method STRING`) s'effectue uniquement au niveau des métadonnées Iceberg, sans réécriture coûteuse des fichiers Parquet historiques (les anciens enregistrements retournent automatiquement `NULL`).

### E. Interopérabilité Multi-Moteurs (Spark ↔ Trino)
Les tables créées et mises à jour par **PySpark** sont immédiatement visibles et requêtables en SQL standard par **Trino** via le catalogue REST partagé, illustrant la séparation totale entre moteur d'ingestion et moteur de restitution BI.

---

## 4. Guide d'Installation et d'Exécution

### Prérequis
* **Docker Desktop** (avec moteur WSL2 activé sous Windows) et **Docker Compose**
* **Git**

### Étape 1 : Cloner le dépôt et préparer les configurations
```bash
git clone [https://github.com/sloy225/lakehouse-iceberg-project.git](https://github.com/sloy225/lakehouse-iceberg-project.git)
cd lakehouse-iceberg-project

# Créer les fichiers de configuration locaux à partir des modèles
cp .env.example .env
cp trino/catalog/lakehouse.properties.example trino/catalog/lakehouse.properties
```
*(Renseignez vos identifiants S3 dans `.env` ainsi que dans `trino/catalog/lakehouse.properties`.)*

### Étape 2 : Démarrer l'infrastructure Lakehouse
```bash
docker compose up -d
```
Vérifiez que les 4 conteneurs sont actifs :
```bash
docker compose ps
```

### Étape 3 : Initialiser le bucket S3
1. Ouvrez la console S3 sur **`http://localhost:9001`**.
2. Connectez-vous avec les identifiants définis dans `.env` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`).
3. Créez un bucket nommé exactement **`lakehouse-warehouse`**.

### Étape 4 : Exécuter le pipeline Médaillon (Spark)
```bash
docker exec -it lakehouse-spark /usr/local/spark/bin/spark-submit /home/jovyan/work/medallion_pipeline.py
```

### Étape 5 : Interroger les données en SQL (Trino)
```bash
docker exec -it lakehouse-trino trino --catalog lakehouse
```
Exemples de requêtes SQL analytiques :
```sql
-- Lister les couches du Lakehouse
SHOW SCHEMAS;

-- Consulter les indicateurs métier (Couche Gold)
SELECT * FROM gold.daily_revenue ORDER BY order_date;

-- Inspecter l'historique des snapshots Iceberg (Couche Silver)
SELECT snapshot_id, committed_at, operation FROM silver."orders_clean$snapshots";
```

---

## 5. Structure du Projet

```text
lakehouse-iceberg-project/
├── docker-compose.yml                  # Orchestration des 4 microservices du Lakehouse
├── .env.example                        # Modèle des variables d'environnement S3
├── .gitignore                          # Exclusion des secrets (.env) et fichiers temporaires
├── spark-defaults/
│   └── spark-defaults.conf             # Configuration Spark (extensions Iceberg, REST Catalog, S3A)
├── trino/
│   └── catalog/
│       └── lakehouse.properties.example # Configuration du connecteur Iceberg REST pour Trino
└── notebooks/
    ├── test_iceberg.py                 # Script de validation de connectivité Spark <-> S3
    ├── medallion_pipeline.py           # Pipeline ETL automatisé (Bronze -> Silver -> Gold)
    └── 01_exploration_iceberg.ipynb    # Notebook interactif (Time Travel & Schema Evolution)
```

---

## 6. Pistes d'Évolution

* **Connexion BI** : Brancher **Power BI** (via connecteur ODBC Trino) ou **DBeaver** sur `localhost:8081` pour visualiser la couche Gold en temps réel.
* **Maintenance Iceberg** : Automatiser la compaction des petits fichiers (`rewrite_data_files`) et la purge des anciens snapshots (`expire_snapshots`).
* **Orchestration** : Intégrer **Apache Airflow** ou **Dagster** pour planifier l'exécution quotidienne du pipeline Médaillon.