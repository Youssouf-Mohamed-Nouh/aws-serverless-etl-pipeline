# Pipeline ETL Serverless AWS - Données Immobilières

## Présentation

Ce projet consiste à concevoir et développer une pipeline ETL (Extract, Transform, Load) serverless sur AWS permettant d'automatiser le traitement de données immobilières.

L'objectif est de transformer des fichiers CSV bruts déposés dans Amazon S3 en données nettoyées et optimisées au format Parquet, prêtes pour l'analyse.

---

## Objectifs

* Automatiser l'ingestion des données immobilières.
* Détecter automatiquement l'arrivée de nouveaux fichiers.
* Nettoyer et transformer les données avec PySpark.
* Optimiser le stockage au format Parquet.
* Centraliser les métadonnées dans AWS Glue Data Catalog.
* Préparer les données pour l'analyse avec Amazon Athena.

---

## Architecture

```text
S3 (raw/)
    │
    ▼
EventBridge
    │
    ▼
AWS Lambda
    │
    ▼
Glue Crawler
    │
    ▼
Glue Data Catalog
    │
    ▼
Glue Job (PySpark)
    │
    ▼
S3 (processed/)
    │
    ▼
Amazon Athena
```

---

## Services AWS utilisés

* Amazon S3
* AWS Glue
* AWS Glue Crawler
* AWS Glue Data Catalog
* AWS Lambda
* Amazon EventBridge
* Amazon Athena
* IAM

---

## Fonctionnement de la pipeline

### 1. Ingestion

Un fichier CSV est déposé dans le dossier :

```text
s3://ysf-data-space-2026/raw/
```

### 2. Détection automatique

Amazon EventBridge détecte l'arrivée du nouveau fichier.

### 3. Déclenchement

Une fonction AWS Lambda est exécutée automatiquement et lance :

* Le Glue Crawler
* Le Job ETL Glue

### 4. Catalogage

Le Glue Crawler analyse les données et met à jour automatiquement le Data Catalog.

### 5. Transformation

Le Job Glue développé en PySpark réalise :

* Le renommage des colonnes
* Le nettoyage des valeurs manquantes
* La suppression des données invalides
* Le traitement des valeurs aberrantes (Winsorization avec IQR)
* La préparation des données pour l'analyse

### 6. Stockage

Les données transformées sont enregistrées au format Parquet dans :

```text
s3://ysf-data-space-2026/processed/housing_parquet/
```

### 7. Analyse

Les données sont ensuite exploitables via Amazon Athena.

---

## Technologies utilisées

### Cloud

* AWS S3
* AWS Glue
* AWS Lambda
* Amazon EventBridge
* Amazon Athena

### Data Engineering

* Python
* PySpark
* ETL
* Data Cleaning
* Data Transformation

### Formats de données

* CSV
* Parquet

---

## Structure du projet

```text
.
├── scripts/
│   └── glue_etl.py
│
├── docs/
│   └── architecture.png
│
├── data/
│   └── Housing.csv
│
└── README.md
```

---

## Compétences démontrées

* Conception de pipelines ETL sur AWS
* Développement de traitements distribués avec PySpark
* Automatisation Event-Driven
* Gestion des métadonnées avec AWS Glue Data Catalog
* Optimisation des données avec le format Parquet
* Architecture Data Lake sur Amazon S3
* Analyse de données avec Amazon Athena

---

## Auteur

Projet réalisé dans le cadre d'un parcours de spécialisation en Data Engineering et Cloud Computing sur AWS.
