# Pipeline NYC Taxi — Snowflake + dbt

Pipeline de donnees pour l'analyse des trajets Yellow Taxi de New York (janvier 2024 - janvier 2025, ~40M trajets).

## Architecture

```
Fichiers Parquet -> RAW -> STAGING -> FINAL
                    |        |         |
             Donnees brutes  Nettoyage  Tables analytiques
```

**Base de donnees** : `NYC_TAXI_DB` | **Warehouse** : `NYC_TAXI_WH`

| Schema | Contenu |
|---|---|
| **RAW** | Donnees brutes importees depuis les fichiers parquet |
| **STAGING** | Donnees nettoyees (filtres qualite) + enrichissements (duree, vitesse, categories) |
| **FINAL** | `daily_summary`, `zone_analysis`, `hourly_patterns` |

## Prerequis

- Python 3.12
- Un compte Snowflake 

## Installation

```bash
uv venv --python python3.12
source .venv/bin/activate
uv pip install dbt-snowflake
```

## Configuration

Creer un fichier `.env` a la racine :

```bash
export SNOWFLAKE_ACCOUNT='orgname-accountname'
export SNOWFLAKE_USER='...'
export SNOWFLAKE_PASSWORD='...'
export SNOWFLAKE_ROLE='ACCOUNTADMIN'
```

## Utilisation

```bash
source .env
cd nyc_taxi_dbt

dbt debug           # Verifier la connexion
dbt run             # Executer les transformations
dbt test            # Lancer les 17 tests de qualite
dbt docs generate   # Generer la documentation
dbt docs serve      # Ouvrir la documentation dans le navigateur
```

## Structure dbt

```
nyc_taxi_dbt/models/
├── staging/
│   └── stg_yellow_taxi_trips.sql    # Nettoyage : filtres montants, distances, durees
├── intermediate/
│   └── int_trip_metrics.sql         # Enrichissements : duree, vitesse, categories, periodes
└── marts/
    ├── daily_summary.sql            # Agregations quotidiennes
    ├── zone_analysis.sql            # Metriques par zone de depart
    └── hourly_patterns.sql          # Patterns de demande horaire
```

## Scripts Python (partie 1)

```bash
python brief_snowflake.py   # Telecharger les fichiers parquet
python verif.py             # Verifier le schema des fichiers
```

## CI/CD

Un workflow GitHub Actions (`.github/workflows/dbt_pipeline.yml`) execute automatiquement le pipeline :
- **Planifie** : le 1er de chaque mois a 6h UTC
- **Manuel** : via le bouton "Run workflow" dans l'onglet Actions


## Source des donnees

[NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — fichiers `yellow_tripdata_YYYY-MM.parquet`.
