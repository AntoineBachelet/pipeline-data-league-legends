# League of Legends — Pipeline Data

Pipeline de données orchestré par **Dagster** qui collecte, nettoie et expose des données sur les compétitions professionnelles League of Legends.

## Architecture

```
Sources externes
  ├── Leaguepedia (MediaWiki API)   → tournois, joueurs, rosters
  └── lolpros.gg                    → comptes soloqueue
        ↓ Bronze (S3 Parquet)
  dagster_lol / assets / bronze
        ↓ Silver (Snowflake)
  dagster_lol / assets / silver
        ↓ Gold (Snowflake views)
  dbt_gold_layer / models / gold
```

## Stack

| Composant | Rôle |
|---|---|
| Dagster 1.9 | Orchestration, asset checks, metadata |
| S3 | Stockage bronze (Parquet) |
| Snowflake | Stockage silver & gold |
| dbt | Modèles gold (vues analytiques) |
| Docker Compose | Déploiement local (webserver, daemon, user code) |

## Structure du projet

```
dagster_lol/
  assets/
    bronze/          # Ingestion brute → S3 (Leaguepedia, lolpros)
    silver/
      leaguepedia.py # Assets silver (tournaments, players, rosters, soloqueue)
      transform.py   # Fonctions de nettoyage pandas
      schemas/       # Schémas de types par asset (source of truth runtime)
      contracts/     # Data Contracts YAML (datacontract.com 0.9.3)
      checks.py      # Asset checks Dagster (doublons, nulls, schema contract)
  ressources/        # Wrappers Dagster : S3, Snowflake, Riot API, Leaguepedia
  jobs.py            # Définition des jobs
dbt_gold_layer/      # Modèles dbt (couche gold)
migrations/snowflake/ # DDL de création des tables silver
tests/               # Tests unitaires (pytest)
```

## Lancer en local

```bash
cp .env.example .env   # remplir les variables
docker-compose up
```

L'UI Dagster est accessible sur `http://localhost:3000`.

## Variables d'environnement requises

Voir [.env.example](.env.example). Les secrets nécessaires sont :
`SNOWFLAKE_*`, `AWS_*`, `RIOT_API_KEY`, `LEAGUEPEDIA_*`.
