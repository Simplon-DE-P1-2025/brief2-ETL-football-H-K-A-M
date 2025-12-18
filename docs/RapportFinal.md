Rapport du  Brief 2 : ETL pour analyser des données footballistique


Projet : Construction d’un dataset unifié des Coupes du Monde de football
Dans le cadre de ce projet, nous avons travaillé sur plusieurs sources de données hétérogènes couvrant différentes éditions de la Coupe du Monde (1930 à 2022). L’objectif était de produire un dataset final unique, propre et cohérent, destiné à être chargé en base de données et utilisé pour l’analyse de KPI.
Étape 1 – Observation des datasets Nous avons étudié six datasets distincts :
Données historiques (1930–2010)
Matches de la Coupe du Monde 2022
Dataset FIFA Kaggle
Dataset détaillé des stades et matches 2022
Matches de la Coupe du Monde 2014
JSON structuré de la Coupe du Monde 2018
Pour chacun, nous avons identifié les colonnes disponibles, les types de données, les valeurs uniques, les doublons et les valeurs manquantes. Nous avons également vérifié la cohérence des dates et des années, ainsi que la présence des 32 équipes et des stades correspondants.
Étape 2 – Analyse des besoins de transformation Nous avons constaté que les colonnes n’étaient pas homogènes entre les datasets (par exemple team1 vs home_team, score en texte vs colonnes numériques). Une table de correspondance des colonnes a été construite pour harmoniser la structure vers le schéma final imposé :
id_match
home_team
away_team
home_result
away_result
result
date
round
city
edition
Nous avons également identifié des variantes dans les noms d’équipes et de villes (ex. IR Iran vs Iran, ENG vs England, São Paulo vs sao paulo). Une table de correspondance des pays et des villes a été définie pour uniformiser ces libellés.
Étape 3 – Pipeline de transformation Le pipeline ETL doit :
Renommer les colonnes selon la table de correspondance.
Transformer les champs composites (score, xg_score) en colonnes numériques.
Créer la colonne result en fonction des scores.
Nettoyer les données textuelles : mise en minuscules, suppression des accents, retrait des espaces superflus.
Appliquer la table de correspondance des pays et villes pour harmoniser les libellés.
Ajouter la colonne edition pour chaque dataset.
Générer un identifiant séquentiel id_match couvrant l’ensemble des éditions.



Schéma des dossiers



project/
├── data/
│   ├── raw/                 # données brutes (sources)
│   │   ├── matches_19302010.csv
│   │   ├── WorldCupMatches2014*.csv
│   │   ├── data_2018.json
│   │   ├── cup.txt / cup_finals.txt (2022)
│   │   └── kaggle/ (matches.json, tournament.json, ...)
│   │
│   ├── processed/           # sorties intermédiaires (unifiées / enrichies)
│   │   ├── matches_2022.csv
│   │   ├── matches_unified_v1.csv
│   │   ├── matches_unified_v2.csv
│   │   ├── matches_unified_v3.csv
│   │   └── matches_unified_v4.csv
│   │
│   ├── clean/               # données “prêtes analyse” / tables normalisées
│   │   ├── dim_teams.csv
│   │   ├── matches_final_kpi.csv
│   │   ├── teams_reference_normalized.csv
│   │   ├── matches_normalized.csv
│   │   ├── home_stats_normalized.csv
│   │   └── away_stats_normalized.csv
│   │
│   └── reference/           # référentiels + QA + rapports
│       ├── teams_v4.csv
│       ├── team_aliases.csv
│       ├── unknown_teams.csv
│       ├── qa_team_collisions.csv
│       └── quality_report_v4.txt
│
├── src/                     # scripts du pipeline (ETL)
│   ├── 01_extract_preview.py
│   ├── 02_extract_2022_from_text.py
│   ├── 03_export_processed_csvs.py
│   ├── 03_observation_des_données.py
│   ├── 04_unify_all_years.py
│   ├── 05_v1-to-v2-kagglejson.py
│   ├── 06_v2-to-v3-clean.py
│   ├── 07_v3_to_v4.py
│   ├── 08_v4_to_db.py
│   ├── 09_tables_construction.py
│   └── database/            # code SQL / init / connexions / scripts DB
│
├── db/                      # base locale (fichiers, dumps, etc. selon ton choix)
├── docs/                    # rapport, documentation, schémas
├── Notebook/                # notebooks d’exploration / tests
├── obsolete/                # vieux scripts / versions abandonnées
└── .env                     # variables (DB, chemins, secrets)


Schéma de flux ETL (de bout en bout)

DATA BRUTE
data/raw
   │
   ├─(02) extraction 2022 texte → data/processed/matches_2022.csv
   │
   └─(04) unify all years (1930-2010 + 2014 + 2018 + 2022)
           → data/processed/matches_unified_v1.csv
                 │
                 └─(05) enrich Kaggle (dates/round/city)
                      → data/processed/matches_unified_v2.csv
                            │
                            └─(06) clean + normalisation teams
                                 → data/processed/matches_unified_v3.csv
                                 → data/clean/dim_teams.csv + data/reference/*QA
                                       │
                                       └─(07) v3 → v4 (DB-ready)
                                            → data/processed/matches_unified_v4.csv
                                            → data/reference/teams_v4.csv
                                                  │
                                                  ├─(08) load en base (PostgreSQL/MySQL/Mongo)
                                                  └─(09) tables normalisées + KPI
                                                      → data/clean/*_normalized.csv
                                                      → data/clean/matches_final_kpi.csv