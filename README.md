# brief2-ETL-football-H-K-A-M
Brief 2 : ETL pour analyser des données footballistique

# Contexte et objectifs du projet
Ce projet vise à construire un pipeline ETL en Python pour centraliser l’historique des matchs de Coupe du Monde FIFA (1930–2022), nettoyer les données et charger un dataset propre dans une base (SGBDR ou NoSQL) afin d’alimenter des analyses (KPI) et, à terme, un modèle d’estimation des probabilités de victoire entre deux équipes ( ce qui ne sera pas fait dans cette étape).
Les sources incluent les fichiers fournis (1930–2010, 2014, 2018) et l’édition 2022 (Kaggle).

# Équipe et organisation

Équipe: H, K, A, M
Chef de projet:
Référent data quality :
Référent infrastructure/BDD:
Référent analytics/KPI:


# Processus de gestion de projet

en cours

## 📋 Tableau de gestion de projet

| Tâche | Responsable | Échéance | Statut |
|-------|-------------|----------|--------|
| Clonage du repo et setup | All | 15/12 |  Fait |
| Création Projects + tâches | K | 15/12 |  En cours |
| Définition outils + hypothèses | Équipe | 15/12 |  En cours |
| Observation des données et recherches nouvelles données | M | Now |  En cours |
| Extraction 1930–2010/2014/2018 | … | … | À faire |
| Recherche/Intégration 2022 | … | … | À faire |
| Nettoyage et harmonisation | … | … | À faire |
| Schéma BDD et chargement | … | … | À faire |
| Requêtes KPI | … | … | À faire |
| Rapport et documentation | … | … | À faire |


# Portée et livrables




# Les outils utilisées


Justification BDD :

- **Langage** : Python 3 (pandas, numpy, sqlalchemy, pymongo)
- **Bases de données** : PostgreSQL / MySQL / MongoDB
- **Gestion de versions** : Git + GitHub
- **Collaboration** : GitHub Projects (Kanban), Issues
- **Nettoyage & ETL** : pandas, unidecode, python-dateutil
- **Documentation** :README, rapport, powerpoint


# Données et sources

    Fichiers fournis:

        matches_19302010.csv (historique 1930–2010)

        WorldCupMatches2014.csv (édition 2014)

        data_2018.json (édition 2018)

    Édition 2022: dataset Kaggle “FIFA World Cup 2022 Match Data” (matchs complets).
