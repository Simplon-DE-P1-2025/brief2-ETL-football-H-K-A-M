# brief2-ETL-football-H-K-A-M
Brief 2 : ETL pour analyser des données footballistique

# Contexte et objectifs du projet
Ce projet vise à construire un pipeline ETL en Python pour centraliser l'historique des matchs de Coupe du Monde FIFA (1930–2022), nettoyer les données et charger un dataset propre dans une base PostgreSQL afin d'alimenter des analyses (KPI) et, à terme, un modèle d'estimation des probabilités de victoire entre deux équipes.

**✅ OBJECTIFS ATTEINTS :**
- **6861 matchs** de Coupe du Monde consolidés (1930-2022)
- **227 équipes** nationales référencées
- **Base PostgreSQL** opérationnelle sur Render Cloud
- **4 tables normalisées** pour optimiser les analyses
- **Pipeline ETL complet** avec 9 scripts automatisés
- **Visualisations interactives** avec insights métier

Les sources incluent les fichiers fournis (1930–2010, 2014, 2018) et l’édition 2022 (Kaggle).

# Équipe et organisation

Équipe: Hafida, Khalid, Ali, Mohammed
Chef de projet: Hafida


# Processus de gestion de projet

en cours

##  Tableau de gestion de projet

| Tâche | Responsable | Échéance | Statut |
|-------|-------------|----------|--------|
| Clonage du repo et setup | All | 15/12 | ✅ **Fait** |
| Création Projects + tâches | K | 15/12 | ✅ **Fait** |
| Définition outils + hypothèses | Équipe | 15/12 | ✅ **Fait** |
| Observation des données (01_extract_preview.py) | M | 16/12 | ✅ **Fait** |
| Extraction 1930–2010/2014/2018 (02-03) | Équipe | 17/12 | ✅ **Fait** |
| Recherche/Intégration 2022 (04_unify_all_years.py) | Équipe | 17/12 | ✅ **Fait** |
| Enrichissement Kaggle (05_v1-to-v2-kagglejson.py) | Équipe | 18/12 | ✅ **Fait** |
| Nettoyage et harmonisation (06-07) | Équipe | 18/12 | ✅ **Fait** |
| Schéma BDD et chargement (08-09 + run_setup.py) | Équipe | 19/12 | ✅ **Fait** |
| Requêtes KPI et visualisations | Équipe | 19/12 | ✅ **Fait** |
| Rapport et documentation | Équipe | 19/12 | ✅ **Fait** |


# Stack technique utilisée

**✅ ARCHITECTURE DEPLOYÉE :**

- **Langage** : Python 3.x
- **Bibliothèques ETL** : pandas, numpy, sqlalchemy, psycopg2-binary
- **Base de données** : **PostgreSQL sur Render Cloud** (gratuit, accessible équipe)
- **Visualisations** : plotly, matplotlib, seaborn (Jupyter Notebook)
- **Gestion de versions** : Git + GitHub
- **Collaboration** : GitHub Projects (Kanban), Issues
- **Nettoyage & ETL** : pandas, unidecode, python-dateutil, requests
- **Infrastructure** : Classe `DatabaseManager` réutilisable
- **Documentation** : README, rapport technique complet, visualisations interactives

**🎯 CHOIX TECHNIQUE :**
- **PostgreSQL Render** : Solution cloud gratuite, accessible par toute l'équipe
- **4 tables normalisées** : Optimisation des performances SQL
- **Pipeline modulaire** : 9 scripts ETL séquentiels pour traçabilité complète


# Données et sources

    Fichiers fournis:

        matches_19302010.csv (historique 1930–2010)

        WorldCupMatches2014.csv (édition 2014)

        data_2018.json (édition 2018)

    Édition 2022: dataset Kaggle “FIFA World Cup 2022 Match Data” (matchs complets).

# Les branches / Etapes de travail

## Branche Observation des données

    But : explorer et comprendre la structure des datasets.

    Actions typiques :

        -- Lire les données

        --Afficher les colonnes (print(df.columns)).
        --Lister les colonnes et leurs types
        --Classer les colonnes
        --Faire un tableau de correspondance des colonnes

        --Vérifier les types (df.dtypes)
        --les valeurs uniques
        --Observer les valeurs manquantes
        --Repérer les doublons
        --les éventuelles incohérences
             --Années manquantes
             --Confusion entre NaN et 0.

##  Enrichissement des données

    Recherche de nouvelles données suite a observations de données manquantes
    extraction de fichier texte

## Nettoyage et Transformation des données
    Fonction de cleannage es csv et de transformation

### FIFA World Cup ETL Pipeline (1930–2022)

Met en œuvre un **pipeline ETL (Extract – Transform – Load)** complet permettant de :

- centraliser **l’historique exhaustif des matchs de Coupe du Monde FIFA (1930 → 2022)** ;
- nettoyer et normaliser des données **hétérogènes et bruitées** ;
- produire un dataset **fiable, cohérent et prêt pour l’analyse et la base de données**.

Le pipeline est conçu selon une logique **Data Engineer / Data Warehouse** :
- séparation claire des étapes,
- traçabilité des données,
- règles métier explicites,
- contrôle qualité intégré.

---

### Architecture globale

Les données évoluent selon les couches suivantes :

```
data/raw        -> données sources inchangées
data/processed  -> données unifiées et enrichies
data/clean      -> données prêtes analyse / base
data/reference  -> dimensions et référentiels
```

---

# Description détaillée d'excution du Pipeline

> Les scripts doivent être exécutés **strictement dans l’ordre**.

### 01_extract_preview.py — Exploration initiale

**Rôle** : audit des sources brutes.

- inspection des colonnes et types
- détection des valeurs manquantes
- compréhension des schémas hétérogènes

--> Script purement exploratoire (aucune écriture disque).

---

### 02_extract_2022_from_text.py — Extraction 2022 (TXT)

**Rôle** : parser une source texte non structurée (édition 2022).

- parsing regex des lignes de matchs
- extraction : équipes, scores, dates, villes
- gestion des prolongations et penalties

--> Sortie : `data/processed/matches_2022.csv`

---

### 03_export_processed_csvs.py — Normalisation des formats

**Rôle** : convertir les sources 2018 (JSON) et autres formats exotiques en CSV standard.

- JSON → DataFrame
- harmonisation minimale des colonnes

--> Objectif : préparer l’unification globale.

---

### 04_unify_all_years.py — Fusion 1930 → 2022

**Rôle** : construire le **dataset maître**.

- chargement des éditions 1930–2010, 2014, 2018, 2022
- harmonisation du schéma
- génération d’un premier `id_match`
- conservation volontaire des imperfections (dates placeholders, villes manquantes)

--> Sortie : `matches_unified_v1.csv`

---

### 05_v1-to-v2-kagglejson.py — Enrichissement Kaggle

**Rôle** : combler les manques via Kaggle (1930–2018).

- matching flou équipes / scores / éditions
- remplacement des dates fictives (`YYYY-01-01`)
- enrichissement des villes et phases

--> Sortie : `matches_unified_v2.csv`

---

### 06_v2-to-v3-clean.py — Nettoyage & référentiels

**Rôle clé du projet**.

- normalisation **canonique** des équipes (IDs internes)
- suppression des faux pays (A1, Group B, etc.)
- création de tables de référence :
  - `dim_teams.csv`
  - `team_aliases.csv`
  - `unknown_teams.csv`
- normalisation complète des phases (`round`)

Sortie principale : `matches_unified_v3.csv` (ID-based)

---

### 07_v3_to_v4_ready_for_db.py — Règles métier finales

**Rôle** : préparer les données pour la base de données.

Principales règles :

#### is_final

```text
True  -> tournoi final (Group → Final)
False -> qualifications / preliminary rounds
```

--> Permet de filtrer instantanément l’historique officiel (~900 matchs).

#### Edition

- transformation : `"1930-URUGUAY" → 1930`
- type : entier

#### Résultat DB-friendly

- `draw` ou `team_id` gagnant

Sortie : `matches_unified_v4.csv`

---

### 08_v4_to_db.py — Version analytique finale

**Rôle** : créer la version finale orientée analyse métier.

- dénormalisation pour BI (IDs → noms de pays)
- traduction du résultat en "business logic" 
- segmentation stratégique (`is_final`)
- dataset final prêt visualisations

### 09_tables_construction.py — Modélisation relationnelle

**Rôle** : transformer en modèle relationnel normalisé.

- modélisation avec séparation domicile/extérieur  
- optimisation performances SQL avec jointures rapides
- 4 tables normalisées haute performance

### run_setup.py — Chargement PostgreSQL Render

**Rôle** : injection finale en base cloud.

- création des tables PostgreSQL sur Render
- chargement des 4 tables normalisées
- validation et contrôles qualité finaux

---

## Modèle de données déployé

**🗄️ BASE POSTGRESQL OPÉRATIONNELLE (Render Cloud)**

### **Architecture : 4 tables normalisées**

#### 1. Table `teams_reference` (227 équipes)
| Colonne | Description |
|---------|-------------|
| team_id | PK - Identifiant unique |
| team_name | Nom canonique officiel |
| iso_code | Code ISO pays |

#### 2. Table `matches_normalized` (6861 matchs)
| Colonne | Description |
|---------|-------------|
| match_id | PK - Identifiant séquentiel |
| date | Date du match (format ISO) |
| round | Phase normalisée (Group, Final, etc.) |
| city | Ville du match |
| edition | Année de la Coupe du Monde |
| result | ID équipe gagnante ou "draw" |

#### 3. Table `home_stats` (Statistiques domicile)
| Colonne | Description |
|---------|-------------|
| match_id | FK vers matches_normalized |
| home_team_id | FK vers teams_reference |
| home_score | Buts marqués à domicile |

#### 4. Table `away_stats` (Statistiques extérieur)
| Colonne | Description |
|---------|-------------|
| match_id | FK vers matches_normalized |
| away_team_id | FK vers teams_reference |
| away_score | Buts marqués à l'extérieur |

---

##  Installation & exécution

```bash
git clone https://github.com/your-repo/fifa-etl.git
cd fifa-etl
pip install -r requirements.txt
```

Configurer `.env` puis exécuter les scripts **dans l’ordre**.

---

##  Compétences démontrées

- ETL Python avancé (pandas)
- Data Quality & normalisation sémantique
- Modélisation Data Warehouse
- Gestion des référentiels
- Pipeline traçable et maintenable

---

##  Contexte académique

Projet réalisé dans le cadre de la formation **Data Engineer** — Décembre 2025
