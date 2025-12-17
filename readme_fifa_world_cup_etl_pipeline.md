# ⚽ FIFA World Cup ETL Pipeline (1930–2022)

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-elephant)
![Pandas](https://img.shields.io/badge/Pandas-ETL-orange)
![Status](https://img.shields.io/badge/Status-Completed-green)

---

## Objectif du projet

Ce projet met en œuvre un **pipeline ETL (Extract – Transform – Load)** complet permettant de :

- centraliser **l’historique exhaustif des matchs de Coupe du Monde FIFA (1930 → 2022)** ;
- nettoyer et normaliser des données **hétérogènes et bruitées** ;
- produire un dataset **fiable, cohérent et prêt pour l’analyse et la base de données**.

Le pipeline est conçu selon une logique **Data Engineer / Data Warehouse** :
- séparation claire des étapes,
- traçabilité des données,
- règles métier explicites,
- contrôle qualité intégré.

---

## Architecture globale

```mermaid
graph LR
    A[Sources brutes<br/>(CSV, JSON, TXT)] --> B[01–03<br/>Extraction]
    B --> C[04<br/>Unification]
    C --> D[05<br/>Enrichissement Kaggle]
    D --> E[06<br/>Nettoyage & Référentiels]
    E --> F[07<br/>Règles métier]
    F --> G[08<br/>Chargement BDD]
    G --> H[(PostgreSQL<br/>Data Warehouse)]
```

Les données évoluent selon les couches suivantes :

```
data/raw        -> données sources inchangées
data/processed  -> données unifiées et enrichies
data/clean      -> données prêtes analyse / base
data/reference  -> dimensions et référentiels
```

---

## Description détaillée des scripts

> Les scripts doivent être exécutés **strictement dans l’ordre**.

### 01_extract_preview.py — Exploration initiale

**Rôle** : audit des sources brutes.

- inspection des colonnes et types
- détection des valeurs manquantes
- compréhension des schémas hétérogènes

➡️ Script purement exploratoire (aucune écriture disque).

---

### 02_extract_2022_from_text.py — Extraction 2022 (TXT)

**Rôle** : parser une source texte non structurée (édition 2022).

- parsing regex des lignes de matchs
- extraction : équipes, scores, dates, villes
- gestion des prolongations et penalties

➡️ Sortie : `data/processed/matches_2022.csv`

---

### 03_export_processed_csvs.py — Normalisation des formats

**Rôle** : convertir les sources 2018 (JSON) et autres formats exotiques en CSV standard.

- JSON → DataFrame
- harmonisation minimale des colonnes

➡️ Objectif : préparer l’unification globale.

---

### 04_unify_all_years.py — Fusion 1930 → 2022

**Rôle** : construire le **dataset maître**.

- chargement des éditions 1930–2010, 2014, 2018, 2022
- harmonisation du schéma
- génération d’un premier `id_match`
- conservation volontaire des imperfections (dates placeholders, villes manquantes)

➡️ Sortie : `matches_unified_v1.csv`

---

### 05_v1-to-v2-kagglejson.py — Enrichissement Kaggle

**Rôle** : combler les manques via Kaggle (1930–2018).

- matching flou équipes / scores / éditions
- remplacement des dates fictives (`YYYY-01-01`)
- enrichissement des villes et phases

➡️ Sortie : `matches_unified_v2.csv`

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

➡️ Permet de filtrer instantanément l’historique officiel (~900 matchs).

#### Edition

- transformation : `"1930-URUGUAY" → 1930`
- type : entier

#### Résultat DB-friendly

- `draw` ou `team_id` gagnant

Sortie : `matches_unified_v4.csv`

---

### 🗄️ 08_v4_to_db.py — Chargement PostgreSQL

**Rôle** : injection finale en base.

- création des tables
- chargement des dimensions (`teams`)
- chargement des faits (`matches`)
- vérification des contraintes

---

## Modèle de données cible

### Table `matches`

| Colonne | Description |
|------|------------|
| id_match | PK |
| home_team_id | FK team |
| away_team_id | FK team |
| home_result | Score |
| away_result | Score |
| result | draw ou team_id |
| date | Date ISO |
| round | Phase normalisée |
| edition | Année |
| is_final | Filtre métier |

---

### 🧩 Table `teams`

| Colonne | Description |
|------|------------|
| team_id | PK |
| team_canonical | Nom officiel |
| iso2 | Code ISO |
| iso3 | Code ISO |

---

## 🚀 Installation & exécution

```bash
git clone https://github.com/your-repo/fifa-etl.git
cd fifa-etl
pip install -r requirements.txt
```

Configurer `.env` puis exécuter les scripts **dans l’ordre**.

---

## 🧠 Compétences démontrées

- ETL Python avancé (pandas)
- Data Quality & normalisation sémantique
- Modélisation Data Warehouse
- Gestion des référentiels
- Pipeline traçable et maintenable

---

## 📌 Contexte académique

Projet réalisé dans le cadre de la formation **Data Engineer** — Décembre 2025.

