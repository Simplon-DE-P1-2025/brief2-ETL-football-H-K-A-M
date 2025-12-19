# Rapport Final - Brief 2 : ETL pour analyser des données footballistiques

## Projet : Construction d'un dataset unifié des Coupes du Monde de football

Dans le cadre de ce projet, nous avons travaillé sur plusieurs sources de données hétérogènes couvrant différentes éditions de la Coupe du Monde (1930 à 2022). L'objectif était de produire un dataset final unique, propre et cohérent, destiné à être chargé en base de données et utilisé pour l'analyse de KPI.

**Objectifs atteints :**
- ✅ Dataset unifié de **6861 matchs + 227 équipes** consolidés
- ✅ Base de données PostgreSQL opérationnelle sur **Render**
- ✅ **4 tables normalisées** pour optimiser les analyses
- ✅ **Visualisations interactives** avec insights métier
- ✅ **Documentation complète** pour reproduction du pipeline

**Résultat final :** Pipeline ETL complet et reproductible générant des KPIs exploitables pour l'analyse footballistique professionnelle.

---

## 🔍 PHASE 1 : EXTRACTION ET OBSERVATION

### **Étape 1 – Observation des datasets**

Nous avons étudié **six datasets distincts** :
- **Données historiques** (1930–2010)
- **Matches de la Coupe du Monde 2022**
- **Dataset FIFA Kaggle**
- **Dataset détaillé des stades et matches 2022**
- **Matches de la Coupe du Monde 2014**
- **JSON structuré de la Coupe du Monde 2018**

Pour chacun, nous avons identifié :
- ✅ Colonnes disponibles et types de données
- ✅ Valeurs uniques, doublons et valeurs manquantes
- ✅ Cohérence des dates et années
- ✅ Présence des équipes et stades correspondants

### **Étape 2 – Analyse des besoins de transformation**

Nous avons constaté que les colonnes n'étaient pas homogènes entre les datasets (ex: `team1` vs `home_team`, score en texte vs colonnes numériques).

**Table de correspondance construite vers le schéma final :**
- `id_match`
- `home_team`
- `away_team` 
- `home_result`
- `away_result`
- `result`
- `date`
- `round`
- `city`
- `edition`

**Variantes identifiées** dans les noms d'équipes et villes :
- IR Iran vs Iran
- ENG vs England
- São Paulo vs sao paulo

Une **table de correspondance** des pays et villes a été définie pour uniformiser ces libellés.

### **Étape 3 – Analyse des besoins du Pipeline de transformation**

**Le pipeline ETL doit :**
- ✅ Renommer les colonnes selon la table de correspondance
- ✅ Transformer les champs composites (score, xg_score) en colonnes numériques
- ✅ Créer la colonne result en fonction des scores
- ✅ Nettoyer les données textuelles : minuscules, suppression accents, espaces
- ✅ Aplatir les dictionnaires
- ✅ Appliquer la table de correspondance des pays et villes
- ✅ Ajouter la colonne `edition` pour chaque dataset
- ✅ Générer un identifiant séquentiel `id_match`

---

## 🛠️ PHASE 2 : PIPELINE ETL - ARTICULATION DES SCRIPTS

### **Script 01_extract_preview.py - Exploration des données brutes**

**🎯 Objectif :** Explorer et comprendre la structure des données brutes pour planifier les transformations

**📂 Sources traitées :**
- `matches_19302010.csv` - Données historiques (1930-2010)
- `WorldCupMatches2014*.csv` - Matches édition 2014
- `data_2018.json` - Structure JSON édition 2018

**🔧 Transformations clés :**
- ✅ Chargement des fichiers bruts dans des DataFrames pandas
- ✅ Uniformisation minimale des colonnes (`home_team`, `away_team`, `home_result`, `away_result`, `date`, `round`, `city`, `edition`)
- ✅ Construction du DataFrame 2018 à partir du fichier JSON complexe
- ✅ Vérifications rapides : structure des colonnes et aperçu des premières lignes (`head()`)

**📊 Résultat :** Trois fichiers CSV générés avec colonnes standardisées et alignées, format uniforme, données brutes conservées sans modification.

---

### **Script 02_extract_2022_from_text.py - Parsing des données textuelles**

**🎯 Objectif :** Extraire les matchs de la Coupe du Monde 2022 depuis des fichiers texte non structurés

**📂 Sources traitées :**
- `cup.txt` - Matchs de phases de groupes 2022
- `cup_finals.txt` - Matchs de phases finales 2022

**🔧 Transformations clés :**
- ✅ **Parsing regex** avancé des lignes texte brutes
- ✅ **Extraction automatique** : équipes, scores, dates, villes
- ✅ **Gestion intelligente des buts** (temps réglementaire, prolongations, penalties)
- ✅ **Reconstruction des tours** (Group, Round of 16, Final, etc.)

**📊 Résultat :** `matches_2022.csv` - Dataset structuré prêt à l'intégration dans le pipeline principal.

---

### **Script 03_export_processed_csvs.py - Consolidation multi-éditions**

**🎯 Objectif :** Combiner toutes les Coupes du Monde (1930→2022) en un fichier uniforme et enrichi avec des informations complémentaires issues des données Kaggle (scores, villes, phases manquantes).

**📂 Sources traitées :**
- CSV extraits : `matches_1930_2010_extracted.csv`, `matches_2014_extracted.csv`, `matches_2018_extracted.csv`, `matches_2022.csv`
- Fichiers JSON Kaggle : `matches.json`, `tournament.json`

**🔧 Transformations clés :**
- ✅ **Normalisation avancée** des noms d'équipes et villes
- ✅ **Conversion** des scores textuels en valeurs numériques
- ✅ **Complétion des dates placeholder** (YYYY-01-01) avec données Kaggle
- ✅ **Fusion chronologique** de toutes les éditions (1930→2022)
- ✅ **Ajout colonne `result`** indiquant vainqueur ou match nul
- ✅ **Attribution `id_match` unique** trié par édition, date, phase et équipes

**📊 Résultat :** `matches_unified_v1.csv` - Premier dataset consolidé de référence.

---

### **Script 05_v1-to-v2-kagglejson.py - Enrichissement Kaggle**

**🎯 Objectif :** Enrichir le dataset V1 avec les données secondaires Kaggle pour maximiser la complétude

**📂 Sources traitées :**
- Entrée : `matches_unified_v1.csv` 
- Enrichissement : [Kaggle FIFA World Cup Dataset](https://www.kaggle.com/datasets/basharalkuwaiti/fifa-world-cup)

**🔧 Transformations clés :**
- ✅ **Intégration** des données Kaggle pour **combler** les informations manquantes (dates, tours, villes)
- ✅ **Deux passes de fusion** assurant cohérence et exhaustivité
- ✅ **Produit le fichier Final**

**📊 Résultat :** `matches_unified_v2.csv` - Dataset enrichi et prêt pour nettoyage avancé.

---

### **Script 06_v2-to-v3-clean.py - Nettoyage et normalisation équipes**

**🎯 Objectif :** Nettoyer en profondeur et créer le référentiel équipes avec validation de l'intégrité

**📂 Sources traitées :**
- Entrée : `matches_unified_v2.csv`

**🔧 Transformations clés :**

**🧹 1.Dédoublonnage et Identification Unique (id_match) :**
- **Quoi :** Génération d'une clé unique (UID) basée sur les détails du match (date, équipes, score) pour détecter et supprimer les doublons stricts. Recréation d'un identifiant séquentiel propre (1, 2, 3...)
- **Pourquoi :** Les données proviennent de plusieurs sources (Kaggle, JSON, CSV historiques) qui se chevauchent. Cette étape garantit qu'un match n'apparaît qu'une seule fois dans la base finale pour ne pas fausser les statistiques

**📅 2.Nettoyage et Standardisation des Dates :**
- **Quoi :** Conversion de toutes les dates au format standard SQL (YYYY-MM-DD). Détection des dates "placeholder" (YYYY-01-01 utilisées quand le jour exact est inconnu) et remplacement par NULL
- **Pourquoi :** Permettre des requêtes temporelles fiables (ex: "Matchs joués en juin") et éviter que des dates artificielles soient interprétées comme de vrais matchs du 1er janvier

**🛡️ 3.Validation de l'Intégrité Référentielle (Filtre "Pare-feu") :**
- **Quoi :** Vérification que chaque match possède bien deux équipes identifiées par un ID valide (`home_team_id` et `away_team_id`). Suppression des lignes avec ID manquant (équipe inconnue)
- **Pourquoi :** Sécurité indispensable pour la base de données. On ne peut pas insérer un match "France vs Inconnu" dans une base relationnelle propre. Cela force la qualité des données en amont

**🔑 4.Formatage du Résultat pour la BDD :**
- **Quoi :** Transformation de la colonne `result` (vainqueur) pour contenir soit l'ID de l'équipe gagnante, soit la mention standard "draw" (match nul)
- **Pourquoi :** Simplification énorme des requêtes SQL futures. Au lieu de comparer les scores à chaque fois (`IF home_score > away_score...`), l'analyste peut directement compter les victoires avec un simple `COUNT` sur la colonne résultat

**📊 Résultat :** `matches_unified_v3.csv` + fichiers référentiels (`dim_teams.csv`, `team_aliases.csv`, contrôles qualité).

---

### **Script 07_v3_to_v4.py - Préparation base de données**

**🎯 Objectif :** Transformer les données nettoyées (V3) en version finale pour la base de données (V4)

**📂 Sources traitées :**
- Entrée : `matches_unified_v3.csv`

**🔧 Transformations clés :**

**✅ 1/ Validation de l'intégrité :**
- Suppression des matchs sans date, sans score ou sans ID d'équipe valide
- Contrôle strict des données critiques pour éviter les erreurs d'insertion en BDD

**📊 2/ Standardisation :**
- Typage des colonnes (scores/IDs en entiers) pour optimisation BDD
- Nettoyage des dates et création d'un résultat simplifié (ID vainqueur ou "draw")
- Format uniforme et compatible avec le schéma relationnel cible

**🧹 3/ Dédoublonnage :**
- Création d'un UID unique par match pour supprimer les doublons stricts
- Garantie d'unicité absolue des enregistrements dans la base finale

**📤 4/ Exportant :**
- Génération de la table de faits (`matches`) optimisée pour requêtes
- Production de la dimension équipes (`teams`) avec référentiels complets

**📊 Résultat :** `matches_unified_v4.csv` + `teams_v4.csv` + `quality_report_v4.txt` - Version finale prête pour déploiement BDD.

---

### **Script 08_v4_to_db.py - Version analytique finale**

**🎯 Objectif :** Créer la version finale orientée analyse métier avec dénormalisation contrôlée

**📂 Sources traitées :**
- Entrée : `matches_unified_v4.csv` + dimension équipes

**🔧 Transformations clés :**

**🔄 Dénormalisation pour la BI (IDs → Noms) :**
- Remplacement identifiants techniques par vrais noms de pays ("France")
- Fichier exploitable par humains ou outils BI (PowerBI)

**💼 Traduction du Résultat ("Business Logic") :**
- Transformation colonne technique `result` en donnée métier explicite
- Nom du vainqueur ou "draw" pour requêtes SQL simplifiées

**🏆 Segmentation Stratégique (`is_final`) :**
- Flag booléen distinguant matchs qualifications vs les matches de la coupe du monde

**📊 Résultat :** `matches_final_kpi.csv` - Dataset final prêt pour analyses métier et visualisations.

---

### **Script 09_tables_construction.py - Modélisation relationnelle**

**🎯 Objectif :** Transformer les données analytiques en modèle relationnel normalisé.

**📂 Sources traitées :**
- `matches_final_kpi.csv` (faits matchs enrichis)
- `dim_teams.csv` (dimension équipes)

**🔧 Transformations clés :**
- ✅ **Modélisation relationnelle avancée** avec séparation domicile/extérieur
- ✅ **Optimisation performances SQL** avec jointures rapides (IDs)
- ✅ **Gestion explicite** des statistiques home/away
- ✅ **Modèle évolutif** (préparation extension joueurs)

**📊 Résultat :** 4 tables normalisées (`teams_reference`, `matches_normalized`, `home_stats`, `away_stats`) - Architecture BDD optimale.

---

### **Script run_setup.py - Orchestrateur de déploiement**

**🎯 Objectif :** Chargement automatisé et sécurisé en base PostgreSQL Render

**📂 Sources traitées :**
- Tables normalisées depuis `data/clean/`

**🔧 Processus de déploiement :**
1. ✅ Connexion PostgreSQL via la classe `DatabaseManager`
2. ✅ Création structure tables optimisée
3. ✅ Insertion données avec validation temps réel
4. ✅ Contrôles qualité finaux et rapports

**📊 Résultat :** Base PostgreSQL opérationnelle avec 6861 matchs, 227 équipes- Infrastructure complète déployée en ligne.

---

## 🎯 Réalisations Techniques Finales

### 🏗️ **Architecture Déployée**
- **Base de données** : PostgreSQL sur **Render** (gratuit, accessible par l'équipe)
- **Modèle relationnel** : 4 tables normalisées (`teams_reference`, `matches_normalized`, `home_stats`, `away_stats`)  
- **Infrastructure modulaire** : Classe `DatabaseManager` réutilisable
- **Pipeline reproductible** : Documentation complète dans `docs/deploiement-base-donnees.md`

### 📊 **Données Finales Consolidées**
- **6861 matchs** de Coupe du Monde (1930-2022) unifiés
- **227 équipes** nationales référencées

### 🎯 **Livrables d'Analyse**
- **Requêtes KPI** : `db/kpi.sql` (indicateurs globaux, par équipe, par édition)
- **Visualisations** : `Notebook/Vizualisation_KPI.ipynb` (graphiques interactifs)

### ⚡ **Innovation Technique**
- **Backup stratégique** : CSV + PostgreSQL (double sécurité)
- **Collaboration facilitée** : Base cloud accessible par toute l'équipe
- **Performance optimisée** : Tables sans contraintes FK (compatibilité Render gratuit)
- **Évolutivité** : Architecture modulaire pour extension future

### 🎉 **Résultats Obtenus**
Le projet dépasse les objectifs initiaux en livrant non seulement un dataset unifié, mais un **écosystème d'analyse complet** prêt pour l'exploitation métier avec base de données opérationnelle et visualisations interactives.

---

## 📁 Schéma des Dossiers

```
project/
├── data/
│   ├── raw/                         # données brutes (sources)
│   │   ├── matches_19302010.csv
│   │   ├── WorldCupMatches2014*.csv
│   │   ├── data_2018.json
│   │   ├── cup.txt / cup_finals.txt (2022)
│   │   └── kaggle/ (matches.json, tournament.json, ...)
│   │
│   ├── processed/                   # sorties intermédiaires (unifiées / enrichies)
│   │   ├── matches_2022.csv
│   │   ├── matches_unified_v1.csv
│   │   ├── matches_unified_v2.csv
│   │   ├── matches_unified_v3.csv
│   │   └── matches_unified_v4.csv
│   │
│   ├── clean/                       # données "prêtes analyse" / tables normalisées
│   │   ├── dim_teams.csv
│   │   ├── matches_final_kpi.csv
│   │   ├── teams_reference_normalized.csv
│   │   ├── matches_normalized.csv
│   │   ├── home_stats_normalized.csv
│   │   └── away_stats_normalized.csv
│   │
│   └── reference/                   # référentiels pour nettoyage des nom des équipes
│       ├── teams_v4.csv
│       ├── team_aliases.csv
│       ├── unknown_teams.csv
│       ├── qa_team_collisions.csv
│       └── quality_report_v4.txt
│
├── src/                             # scripts du pipeline (ETL)
│   ├── 01_extract_preview.py
│   ├── 02_extract_2022_from_text.py
│   ├── 03_export_processed_csvs.py
│   ├── 03_observation_des_données.py
│   ├── 04_unify_all_years.py
│   ├── 05_v1-to-v2-kagglejson.py
│   ├── 06_v2-to-v3-clean.py
│   ├── 07_v3_to_v4.py
│   ├── 08_v4_to_db.py
│   ├── 09_tables_construction.py   # normalisation 4 tables relationnelles
│   ├── run_setup.py                # orchestrateur chargement BDD
│   └── database/                   # infrastructure BDD + classe DatabaseManager
│       └── setup_database.py       # connexion PostgreSQL Render
│
├── db/                              # requêtes SQL d'analyse KPI
│   └── kpi.sql                      # requêtes extraction indicateurs métier
│
├── docs/                            # documentation technique complète
│   ├── RapportFinal.md              # rapport de projet (ce fichier)
│   └── deploiement-base-donnees.md  # guide setup PostgreSQL Render
│
├── Notebook/                        # analyses et visualisations interactives
│   └── Vizualisation_KPI.ipynb     # graphiques
│
└── .env.example                             # variables environnement (PostgreSQL Render URL)
```

---

## 🔄 Schéma de Flux ETL (de bout en bout)

```
📂 DATA BRUTE (data/raw/)
   ├── matches_19302010.csv
   ├── WorldCupMatches2014*.csv  
   ├── data_2018.json
   ├── cup.txt / cup_finals.txt (2022)
   └── kaggle/ (matches.json, tournament.json, ...)
   │
   ├─🔄(02) extraction 2022 texte
   │   └── → data/processed/matches_2022.csv
   │
   └─🔄(04) unify all years (1930-2010 + 2014 + 2018 + 2022)
       └── → data/processed/matches_unified_v1.csv
           │
           └─🔄(05) enrich Kaggle (dates/round/city)
               └── → data/processed/matches_unified_v2.csv
                   │
                   └─🔄(06) clean + normalisation teams
                       ├── → data/processed/matches_unified_v3.csv
                       ├── → data/clean/dim_teams.csv
                       └── → data/reference/ (QA files)
                           │
                           └─🔄(07) v3 → v4 (DB-ready)
                               ├── → data/processed/matches_unified_v4.csv
                               ├── → data/reference/teams_v4.csv
                               └── → data/reference/quality_report_v4.txt
                                   │
                                   ├─🔄(08) matches final KPI
                                   │   └── → data/clean/matches_final_kpi.csv
                                   │
                                   └─🔄(09) normalisation relationnelle
                                       ├── → data/clean/teams_reference_normalized.csv
                                       ├── → data/clean/matches_normalized.csv
                                       ├── → data/clean/home_stats_normalized.csv
                                       └── → data/clean/away_stats_normalized.csv
                                           │
                                           └─🔄(run_setup) chargement PostgreSQL Render
                                               ├── → 🗄️ Base cloud opérationnelle (833 matchs)
                                               ├── → 🗄️ 4 tables relationnelles normalisées
                                               ├── → 📊 Requêtes KPI prêtes (db/kpi.sql)
                                               └── → 📈 Visualisations interactives (Notebook/)
```

### **🎯 Résultats Finaux**
- ✅ **6861 matchs** consolidés (1930-2022)
- ✅ **227 équipes** référencées avec IDs
- ✅ **Base PostgreSQL** opérationnelle sur Render
- ✅ **Requêtes KPI** prêtes pour analyse métier
- ✅ **Visualisations interactives** avec data story complète