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

