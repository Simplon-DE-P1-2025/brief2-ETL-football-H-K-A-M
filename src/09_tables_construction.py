"""
Construction tables normalisées pour BDD
=======================================

Transformation des données vers modèle relationnel avec IDs numériques :
1. Vérification exhaustivité équipes dans dim_teams
2. Construction de 4 tables normalisées (matches, home_stats, away_stats, teams)

Usage:
    python src/09_tables_construction.py
"""

import pandas as pd
import numpy as np

def load_data():
    """Charger les données sources"""
    print(" Chargement des données...")

    matches_df = pd.read_csv("data/clean/matches_final_kpi.csv")
    teams_df = pd.read_csv("data/clean/dim_teams.csv")

    print(f" Matches loaded: {len(matches_df)} lignes")
    print(f" Teams loaded: {len(teams_df)} équipes")

    return matches_df, teams_df

def check_teams_completeness(matches_df, teams_df):
    """1. Vérifier que toutes les équipes des matchs sont dans dim_teams"""
    print("\n🔍 VÉRIFICATION EXHAUSTIVITÉ ÉQUIPES")
    print("=" * 50)

    # Récupérer tous les noms d'équipes uniques des matchs
    home_teams = set(matches_df['home_team'].dropna().unique())
    away_teams = set(matches_df['away_team'].dropna().unique())
    all_match_teams = home_teams.union(away_teams)

    # Utiliser la structure spécifique de dim_teams.csv
    # Colonnes disponibles: team_canonical, team_clean_example, canonical_key, iso2, iso3, team_id
    if 'team_canonical' in teams_df.columns:
        team_col = 'team_canonical'
    elif 'team_clean_example' in teams_df.columns:
        team_col = 'team_clean_example'
    else:
        print(" Colonnes 'team_canonical' ou 'team_clean_example' introuvables dans dim_teams")
        print(f" Colonnes disponibles: {list(teams_df.columns)}")
        return False

    print(f" Utilisation colonne: '{team_col}' pour comparaison équipes")
    dim_teams = set(teams_df[team_col].dropna().unique())

    # Vérifier les équipes manquantes
    missing_teams = all_match_teams - dim_teams

    print(f" Total équipes dans matches: {len(all_match_teams)}")
    print(f" Total équipes dans dim_teams: {len(dim_teams)}")

    if missing_teams:
        print(f" {len(missing_teams)} équipes MANQUANTES dans dim_teams:")
        for team in sorted(missing_teams):
            print(f"   - {team}")
        return False
    else:
        print(" Toutes les équipes des matchs sont présentes dans dim_teams")
        return True

def create_teams_reference(matches_df):
    """2.4 Créer référentiel exhaustif des équipes avec IDs"""
    print("\n CONSTRUCTION RÉFÉRENTIEL ÉQUIPES")
    print("=" * 50)

    # Récupérer toutes les équipes uniques
    home_teams = set(matches_df['home_team'].dropna().unique())
    away_teams = set(matches_df['away_team'].dropna().unique())
    all_teams = sorted(home_teams.union(away_teams))

    # Créer DataFrame avec IDs séquentiels
    teams_ref = pd.DataFrame({
        'id_team': range(1, len(all_teams) + 1),
        'Team_name': all_teams
    })

    print(f" Référentiel créé: {len(teams_ref)} équipes avec IDs 1-{len(all_teams)}")
    return teams_ref

def create_matches_table(matches_df, teams_ref):
    """2.1 Créer table matches avec result = ID du gagnant ou 0 si nul"""
    print("\n CONSTRUCTION TABLE MATCHES")
    print("=" * 50)

    # Créer mapping team_name -> id_team
    team_to_id = dict(zip(teams_ref['Team_name'], teams_ref['id_team']))

    # Copier les colonnes de base
    matches_table = matches_df.copy()

    # Ajouter id_match séquentiel
    matches_table['id_match'] = range(1, len(matches_table) + 1)

    # Calculer result basé sur les scores
    def calculate_result(row):
        home_score = row['home_result']
        away_score = row['away_result']

        # Gérer les valeurs manquantes
        if pd.isna(home_score) or pd.isna(away_score):
            return 0  # Match nul par défaut

        if home_score > away_score:
            return team_to_id.get(row['home_team'], 0)  # ID équipe domicile
        elif away_score > home_score:
            return team_to_id.get(row['away_team'], 0)  # ID équipe extérieur
        else:
            return 0  # Match nul

    matches_table['result'] = matches_table.apply(calculate_result, axis=1)
    
    # Sélectionner colonnes finales
    final_matches = matches_table[['id_match', 'result', 'date', 'round', 'city', 'edition', 'is_final']].copy()

    print(f" Table matches créée: {len(final_matches)} lignes")
    print(f" Répartition résultats: {final_matches['result'].value_counts().sort_index().head()}")

    return final_matches

def create_home_stats(matches_df, teams_ref):
    """2.2 Créer table stats équipes domicile"""
    print("\n CONSTRUCTION TABLE HOME STATS")
    print("=" * 50)

    # Mapping team_name -> id_team
    team_to_id = dict(zip(teams_ref['Team_name'], teams_ref['id_team']))

    # Créer table home stats
    home_stats = pd.DataFrame({
        'id_match': range(1, len(matches_df) + 1),
        'id_team': matches_df['home_team'].map(team_to_id),
        'Number_of_goals_scored': matches_df['home_result'],
        'Number_of_goals_conceded': matches_df['away_result']
    })

    # Supprimer lignes avec id_team manquant
    home_stats = home_stats.dropna(subset=['id_team'])
    home_stats['id_team'] = home_stats['id_team'].astype(int)

    print(f" Home stats créées: {len(home_stats)} lignes")
    return home_stats

def create_away_stats(matches_df, teams_ref):
    """2.3 Créer table stats équipes extérieur"""
    print("\n CONSTRUCTION TABLE AWAY STATS")
    print("=" * 50)

    # Mapping team_name -> id_team
    team_to_id = dict(zip(teams_ref['Team_name'], teams_ref['id_team']))

    # Créer table away stats
    away_stats = pd.DataFrame({
        'id_match': range(1, len(matches_df) + 1),
        'id_team': matches_df['away_team'].map(team_to_id),
        'Number_of_goals_scored': matches_df['away_result'],
        'Number_of_goals_conceded': matches_df['home_result']
    })

    # Supprimer lignes avec id_team manquant
    away_stats = away_stats.dropna(subset=['id_team'])
    away_stats['id_team'] = away_stats['id_team'].astype(int)

    print(f" Away stats créées: {len(away_stats)} lignes")
    return away_stats

def main():
    """Workflow principal"""
    print(" CONSTRUCTION TABLES NORMALISÉES")
    print("=" * 60)

    # Chargement données
    matches_df, teams_df = load_data()

    # 1. Vérification exhaustivité équipes
    teams_complete = check_teams_completeness(matches_df, teams_df)
    if not teams_complete:
        print("\n  Continuer malgré équipes manquantes ? (y/n)")
        response = input().lower()
        if response != 'y':
            print(" Arrêt du traitement")
            return

    # 2.4 Créer référentiel équipes avec IDs
    teams_reference = create_teams_reference(matches_df)

    # 2.1 Créer table matches
    matches_table = create_matches_table(matches_df, teams_reference)

    # 2.2 Créer table home stats
    home_stats_table = create_home_stats(matches_df, teams_reference)

    # 2.3 Créer table away stats
    away_stats_table = create_away_stats(matches_df, teams_reference)

    # Affichage résultats
    print("\n RÉSUMÉ TABLES CRÉÉES")
    print("=" * 50)
    print(f" teams_reference: {len(teams_reference)} équipes")
    print(f" matches_table: {len(matches_table)} matchs")
    print(f" home_stats_table: {len(home_stats_table)} lignes")
    print(f" away_stats_table: {len(away_stats_table)} lignes")

    # Aperçu des données
    print("\n APERÇU DES DONNÉES")
    print("=" * 50)

    print("\n Teams Reference:")
    print(teams_reference.head())

    print("\n Matches Table:")
    print(matches_table.head())

    print("\n Home Stats:")
    print(home_stats_table.head())

    print("\n Away Stats:")
    print(away_stats_table.head())

    # Optionnel : Export vers CSV
    print("\n Export vers CSV ? (y/n)")
    response = input().lower()
    if response == 'y':
        teams_reference.to_csv("data/clean/teams_reference_normalized.csv", index=False)
        matches_table.to_csv("data/clean/matches_normalized.csv", index=False)
        home_stats_table.to_csv("data/clean/home_stats_normalized.csv", index=False)
        away_stats_table.to_csv("data/clean/away_stats_normalized.csv", index=False)
        print(" Fichiers exportés vers data/clean/")

if __name__ == "__main__":
    main()
