import pandas as pd
import numpy as np

def generate_final_kpi_table():
    print("Chargement des données...")
    # 1. Charger les deux fichiers propres
    df_matches = pd.read_csv("data/processed/matches_unified_v4.csv")
    df_teams = pd.read_csv("data/reference/teams_v4.csv")

    # 2. Créer un dictionnaire pour traduire ID -> Nom
    # Ex: {78: 'France', 10: 'Argentina'}
    team_map = pd.Series(df_teams.team_canonical.values, index=df_teams.team_id).to_dict()

    print("Traduction des IDs en Noms...")
    # 3. Remplacer les IDs par les noms pour les équipes domicile et extérieur
    df_matches['home_team'] = df_matches['home_team_id'].map(team_map)
    df_matches['away_team'] = df_matches['away_team_id'].map(team_map)

    # 4. Gérer la colonne résultat (qui contient soit un ID, soit "draw")
    # On crée une fonction qui essaie de traduire si c'est un nombre
    def translate_result(val):
        if str(val).isdigit(): # Si c'est un ID (ex: "78")
            return team_map.get(int(val), "Unknown")
        return val # Si c'est "draw", on laisse "draw"

    df_matches['result_name'] = df_matches['result'].apply(translate_result)

    # 5. Nettoyer l'édition (Ex: "1930-URUGUAY" -> "1930")
    df_matches['edition_year'] = df_matches['edition'].astype(str).apply(lambda x: x.split('-')[0])

    # 6. Ajouter la colonne is_final
    # On considère que tout ce qui n'est pas "Preliminary" ou "Qualification" fait partie du tournoi final
    # A adapter selon les valeurs exactes dans la colonne 'round'
    # 6) Ajouter la colonne is_final (WHITELIST, plus fiable)
    final_rounds = {
        "Group",
        "Round of 16",
        "Quarter-finals",
        "Semi-finals",
        "Final",
        "Match for third place",
    }

    # On normalise un peu pour éviter les soucis de casse/espaces
    r = df_matches["round"].astype(str).str.strip()

    df_matches["is_final"] = r.isin(final_rounds)

    # 7. Sélectionner et ordonner EXACTEMENT les colonnes demandées
    # On renomme result_name en result pour écraser l'ancien
    final_cols = [
        'id_match', 
        'home_team', 
        'away_team', 
        'home_result', 
        'away_result', 
        'result_name', # Sera renommé en 'result'
        'date', 
        'round', 
        'city', 
        'edition_year', # Sera renommé en 'edition'
        'is_final'      # Colonne bonus très utile
    ]
    
    df_final = df_matches[final_cols].copy()
    df_final.rename(columns={
        'result_name': 'result',
        'edition_year': 'edition'
    }, inplace=True)

    # 8. Vérifications
    print("\n--- Aperçu des 5 dernières lignes (2022) ---")
    print(df_final.tail())
    
    print(f"\n--- Vérification doublons/manquants ---")
    print(f"Total matchs: {len(df_final)}")
    print(f"Équipes manquantes: {df_final['home_team'].isnull().sum()}")

    # 9. Sauvegarde
    output_path = "data/clean/matches_final_kpi.csv"
    df_final.to_csv(output_path, index=False)
    print(f"\nFichier final généré avec succès : {output_path}")

if __name__ == "__main__":
    generate_final_kpi_table()