"""
Setup complet base PostgreSQL - Version optimisée
================================================

Script principal qui charge les tables normalisées déjà créées
par 09_tables_construction.py dans la base PostgreSQL.

Usage:
    python src/run_setup.py

Tables chargées depuis data/clean/:
    - teams_reference_normalized.csv
    - matches_normalized.csv  
    - home_stats_normalized.csv
    - away_stats_normalized.csv
"""

import pandas as pd
from pathlib import Path
from database.setup_database import DatabaseManager

def load_normalized_tables():
    """Charger les 4 tables normalisées depuis data/clean/"""
    print("📂 CHARGEMENT TABLES NORMALISÉES")
    print("=" * 40)
    
    data_path = Path("data/clean")
    
    # Dictionnaire des fichiers à charger
    files_to_load = {
        'teams_reference': 'teams_reference_normalized.csv',
        'matches_normalized': 'matches_normalized.csv',
        'home_stats': 'home_stats_normalized.csv',
        'away_stats': 'away_stats_normalized.csv'
    }
    
    loaded_data = {}
    
    for table_name, filename in files_to_load.items():
        file_path = data_path / filename
        
        if not file_path.exists():
            print(f"❌ {filename} introuvable dans {data_path}")
            return None
            
        df = pd.read_csv(file_path)
        loaded_data[table_name] = df
        print(f"✅ {table_name}: {len(df)} lignes chargées depuis {filename}")
    
    return loaded_data

def validate_database(db_manager):
    """Étape 7: Validation finale"""
    print("\n📊 VALIDATION FINALE")
    print("=" * 40)
    
    queries = {
        "Équipes": "SELECT COUNT(*) FROM teams_reference",
        "Matchs": "SELECT COUNT(*) FROM matches_normalized", 
        "Stats home": "SELECT COUNT(*) FROM home_stats",
        "Stats away": "SELECT COUNT(*) FROM away_stats"
    }
    
    try:
        with db_manager.engine.connect() as conn:
            for name, query in queries.items():
                from sqlalchemy import text
                count = conn.execute(text(query)).scalar()
                print(f"✅ {name}: {count}")
        return True
    except Exception as e:
        print(f"❌ Erreur validation: {e}")
        return False

def main():
    """Orchestrateur principal - Version optimisée"""
    print("🚀 SETUP BASE FIFA WORLD CUP - TABLES PRÉEXISTANTES")
    print("=" * 55)
    
    # Charger toutes les tables normalisées déjà créées
    normalized_data = load_normalized_tables()
    if normalized_data is None:
        print("❌ Impossible de charger les tables normalisées")
        exit(1)
    
    # Setup base de données
    print("\n💾 SETUP BASE DE DONNÉES")
    print("=" * 40)
    
    db_manager = DatabaseManager()
    
    try:
        # Connexion
        db_manager.connect_database()
        
        # Créer structure
        db_manager.create_simple_tables()
        
        # Charger chaque table
        counts = {}
        for table_name, df in normalized_data.items():
            counts[table_name] = db_manager.load_single_table(table_name, df)
        
        # Validation finale
        if validate_database(db_manager):
            print("\n🎉 SETUP TERMINÉ AVEC SUCCÈS !")
            print(f"📊 Chargement: {counts['teams_reference']} équipes, {counts['matches_normalized']} matchs")
            print(f"📊 Stats: {counts['home_stats']} home + {counts['away_stats']} away")
            print("🔗 Base PostgreSQL prête pour requêtes KPI !")
        else:
            print("⚠️  Setup terminé mais validation échouée")
            
    except Exception as e:
        print(f"❌ Erreur setup: {e}")
        exit(1)

if __name__ == "__main__":
    main()