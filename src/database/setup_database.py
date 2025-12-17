"""
Setup et chargement base PostgreSQL Render
==========================================

Script rapide pour :
1. Connexion PostgreSQL Render  
2. Création tables optimales
3. Chargement matches_unified_v4.csv (ZÉRO modification)
4. Validation données chargées

Usage:
    python scripts/setup_database.py

Prérequis:
    pip install psycopg2-binary python-dotenv
"""

import os
from pathlib import Path
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Chemins
ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "processed"
ENV_FILE = ROOT / ".env"

# Fichiers données
MATCHES_FILE = DATA / "matches_unified_v4.csv"
TEAMS_FILE = DATA / "teams_v4.csv"

def env_file_does_not_exist():
    """Vérifier existence .env (ne crée PAS de fichier)"""
    if ENV_FILE.exists():
        print(f"✅ Fichier {ENV_FILE} trouvé")
        return False
    
    print(f"❌ Fichier .env introuvable : {ENV_FILE}")
    print("⚠️  IMPORTANT : Créer un fichier .env depuis le template .env.example")
    print("   Commande: cp .env.example .env")
    print("   Puis éditer .env avec vos vraies credentials Render")
    return True

def get_database_connection():
    """Connexion PostgreSQL avec gestion erreurs"""
    load_dotenv(ENV_FILE)
    
    # Connexion via URL complète
    db_url = os.getenv("RENDER_DATABASE_URL")
    if not db_url or "VOTRE_MOT_DE_PASSE" in db_url:
        print("❌ RENDER_DATABASE_URL manquante ou non configurée dans .env")
        raise ValueError("RENDER_DATABASE_URL manquante dans .env")
        
    try:
        print("🔗 Tentative connexion...")
        engine = create_engine(db_url)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"✅ CONNEXION RÉUSSIE !")
            print(f"📊 Version PostgreSQL: {version}")
            return engine
            
    except Exception as e:
        print(f"❌ Erreur connexion: {e}")
        raise

def create_tables(engine):
    """Créer tables BDD si inexistantes"""
    
    # Schema exact du livrable matches_unified_v4.csv
    create_matches_sql = """
    CREATE TABLE IF NOT EXISTS matches (
        id_match     SERIAL PRIMARY KEY,
        home_team    VARCHAR(100) NOT NULL,
        away_team    VARCHAR(100) NOT NULL,  
        home_result  INTEGER,
        away_result  INTEGER,
        result       VARCHAR(100),
        date         DATE,
        round        VARCHAR(50),
        city         VARCHAR(100),
        edition      VARCHAR(4)
    );
    
    CREATE INDEX IF NOT EXISTS idx_matches_edition ON matches(edition);
    CREATE INDEX IF NOT EXISTS idx_matches_teams ON matches(home_team, away_team);
    CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date);
    """
    
    # Table dimension équipes (pour KPIs)
    create_teams_sql = """
    CREATE TABLE IF NOT EXISTS teams (
        team_id      SERIAL PRIMARY KEY,
        team_name    VARCHAR(100) UNIQUE NOT NULL
    );
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_matches_sql))
            conn.execute(text(create_teams_sql))
            conn.commit()
        print("✅ Tables créées avec succès")
        
    except Exception as e:
        print(f"❌ Erreur création tables: {e}")
        raise

def load_matches_data(engine):
    """Charger matches_unified_v4.csv (ZERO modification)"""
    
    if not MATCHES_FILE.exists():
        raise FileNotFoundError(f"Fichier introuvable: {MATCHES_FILE}")
    
    try:
        # Lecture CSV exact
        df = pd.read_csv(MATCHES_FILE)
        print(f"📂 Fichier lu: {len(df)} matchs, colonnes: {list(df.columns)}")
        
        # Conversion types pour PostgreSQL
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['home_result'] = pd.to_numeric(df['home_result'], errors='coerce')  
        df['away_result'] = pd.to_numeric(df['away_result'], errors='coerce')
        
        # Chargement PostgreSQL (remplace si existe)
        df.to_sql('matches', engine, if_exists='replace', index=False, method='multi')
        
        print(f"✅ {len(df)} matchs chargés en BDD")
        return len(df)
        
    except Exception as e:
        print(f"❌ Erreur chargement matches: {e}")
        raise

def load_teams_data(engine):
    """Charger dimension teams (optionnel)"""
    
    if not TEAMS_FILE.exists():
        print("⚠️  teams_v4.csv introuvable, création à la volée...")
        # Extraction équipes depuis matches
        df_matches = pd.read_csv(MATCHES_FILE)
        teams = set()
        teams.update(df_matches['home_team'].dropna())
        teams.update(df_matches['away_team'].dropna())
        df_teams = pd.DataFrame({'team_name': sorted(teams)})
    else:
        df_teams = pd.read_csv(TEAMS_FILE)
        df_teams = df_teams.rename(columns={'team': 'team_name'})
        df_teams = df_teams[['team_name']].drop_duplicates()
    
    try:
        df_teams.to_sql('teams', engine, if_exists='replace', index=False)
        print(f"✅ {len(df_teams)} équipes chargées")
        return len(df_teams)
        
    except Exception as e:
        print(f"❌ Erreur chargement teams: {e}")
        return 0

def validate_database(engine):
    """Validation données chargées"""
    
    queries = {
        "Total matchs": "SELECT COUNT(*) FROM matches",
        "Matchs par édition": """
            SELECT edition, COUNT(*) as matchs 
            FROM matches 
            GROUP BY edition 
            ORDER BY edition DESC 
            LIMIT 5
        """,
        "Échantillon données": """
            SELECT id_match, home_team, away_team, 
                   home_result, away_result, edition
            FROM matches 
            ORDER BY id_match 
            LIMIT 3
        """,
        "Dates nulles": "SELECT COUNT(*) FROM matches WHERE date IS NULL",
        "Total équipes": "SELECT COUNT(*) FROM teams"
    }
    
    print("\n📊 VALIDATION DONNÉES")
    print("=" * 50)
    
    try:
        with engine.connect() as conn:
            for name, query in queries.items():
                result = conn.execute(text(query))
                if "par édition" in name or "Échantillon" in name:
                    rows = result.fetchall()
                    print(f"{name}:")
                    for row in rows:
                        print(f"  {row}")
                else:
                    count = result.scalar()
                    print(f"{name}: {count}")
                    
    except Exception as e:
        print(f"❌ Erreur validation: {e}")

def main():
    """Workflow complet"""
    print("🚀 SETUP BASE POSTGRESQL RENDER")
    print("=" * 50)
    
    # 1. Vérifier l'existance du .env
    if env_file_does_not_exist():
        print("🛑 Configurer .env puis relancer le script")
        return
    
    # 2. Connexion BDD
    try:
        engine = get_database_connection()
    except Exception:
        print("🛑 Impossible de se connecter. Vérifier credentials Render dans .env")
        return
    
    # 3. Créer tables
    create_tables(engine)
    
    # 4. Charger données
    matches_count = load_matches_data(engine)
    teams_count = load_teams_data(engine)
    
    # 5. Validation
    validate_database(engine)
    
    print("\n🎉 SETUP TERMINÉ AVEC SUCCÈS !")
    print(f"📊 {matches_count} matchs + {teams_count} équipes chargées")
    print("🔗 Base prête pour requêtes KPI")

if __name__ == "__main__":
    main()