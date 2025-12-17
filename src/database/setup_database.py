"""
Setup et chargement base PostgreSQL Render - Version Classe
==========================================================

Classe modulaire pour :
1. Vérification existence .env
2. Connexion PostgreSQL Render  
3. Création tables configurables
4. Chargement données CSV
5. Validation données chargées

Usage:
    from src.database.setup_database import DatabaseManager
    
    db = DatabaseManager()
    db.setup_complete()

Prérequis:
    pip install psycopg2-binary python-dotenv
"""

import os
from pathlib import Path
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

class DatabaseManager:
    """Gestionnaire modulaire base PostgreSQL Render"""
    
    def __init__(self, root_path=None, data_folder="processed"):
        """
        Initialiser gestionnaire BDD
        
        Args:
            root_path: Chemin racine projet (auto-détecté si None)
            data_folder: Dossier des données ("processed" par défaut)
        """
        self.root = Path(root_path) if root_path else Path(__file__).resolve().parents[2]
        self.data_path = self.root / "data" / data_folder
        self.env_file = self.root / ".env"
        self.engine = None
        
        # Configuration tables par défaut (modulable)
        self.table_schemas = {
            "matches": {
                "sql": """
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
                """,
                "indexes": [
                    "CREATE INDEX IF NOT EXISTS idx_matches_edition ON matches(edition);",
                    "CREATE INDEX IF NOT EXISTS idx_matches_teams ON matches(home_team, away_team);",
                    "CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date);"
                ]
            },
            "teams": {
                "sql": """
                CREATE TABLE IF NOT EXISTS teams (
                    team_id      SERIAL PRIMARY KEY,
                    team_name    VARCHAR(100) UNIQUE NOT NULL
                );
                """,
                "indexes": []
            }
        }
        
        # Configuration données par défaut (modulable)
        self.data_config = {
            "matches": {
                "file": "matches_unified_v4.csv",
                "table": "matches",
                "type_conversions": {
                    "date": "datetime",
                    "home_result": "numeric",
                    "away_result": "numeric"
                }
            },
            "teams": {
                "file": "teams_v4.csv",
                "table": "teams",
                "column_mapping": {"team": "team_name"},
                "fallback_from_matches": True
            }
        }
    
    def check_env_file(self):
        """1. Vérifier existence .env (ne crée PAS de fichier)"""
        if self.env_file.exists():
            print(f"✅ Fichier {self.env_file} trouvé")
            return True
        
        print(f"❌ Fichier .env introuvable : {self.env_file}")
        print("⚠️  IMPORTANT : Créer un fichier .env depuis le template .env.example")
        print("   Commande: cp .env.example .env")
        print("   Puis éditer .env avec vos vraies credentials Render")
        return False
    
    def connect_database(self):
        """2. Connexion PostgreSQL avec gestion erreurs"""
        load_dotenv(self.env_file)
        
        # Connexion via URL complète
        db_url = os.getenv("RENDER_DATABASE_URL")
        if not db_url or "VOTRE_MOT_DE_PASSE" in db_url:
            print("❌ RENDER_DATABASE_URL manquante ou non configurée dans .env")
            raise ValueError("RENDER_DATABASE_URL manquante dans .env")
            
        try:
            print("🔗 Tentative connexion...")
            self.engine = create_engine(db_url)
            
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.scalar()
                print(f"✅ CONNEXION RÉUSSIE !")
                print(f"📊 Version PostgreSQL: {version}")
                return True
                
        except Exception as e:
            print(f"❌ Erreur connexion: {e}")
            raise
    
    def create_tables(self, custom_schemas=None):
        """3. Créer tables BDD (modulable)"""
        schemas = custom_schemas or self.table_schemas
        
        try:
            with self.engine.connect() as conn:
                for table_name, config in schemas.items():
                    # Créer table
                    conn.execute(text(config["sql"]))
                    print(f"✅ Table '{table_name}' créée")
                    
                    # Créer indexes
                    for index_sql in config.get("indexes", []):
                        conn.execute(text(index_sql))
                    
                conn.commit()
            print("✅ Toutes les tables créées avec succès")
            
        except Exception as e:
            print(f"❌ Erreur création tables: {e}")
            raise
    
    def load_data(self, custom_config=None):
        """4. Charger données CSV (modulable)"""
        config = custom_config or self.data_config
        loaded_counts = {}
        
        for data_name, data_info in config.items():
            try:
                count = self._load_single_dataset(data_name, data_info)
                loaded_counts[data_name] = count
            except Exception as e:
                print(f"❌ Erreur chargement {data_name}: {e}")
                loaded_counts[data_name] = 0
        
        return loaded_counts
    
    def _load_single_dataset(self, name, config):
        """Charger un seul dataset"""
        file_path = self.data_path / config["file"]
        
        # Vérifier existence fichier
        if not file_path.exists():
            if config.get("fallback_from_matches") and name == "teams":
                return self._create_teams_from_matches(config)
            else:
                raise FileNotFoundError(f"Fichier introuvable: {file_path}")
        
        # Charger CSV
        df = pd.read_csv(file_path)
        print(f"📂 {name}: {len(df)} lignes, colonnes: {list(df.columns)}")
        
        # Renommer colonnes si nécessaire
        if "column_mapping" in config:
            df = df.rename(columns=config["column_mapping"])
        
        # Conversion types
        if "type_conversions" in config:
            for col, dtype in config["type_conversions"].items():
                if col in df.columns:
                    if dtype == "datetime":
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    elif dtype == "numeric":
                        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Sélectionner colonnes si spécifié
        if "columns" in config:
            df = df[config["columns"]].drop_duplicates()
        
        # Charger en BDD
        df.to_sql(config["table"], self.engine, if_exists='replace', index=False, method='multi')
        print(f"✅ {name}: {len(df)} lignes chargées en BDD")
        
        return len(df)
    
    def _create_teams_from_matches(self, config):
        """Créer table teams depuis matches (fallback)"""
        print("⚠️  teams_v4.csv introuvable, création à la volée...")
        
        matches_file = self.data_path / self.data_config["matches"]["file"]
        df_matches = pd.read_csv(matches_file)
        
        teams = set()
        teams.update(df_matches['home_team'].dropna())
        teams.update(df_matches['away_team'].dropna())
        df_teams = pd.DataFrame({'team_name': sorted(teams)})
        
        df_teams.to_sql(config["table"], self.engine, if_exists='replace', index=False)
        return len(df_teams)
    
    def validate_data(self, custom_queries=None):
        """5. Validation données chargées (modulable)"""
        queries = custom_queries or {
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
            with self.engine.connect() as conn:
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
    
    def setup_complete(self):
        """Workflow complet automatique"""
        print("🚀 SETUP BASE POSTGRESQL RENDER")
        print("=" * 50)
        
        # 1. Vérifier .env
        if not self.check_env_file():
            print("🛑 Configurer .env puis relancer")
            return False
        
        # 2. Connexion BDD
        try:
            self.connect_database()
        except Exception:
            print("🛑 Impossible de se connecter. Vérifier credentials Render dans .env")
            return False
        
        # 3. Créer tables
        self.create_tables()
        
        # 4. Charger données
        counts = self.load_data()
        
        # 5. Validation
        self.validate_data()
        
        print("\n🎉 SETUP TERMINÉ AVEC SUCCÈS !")
        print(f"📊 {counts.get('matches', 0)} matchs + {counts.get('teams', 0)} équipes chargées")
        print("🔗 Base prête pour requêtes KPI")
        return True