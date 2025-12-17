"""
DatabaseManager - Gestionnaire PostgreSQL FIFA World Cup
========================================================

Classe simplifiée pour charger les tables normalisées FIFA World Cup
dans PostgreSQL Render sans contraintes.

Usage:
    db = DatabaseManager()
    db.connect_database()
    db.create_simple_tables() 
    db.load_single_table('teams_reference', df)

Prérequis: .env avec RENDER_DATABASE_URL
"""

import os
from pathlib import Path
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

class DatabaseManager:
    """Gestionnaire simple base PostgreSQL Render"""
    
    def __init__(self):
        """Initialiser gestionnaire BDD simplifié"""
        self.root = Path(__file__).resolve().parents[2]
        self.env_file = self.root / ".env"
        self.engine = None
    
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
        """Connexion simple à PostgreSQL"""
        load_dotenv(self.env_file)
        
        db_url = os.getenv("RENDER_DATABASE_URL")
        if not db_url:
            raise ValueError("RENDER_DATABASE_URL manquante dans .env")
            
        self.engine = create_engine(db_url)
        print("✅ Connexion PostgreSQL établie")
        return True
    
    def create_simple_tables(self):
        """Créer tables simples SANS contraintes ni index"""
        sql_commands = [
            """
            DROP TABLE IF EXISTS teams_reference CASCADE;
            CREATE TABLE teams_reference (
                id_team      INTEGER,
                team_name    VARCHAR(100)
            );
            """,
            """
            DROP TABLE IF EXISTS matches_normalized CASCADE;
            CREATE TABLE matches_normalized (
                id_match     INTEGER,
                result       INTEGER,
                date         DATE,
                round        VARCHAR(50),
                city         VARCHAR(100),
                edition      VARCHAR(4),
                is_final     BOOLEAN
            );
            """,
            """
            DROP TABLE IF EXISTS home_stats CASCADE;
            CREATE TABLE home_stats (
                id_match                    INTEGER,
                id_team                     INTEGER,
                number_of_goals_scored      INTEGER,
                number_of_goals_conceded    INTEGER
            );
            """,
            """
            DROP TABLE IF EXISTS away_stats CASCADE;
            CREATE TABLE away_stats (
                id_match                    INTEGER,
                id_team                     INTEGER,
                number_of_goals_scored      INTEGER,
                number_of_goals_conceded    INTEGER
            );
            """
        ]
        
        with self.engine.connect() as conn:
            for sql in sql_commands:
                conn.execute(text(sql))
            conn.commit()
        
        print("✅ Tables simples créées (sans contraintes)")
        return True
    
    def load_single_table(self, table_name, df):
        """Charger un DataFrame dans une table"""
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"✅ {table_name}: {len(df)} lignes chargées")
            return len(df)
        except Exception as e:
            print(f"❌ Erreur {table_name}: {e}")
            return 0