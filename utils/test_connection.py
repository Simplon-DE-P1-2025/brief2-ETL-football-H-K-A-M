"""
Test rapide connexion PostgreSQL Render
=======================================

Script pour vérifier que les credentials .env fonctionnent

Usage:
    python scripts/test_connection.py
"""

import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Chemin vers .env
ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT /".env"

def test_connection():
    """Tester connexion PostgreSQL Render"""

    print("🔧 TEST CONNEXION POSTGRESQL RENDER")
    print("=" * 50)

    # Charger .env
    if not ENV_FILE.exists():
        print(f" Fichier .env introuvable: {ENV_FILE}")
        return False

    load_dotenv(ENV_FILE)
    print(f" Fichier .env chargé: {ENV_FILE}")

    # Connexion via URL complète
    db_url = os.getenv("RENDER_DATABASE_URL")
    if not db_url or "VOTRE_MOT_DE_PASSE" in db_url:
        print(" RENDER_DATABASE_URL manquante ou non configurée dans .env")
        return False

    try:
        print(" Tentative connexion...")
        engine = create_engine(db_url)

        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f" CONNEXION RÉUSSIE !")
            print(f" Version PostgreSQL: {version}")

            # Test écriture/lecture rapide
            conn.execute(text("CREATE TABLE IF NOT EXISTS test_connection (id SERIAL, created_at TIMESTAMP DEFAULT NOW())"))
            conn.execute(text("INSERT INTO test_connection DEFAULT VALUES"))
            count = conn.execute(text("SELECT COUNT(*) FROM test_connection")).scalar()
            conn.execute(text("DROP TABLE test_connection"))
            conn.commit()

            print(f" Test écriture/lecture: OK ({count} lignes)")
            return True

    except Exception as e:
        print(f" Erreur connexion: {e}")
        return False

def main():
    success = test_connection()

    if success:
        print("\n🎉 CONNEXION OPÉRATIONNELLE !")
        print("➡️  Prochaine étape: python scripts/setup_database.py")
    else:
        print("\n🛑 CONNEXION ÉCHOUÉE")
        print("➡️  Vérifier credentials dans .env")

if __name__ == "__main__":
    main()
