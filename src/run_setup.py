"""
Script d'exécution pour setup base PostgreSQL
============================================

Point d'entrée principal pour initialiser la base de données PostgreSQL Render.
Utilise la classe DatabaseManager pour un setup complet automatisé.

Usage:
    python src/run_setup.py

Prérequis:
    - Fichier .env configuré avec credentials Render
    - Données CSV v4 dans data/processed/
"""

from database.setup_database import DatabaseManager

def main():
    """Point d'entrée pour setup complet base PostgreSQL"""
    db_manager = DatabaseManager()
    success = db_manager.setup_complete()
    
    if not success:
        print("🛑 Setup échoué. Vérifier configuration et relancer.")
        exit(1)
    
    print("✅ Setup terminé avec succès !")

if __name__ == "__main__":
    main()