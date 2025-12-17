# Guide Setup Base PostgreSQL Render

Guide complet pour déployer la base de données du projet FIFA World Cup sur Render (gratuit).

## 📋 **Prérequis**
- Compte GitHub (recommandé pour Render)
- Python 3.8+ installé
- Accès au repository cloné

---

## 🚀 **1. SETUP BASE DE DONNÉES SUR RENDER**

### 1.1 Créer un compte Render
1. Aller sur **https://render.com/**
2. **Sign Up** avec votre compte GitHub
3. Confirmer votre email

### 1.2 Créer la base PostgreSQL
1. **Dashboard Render** → Cliquer **"New +"**
2. Sélectionner **"PostgreSQL"**
3. **Configuration** :
   - **Name**: `fifa-worldcup-db`
   - **Database**: `fifa_worldcup`  
   - **User**: `fifa_user`
   - **Region**: `Frankfurt (EU Central)`
   - **PostgreSQL Version**: `15`
   - **Plan**: **Free** (512MB RAM)
4. Cliquer **"Create Database"**

### 1.3 Récupérer les identifiants
**⏳ Attendre 2-3 minutes** que la base soit prête, puis :
1. Aller sur votre dashboard Render
2. Cliquer sur **"fifa-worldcup-db"**
3. Onglet **"Info"** 
4. **Copier** l'**External Database URL** complète

---

## 🔧 **2. TEST DE CONNEXION ET CONFIGURATION REPO**

### 2.1 Préparer l'environnement Python
```bash
# Activer l'environnement (Git Bash)
source scripts/start.sh

# Ou manuellement :
python -m venv venv
source venv/Scripts/activate  # Git Bash
pip install -r requirements.txt
```

### 2.2 Configurer les credentials
1. **Ouvrir** le fichier `.env` à la racine du projet
2. **Remplacer** la ligne `RENDER_DATABASE_URL` par votre URL Render complète :
   ```
   RENDER_DATABASE_URL=postgresql://fifa_user:MOT_DE_PASSE@dpg-xxxxx-a.frankfurt-postgres.render.com/fifa_worldcup
   ```

### 2.3 Tester la connexion
```bash
python scripts/test_connection.py
```

**✅ Résultat attendu :**
```
🔧 TEST CONNEXION POSTGRESQL RENDER
==================================================
✅ Fichier .env chargé: /path/to/.env
🔗 Tentative connexion...
✅ CONNEXION RÉUSSIE !
📊 Version PostgreSQL: PostgreSQL 15.x
✅ Test écriture/lecture: OK (1 lignes)

🎉 CONNEXION OPÉRATIONNELLE !
➡️  Prochaine étape: python scripts/setup_database.py
```

---

## 📊 **3. CHARGEMENT DES DONNÉES**

### 3.1 Structure de la base
Le script va créer automatiquement :
- **Table `matches`** : 833 matchs FIFA (1930-2022) avec 10 colonnes exactes
- **Table `teams`** : 124 équipes uniques pour les analyses

### 3.2 Charger les données finales
```bash
python scripts/setup_database.py
```

**✅ Résultat attendu :**
```
🚀 SETUP BASE POSTGRESQL RENDER
==================================================
✅ Connexion réussie (URL)
✅ Tables créées avec succès
📂 Fichier lu: 833 matchs, colonnes: ['id_match', 'home_team', ...]
✅ 833 matchs chargés en BDD
✅ 124 équipes chargées

📊 VALIDATION DONNÉES
==================================================
Total matchs: 833
Matchs par édition:
  ('2022', 64)
  ('2014', 73)
  ...

🎉 SETUP TERMINÉ AVEC SUCCÈS !
```

### 3.3 Vérification finale
Votre base PostgreSQL sur Render contient maintenant :
- ✅ **833 matchs** de Coupe du Monde (1930-2022)
- ✅ **124 équipes** référencées  
- ✅ **Tables optimisées** pour requêtes KPI rapides

---

## 🎯 **PROCHAINES ÉTAPES**
1. **Analyses KPI** : Lancer les scripts d'analyse des données
2. **Dashboard** : Créer visualisations avec les données en base
3. **API** : Exposer les données via endpoints REST

## 🆘 **Dépannage**
- **Erreur connexion** : Vérifier l'URL dans `.env`
- **Erreur chargement** : S'assurer que `data/processed/matches_unified_v4.csv` existe
- **Base pleine** : Plan gratuit limité à 512MB (suffisant pour ce projet)