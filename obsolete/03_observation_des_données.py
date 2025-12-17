import pandas as pd

# Charger la copie Kaggle
df = pd.read_csv("data/processed/matches_2022_copy.csv")

# 1. Types des colonnes
print("Types des colonnes :")
print(df.dtypes)

# 2. Valeurs manquantes
print("\nValeurs manquantes par colonne :")
print(df.isna().sum())

# 3. Doublons
print("\nNombre de doublons :", df.duplicated().sum())

# 4. Aperçu des colonnes textuelles, numériques, catégorielles, binaires
text_cols = df.select_dtypes(include="object").columns
num_cols = df.select_dtypes(include="number").columns

print("\nColonnes textuelles :", list(text_cols))
print("Colonnes numériques :", list(num_cols))

# 5. Obersations de quelques d’incohérences
print("\nValeurs uniques dans 'stadium' :", df['stadium'].unique()[:10])
print("Valeurs uniques dans 'score1' :", df['score1'].unique()[:10])
