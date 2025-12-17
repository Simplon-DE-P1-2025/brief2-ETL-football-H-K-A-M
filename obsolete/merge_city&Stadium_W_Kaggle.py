import pandas as pd
import shutil

# 1. Copier Kaggle original vers processed
src_file = "data/raw/Fifa_world_cup_matches.csv"
copy_file = "data/processed/matches_2022_copy.csv"
shutil.copy(src_file, copy_file)
print(f" Copie créée : {copy_file}")

# 2. Charger la copie Kaggle
kaggle_copy = pd.read_csv(copy_file)

# 3. Charger le mapping Stadium -> City
stadiums = pd.read_csv("data/raw/world_cup_data_2022_stadium.csv")

# Vérifier les colonnes du mapping
print("Colonnes du mapping :", stadiums.columns)

# 4. Harmoniser les colonnes (renommer 'venue' en 'Stadium' si nécessaire)
if "venue" in stadiums.columns:
    stadiums = stadiums.rename(columns={"venue": "Stadium"})

# 5. Fusion Kaggle (colonne 'stadium') avec mapping (colonne 'Stadium')
enriched = kaggle_copy.merge(stadiums[["Stadium", "City"]],
                             left_on="stadium", right_on="Stadium", how="left")

# 6. Sauvegarde
enriched_file = "data/processed/matches_2022_enriched.csv"
enriched.to_csv(enriched_file, index=False)
print(f"Dataset enrichi sauvegardé : {enriched_file}")
