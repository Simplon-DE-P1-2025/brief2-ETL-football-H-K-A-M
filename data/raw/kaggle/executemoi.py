import kagglehub
import os
import shutil
from pathlib import Path

# 1️⃣ Télécharger le dataset (cache kagglehub)
dataset_path = kagglehub.dataset_download("basharalkuwaiti/fifa-world-cup")

print("Dataset téléchargé dans :", dataset_path)

# 2️⃣ Dossier cible : /data/raw/kaggle (relatif au projet)
target_dir = Path("data/raw") / "kaggle"
target_dir.mkdir(parents=True, exist_ok=True)

print("Dossier cible :", target_dir.resolve())

# 3️⃣ Copier les fichiers dans /data/raw/kaggle
for file in os.listdir(dataset_path):
    src = Path(dataset_path) / file
    dst = target_dir / file

    if src.is_file():
        shutil.copy2(src, dst)
        print(f"Copié : {file}")

print("✅ Dataset disponible dans /data/raw/kaggle")