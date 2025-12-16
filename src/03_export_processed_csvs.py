from pathlib import Path
import pandas as pd
import json

# On réutilise le parseur 2022 en important ses fonctions (si tu veux)
# Sinon, on peut appeler le script 2022 séparément.
# Ici je te mets une version autonome "simple": on lit déjà matches_2022.csv si existant,
# sinon on t'indique de lancer extract_2022_from_text.py.

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

def export_1930_2010_raw():
    path = RAW / "matches_19302010.csv"
    df = pd.read_csv(path)
    out = PROCESSED / "matches_1930_2010_raw.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    print("Saved:", out)

def export_2014_raw():
    candidates = list(RAW.glob("WorldCupMatches2014*.csv"))
    if not candidates:
        raise FileNotFoundError("Aucun fichier WorldCupMatches2014*.csv dans data/raw")
    df = pd.read_csv(candidates[0])
    out = PROCESSED / "matches_2014_raw.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    print("Saved:", out)

def export_2018_clean():
    path = RAW / "data_2018.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    teams = {t["id"]: t["name"] for t in data["teams"]}
    stadiums = {s["id"]: s["city"] for s in data["stadiums"]}

    matches = []
    for group in data["groups"].values():
        for m in group["matches"]:
            matches.append({
                "home_team": teams[m["home_team"]],
                "away_team": teams[m["away_team"]],
                "home_result": m["home_result"],
                "away_result": m["away_result"],
                "date": m["date"],
                "round": "Group",
                "city": stadiums[m["stadium"]],
                "edition": "2018",
            })

    for round_data in data["knockout"].values():
        for m in round_data["matches"]:
            matches.append({
                "home_team": teams[m["home_team"]],
                "away_team": teams[m["away_team"]],
                "home_result": m["home_result"],
                "away_result": m["away_result"],
                "date": m["date"],
                "round": round_data["name"],
                "city": stadiums[m["stadium"]],
                "edition": "2018",
            })

    df = pd.DataFrame(matches)
    out = PROCESSED / "matches_2018_clean.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    print("Saved:", out)

def ensure_2022_csv():
    out = PROCESSED / "matches_2022.csv"
    if out.exists():
        print("Found:", out)
        return

    print("matches_2022.csv not found in data/processed.")
    print("Run this first:")
    print("  py src/extract_2022_from_text.py")

def main():
    export_1930_2010_raw()
    export_2014_raw()
    export_2018_clean()
    ensure_2022_csv()

if __name__ == "__main__":
    main()
