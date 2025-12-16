from pathlib import Path
import pandas as pd
import json

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"


def load_1930_2010() -> pd.DataFrame:
    path = RAW / "matches_19302010.csv"
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")

    # Astuce: si jamais ça plante, on changera encoding / sep
    df = pd.read_csv(path)
    return df


def load_2014() -> pd.DataFrame:
    # ton fichier s'appelle souvent "WorldCupMatches2014-WorldCupMatches2014.csv"
    candidates = list(RAW.glob("WorldCupMatches2014*.csv"))
    if not candidates:
        raise FileNotFoundError(f"Aucun fichier WorldCupMatches2014*.csv trouvé dans {RAW}")

    path = candidates[0]
    df = pd.read_csv(path)
    return df


def load_2018() -> pd.DataFrame:
    path = RAW / "data_2018.json"
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")

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
                "edition": "2018"
            })

    for round_data in data["knockout"].values():
        round_name = round_data["name"]
        for m in round_data["matches"]:
            matches.append({
                "home_team": teams[m["home_team"]],
                "away_team": teams[m["away_team"]],
                "home_result": m["home_result"],
                "away_result": m["away_result"],
                "date": m["date"],
                "round": round_name,
                "city": stadiums[m["stadium"]],
                "edition": "2018"
            })

    return pd.DataFrame(matches)


def quick_check(name: str, df: pd.DataFrame) -> None:
    print("\n" + "=" * 80)
    print(name)
    print("shape:", df.shape)
    print("columns:", df.columns.tolist())
    print(df.head(3))


def main():
    df_1930_2010 = load_1930_2010()
    df_2014 = load_2014()
    df_2018 = load_2018()

    quick_check("1930-2010 (raw)", df_1930_2010)
    quick_check("2014 (raw)", df_2014)
    quick_check("2018 (clean-ish)", df_2018)


if __name__ == "__main__":
    main()
