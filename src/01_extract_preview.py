from __future__ import annotations

from pathlib import Path
import pandas as pd
import json
import re

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"

RAW.mkdir(parents=True, exist_ok=True)
PROCESSED.mkdir(parents=True, exist_ok=True)

OUT_1930_2010 = PROCESSED / "matches_1930_2010_extracted.csv"
OUT_2014 = PROCESSED / "matches_2014_extracted.csv"
OUT_2018 = PROCESSED / "matches_2018_extracted.csv"

SCORE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)")


def normalize_text(s: object) -> str:
    if s is None or pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def clean_city(s: object) -> str:
    return normalize_text(s).rstrip(".").strip()


def load_1930_2010_extracted() -> pd.DataFrame:
    path = RAW / "matches_19302010.csv"
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")

    df = pd.read_csv(path)

    scores = df["score"].astype(str).str.extract(SCORE_RE)
    home_result = pd.to_numeric(scores[0], errors="coerce")
    away_result = pd.to_numeric(scores[1], errors="coerce")

    year_col = df["year"] if "year" in df.columns else df["edition"]
    date_placeholder = pd.to_datetime(year_col, format="%Y", errors="coerce").dt.strftime("%Y-01-01")

    out = pd.DataFrame(
        {
            "home_team": df["team1"].apply(normalize_text),
            "away_team": df["team2"].apply(normalize_text),
            "home_result": home_result,
            "away_result": away_result,
            "date": date_placeholder,
            "round": df["round"].apply(normalize_text),
            "city": df["venue"].apply(clean_city),
            "edition": pd.to_numeric(year_col, errors="coerce").astype("Int64").astype(str),
        }
    )
    return out


def load_2014_extracted() -> pd.DataFrame:
    candidates = list(RAW.glob("WorldCupMatches2014*.csv"))
    if not candidates:
        raise FileNotFoundError(f"Aucun fichier WorldCupMatches2014*.csv trouvé dans {RAW}")

    path = candidates[0]
    df = pd.read_csv(path)

    if "MatchID" in df.columns:
        df = df.drop_duplicates(subset=["MatchID"]).copy()

    out = pd.DataFrame(
        {
            "home_team": df["Home Team Name"].apply(normalize_text),
            "away_team": df["Away Team Name"].apply(normalize_text),
            "home_result": pd.to_numeric(df["Home Team Goals"], errors="coerce"),
            "away_result": pd.to_numeric(df["Away Team Goals"], errors="coerce"),
            "date": pd.to_datetime(df["Datetime"], errors="coerce").dt.strftime("%Y-%m-%d"),
            "round": df["Stage"].apply(normalize_text),
            "city": df["City"].apply(clean_city),
            "edition": (df["Year"].astype(str) if "Year" in df.columns else "2014"),
        }
    )
    return out


def load_2018_extracted_loose() -> pd.DataFrame:
    """
    Version NON stricte: on prend TOUT (group + knockout),
    et on évite toute logique "2018-RUSSIA".
    """
    path = RAW / "data_2018.json"
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    teams = {t["id"]: t["name"] for t in data["teams"]}
    stadiums = {s["id"]: s["city"] for s in data["stadiums"]}

    matches: list[dict] = []

    # groups
    for group in data.get("groups", {}).values():
        for m in group.get("matches", []):
            matches.append(
                {
                    "home_team": teams.get(m.get("home_team"), ""),
                    "away_team": teams.get(m.get("away_team"), ""),
                    "home_result": m.get("home_result"),
                    "away_result": m.get("away_result"),
                    "date": m.get("date"),
                    "round": "Group",
                    "city": stadiums.get(m.get("stadium"), ""),
                    "edition": "2018",
                }
            )

    # knockout
    for round_data in data.get("knockout", {}).values():
        round_name = normalize_text(round_data.get("name", ""))
        for m in round_data.get("matches", []):
            matches.append(
                {
                    "home_team": teams.get(m.get("home_team"), ""),
                    "away_team": teams.get(m.get("away_team"), ""),
                    "home_result": m.get("home_result"),
                    "away_result": m.get("away_result"),
                    "date": m.get("date"),
                    "round": round_name,
                    "city": stadiums.get(m.get("stadium"), ""),
                    "edition": "2018",
                }
            )

    df = pd.DataFrame(matches)

    # nettoyage soft
    df["home_team"] = df["home_team"].apply(normalize_text)
    df["away_team"] = df["away_team"].apply(normalize_text)
    df["city"] = df["city"].apply(clean_city)
    df["round"] = df["round"].apply(normalize_text)
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")

    return df


def main() -> None:
    df1 = load_1930_2010_extracted()
    df2 = load_2014_extracted()
    df3 = load_2018_extracted_loose()

    df1.to_csv(OUT_1930_2010, index=False, encoding="utf-8")
    df2.to_csv(OUT_2014, index=False, encoding="utf-8")
    df3.to_csv(OUT_2018, index=False, encoding="utf-8")

    print("Saved:", OUT_1930_2010)
    print("Saved:", OUT_2014)
    print("Saved:", OUT_2018)
    print("Rows:", {"1930_2010": len(df1), "2014": len(df2), "2018": len(df3)})


if __name__ == "__main__":
    main()
