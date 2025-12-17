from pathlib import Path
import pandas as pd
import json
import re
import numpy as np

# -------------------------
# PATHS
# -------------------------
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
TEST = ROOT / "data" / "test"
TEST.mkdir(parents=True, exist_ok=True)

ROUND_RANK = {
    "GROUP_STAGE": 1,
    "ROUND_OF_16": 2,
    "QUARTER_FINAL": 3,
    "SEMI_FINAL": 4,
    "THIRD_PLACE": 5,
    "FINAL": 6,
}

# -------------------------
# UTILS
# -------------------------
def normalize_text(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).strip())

def clean_city(city: str) -> str:
    return normalize_text(city).rstrip(".")

def compute_result(home_team, away_team, home_result, away_result):
    if pd.isna(home_result) or pd.isna(away_result):
        return pd.NA
    if home_result > away_result:
        return f"vainqueur {home_team}"
    elif away_result > home_result:
        return f"vainqueur {away_team}"
    else:
        return "draw"

# -------------------------
# 1930–2010
# -------------------------
def load_transform_1930_2010() -> pd.DataFrame:
    df = pd.read_csv(RAW / "matches_19302010.csv")

    scores = df["score"].astype(str).str.extract(r"^\s*(\d+)\s*-\s*(\d+)")
    df["home_result"] = pd.to_numeric(scores[0], errors="coerce")
    df["away_result"] = pd.to_numeric(scores[1], errors="coerce")

    df["home_team"] = df["team1"].apply(normalize_text)
    df["away_team"] = df["team2"].apply(normalize_text)

    df["result"] = df.apply(
        lambda r: compute_result(
            r["home_team"], r["away_team"], r["home_result"], r["away_result"]
        ),
        axis=1
    )

    return pd.DataFrame({
        "home_team": df["home_team"],
        "away_team": df["away_team"],
        "home_result": df["home_result"],
        "away_result": df["away_result"],
        "result": df["result"],
        "date": pd.NA,
        "round": df["round"].apply(normalize_text),
        "city": df["venue"].apply(clean_city),
        "edition": df["edition"].astype(str),
        "_match_no": pd.NA,
    })

# -------------------------
# 2014
# -------------------------
def load_transform_2014() -> pd.DataFrame:
    path = list(RAW.glob("WorldCupMatches2014*.csv"))[0]
    df = pd.read_csv(path)

    if "MatchID" in df.columns:
        df = df.drop_duplicates(subset=["MatchID"]).copy()

    df["home_team"] = df["Home Team Name"].apply(normalize_text)
    df["away_team"] = df["Away Team Name"].apply(normalize_text)
    df["home_result"] = pd.to_numeric(df["Home Team Goals"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["Away Team Goals"], errors="coerce")

    df["result"] = df.apply(
        lambda r: compute_result(
            r["home_team"], r["away_team"], r["home_result"], r["away_result"]
        ),
        axis=1
    )

    return pd.DataFrame({
        "home_team": df["home_team"],
        "away_team": df["away_team"],
        "home_result": df["home_result"],
        "away_result": df["away_result"],
        "result": df["result"],
        "date": pd.to_datetime(df["Datetime"], errors="coerce").dt.strftime("%Y-%m-%d"),
        "round": df["Stage"].apply(normalize_text),
        "city": df["City"].apply(clean_city),
        "edition": "2014-BRAZIL",
        "_match_no": pd.NA,
    })

# -------------------------
# 2018
# -------------------------
def load_transform_2018() -> pd.DataFrame:
    with open(RAW / "data_2018.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    teams = {t["id"]: t["name"] for t in data["teams"]}
    stadiums = {s["id"]: s["city"] for s in data["stadiums"]}

    rows = []

    def add_match(m, round_name):
        home_team = normalize_text(teams[m["home_team"]])
        away_team = normalize_text(teams[m["away_team"]])
        home_result = m["home_result"]
        away_result = m["away_result"]

        rows.append({
            "home_team": home_team,
            "away_team": away_team,
            "home_result": home_result,
            "away_result": away_result,
            "result": compute_result(home_team, away_team, home_result, away_result),
            "date": m["date"],
            "round": round_name,
            "city": clean_city(stadiums[m["stadium"]]),
            "edition": "2018-RUSSIE",
            "_match_no": pd.NA,
        })

    for group in data["groups"].values():
        for m in group["matches"]:
            add_match(m, "Group")

    for rd in data["knockout"].values():
        for m in rd["matches"]:
            add_match(m, rd["name"])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    return df

# -------------------------
# 2022
# -------------------------
def load_transform_2022() -> pd.DataFrame:
    path = PROCESSED / "matches_2022.csv"
    if not path.exists():
        raise FileNotFoundError("matches_2022.csv introuvable dans data/processed")

    df = pd.read_csv(path)

    df["home_team"] = df["home_team"].apply(normalize_text)
    df["away_team"] = df["away_team"].apply(normalize_text)

    df["result"] = df.apply(
        lambda r: compute_result(
            r["home_team"], r["away_team"], r["home_result"], r["away_result"]
        ),
        axis=1
    )

    df["city"] = df["city"].apply(clean_city)
    df["round"] = df["round"].apply(normalize_text)
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    df["edition"] = "2022-QUATAR"
    df["_match_no"] = pd.NA

    return df[[
        "home_team","away_team","home_result","away_result",
        "result","date","round","city","edition","_match_no"
    ]]

# -------------------------
# ADD id_match
# -------------------------
def add_id_match(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["edition_int"] = pd.to_numeric(df["edition"], errors="coerce")
    df["round_rank"] = df["round"].map(ROUND_RANK).fillna(99)
    df["_date_sort"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.sort_values(
        by=["edition_int", "_date_sort", "round_rank", "home_team", "away_team"],
        na_position="last"
    ).reset_index(drop=True)

    df["id_match"] = range(1, len(df) + 1)
    return df

# -------------------------
# MAIN
# -------------------------
def main():
    df_all = pd.concat([
        load_transform_1930_2010(),
        load_transform_2014(),
        load_transform_2018(),
        load_transform_2022()
    ], ignore_index=True)

    df_all = add_id_match(df_all)

    df_final = df_all[[
        "id_match","home_team","away_team",
        "home_result","away_result","result",
        "date","round","city","edition"
    ]]

    out = TEST / "matches_unified_v2.csv"
    df_final.to_csv(out, index=False, encoding="utf-8")

    print("✅ Fichier généré :", out)
    print(df_final.head())

if __name__ == "__main__":
    main()
