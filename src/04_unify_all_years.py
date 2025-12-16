from pathlib import Path
import pandas as pd
import json
import re

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

SCORE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)")
MATCHNO_RE = re.compile(r"#(\d+)-")  # ex: ...htm#1-WC-30-I

ROUND_MAP = {
    "GROUP_STAGE": "Group",
    "ROUND_OF_16": "Round of 16",
    "QUARTER_FINAL": "Quarter-finals",
    "SEMI_FINAL": "Semi-finals",
    "THIRD_PLACE": "Match for third place",
    "FINAL": "Final",
}

ROUND_RANK = {
    "Group": 1,
    "Round of 16": 2,
    "Quarter-finals": 3,
    "Semi-finals": 4,
    "Match for third place": 5,
    "Final": 6,
}

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def clean_city(city: str) -> str:
    c = normalize_text(city)
    # certains venues finissent par "."
    c = c.rstrip(".").strip()
    return c

# -------------------------
# LOAD / TRANSFORM 1930-2010
# -------------------------
def load_transform_1930_2010() -> pd.DataFrame:
    df = pd.read_csv(RAW / "matches_19302010.csv")

    # Heuristique "finals only" : les urls de phase finale ont souvent "_FS.htm" (dans ton exemple)
    if "url" in df.columns:
        df = df[df["url"].astype(str).str.contains(r"_FS\.htm", regex=True, na=False)].copy()

    # extraire score
    scores = df["score"].astype(str).str.extract(SCORE_RE)
    home_result = pd.to_numeric(scores[0], errors="coerce")
    away_result = pd.to_numeric(scores[1], errors="coerce")

    # round mapping
    rnd_raw = df["round"].astype(str)
    rnd = rnd_raw.map(ROUND_MAP).fillna(rnd_raw).apply(normalize_text)

    # match_no pour ordonner (extrait de l'url)
    match_no = None
    if "url" in df.columns:
        match_no = df["url"].astype(str).str.extract(MATCHNO_RE)[0]
        match_no = pd.to_numeric(match_no, errors="coerce")
    else:
        match_no = pd.Series([pd.NA] * len(df))

    df_out = pd.DataFrame({
        "home_team": df["team1"].apply(normalize_text),
        "away_team": df["team2"].apply(normalize_text),
        "home_result": home_result,
        "away_result": away_result,
        # on n'a que l'année -> date "placeholder" (à documenter)
        "date": pd.to_datetime(df["year"], format="%Y", errors="coerce").dt.strftime("%Y-01-01"),
        "round": rnd,
        "city": df["venue"].apply(clean_city),
        "edition": df["year"].astype(str),
        "_match_no": match_no,
    })

    return df_out

# -------------------------
# LOAD / TRANSFORM 2014
# -------------------------
def load_transform_2014() -> pd.DataFrame:
    path = list(RAW.glob("WorldCupMatches2014*.csv"))[0]
    df = pd.read_csv(path)

    # Beaucoup de fichiers "WorldCupMatches" ont des doublons -> on déduplique par MatchID si présent
    if "MatchID" in df.columns:
        df = df.drop_duplicates(subset=["MatchID"]).copy()

    df_out = pd.DataFrame({
        "home_team": df["Home Team Name"].apply(normalize_text),
        "away_team": df["Away Team Name"].apply(normalize_text),
        "home_result": pd.to_numeric(df["Home Team Goals"], errors="coerce"),
        "away_result": pd.to_numeric(df["Away Team Goals"], errors="coerce"),
        "date": pd.to_datetime(df["Datetime"], errors="coerce").dt.strftime("%Y-%m-%d"),
        "round": df["Stage"].apply(normalize_text),
        "city": df["City"].apply(clean_city),
        "edition": df["Year"].astype(str),
        "_match_no": pd.Series([pd.NA] * len(df)),
    })

    return df_out

# -------------------------
# LOAD / TRANSFORM 2018
# -------------------------
def load_transform_2018() -> pd.DataFrame:
    with open(RAW / "data_2018.json", "r", encoding="utf-8") as f:
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
                "_match_no": pd.NA,
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
                "_match_no": pd.NA,
            })

    df = pd.DataFrame(matches)
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    df["home_team"] = df["home_team"].apply(normalize_text)
    df["away_team"] = df["away_team"].apply(normalize_text)
    df["city"] = df["city"].apply(clean_city)
    df["round"] = df["round"].apply(normalize_text)
    return df

# -------------------------
# LOAD / TRANSFORM 2022
# -------------------------
def load_transform_2022() -> pd.DataFrame:
    path = PROCESSED / "matches_2022.csv"
    if not path.exists():
        raise FileNotFoundError("matches_2022.csv introuvable. Lance d'abord: py src/extract_2022_from_text.py")

    df = pd.read_csv(path)
    df["home_team"] = df["home_team"].apply(normalize_text)
    df["away_team"] = df["away_team"].apply(normalize_text)
    df["city"] = df["city"].apply(clean_city)
    df["round"] = df["round"].apply(normalize_text)
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    df["edition"] = "2022"
    df["_match_no"] = pd.NA
    return df[["home_team","away_team","home_result","away_result","date","round","city","edition","_match_no"]]

# -------------------------
# FINAL BUSINESS FIELDS
# -------------------------
def add_result(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["result"] = "draw"
    df.loc[df["home_result"] > df["away_result"], "result"] = df["home_team"]
    df.loc[df["away_result"] > df["home_result"], "result"] = df["away_team"]
    return df

def add_id_match(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["edition_int"] = pd.to_numeric(df["edition"], errors="coerce")
    df["round_rank"] = df["round"].map(ROUND_RANK).fillna(99).astype(int)

    # tri stable : edition, date, round_rank, match_no si dispo
    df["_date_sort"] = pd.to_datetime(df["date"], errors="coerce")
    df["_match_no_sort"] = pd.to_numeric(df["_match_no"], errors="coerce")

    df = df.sort_values(
        by=["edition_int", "_date_sort", "round_rank", "_match_no_sort", "home_team", "away_team"],
        na_position="last"
    ).reset_index(drop=True)

    df["id_match"] = range(1, len(df) + 1)
    return df

def main():
    df_1930_2010 = load_transform_1930_2010()
    df_2014 = load_transform_2014()
    df_2018 = load_transform_2018()
    df_2022 = load_transform_2022()

    df_all = pd.concat([df_1930_2010, df_2014, df_2018, df_2022], ignore_index=True)

    # checks rapides
    print("Rows by edition:")
    print(df_all.groupby("edition").size().sort_index())

    # champs métier
    df_all = add_result(df_all)
    df_all = add_id_match(df_all)

    # colonnes finales demandées
    final_cols = ["id_match","home_team","away_team","home_result","away_result","result","date","round","city","edition"]
    df_final = df_all[final_cols].copy()

    # check doublons sur clés "logiques"
    dup = df_final.duplicated(subset=["edition","home_team","away_team","date","round"], keep=False).sum()
    print("Potential duplicates (same edition/home/away/date/round):", int(dup))

    out = PROCESSED / "matches_unified_v1.csv"
    df_final.to_csv(out, index=False, encoding="utf-8")
    print("Saved:", out)

    # mini preview
    print(df_final.head(5))
    print(df_final.tail(5))

if __name__ == "__main__":
    main()
