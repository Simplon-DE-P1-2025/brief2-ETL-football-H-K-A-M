from __future__ import annotations

from pathlib import Path
import pandas as pd
import json
import re
import unicodedata

# -------------------------
# PATHS
# -------------------------
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"

# sortie demandée : data/processed
OUT = PROCESSED / "matches_unified_v1.csv"

RAW.mkdir(parents=True, exist_ok=True)
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


# -------------------------
# Helpers
# -------------------------
def normalize_text(s: object) -> str:
    if s is None or pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def clean_city(city: object) -> str:
    c = normalize_text(city)
    return c.rstrip(".").strip()


def norm_txt(s: object) -> str:
    """clé de matching (lower + accents off + parenthèses off)"""
    if s is None or pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\(.*?\)", "", s)
    s = s.replace("&", "and")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def is_placeholder_date(date_str: object) -> bool:
    if pd.isna(date_str):
        return False
    return bool(re.match(r"^\d{4}-01-01$", str(date_str)))


def add_result(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # result attendu par l'énoncé : home_team / away_team / draw
    df["result"] = "draw"
    df.loc[df["home_result"] > df["away_result"], "result"] = df["home_team"]
    df.loc[df["away_result"] > df["home_result"], "result"] = df["away_team"]
    return df


def add_id_match(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # robuste si un merge a renommé edition
    if "edition" not in df.columns:
        for alt in ["edition_x", "edition_y", "edition_int"]:
            if alt in df.columns:
                df["edition"] = df[alt].astype(str)
                break
    if "edition" not in df.columns:
        raise KeyError(f"edition introuvable. Colonnes: {df.columns.tolist()}")

    df["edition_int"] = pd.to_numeric(df["edition"], errors="coerce")
    df["round_rank"] = df["round"].map(ROUND_RANK).fillna(99).astype(int)
    df["_date_sort"] = pd.to_datetime(df["date"], errors="coerce")
    df["_match_no_sort"] = pd.to_numeric(df.get("_match_no", pd.NA), errors="coerce")

    df = (
        df.sort_values(
            by=["edition_int", "_date_sort", "round_rank", "_match_no_sort", "home_team", "away_team"],
            na_position="last",
        )
        .reset_index(drop=True)
    )

    df["id_match"] = range(1, len(df) + 1)

    # nettoyage colonnes de tri
    df = df.drop(columns=["_date_sort"], errors="ignore")
    return df


def find_in_data(filename: str) -> Path:
    hits = list((ROOT / "data").rglob(filename))
    if not hits:
        raise FileNotFoundError(f"Impossible de trouver {filename} dans {ROOT/'data'}")
    return hits[0]


# -------------------------
# LOAD / TRANSFORM 1930-2010
# -------------------------
def load_transform_1930_2010() -> pd.DataFrame:
    path = RAW / "matches_19302010.csv"
    if not path.exists():
        raise FileNotFoundError(f"{path} introuvable")

    df = pd.read_csv(path)

    # IMPORTANT : on ne filtre PAS "finals only"
    scores = df["score"].astype(str).str.extract(SCORE_RE)
    home_result = pd.to_numeric(scores[0], errors="coerce")
    away_result = pd.to_numeric(scores[1], errors="coerce")

    # round mapping si présent
    rnd_raw = df["round"].astype(str)
    rnd = rnd_raw.map(ROUND_MAP).fillna(rnd_raw).apply(normalize_text)

    # match number si présent dans url
    if "url" in df.columns:
        match_no = df["url"].astype(str).str.extract(MATCHNO_RE)[0]
        match_no = pd.to_numeric(match_no, errors="coerce")
    else:
        match_no = pd.Series([pd.NA] * len(df))

    # date placeholder (sera enrichie par Kaggle si possible)
    year_col = df["year"] if "year" in df.columns else df["edition"]
    date_placeholder = pd.to_datetime(year_col, format="%Y", errors="coerce").dt.strftime("%Y-01-01")

    df_out = pd.DataFrame(
        {
            "home_team": df["team1"].apply(normalize_text),
            "away_team": df["team2"].apply(normalize_text),
            "home_result": home_result,
            "away_result": away_result,
            "date": date_placeholder,
            "round": rnd,
            "city": df["venue"].apply(clean_city),
            "edition": df["edition"].astype(str),  # ✅ FIX CRITIQUE
            "_match_no": match_no,
        }
    )
    return df_out


# -------------------------
# LOAD / TRANSFORM 2014
# -------------------------
def load_transform_2014() -> pd.DataFrame:
    files = list(RAW.glob("WorldCupMatches2014*.csv"))
    if not files:
        raise FileNotFoundError("Fichier WorldCupMatches2014*.csv introuvable dans data/raw")
    path = files[0]

    df = pd.read_csv(path)

    if "MatchID" in df.columns:
        df = df.drop_duplicates(subset=["MatchID"]).copy()

    df_out = pd.DataFrame(
        {
            "home_team": df["Home Team Name"].apply(normalize_text),
            "away_team": df["Away Team Name"].apply(normalize_text),
            "home_result": pd.to_numeric(df["Home Team Goals"], errors="coerce"),
            "away_result": pd.to_numeric(df["Away Team Goals"], errors="coerce"),
            "date": pd.to_datetime(df["Datetime"], errors="coerce").dt.strftime("%Y-%m-%d"),
            "round": df["Stage"].apply(normalize_text),
            "city": df["City"].apply(clean_city),
            "edition": "2014-BRAZIL",
            "_match_no": pd.Series([pd.NA] * len(df)),
        }
    )
    return df_out


# -------------------------
# LOAD / TRANSFORM 2018
# -------------------------
def load_transform_2018() -> pd.DataFrame:
    path = RAW / "data_2018.json"
    if not path.exists():
        raise FileNotFoundError("data_2018.json introuvable (data/raw).")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    teams = {t["id"]: t["name"] for t in data["teams"]}
    stadiums = {s["id"]: s["city"] for s in data["stadiums"]}

    matches = []

    for round_data in data["knockout"].values():
        for m in round_data["matches"]:
            matches.append(
                {
                    "home_team": teams[m["home_team"]],
                    "away_team": teams[m["away_team"]],
                    "home_result": m["home_result"],
                    "away_result": m["away_result"],
                    "date": m["date"],
                    "round": round_data.get("name", ""),
                    "city": stadiums[m["stadium"]],
                    "edition": "2018-RUSSIA",
                    "_match_no": pd.NA,
                }
            )

    df = pd.DataFrame(matches)
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    df["home_team"] = df["home_team"].apply(normalize_text)
    df["away_team"] = df["away_team"].apply(normalize_text)
    df["city"] = df["city"].apply(clean_city)
    df["round"] = df["round"].apply(normalize_text)

    # Harmonisation simple (utile si noms alternatifs)
    df["round"] = df["round"].replace(
        {
            "Last 16": "Round of 16",
            "Quarterfinals": "Quarter-finals",
            "Semifinals": "Semi-finals",
        }
    )

    return df


# -------------------------
# LOAD / TRANSFORM 2022
# -------------------------
def load_transform_2022() -> pd.DataFrame:
    path = PROCESSED / "matches_2022.csv"
    if not path.exists():
        raise FileNotFoundError("matches_2022.csv introuvable (data/processed).")

    df = pd.read_csv(path)
    df["home_team"] = df["home_team"].apply(normalize_text)
    df["away_team"] = df["away_team"].apply(normalize_text)
    df["city"] = df["city"].apply(clean_city)
    df["round"] = df["round"].apply(normalize_text)
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    df["edition"] = "2022-QATAR"
    df["_match_no"] = pd.NA

    return df[["home_team", "away_team", "home_result", "away_result", "date", "round", "city", "edition", "_match_no"]]


# -------------------------
# ENRICH WITH KAGGLE (1930..2018)
# -------------------------
def enrich_with_kaggle(df_all: pd.DataFrame) -> pd.DataFrame:
    matches_path = find_in_data("matches.json")
    tournament_path = find_in_data("tournament.json")

    with open(tournament_path, "r", encoding="utf-8") as f:
        tournaments = json.load(f)["tournament"]

    tmap = {}
    for t in tournaments:
        try:
            tmap[int(t["idTournament"])] = int(t["year"])
        except Exception:
            continue

    with open(matches_path, "r", encoding="utf-8") as f:
        matches_json = json.load(f)["match"]

    rows = []
    for m in matches_json:
        try:
            year = tmap.get(int(m["idTournament"]))
            if year is None:
                continue

            rows.append(
                {
                    "edition_k": year,  # IMPORTANT: pas "edition" pour éviter edition_x/edition_y
                    "date_k": str(m.get("date", ""))[:10],
                    "home_team_k": m["homeTeam"]["teamName"],
                    "away_team_k": m["awayTeam"]["teamName"],
                    "home_result_k": m["homeTeam"]["score"],
                    "away_result_k": m["awayTeam"]["score"],
                    "round_k": normalize_text(m.get("stageName", "")),
                    "city_k": clean_city(m.get("cityName", "")),
                }
            )
        except Exception:
            continue

    df_k = pd.DataFrame(rows)
    if df_k.empty:
        return df_all

    # types + keys
    df_k["edition_k"] = pd.to_numeric(df_k["edition_k"], errors="coerce").astype("Int64")
    df_k["home_result_k"] = pd.to_numeric(df_k["home_result_k"], errors="coerce").astype("Int64")
    df_k["away_result_k"] = pd.to_numeric(df_k["away_result_k"], errors="coerce").astype("Int64")
    df_k["home_key"] = df_k["home_team_k"].map(norm_txt)
    df_k["away_key"] = df_k["away_team_k"].map(norm_txt)
    df_k["round_key"] = df_k["round_k"].map(norm_txt)
    df_k["city_key"] = df_k["city_k"].map(norm_txt)

    # dédup kaggle
    df_k = df_k.drop_duplicates(subset=["edition_k", "home_key", "away_key", "home_result_k", "away_result_k", "date_k"])

    out = df_all.copy()
    out["edition_int"] = pd.to_numeric(out["edition"], errors="coerce").astype("Int64")
    out["home_key"] = out["home_team"].map(norm_txt)
    out["away_key"] = out["away_team"].map(norm_txt)
    out["round_key"] = out["round"].map(norm_txt)
    out["city_key"] = out["city"].map(norm_txt)

    # PASS 1 : edition + teams + scores
    m1 = out.merge(
        df_k[["edition_k", "home_key", "away_key", "home_result_k", "away_result_k", "date_k", "round_k", "city_k"]],
        left_on=["edition_int", "home_key", "away_key", "home_result", "away_result"],
        right_on=["edition_k", "home_key", "away_key", "home_result_k", "away_result_k"],
        how="left",
    )

    # fill round/city
    m1["round"] = m1["round"].fillna(m1["round_k"])
    m1["city"] = m1["city"].fillna(m1["city_k"])

    # replace placeholder dates ONLY
    mask_ph = m1["date"].astype(str).map(is_placeholder_date)
    m1.loc[mask_ph, "date"] = m1.loc[mask_ph, "date_k"].fillna(m1.loc[mask_ph, "date"])

    # PASS 2 : fallback edition + teams + city + round (si pas match au pass1)
    need2 = m1["date_k"].isna() & (m1["edition_int"] <= 2018)
    if need2.any():
        tmp = m1.loc[need2].merge(
            df_k[
                [
                    "edition_k",
                    "home_key",
                    "away_key",
                    "city_key",
                    "round_key",
                    "date_k",
                    "home_result_k",
                    "away_result_k",
                    "round_k",
                    "city_k",
                ]
            ],
            left_on=["edition_int", "home_key", "away_key", "city_key", "round_key"],
            right_on=["edition_k", "home_key", "away_key", "city_key", "round_key"],
            how="left",
            suffixes=("_v", "_k2"),
        )

        idx = m1.index[need2]

        date_col = "date_k_k2" if "date_k_k2" in tmp.columns else "date_k"
        hr_col = "home_result_k_k2" if "home_result_k_k2" in tmp.columns else "home_result_k"
        ar_col = "away_result_k_k2" if "away_result_k_k2" in tmp.columns else "away_result_k"
        round_col = "round_k_k2" if "round_k_k2" in tmp.columns else "round_k"
        city_col = "city_k_k2" if "city_k_k2" in tmp.columns else "city_k"

        m1.loc[idx, "date"] = tmp[date_col].fillna(m1.loc[idx, "date"]).values
        m1.loc[idx, "home_result"] = (
            pd.to_numeric(tmp[hr_col], errors="coerce").fillna(m1.loc[idx, "home_result"]).values
        )
        m1.loc[idx, "away_result"] = (
            pd.to_numeric(tmp[ar_col], errors="coerce").fillna(m1.loc[idx, "away_result"]).values
        )
        m1.loc[idx, "round"] = tmp[round_col].fillna(m1.loc[idx, "round"]).values
        m1.loc[idx, "city"] = tmp[city_col].fillna(m1.loc[idx, "city"]).values

    # cleanup (on garde edition !)
    drop_cols = [
        c
        for c in m1.columns
        if c
        in [
            "edition_int",
            "home_key",
            "away_key",
            "round_key",
            "city_key",
            "edition_k",
            "home_team_k",
            "away_team_k",
            "home_result_k",
            "away_result_k",
            "date_k",
            "round_k",
            "city_k",
        ]
    ]
    m1 = m1.drop(columns=[c for c in drop_cols if c in m1.columns], errors="ignore")

    return m1


# -------------------------
# MAIN
# -------------------------
def main():
    df_1930_2010 = load_transform_1930_2010()
    df_2014 = load_transform_2014()
    df_2018 = load_transform_2018()
    df_2022 = load_transform_2022()

    df_all = pd.concat([df_1930_2010, df_2014, df_2018, df_2022], ignore_index=True)

    # enrich Kaggle (1930..2018)
    df_all = enrich_with_kaggle(df_all)

    # champs numériques + result + id
    df_all["home_result"] = pd.to_numeric(df_all["home_result"], errors="coerce")
    df_all["away_result"] = pd.to_numeric(df_all["away_result"], errors="coerce")

    df_all = add_result(df_all)
    df_all = add_id_match(df_all)

    final_cols = [
        "id_match",
        "home_team",
        "away_team",
        "home_result",
        "away_result",
        "result",
        "date",
        "round",
        "city",
        "edition",
    ]
    df_final = df_all[final_cols].copy()

    df_final.to_csv(OUT, index=False, encoding="utf-8")
    print("Saved:", OUT)
    print("Matches per edition (last 10):")
    print(df_final.groupby("edition").size().sort_index().tail(10))


if __name__ == "__main__":
    main()
