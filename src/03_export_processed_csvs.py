from __future__ import annotations

from pathlib import Path
import pandas as pd
import json
import re
import unicodedata

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"

RAW.mkdir(parents=True, exist_ok=True)
PROCESSED.mkdir(parents=True, exist_ok=True)

# Entrées (produites par le script 01)
IN_1930_2010 = PROCESSED / "matches_1930_2010_extracted.csv"
IN_2014 = PROCESSED / "matches_2014_extracted.csv"
IN_2018 = PROCESSED / "matches_2018_extracted.csv"
IN_2022 = PROCESSED / "matches_2022.csv"

OUT = PROCESSED / "matches_unified_v1.csv"

ROUND_RANK = {
    "Group": 1,
    "Round of 16": 2,
    "Quarter-finals": 3,
    "Semi-finals": 4,
    "Match for third place": 5,
    "Final": 6,
}


def normalize_text(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)) or pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def clean_city(city: object) -> str:
    c = normalize_text(city)
    return c.rstrip(".").strip()


def norm_txt(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)) or pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\(.*?\)", "", s)
    s = s.replace("&", "and")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def is_placeholder_date(date_str: object) -> bool:
    if date_str is None or (isinstance(date_str, float) and pd.isna(date_str)) or pd.isna(date_str):
        return False
    return bool(re.match(r"^\d{4}-01-01$", str(date_str)))


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
    df["_date_sort"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(
        by=["edition_int", "_date_sort", "round_rank", "home_team", "away_team"],
        na_position="last",
    ).reset_index(drop=True)

    df["id_match"] = range(1, len(df) + 1)
    df = df.drop(columns=["_date_sort"], errors="ignore")
    return df


def find_in_data(filename: str) -> Path:
    hits = list((ROOT / "data").rglob(filename))
    if not hits:
        raise FileNotFoundError(f"Impossible de trouver {filename} dans {ROOT/'data'}")
    return hits[0]


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
                    "edition_k": year,
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

    df_k["edition_k"] = pd.to_numeric(df_k["edition_k"], errors="coerce").astype("Int64")
    df_k["home_result_k"] = pd.to_numeric(df_k["home_result_k"], errors="coerce").astype("Int64")
    df_k["away_result_k"] = pd.to_numeric(df_k["away_result_k"], errors="coerce").astype("Int64")
    df_k["home_key"] = df_k["home_team_k"].map(norm_txt)
    df_k["away_key"] = df_k["away_team_k"].map(norm_txt)

    df_k = df_k.drop_duplicates(subset=["edition_k", "home_key", "away_key", "home_result_k", "away_result_k", "date_k"])

    out = df_all.copy()
    out["edition_int"] = pd.to_numeric(out["edition"], errors="coerce").astype("Int64")
    out["home_key"] = out["home_team"].map(norm_txt)
    out["away_key"] = out["away_team"].map(norm_txt)

    # PASS 1 : edition + teams + scores
    m1 = out.merge(
        df_k[["edition_k", "home_key", "away_key", "home_result_k", "away_result_k", "date_k", "round_k", "city_k"]],
        left_on=["edition_int", "home_key", "away_key", "home_result", "away_result"],
        right_on=["edition_k", "home_key", "away_key", "home_result_k", "away_result_k"],
        how="left",
    )

    m1["round"] = m1["round"].fillna(m1["round_k"])
    m1["city"] = m1["city"].fillna(m1["city_k"])

    mask_ph = m1["date"].astype(str).map(is_placeholder_date)
    m1.loc[mask_ph, "date"] = m1.loc[mask_ph, "date_k"].fillna(m1.loc[mask_ph, "date"])

    drop_cols = [c for c in m1.columns if c in ["edition_int", "home_key", "away_key", "edition_k", "home_result_k", "away_result_k", "date_k", "round_k", "city_k"]]
    m1 = m1.drop(columns=[c for c in drop_cols if c in m1.columns], errors="ignore")

    return m1


def load_extracted(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"[{label}] introuvable: {path}")
    df = pd.read_csv(path)

    needed = ["home_team", "away_team", "home_result", "away_result", "date", "round", "city", "edition"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"[{label}] colonnes manquantes: {missing} | colonnes: {df.columns.tolist()}")

    # normalisation light
    df["home_team"] = df["home_team"].apply(normalize_text)
    df["away_team"] = df["away_team"].apply(normalize_text)
    df["city"] = df["city"].apply(clean_city)
    df["round"] = df["round"].apply(normalize_text)
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")
    df["edition"] = df["edition"].astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    return df[needed]


def load_2022() -> pd.DataFrame:
    if not IN_2022.exists():
        raise FileNotFoundError(f"[2022] introuvable: {IN_2022}")

    df = pd.read_csv(IN_2022)
    needed = ["home_team", "away_team", "home_result", "away_result", "date", "round", "city"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"[2022] colonnes manquantes: {missing} | colonnes: {df.columns.tolist()}")

    df["home_team"] = df["home_team"].apply(normalize_text)
    df["away_team"] = df["away_team"].apply(normalize_text)
    df["city"] = df["city"].apply(clean_city)
    df["round"] = df["round"].apply(normalize_text)
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")

    df["edition"] = "2022"
    return df[needed + ["edition"]]


def main() -> None:
    df_1930_2010 = load_extracted(IN_1930_2010, "1930-2010")
    df_2014 = load_extracted(IN_2014, "2014")
    df_2018 = load_extracted(IN_2018, "2018")
    df_2022 = load_2022()

    df_all = pd.concat([df_1930_2010, df_2014, df_2018, df_2022], ignore_index=True)

    # enrich Kaggle (sur les éditions <= 2018 principalement)
    df_all = enrich_with_kaggle(df_all)

    df_all = add_result(df_all)
    df_all = add_id_match(df_all)

    final_cols = ["id_match", "home_team", "away_team", "home_result", "away_result", "result", "date", "round", "city", "edition"]
    df_final = df_all[final_cols].copy()

    df_final.to_csv(OUT, index=False, encoding="utf-8")
    print("Saved:", OUT)
    print(df_final.groupby("edition").size().sort_index())


if __name__ == "__main__":
    main()
