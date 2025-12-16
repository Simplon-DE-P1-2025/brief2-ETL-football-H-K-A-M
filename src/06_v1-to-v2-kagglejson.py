from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import pandas as pd


# ---------------------------
# Paths (robuste : marche peu importe où tu lances le script)
# ---------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

V1_FILE = DATA / "processed" / "matches_unified_v1.csv"
KAGGLE_MATCHES = DATA / "raw" / "kaggle" / "matches.json"
KAGGLE_TOURNAMENT = DATA / "raw" / "kaggle" / "tournament.json"
OUT_V2 = DATA / "processed" / "matches_unified_v2.csv"


# ---------------------------
# Helpers
# ---------------------------
def norm_txt(s: object) -> str:
    """Lower, trim, remove accents, remove content in parentheses, collapse spaces."""
    if s is None or (isinstance(s, float) and pd.isna(s)) or pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\(.*?\)", "", s)  # remove "(México)" etc.
    s = s.replace("&", "and")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


ROUND_MAP_KAGGLE = {
    "group matches": "Group",
    "round of 16": "Round of 16",
    "quarter-finals": "Quarter-finals",
    "quarter final": "Quarter-finals",
    "semi-finals": "Semi-finals",
    "semi final": "Semi-finals",
    "play-off for third place": "Match for third place",
    "third place": "Match for third place",
    "final": "Final",
}


def norm_round_kaggle(stage: object) -> str:
    k = norm_txt(stage)
    return ROUND_MAP_KAGGLE.get(k, str(stage) if stage is not None else "")


def is_placeholder_date(date_str: object) -> bool:
    # Beaucoup de V1 mettent YYYY-01-01 pour toute l'édition (WC jamais le 1er janvier)
    if date_str is None or (isinstance(date_str, float) and pd.isna(date_str)) or pd.isna(date_str):
        return False
    s = str(date_str)
    return bool(re.match(r"^\d{4}-01-01$", s))


def compute_result(home_team: str, away_team: str, hg: int, ag: int) -> str:
    if pd.isna(hg) or pd.isna(ag):
        return "draw"  # fallback (tu peux aussi laisser vide)
    if hg > ag:
        return home_team
    if hg < ag:
        return away_team
    return "draw"


# ---------------------------
def main() -> None:
    # Sanity checks paths
    for p in [V1_FILE, KAGGLE_MATCHES, KAGGLE_TOURNAMENT]:
        if not p.exists():
            raise FileNotFoundError(f"Fichier introuvable: {p}")

    # Load V1
    df = pd.read_csv(V1_FILE)

    # Score types
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce").astype("Int64")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce").astype("Int64")

    # Matching keys
    df["home_key"] = df["home_team"].map(norm_txt)
    df["away_key"] = df["away_team"].map(norm_txt)
    df["round_key"] = df["round"].map(norm_txt)
    df["city_key"] = df["city"].map(norm_txt)

    # Load Kaggle tournament mapping
    with open(KAGGLE_TOURNAMENT, "r", encoding="utf-8") as f:
        tournaments = json.load(f)["tournament"]

    tmap = {}
    for t in tournaments:
        try:
            tmap[int(t["idTournament"])] = int(t["year"])
        except Exception:
            continue

    # Load Kaggle matches
    with open(KAGGLE_MATCHES, "r", encoding="utf-8") as f:
        matches_json = json.load(f)["match"]

    rows = []
    for m in matches_json:
        try:
            tid = int(m["idTournament"])
            year = tmap.get(tid)
            if year is None:
                continue
            rows.append({
                "edition": year,
                "date_kaggle": str(m["date"])[:10],
                "home_team_kaggle": m["homeTeam"]["teamName"],
                "away_team_kaggle": m["awayTeam"]["teamName"],
                "home_result_kaggle": m["homeTeam"]["score"],
                "away_result_kaggle": m["awayTeam"]["score"],
                "round_kaggle": norm_round_kaggle(m.get("stageName")),
                "city_kaggle": m.get("cityName"),
            })
        except Exception:
            continue

    df_k = pd.DataFrame(rows)

    # Types + keys Kaggle
    df_k["home_result_kaggle"] = pd.to_numeric(df_k["home_result_kaggle"], errors="coerce").astype("Int64")
    df_k["away_result_kaggle"] = pd.to_numeric(df_k["away_result_kaggle"], errors="coerce").astype("Int64")

    df_k["home_key"] = df_k["home_team_kaggle"].map(norm_txt)
    df_k["away_key"] = df_k["away_team_kaggle"].map(norm_txt)
    df_k["round_key"] = df_k["round_kaggle"].map(norm_txt)
    df_k["city_key"] = df_k["city_kaggle"].map(norm_txt)

    df_k = df_k.drop_duplicates(
        subset=["edition", "home_key", "away_key", "home_result_kaggle", "away_result_kaggle", "date_kaggle"]
    )

    # Stats avant
    before_round_missing = int(df["round"].isna().sum())
    before_city_missing = int(df["city"].isna().sum())
    before_placeholder_dates = int(df["date"].astype(str).map(is_placeholder_date).sum())

    # ---------------------------
    # PASS 1
    # ---------------------------
    df1 = df.merge(
        df_k[[
            "edition","home_key","away_key",
            "home_result_kaggle","away_result_kaggle",
            "date_kaggle","round_kaggle","city_kaggle"
        ]],
        left_on=["edition","home_key","away_key","home_result","away_result"],
        right_on=["edition","home_key","away_key","home_result_kaggle","away_result_kaggle"],
        how="left",
    )

    df1["round"] = df1["round"].fillna(df1["round_kaggle"])
    df1["city"] = df1["city"].fillna(df1["city_kaggle"])

    mask_placeholder = df1["date"].astype(str).map(is_placeholder_date)
    df1.loc[mask_placeholder, "date"] = df1.loc[mask_placeholder, "date_kaggle"].fillna(df1.loc[mask_placeholder, "date"])

    # ---------------------------
    # PASS 2 (bien DANS main)
    # ---------------------------
    need2 = df1["date_kaggle"].isna() & (df1["edition"] <= 2018)

    if need2.any():
        df2 = df1.loc[need2].merge(
            df_k[[
                "edition","home_key","away_key","city_key","round_key",
                "date_kaggle","home_result_kaggle","away_result_kaggle",
                "round_kaggle","city_kaggle"
            ]],
            on=["edition","home_key","away_key","city_key","round_key"],
            how="left",
            suffixes=("_v1", "_k")
        )

        date_col  = "date_kaggle_k" if "date_kaggle_k" in df2.columns else "date_kaggle"
        hr_col    = "home_result_kaggle_k" if "home_result_kaggle_k" in df2.columns else "home_result_kaggle"
        ar_col    = "away_result_kaggle_k" if "away_result_kaggle_k" in df2.columns else "away_result_kaggle"
        round_col = "round_kaggle_k" if "round_kaggle_k" in df2.columns else "round_kaggle"
        city_col  = "city_kaggle_k" if "city_kaggle_k" in df2.columns else "city_kaggle"

        idx = df1.index[need2]
        df1.loc[idx, "date"] = df2[date_col].fillna(df1.loc[idx, "date"]).values
        df1.loc[idx, "home_result"] = df2[hr_col].fillna(df1.loc[idx, "home_result"]).values
        df1.loc[idx, "away_result"] = df2[ar_col].fillna(df1.loc[idx, "away_result"]).values
        df1.loc[idx, "round"] = df2[round_col].fillna(df1.loc[idx, "round"]).values
        df1.loc[idx, "city"] = df2[city_col].fillna(df1.loc[idx, "city"]).values

    # (optionnel) recalcul result si vide
    if "result" in df1.columns:
        miss_result = df1["result"].isna()
        if miss_result.any():
            df1.loc[miss_result, "result"] = df1.loc[miss_result].apply(
                lambda r: compute_result(r["home_team"], r["away_team"], r["home_result"], r["away_result"]),
                axis=1,
            )

    # Cleanup + export (TOUJOURS, même si need2 vide)
    drop_cols = [
        c for c in df1.columns
        if c.endswith("_kaggle")
        or c in ["home_key","away_key","round_key","city_key","home_result_kaggle","away_result_kaggle","date_kaggle"]
    ]
    df_out = df1.drop(columns=[c for c in drop_cols if c in df1.columns])

    after_round_missing = int(df_out["round"].isna().sum())
    after_city_missing = int(df_out["city"].isna().sum())
    after_placeholder_dates = int(df_out["date"].astype(str).map(is_placeholder_date).sum())

    print(f"[ENRICH] round missing: {before_round_missing} -> {after_round_missing}")
    print(f"[ENRICH] city  missing: {before_city_missing} -> {after_city_missing}")
    print(f"[ENRICH] placeholder dates: {before_placeholder_dates} -> {after_placeholder_dates}")

    OUT_V2.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUT_V2, index=False)
    print(f"OK -> {OUT_V2}")

if __name__ == "__main__":
    main()