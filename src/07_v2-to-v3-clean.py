from pathlib import Path
import re

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

IN_V2 = DATA / "processed" / "matches_unified_v2.csv"
OUT_V3 = DATA / "processed" / "matches_unified_v3.csv"


def is_placeholder_date(s: str) -> bool:
    return bool(re.match(r"^\d{4}-01-01$", str(s)))


def compute_result(row: pd.Series) -> str:
    hg, ag = row["home_result"], row["away_result"]
    if pd.isna(hg) or pd.isna(ag):
        return "draw"
    if hg > ag:
        return row["home_team"]
    if hg < ag:
        return row["away_team"]
    return "draw"


def main() -> None:
    df = pd.read_csv(IN_V2)

    # types
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")
    df["edition"] = pd.to_numeric(df["edition"], errors="coerce").astype("Int64")

    before = len(df)

    # 1) supprimer lignes "fantômes" (aucune info utile)
    ghost = df["date"].isna() & df["home_result"].isna() & df["away_result"].isna()
    df = df.loc[~ghost].copy()

    # 2) supprimer les dates placeholder si on n'a pas mieux
    # (ici on ne peut pas inventer une date; on garde mais on pourra les traiter ensuite)
    # -> option: les marquer pour contrôle
    df["is_placeholder_date"] = df["date"].astype(str).map(is_placeholder_date)

    # 3) recompute result (si vide/NaN)
    df["result"] = df["result"].fillna(df.apply(compute_result, axis=1))

    # 4) dédoublonnage "match key" (la plus sûre)
    # (Si tu veux forcer 2014 à 64, c’est ce bloc qui le fera si tes doublons sont réels)
    match_key = ["edition", "date", "home_team", "away_team", "home_result", "away_result"]
    df = df.drop_duplicates(subset=match_key, keep="first")

    # 5) Recréer id_match propre
    # Tri chronologique: edition puis date (quand possible) puis id_match d'origine
    df["date_sort"] = pd.to_datetime(df["date"], errors="coerce")
    df = (
        df.sort_values(
            ["edition", "date_sort", "home_team", "away_team"],
            kind="mergesort",
        )
        .drop(columns=["date_sort"])
    )

    teams = (
        pd.concat(
            [
                df[["home_team"]].rename(columns={"home_team": "team"}),
                df[["away_team"]].rename(columns={"away_team": "team"}),
            ]
        )
        .dropna()
        .drop_duplicates()
        .sort_values("team")
        .reset_index(drop=True)
    )

    teams["id_team"] = range(1, len(teams) + 1)

    # mapping
    team_map = dict(zip(teams["team"], teams["id_team"]))

    df["home_team_id"] = df["home_team"].map(team_map)
    df["away_team_id"] = df["away_team"].map(team_map)

    df["id_match"] = range(1, len(df) + 1)

    after = len(df)

    print(f"[V2->V3] rows: {before} -> {after}")
    print("[V2->V3] ghosts removed:", int(ghost.sum()))
    print("[V2->V3] placeholder dates remaining:", int(df["is_placeholder_date"].sum()))
    print("[V2->V3] matches per edition (top):")
    print(df.groupby("edition").size().sort_values(ascending=False).head(10))

    # on enlève la colonne de contrôle si tu veux rester strict sur le schéma
    df = df.drop(columns=["is_placeholder_date"])

    df.to_csv(OUT_V3, index=False)
    print("OK ->", OUT_V3)


if __name__ == "__main__":
    main()
