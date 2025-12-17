from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

IN_V3 = DATA / "processed" / "matches_unified_v3.csv"

# sorties V4
OUT_MATCHES_V4 = DATA / "processed" / "matches_unified_v4.csv"  
OUT_TEAMS_V4 = DATA / "reference" / "teams_v4.csv" 
OUT_REPORT = DATA / "reference" / "quality_report_v4.txt"

PLACEHOLDER_DATE_RE = re.compile(r"^\d{4}-01-01$")


def is_placeholder_date(s: object) -> bool:
    if s is None or pd.isna(s):
        return False
    return bool(PLACEHOLDER_DATE_RE.match(str(s).strip()))


def clean_text(s: object) -> str:
    """Nettoyage soft (espaces/NBSP)."""
    if s is None or pd.isna(s):
        return ""
    t = str(s).replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def compute_result_ids(home_team_id: int, away_team_id: int, hg: float, ag: float) -> str:
    """
    Résultat côté DB :
    - 'draw' ou bien l'id de l'équipe gagnante (sous forme de string pour rester homogène).
    """
    if pd.isna(hg) or pd.isna(ag):
        return "draw"
    if hg > ag:
        return str(int(home_team_id))
    if hg < ag:
        return str(int(away_team_id))
    return "draw"


def make_match_uid(row: pd.Series) -> str:
    """
    UID stable pour dédoublonnage/debug : edition + date + team_ids + scores + round + city
    """
    parts = [
        str(row.get("edition", "")),
        str(row.get("date", "")),
        str(row.get("home_team_id", "")),
        str(row.get("away_team_id", "")),
        str(row.get("home_result", "")),
        str(row.get("away_result", "")),
        str(row.get("round", "")),
        str(row.get("city", "")),
    ]
    raw = "|".join(parts).encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()


def main() -> None:
    df = pd.read_csv(IN_V3)

    # -----------------------
    # 0) Colonnes attendues (V3 ID-based)
    # -----------------------
    required = [
        "edition",
        "date",
        "round",
        "city",
        "home_team_id",
        "away_team_id",
        "home_team_canonical",
        "away_team_canonical",
        "home_result",
        "away_result",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes dans V3: {missing} (colonnes: {df.columns.tolist()})")

    before = len(df)

    # -----------------------
    # 1) Nettoyage texte
    # -----------------------
    for col in ["round", "city", "edition", "home_team_canonical", "away_team_canonical"]:
        df[col] = df[col].map(clean_text)

    # types
    df["home_team_id"] = pd.to_numeric(df["home_team_id"], errors="coerce").astype("Int64")
    df["away_team_id"] = pd.to_numeric(df["away_team_id"], errors="coerce").astype("Int64")
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")

    # -----------------------
    # 2) Supprimer ghost / lignes invalides
    # -----------------------
    ghost = df["date"].isna() & df["home_result"].isna() & df["away_result"].isna()
    df = df.loc[~ghost].copy()

    bad_ids = df["home_team_id"].isna() | df["away_team_id"].isna()
    df = df.loc[~bad_ids].copy()

    # -----------------------
    # 3) Dates : placeholder -> NULL
    # -----------------------
    df["date_is_placeholder"] = df["date"].map(is_placeholder_date)
    dt = pd.to_datetime(df["date"], errors="coerce")
    dt = dt.mask(df["date_is_placeholder"], pd.NaT)
    df["date"] = dt.dt.strftime("%Y-%m-%d")

    # -----------------------
    # 4) result (DB-friendly)
    # -----------------------
    df["result"] = df.apply(
        lambda r: compute_result_ids(r["home_team_id"], r["away_team_id"], r["home_result"], r["away_result"]),
        axis=1,
    )

    # -----------------------
    # 5) UID + dédoublonnage
    # -----------------------
    df["match_uid"] = df.apply(make_match_uid, axis=1)
    df = df.drop_duplicates(subset=["match_uid"], keep="first").copy()

    # -----------------------
    # 6) Recréer id_match séquentiel
    # -----------------------
    df["_date_sort"] = pd.to_datetime(df["date"], errors="coerce")
    df = (
        df.sort_values(
            ["edition_year", "_date_sort", "round", "home_team_id", "away_team_id"]
            if "edition_year" in df.columns
            else ["edition", "_date_sort", "round", "home_team_id", "away_team_id"],
            kind="mergesort",
            na_position="last",
        )
        .drop(columns=["_date_sort"])
        .copy()
    )
    df = df.reset_index(drop=True)
    df["id_match"] = range(1, len(df) + 1)

    # -----------------------
    # 7) Dimension teams (simple)
    # -----------------------
    teams = (
        pd.concat(
            [
                df[["home_team_id", "home_team_canonical"]].rename(
                    columns={"home_team_id": "team_id", "home_team_canonical": "team_canonical"}
                ),
                df[["away_team_id", "away_team_canonical"]].rename(
                    columns={"away_team_id": "team_id", "away_team_canonical": "team_canonical"}
                ),
            ],
            ignore_index=True,
        )
        .dropna(subset=["team_id", "team_canonical"])
        .drop_duplicates(subset=["team_id"])
        .sort_values("team_id")
        .reset_index(drop=True)
    )

    # -----------------------
    # 8) Sortie V4 = 10 colonnes (fact ID-based)
    # -----------------------
    out_cols = [
        "id_match",
        "home_team_id",
        "away_team_id",
        "home_result",
        "away_result",
        "result",
        "date",
        "round",
        "city",
        "edition",
    ]
    df_out = df[out_cols].copy()

    df_out["home_result"] = df_out["home_result"].astype("Int64")
    df_out["away_result"] = df_out["away_result"].astype("Int64")
    df_out["home_team_id"] = df_out["home_team_id"].astype("Int64")
    df_out["away_team_id"] = df_out["away_team_id"].astype("Int64")

    # -----------------------
    # 9) Rapport qualité
    # -----------------------
    after = len(df_out)

    report_lines = [
        f"V3 -> V4 rows: {before} -> {after}",
        f"ghost removed: {int(ghost.sum())}",
        f"bad team_id rows removed: {int(bad_ids.sum())}",
        f"placeholder dates (set to NULL): {int(df['date_is_placeholder'].sum())}",
        f"n_teams_dim: {len(teams)}",
        "",
        "round value_counts:",
        df_out["round"].value_counts().to_string(),
    ]

    OUT_MATCHES_V4.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUT_MATCHES_V4, index=False, encoding="utf-8")
    teams.to_csv(OUT_TEAMS_V4, index=False, encoding="utf-8")
    OUT_REPORT.write_text("\n".join(report_lines), encoding="utf-8")

    print("OK ->", OUT_MATCHES_V4)
    print("OK ->", OUT_TEAMS_V4)
    print("OK ->", OUT_REPORT)


if __name__ == "__main__":
    main()