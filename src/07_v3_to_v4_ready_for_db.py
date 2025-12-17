from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

IN_V3 = DATA / "processed" / "matches_unified_v3.csv"

# sorties V4
OUT_MATCHES_V4 = DATA / "clean" / "matches_unified_v4.csv"  # 10 colonnes, prêt BDD
OUT_TEAMS_V4 = DATA / "clean" / "teams_v4.csv"  # dimension simple
OUT_REPORT = DATA / "clean" / "quality_report_v4.txt"  # mini rapport

PLACEHOLDER_DATE_RE = re.compile(r"^\d{4}-01-01$")


def is_placeholder_date(s: object) -> bool:
    if s is None or pd.isna(s):
        return False
    return bool(PLACEHOLDER_DATE_RE.match(str(s).strip()))


def looks_like_placeholder_team(s: object) -> bool:
    """
    Détecte les lignes foireuses type '1', 'F1', etc. + vides/espaces.
    """
    if s is None or pd.isna(s):
        return True
    t = str(s).strip()
    if t == "":
        return True
    if t.isdigit():
        return True
    if re.fullmatch(r"[A-Za-z]\d+", t):  # ex: F1
        return True
    return False


def clean_text(s: object) -> str:
    """Nettoyage soft pour éviter les ' ' / NBSP / multiples espaces."""
    if s is None or pd.isna(s):
        return ""
    t = str(s).replace("\xa0", " ")  # NBSP
    t = re.sub(r"\s+", " ", t).strip()
    return t


def clean_team_name(s: object) -> str:
    """
    Supprime le bruit en préfixe qu'on trouve parfois dans les équipes, ex:
    "(0-0, 0-0) Brazil" -> "Brazil"
    """
    if s is None or pd.isna(s):
        return ""
    t = str(s).replace("\xa0", " ")
    # supprime un groupe "(...)" au début + espaces
    t = re.sub(r"^\([^)]*\)\s*", "", t)
    # normalise les espaces
    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_edition_year(edition_label: object) -> pd.Series:
    """Extrait YYYY depuis '2018-RUSSIA' ou '2018'."""
    ser = pd.Series(edition_label, dtype="string")
    year = ser.str.extract(r"(\d{4})")[0]
    return pd.to_numeric(year, errors="coerce").astype("Int64")


def compute_result(home_team: str, away_team: str, hg: float, ag: float) -> str:
    if pd.isna(hg) or pd.isna(ag):
        return "draw"
    if hg > ag:
        return home_team
    if hg < ag:
        return away_team
    return "draw"


def make_match_uid(row: pd.Series) -> str:
    """
    UID stable pour debug/dédoublonnage : edition + date + teams + scores + round + city
    """
    parts = [
        str(row.get("edition", "")),
        str(row.get("date", "")),
        str(row.get("home_team", "")),
        str(row.get("away_team", "")),
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
    # 0) Colonnes attendues
    # -----------------------
    required = [
        "home_team",
        "away_team",
        "home_result",
        "away_result",
        "date",
        "round",
        "city",
        "edition",
        "result",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes dans V3: {missing} (colonnes: {df.columns.tolist()})")

    before = len(df)

    # -----------------------
    # 1) Nettoyage texte de base
    # -----------------------
    # (on nettoie d'abord)
    for col in ["home_team", "away_team", "round", "city", "edition", "result"]:
        df[col] = df[col].map(clean_text)

    # ✅ fix équipes : on enlève le bruit du type "(0-0, 0-0) Brazil"
    df["home_team"] = df["home_team"].map(clean_team_name)
    df["away_team"] = df["away_team"].map(clean_team_name)

    # ✅ IMPORTANT : on garde edition en STRING (ex: 2018-RUSSIA)
    df["edition"] = df["edition"].astype(str)

    # ✅ clé technique uniquement pour tri / KPI / filtres
    df["edition_year"] = extract_edition_year(df["edition"])

    # Types scores
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")

    # -----------------------
    # 2) Supprimer ghost + placeholders teams
    # -----------------------
    ghost = df["date"].isna() & df["home_result"].isna() & df["away_result"].isna()
    df = df.loc[~ghost].copy()

    bad_team = df["home_team"].map(looks_like_placeholder_team) | df["away_team"].map(
        looks_like_placeholder_team
    )
    df = df.loc[~bad_team].copy()

    # -----------------------
    # 3) Dates : parse + placeholder -> NULL
    # -----------------------
    df["date_is_placeholder"] = df["date"].map(is_placeholder_date)
    dt = pd.to_datetime(df["date"], errors="coerce")
    dt = dt.mask(df["date_is_placeholder"], pd.NaT)
    df["date"] = dt.dt.strftime("%Y-%m-%d")

    # -----------------------
    # 4) result : cohérent avec scores
    # -----------------------
    df["result"] = df.apply(
        lambda r: compute_result(r["home_team"], r["away_team"], r["home_result"], r["away_result"]),
        axis=1,
    )

    # -----------------------
    # 5) UID + dédoublonnage
    # -----------------------
    df["match_uid"] = df.apply(make_match_uid, axis=1)
    df = df.drop_duplicates(subset=["match_uid"], keep="first").copy()

    # -----------------------
    # 6) teams dim + IDs (et on exclut explicitement les vides)
    # -----------------------
    teams = (
        pd.concat(
            [
                df[["home_team"]].rename(columns={"home_team": "team"}),
                df[["away_team"]].rename(columns={"away_team": "team"}),
            ],
            ignore_index=True,
        )
        .assign(team=lambda x: x["team"].map(clean_team_name))
    )

    teams = teams.loc[~teams["team"].map(looks_like_placeholder_team)].copy()
    teams = teams.drop_duplicates(subset=["team"]).sort_values("team").reset_index(drop=True)

    teams["team_id"] = range(1, len(teams) + 1)
    team_map = dict(zip(teams["team"], teams["team_id"]))

    df["home_team_id"] = df["home_team"].map(team_map)
    df["away_team_id"] = df["away_team"].map(team_map)

    # -----------------------
    # 7) Recréer id_match séquentiel (tri “raisonnable”)
    # -----------------------
    df["_date_sort"] = pd.to_datetime(df["date"], errors="coerce")
    df = (
        df.sort_values(
            ["edition_year", "_date_sort", "round", "home_team", "away_team"],
            kind="mergesort",
            na_position="last",
        )
        .drop(columns=["_date_sort"])
        .copy()
    )
    df = df.reset_index(drop=True)
    df["id_match"] = range(1, len(df) + 1)

    # -----------------------
    # 8) Sortie V4 = exactement 10 colonnes (livrable)
    # -----------------------
    out_cols = [
        "id_match",
        "home_team",
        "away_team",
        "home_result",
        "away_result",
        "result",
        "date",
        "round",
        "city",
        "edition",  # ✅ string conservé (2018-RUSSIA)
    ]
    df_out = df[out_cols].copy()

    df_out["home_result"] = df_out["home_result"].astype("Int64")
    df_out["away_result"] = df_out["away_result"].astype("Int64")

    # -----------------------
    # 9) Rapport qualité
    # -----------------------
    after = len(df_out)

    per_edition_label = df_out.groupby("edition").size().sort_index()
    per_edition_year = df.groupby("edition_year").size().sort_index()

    report_lines = [
        f"V3 -> V4 rows: {before} -> {after}",
        f"ghost removed: {int(ghost.sum())}",
        f"bad team rows removed: {int(bad_team.sum())}",
        f"placeholder dates (set to NULL): {int(df['date_is_placeholder'].sum())}",
        "",
        "matches per edition (label):",
        per_edition_label.to_string(),
        "",
        "matches per edition_year:",
        per_edition_year.to_string(),
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
