from __future__ import annotations

from pathlib import Path
import re
import unicodedata

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

IN_V2 = DATA / "processed" / "matches_unified_v2.csv"
OUT_V3 = DATA / "processed" / "matches_unified_v3.csv"

PLACEHOLDER_DATE_RE = re.compile(r"^\d{4}-01-01$")

# ex: "(0-0, 0-0) Brazil"  -> "Brazil"
LEADING_SCORE_TEAM_RE = re.compile(r"^\s*\([^)]*\)\s*")

# ex: ""rn"">Bosnia and Herzegovina",266  -> Bosnia and Herzegovina
LEADING_GARBAGE_RE = re.compile(r'^\s*["\']*(?:\\r\\n|\\n|\\r|rn|r?n)?["\']*\s*>\s*', re.IGNORECASE)

# ex: Bosnia and Herzegovina",266  -> Bosnia and Herzegovina
TRAILING_ID_RE = re.compile(r'"\s*,\s*\d+\s*$|,\s*\d+\s*$')

# parenthèses: Afghanistan (افغانستان) -> Afghanistan
PAREN_CONTENT_RE = re.compile(r"\s*\(([^)]*)\)\s*")

# contrôle chars invisibles / BOM / etc.
CONTROL_CHARS_RE = re.compile(r"[\u0000-\u001F\u007F-\u009F]+")


# ---------------------------
# Normalisation texte
# ---------------------------
def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def contains_non_latin(text: str) -> bool:
    """
    Détecte vite fait la présence de caractères non-latins.
    (utile pour enlever les noms locaux en parenthèses)
    """
    for ch in text:
        if ch.isalpha():
            name = unicodedata.name(ch, "")
            if not (
                "LATIN" in name
                or "COMBINING" in name
            ):
                return True
    return False


def clean_name(s: object, *, keep_accents: bool = False) -> str:
    """
    Nettoyage générique.
    keep_accents=False recommandé (DW/ETL) pour éviter les soucis d'encodage/jointures.
    """
    if s is None or pd.isna(s):
        return ""
    t = str(s)

    # supprime contrôle/invisibles
    t = CONTROL_CHARS_RE.sub(" ", t)

    # normalise espaces
    t = normalize_spaces(t)

    # enlève guillemets superflus autour
    t = t.strip().strip('"').strip("'").strip()

    # enlève points finaux
    t = t.rstrip(".").strip()

    # normalise apostrophes typographiques vers '
    t = t.replace("’", "'").replace("`", "'")

    if not keep_accents:
        t = strip_accents(t)

    return t


def clean_round(s: object) -> str:
    t = clean_name(s, keep_accents=False)
    mapping = {
        "Group": "Group",
        "Group Stage": "Group",
        "GROUP_STAGE": "Group",
        "Round of 16": "Round of 16",
        "Last 16": "Round of 16",
        "Quarterfinals": "Quarter-finals",
        "Quarter-finals": "Quarter-finals",
        "Semifinals": "Semi-finals",
        "Semi-finals": "Semi-finals",
        "Third place": "Match for third place",
        "Match for third place": "Match for third place",
        "Final": "Final",
    }
    return mapping.get(t, t)


TEAM_VARIANTS = {
    # Fréquents dans datasets football
    "Korea Republic": "South Korea",
    "Republic of Korea": "South Korea",
    "Korea DPR": "North Korea",
    "IR Iran": "Iran",
    "USA": "United States",
    "Russian Federation": "Russia",
    "Cote d'Ivoire": "Ivory Coast",
    "Cote dIvoire": "Ivory Coast",
    "Cote d Ivoire": "Ivory Coast",
    "Cote d’ivoire": "Ivory Coast",
    "Cote d'ivoire": "Ivory Coast",
    "Cote dIvoire ": "Ivory Coast",
    "Ivory Coast": "Ivory Coast",
    "German DR": "East Germany",
    "Germany DR": "East Germany",
    "Germany FR": "West Germany",
    "Soviet Union": "USSR",
    "Serbia and Montenegro": "Serbia and Montenegro",
}


def clean_team_raw(s: object, *, keep_accents: bool = False) -> str:
    """
    Nettoyage robuste des équipes (cas 2022 inclus).
    """
    t = clean_name(s, keep_accents=keep_accents)
    if not t:
        return ""

    # 1) retire "(0-0, 0-0) "
    t = LEADING_SCORE_TEAM_RE.sub("", t).strip()

    # 2) retire préfixes cassés du style rn"> / ""rn"">
    t = LEADING_GARBAGE_RE.sub("", t).strip()

    # 3) retire IDs traînants du style '",266' ou ',266'
    t = TRAILING_ID_RE.sub("", t).strip()

    # 4) retire un éventuel '>' restant
    t = t.lstrip(">").strip()

    # 5) gère parenthèses : garde le nom anglais, jette le local si non-latin
    #    ex: Afghanistan (افغانستان) -> Afghanistan
    m = PAREN_CONTENT_RE.search(t)
    if m:
        inside = m.group(1)
        if contains_non_latin(inside):
            t = PAREN_CONTENT_RE.sub("", t).strip()
        else:
            # parenthèses "normales" : on peut aussi les supprimer pour standardiser
            t = PAREN_CONTENT_RE.sub("", t).strip()

    # 6) normalise espaces à nouveau
    t = normalize_spaces(t)

    # 7) mapping variantes
    t = TEAM_VARIANTS.get(t, t)

    # 8) élimine valeurs parasites (ex: "A")
    #    (tu peux élargir si tu vois d'autres artefacts)
    if t.upper() == "A":
        return ""

    # si trop court ou pas de lettres -> vide
    if len(t) < 2 or not any(ch.isalpha() for ch in t):
        return ""

    return t


def clean_team(s: object) -> str:
    # Best practice ETL/DW: accents OFF pour joins + stabilité
    return clean_team_raw(s, keep_accents=False)


def extract_year_from_edition_label(edition: object) -> pd.Series:
    ser = pd.Series(edition, dtype="string")
    year = ser.str.extract(r"(\d{4})")[0]
    return pd.to_numeric(year, errors="coerce").astype("Int64")


def is_placeholder_date(s: object) -> bool:
    if s is None or pd.isna(s):
        return False
    return bool(PLACEHOLDER_DATE_RE.match(str(s).strip()))


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

    before = len(df)

    required = ["home_team", "away_team", "home_result", "away_result", "date", "round", "city", "edition"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes dans V2: {missing} (colonnes: {df.columns.tolist()})")

    # 1) Types scores
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")

    # 2) Nettoyage texte
    df["home_team"] = df["home_team"].map(clean_team)
    df["away_team"] = df["away_team"].map(clean_team)
    df["city"] = df["city"].map(lambda x: clean_name(x, keep_accents=False))
    df["round"] = df["round"].map(clean_round)

    # 2bis) Supprime lignes où une équipe est invalide (ex: "A", vide, etc.)
    invalid_team = (df["home_team"] == "") | (df["away_team"] == "")
    df = df.loc[~invalid_team].copy()

    # 3) Edition : label string + edition_year
    df["edition"] = df["edition"].astype(str)
    df["edition_year"] = extract_year_from_edition_label(df["edition"])

    # 4) Supprimer lignes fantômes
    ghost = df["date"].isna() & df["home_result"].isna() & df["away_result"].isna()
    df = df.loc[~ghost].copy()

    # 5) Placeholder dates
    df["is_placeholder_date"] = df["date"].astype(str).map(is_placeholder_date)

    # 6) Result
    df["result"] = df.apply(compute_result, axis=1)

    # 7) Dédoublonnage robuste
    match_key = ["edition_year", "date", "home_team", "away_team", "home_result", "away_result", "round", "city"]
    df = df.drop_duplicates(subset=match_key, keep="first").copy()

    # 8) Tri + id_match
    df["_date_sort"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(
        ["edition_year", "_date_sort", "round", "home_team", "away_team"],
        kind="mergesort",
        na_position="last",
    ).drop(columns=["_date_sort"])

    df = df.reset_index(drop=True)
    df["id_match"] = range(1, len(df) + 1)

    # 9) Table teams + ids
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

    # au cas où (sécurité)
    teams = teams.loc[teams["team"].astype(str).str.len() >= 2].copy()

    teams["team_id"] = range(1, len(teams) + 1)
    team_map = dict(zip(teams["team"], teams["team_id"]))

    df["home_team_id"] = df["home_team"].map(team_map)
    df["away_team_id"] = df["away_team"].map(team_map)

    after = len(df)

    # 10) Export
    OUT_V3.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_V3, index=False, encoding="utf-8")

    print(f"[V2->V3] rows: {before} -> {after}")
    print("[V2->V3] ghosts removed:", int(ghost.sum()))
    print("[V2->V3] invalid_team removed:", int(invalid_team.sum()))
    print("[V2->V3] placeholder dates remaining:", int(df["is_placeholder_date"].sum()))
    print("[V2->V3] matches per edition_year (top):")
    print(df.groupby("edition_year").size().sort_values(ascending=False).head(10))
    print("OK ->", OUT_V3)


if __name__ == "__main__":
    main()
