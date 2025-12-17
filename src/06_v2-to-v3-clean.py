from __future__ import annotations

from pathlib import Path
import re
import unicodedata

import pandas as pd

# -----------------------
# Paths
# -----------------------
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

IN_V2 = DATA / "processed" / "matches_unified_v2.csv"

OUT_V3 = DATA / "processed" / "matches_unified_v3.csv"
OUT_DIM = DATA / "clean" / "dim_teams.csv"
OUT_ALIASES = DATA / "reference" / "team_aliases.csv"
OUT_UNKNOWN = DATA / "reference" / "unknown_teams.csv"
OUT_QA = DATA / "reference" / "qa_team_collisions.csv"

OUT_V3.parent.mkdir(parents=True, exist_ok=True)
OUT_DIM.parent.mkdir(parents=True, exist_ok=True)


# -----------------------
# Regex / constants
# -----------------------
PLACEHOLDER_DATE_RE = re.compile(r"^\d{4}-01-01$")

CONTROL_CHARS_RE = re.compile(r"[\u0000-\u001F\u007F-\u009F]+")
PAREN_RE = re.compile(r"\s*\(([^)]*)\)\s*")

# ex: "(0-0, 0-0) Brazil" -> "Brazil"
LEADING_SCORE_TEAM_RE = re.compile(r"^\s*\([^)]*\)\s*")

# ex: ""rn"">Bosnia and Herzegovina",266  -> Bosnia and Herzegovina
LEADING_GARBAGE_RE = re.compile(r'^\s*["\']*(?:\\r\\n|\\n|\\r|rn|r?n)?["\']*\s*>\s*', re.IGNORECASE)
TRAILING_ID_RE = re.compile(r'"\s*,\s*\d+\s*$|,\s*\d+\s*$')

# Faux teams type A1..H2 / Group A etc.
GROUP_LABEL_RE = re.compile(r"^[A-H]\s*[12]$", re.IGNORECASE)  # A1..H2
GROUP_WORD_RE = re.compile(r"^group\s*[a-h]$", re.IGNORECASE)


# -----------------------
# Utils text
# -----------------------
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def fix_mojibake_basic(s: str) -> str:
    """
    Fix simple des cas comme 'C�te' / 'CÃ´te'.
    """
    if "�" in s:
        s = s.replace("�", "")
    if "Ã" in s or "Â" in s:
        try:
            cand = s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
            if cand:
                return cand
        except Exception:
            pass
    return s


def contains_non_latin(text: str) -> bool:
    for ch in text:
        if ch.isalpha():
            name = unicodedata.name(ch, "")
            if "LATIN" not in name:
                return True
    return False


def clean_name(s: object, *, keep_accents: bool = False) -> str:
    """
    Nettoyage générique.
    keep_accents=False recommandé (DW/ETL) pour stabilité.
    """
    if s is None or pd.isna(s):
        return ""
    t = str(s)

    t = CONTROL_CHARS_RE.sub(" ", t)
    t = normalize_spaces(t)

    t = t.strip().strip('"').strip("'").strip()
    t = t.rstrip(".").strip()

    t = fix_mojibake_basic(t)
    t = t.replace("’", "'").replace("`", "'")

    if not keep_accents:
        t = strip_accents(t)

    return normalize_spaces(t)


def clean_round(s: object) -> str:
    if s is None or pd.isna(s):
        return ""

    t = str(s).strip().lower()
    t = strip_accents(t)

    t = re.sub(r"[_\-]+", " ", t)
    t = re.sub(r"\s+", " ", t)

    if t.startswith("group"):
        return "Group"

    if t.startswith("preliminary"):
        return "Preliminary round"

    ROUND_MAP = {
        "1/8 final": "Round of 16",
        "round of 16": "Round of 16",

        "1/4 final": "Quarter-finals",
        "quarterfinal stage": "Quarter-finals",
        "quarter final": "Quarter-finals",

        "1/2 final": "Semi-finals",
        "semifinal stage": "Semi-finals",
        "semi final": "Semi-finals",

        "places 3&4": "Match for third place",
        "play off for third place": "Match for third place",
        "third place play off": "Match for third place",
        "match for third place": "Match for third place",

        "final": "Final",
        "final round": "Final",
        "first": "Final",

        "quarter finals": "Quarter-finals",
        "quarter final": "Quarter-finals",
        "semi finals": "Semi-finals",
        "semi final": "Semi-finals",

    }

    return ROUND_MAP.get(t, t.title()).strip()


def clean_team_raw(x: object) -> str:
    """
    Nettoyage robuste des équipes (version "référentiel").
    Ne supprime pas les accents ici (on le fait dans alias_key).
    """
    if x is None or pd.isna(x):
        return ""

    t = str(x)
    t = CONTROL_CHARS_RE.sub(" ", t)
    t = normalize_spaces(t)
    t = t.strip().strip('"').strip("'").strip()

    t = fix_mojibake_basic(t)

    # retire "(0-0, 0-0) "
    t = LEADING_SCORE_TEAM_RE.sub("", t).strip()

    # retire préfixes cassés
    t = LEADING_GARBAGE_RE.sub("", t).strip()
    t = TRAILING_ID_RE.sub("", t).strip()
    t = t.lstrip(">").strip()

    # apostrophes typographiques
    t = t.replace("’", "'").replace("`", "'")

    # retire parenthèses (langues locales, etc.)
    m = PAREN_RE.search(t)
    if m:
        inside = m.group(1)
        if contains_non_latin(inside) or True:
            t = PAREN_RE.sub("", t).strip()

    t = normalize_spaces(t)

    # supprime artefacts
    if t.upper() == "A":
        return ""
    if GROUP_LABEL_RE.fullmatch(t.replace(" ", "")):  # "A1" ou "A 1"
        return ""
    if GROUP_WORD_RE.fullmatch(t):
        return ""

    if len(t) < 2 or not any(ch.isalpha() for ch in t):
        return ""

    return t


def alias_key(s: str) -> str:
    """
    Clé stable et agressive pour matching.
    """
    s = strip_accents(s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


# -----------------------
# Match helpers
# -----------------------
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
        return row["home_team_canonical"] if pd.notna(row.get("home_team_canonical")) else row["home_team_raw"]
    if hg < ag:
        return row["away_team_canonical"] if pd.notna(row.get("away_team_canonical")) else row["away_team_raw"]
    return "draw"


# -----------------------
# Canonical resolver
# -----------------------
def build_country_resolver():
    """
    resolve(name)->(canonical_name, iso2, iso3)
    """
    try:
        import pycountry  # pip install pycountry
    except Exception:
        pycountry = None

    overrides = {
        "usa": ("United States", "US", "USA"),
        "u.s.a.": ("United States", "US", "USA"),
        "iriran": ("Iran", "IR", "IRN"),
        "iran": ("Iran", "IR", "IRN"),
        "korearepublic": ("South Korea", "KR", "KOR"),
        "republicofkorea": ("South Korea", "KR", "KOR"),
        "koreadpr": ("North Korea", None, None),
        "russianfederation": ("Russia", "RU", "RUS"),
        "russia": ("Russia", "RU", "RUS"),
        "cotedivoire": ("Ivory Coast", "CI", "CIV"),
        "ivorycoast": ("Ivory Coast", "CI", "CIV"),
        # historiques
        "czechoslovakia": ("Czechoslovakia", None, None),
        "yugoslavia": ("Yugoslavia", None, None),
        "sovietunion": ("USSR", None, None),
        "ussr": ("USSR", None, None),
        "westgermany": ("West Germany", None, None),
        "eastgermany": ("East Germany", None, None),
        "serbiaandmontenegro": ("Serbia and Montenegro", None, None),
    }

    def resolve(name: str):
        if not name:
            return ("", None, None)

        k = alias_key(name)
        if k in overrides:
            return overrides[k]

        if pycountry is None:
            return (strip_accents(name), None, None)

        try:
            q = strip_accents(name)
            c = pycountry.countries.search_fuzzy(q)[0]
            canonical = getattr(c, "common_name", None) or c.name
            return (canonical, getattr(c, "alpha_2", None), getattr(c, "alpha_3", None))
        except Exception:
            return (strip_accents(name), None, None)

    return resolve

    

# -----------------------
# Main
# -----------------------
def main() -> None:
    df = pd.read_csv(IN_V2)

    required = ["home_team", "away_team", "home_result", "away_result", "date", "round", "city", "edition"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes dans V2: {missing} (colonnes: {df.columns.tolist()})")

    before = len(df)

    # 1) scores
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")

    # 2) Nettoyage "match" (round/city/edition)
    df["round"] = df["round"].map(clean_round)
    df["city"] = df["city"].map(lambda x: clean_name(x, keep_accents=False))
    df["edition"] = df["edition"].astype(str)
    df["edition_year"] = extract_year_from_edition_label(df["edition"])

    # 3) Nettoyage "teams" (on conserve le RAW original pour pouvoir mapper)
    df["home_team_raw"] = df["home_team"].astype(str)
    df["away_team_raw"] = df["away_team"].astype(str)

    # Clean léger pour préparer mapping
    df["home_team_clean"] = df["home_team_raw"].map(clean_team_raw)
    df["away_team_clean"] = df["away_team_raw"].map(clean_team_raw)

    # Supprime lignes invalides (team clean vide)
    invalid_team = (df["home_team_clean"] == "") | (df["away_team_clean"] == "")
    df = df.loc[~invalid_team].copy()

    # 4) Supprimer lignes fantômes
    ghost = df["date"].isna() & df["home_result"].isna() & df["away_result"].isna()
    df = df.loc[~ghost].copy()

    # 5) placeholder date
    df["is_placeholder_date"] = df["date"].astype(str).map(is_placeholder_date)
    
    
    # -----------------------
    # 6) Build dim_teams + aliases + mapping IDs
    # -----------------------
    resolve = build_country_resolver()

    # raw teams uniques
    raw_teams = (
        pd.concat([df["home_team_raw"], df["away_team_raw"]])
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    alias_rows = []
    for raw in raw_teams:
        clean = clean_team_raw(raw)
        if not clean:
            continue
        alias_rows.append({"team_raw": raw, "team_clean": clean, "alias_key": alias_key(clean)})

    aliases_df = pd.DataFrame(alias_rows).drop_duplicates(subset=["team_raw"]).reset_index(drop=True)

    tmp_dim_rows = []
    for clean in sorted(aliases_df["team_clean"].dropna().unique()):
        canonical, iso2, iso3 = resolve(clean)
        canonical_key = alias_key(canonical) if canonical else alias_key(clean)
        tmp_dim_rows.append(
            {
                "team_canonical": canonical,
                "team_clean_example": clean,
                "canonical_key": canonical_key,
                "iso2": iso2,
                "iso3": iso3,
            }
        )

    tmp_dim = pd.DataFrame(tmp_dim_rows)
    tmp_dim["_iso_score"] = tmp_dim["iso3"].notna().astype(int) + tmp_dim["iso2"].notna().astype(int)

    dim = (
        tmp_dim.sort_values(["canonical_key", "_iso_score", "team_canonical"], ascending=[True, False, True])
        .drop_duplicates(subset=["canonical_key"], keep="first")
        .drop(columns=["_iso_score"])
        .sort_values("team_canonical")
        .reset_index(drop=True)
    )
    dim["team_id"] = range(1, len(dim) + 1)

    clean_to_canonical_key = {row["team_clean_example"]: row["canonical_key"] for _, row in tmp_dim.iterrows()}
    aliases_df["canonical_key"] = aliases_df["team_clean"].map(clean_to_canonical_key)

    canonical_key_to_id = dict(zip(dim["canonical_key"], dim["team_id"]))
    aliases_df["team_id"] = aliases_df["canonical_key"].map(canonical_key_to_id)

    unknown = aliases_df.loc[aliases_df["team_id"].isna(), ["team_raw", "team_clean", "alias_key"]].copy()
    unknown = unknown.sort_values(["team_clean", "team_raw"]).reset_index(drop=True)

    qa = (
        aliases_df.groupby(["team_id"], dropna=False)
        .agg(
            n_variants=("team_raw", "nunique"),
            variants=("team_clean", lambda s: ", ".join(sorted(set(s))[:30])),
        )
        .reset_index()
        .sort_values("n_variants", ascending=False)
    )

    # apply mapping RAW -> team_id
    raw_to_id = dict(zip(aliases_df["team_raw"], aliases_df["team_id"]))
    id_to_canonical = dict(zip(dim["team_id"], dim["team_canonical"]))

    df["home_team_id"] = df["home_team_raw"].map(raw_to_id)
    df["away_team_id"] = df["away_team_raw"].map(raw_to_id)
    df["home_team_canonical"] = df["home_team_id"].map(id_to_canonical)
    df["away_team_canonical"] = df["away_team_id"].map(id_to_canonical)

    # drop si mapping échoue (normalement rare/0)
    df = df.loc[df["home_team_id"].notna() & df["away_team_id"].notna()].copy()

    # -----------------------
    # 7) Dédupe + tri + id_match + result
    # -----------------------
    match_key = ["edition_year", "date", "home_team_id", "away_team_id", "home_result", "away_result", "round", "city"]
    df = df.drop_duplicates(subset=match_key, keep="first").copy()

    df["_date_sort"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(
        ["edition_year", "_date_sort", "round", "home_team_id", "away_team_id"],
        kind="mergesort",
        na_position="last",
    ).drop(columns=["_date_sort"])

    df = df.reset_index(drop=True)
    df["id_match"] = range(1, len(df) + 1)

    df["round"] = df["round"].map(clean_round).str.strip()

    df["result"] = df.apply(compute_result, axis=1)

    after = len(df)

    # -----------------------
    # 8) Export
    # -----------------------
    dim.to_csv(OUT_DIM, index=False, encoding="utf-8")
    aliases_df[["team_raw", "team_clean", "alias_key", "team_id"]].to_csv(OUT_ALIASES, index=False, encoding="utf-8")
    unknown.to_csv(OUT_UNKNOWN, index=False, encoding="utf-8")
    qa.to_csv(OUT_QA, index=False, encoding="utf-8")

    # Fact final V3 : tu peux choisir quelles colonnes garder
    cols_out = [
        "id_match",
        "edition",
        "edition_year",
        "date",
        "round",
        "city",
        "home_team_id",
        "away_team_id",
        "home_team_canonical",
        "away_team_canonical",
        "home_result",
        "away_result",
        "result",
        "is_placeholder_date",
    ]
    # garde seulement ce qui existe
    cols_out = [c for c in cols_out if c in df.columns]
    df[cols_out].to_csv(OUT_V3, index=False, encoding="utf-8")

    print(f"[V2->V3 PERFECT] rows: {before} -> {after}")
    print("[V2->V3 PERFECT] ghosts removed:", int(ghost.sum()))
    print("[V2->V3 PERFECT] invalid_team removed:", int(invalid_team.sum()))
    print("[V2->V3 PERFECT] placeholder dates remaining:", int(df["is_placeholder_date"].sum()))
    print("[V2->V3 PERFECT] dim_teams:", OUT_DIM, f"({len(dim)} rows)")
    print("[V2->V3 PERFECT] aliases:", OUT_ALIASES, f"({len(aliases_df)} rows)")
    print("[V2->V3 PERFECT] unknown:", OUT_UNKNOWN, f"({len(unknown)} rows)")
    print("OK ->", OUT_V3)



if __name__ == "__main__":
    main()