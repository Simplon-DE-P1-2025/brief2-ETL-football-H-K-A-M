from __future__ import annotations

from pathlib import Path
import re
import unicodedata
import pandas as pd

# =============================================================================
# 1. CONFIGURATION & CHEMINS
# =============================================================================
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


# =============================================================================
# 2. DICTIONNAIRES DE CORRECTION
# =============================================================================

GARBAGE_KEYWORDS = [
    "WINNER", "LOSER", "RUNNER-UP", "RUNNER UP", 
    "GROUP", "TEAMS", "MATCH", "SEMI-FINAL", "FINALIST",
    "PLAY-OFF", "REPECHAGE"
]

# On force "Cote d Ivoire" (Avec ESPACE à la place de l'apostrophe)
MANUAL_CORRECTIONS = {
    # --- Côte d'Ivoire ---
    "Cte d'Ivoire": "Cote d Ivoire",
    "Ivory Coast": "Cote d Ivoire",
    "CÃ´te d'Ivoire": "Cote d Ivoire",
    "Côte d’Ivoire": "Cote d Ivoire",
    "Côte d'Ivoire": "Cote d Ivoire",
    "Cote d'Ivoire": "Cote d Ivoire",
    "Cote dIvoire": "Cote d Ivoire",

    # --- Autres ---
    "Zaire": "Democratic Republic of the Congo",
    "DR Congo": "Democratic Republic of the Congo",
    "Congo DR": "Democratic Republic of the Congo",
    "Democratic Republic of the Congo": "Democratic Republic of the Congo",
    "Congo": "Congo", 
    "Holland": "Netherlands",
    "The Netherlands": "Netherlands",
    "West Germany": "Germany",
    "East Germany": "Germany",
    "Soviet Union": "USSR",
    "China PR": "China",
    "United States": "USA",
    "Korea Republic": "South Korea",
    "South Korea": "South Korea",
    "Korea DPR": "North Korea",
    "North Korea": "North Korea",
    "Iran": "Iran",
    "IR Iran": "Iran",
    "Bosnia-Herzegovina": "Bosnia and Herzegovina"
}


# =============================================================================
# 3. REGEX & UTILITAIRES
# =============================================================================
PLACEHOLDER_DATE_RE = re.compile(r"^\d{4}-01-01$")
CONTROL_CHARS_RE = re.compile(r"[\u0000-\u001F\u007F-\u009F]+")
PAREN_RE = re.compile(r"\s*\(([^)]*)\)\s*")
LEADING_SCORE_TEAM_RE = re.compile(r"^\s*\([^)]*\)\s*")
LEADING_GARBAGE_RE = re.compile(r'^\s*["\']*(?:\\r\\n|\\n|\\r|rn|r?n)?["\']*\s*>\s*', re.IGNORECASE)
TRAILING_ID_RE = re.compile(r'"\s*,\s*\d+\s*$|,\s*\d+\s*$')
GROUP_LABEL_RE = re.compile(r"^[A-H]\s*[12]$", re.IGNORECASE)
GROUP_WORD_RE = re.compile(r"^group\s*[a-h]$", re.IGNORECASE)

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def fix_mojibake_basic(s: str) -> str:
    if "" in s: s = s.replace("", "")
    return s

def is_garbage_row(row_name: str) -> bool:
    if not isinstance(row_name, str): return False
    name_upper = row_name.upper()
    for bad in GARBAGE_KEYWORDS:
        if bad in name_upper: return True
    return False

def clean_name(s: object, *, keep_accents: bool = False) -> str:
    if s is None or pd.isna(s): return ""
    t = str(s)
    t = CONTROL_CHARS_RE.sub(" ", t)
    t = normalize_spaces(t)
    t = t.strip().strip('"').strip("'").strip()
    t = t.rstrip(".").strip()
    t = fix_mojibake_basic(t)
    t = t.replace("’", "'").replace("`", "'")
    if not keep_accents: t = strip_accents(t)
    return normalize_spaces(t)

def clean_round(s: object) -> str:
    if s is None or pd.isna(s): return ""
    t = str(s).strip().lower()
    t = strip_accents(t)
    t = re.sub(r"[_\-]+", " ", t)
    t = re.sub(r"\s+", " ", t)

    if t.startswith("group"): return "Group"
    if t.startswith("preliminary"): return "Preliminary round"

    ROUND_MAP = {
        "1/8 final": "Round of 16", "round of 16": "Round of 16",
        "1/4 final": "Quarter-finals", "quarterfinal stage": "Quarter-finals", 
        "quarter final": "Quarter-finals", "quarter finals": "Quarter-finals",
        "1/2 final": "Semi-finals", "semifinal stage": "Semi-finals", 
        "semi final": "Semi-finals", "semi finals": "Semi-finals",
        "places 3&4": "Match for third place", "play off for third place": "Match for third place",
        "third place play off": "Match for third place", "match for third place": "Match for third place",
        "final": "Final", "final round": "Final", "first": "Final",
    }
    return ROUND_MAP.get(t, t.title()).strip()

def clean_team_raw(x: object) -> str:
    """Nettoyage ROBUSTE d'un nom d'équipe brute."""
    if x is None or pd.isna(x): return ""
    t = str(x).strip()

    # 1. Vérification AVANT nettoyage (pour les cas exacts)
    if t in MANUAL_CORRECTIONS:
        return MANUAL_CORRECTIONS[t]

    # Nettoyage regex (suppression guillemets, espaces, etc.)
    t = CONTROL_CHARS_RE.sub(" ", t)
    t = normalize_spaces(t)
    t = t.strip('"').strip("'").strip()
    t = fix_mojibake_basic(t)
    t = LEADING_SCORE_TEAM_RE.sub("", t).strip()
    t = LEADING_GARBAGE_RE.sub("", t).strip()
    t = TRAILING_ID_RE.sub("", t).strip()
    t = t.lstrip(">").strip()
    t = t.replace("’", "'").replace("`", "'")

    m = PAREN_RE.search(t)
    if m: t = PAREN_RE.sub("", t).strip()

    t = normalize_spaces(t)

    # 2. Vérification APRÈS nettoyage (C'EST ICI LA CORRECTION CRITIQUE)
    # Si après avoir enlevé les guillemets on trouve "Cte d'Ivoire", on le corrige.
    if t in MANUAL_CORRECTIONS:
        return MANUAL_CORRECTIONS[t]

    # 3. Filet de sécurité ultime (pattern matching)
    if "Cte d" in t and "Ivoire" in t:
        return "Cote d Ivoire"

    # Vérifs finales
    if t.upper() == "A": return ""
    if GROUP_LABEL_RE.fullmatch(t.replace(" ", "")): return ""
    if GROUP_WORD_RE.fullmatch(t): return ""
    if len(t) < 2 or not any(ch.isalpha() for ch in t): return ""

    return t

def alias_key(s: str) -> str:
    s = strip_accents(s).lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def extract_year_from_edition_label(edition: object) -> pd.Series:
    ser = pd.Series(edition, dtype="string")
    year = ser.str.extract(r"(\d{4})")[0]
    return pd.to_numeric(year, errors="coerce").astype("Int64")

def is_placeholder_date(s: object) -> bool:
    if s is None or pd.isna(s): return False
    return bool(PLACEHOLDER_DATE_RE.match(str(s).strip()))

def compute_result(row: pd.Series) -> str:
    home_name = row.get("home_team_canonical") if pd.notna(row.get("home_team_canonical")) else row["home_team_raw"]
    away_name = row.get("away_team_canonical") if pd.notna(row.get("away_team_canonical")) else row["away_team_raw"]
    hg, ag = row["home_result"], row["away_result"]
    if pd.isna(hg) or pd.isna(ag): return "draw"
    if hg > ag: return home_name
    if hg < ag: return away_name
    return "draw"

def build_country_resolver():
    try:
        import pycountry 
    except Exception:
        pycountry = None

    overrides = {
        "usa": ("United States", "US", "USA"),
        "unitedstates": ("United States", "US", "USA"),
        "iriran": ("Iran", "IR", "IRN"),
        "iran": ("Iran", "IR", "IRN"),
        "korearepublic": ("South Korea", "KR", "KOR"),
        "southkorea": ("South Korea", "KR", "KOR"),
        "koreadpr": ("North Korea", "KP", "PRK"),
        "northkorea": ("North Korea", "KP", "PRK"),
        "russia": ("Russia", "RU", "RUS"),
        
        # Override pour la Côte d'Ivoire
        "cotedivoire": ("Cote d Ivoire", "CI", "CIV"),
        "ivorycoast": ("Cote d Ivoire", "CI", "CIV"),
        
        "drcongo": ("Democratic Republic of the Congo", "CD", "COD"),
        "democraticrepublicofthecongo": ("Democratic Republic of the Congo", "CD", "COD"),
        "zaire": ("Democratic Republic of the Congo", "CD", "COD"),
        "congo": ("Congo", "CG", "COG"),
        "china": ("China", "CN", "CHN"),
        "chinapr": ("China", "CN", "CHN"),
        "czechoslovakia": ("Czechoslovakia", None, None),
        "yugoslavia": ("Yugoslavia", None, None),
        "ussr": ("USSR", None, None),
        "westgermany": ("Germany", "DE", "DEU"),
        "eastgermany": ("Germany", "DE", "DEU"),
        "serbiaandmontenegro": ("Serbia", "RS", "SRB"),
    }

    def resolve(name: str):
        if not name: return ("", None, None)
        k = alias_key(name)
        if k in overrides: return overrides[k]
        if pycountry is None: return (strip_accents(name), None, None)
        try:
            q = strip_accents(name)
            c = pycountry.countries.search_fuzzy(q)[0]
            canonical = getattr(c, "common_name", None) or c.name
            return (canonical, getattr(c, "alpha_2", None), getattr(c, "alpha_3", None))
        except Exception:
            return (strip_accents(name), None, None)
    return resolve


# =============================================================================
# 4. MAIN SCRIPT
# =============================================================================
def main() -> None:
    print(f"Chargement V2 depuis {IN_V2}")
    if not IN_V2.exists():
        print("ERREUR : Le fichier matches_unified_v2.csv n'existe pas !")
        return

    df = pd.read_csv(IN_V2)

    required = ["home_team", "away_team", "home_result", "away_result", "date", "round", "city", "edition"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes dans V2: {missing}")

    before = len(df)

    # 1. APPLICATION DES HARD FIXES
    print("Application des correctifs manuels (Force 'Cote d Ivoire')...")
    for col in ["home_team", "away_team", "result"]:
        if col in df.columns:
            # On remplace par le dictionnaire
            df[col] = df[col].replace(MANUAL_CORRECTIONS)
            df[col] = df[col].astype(str).str.strip().replace(MANUAL_CORRECTIONS)

    # 2. FILTRE ANTI-GARBAGE
    print("Filtrage des lignes fantômes (Winner X, Loser Y)...")
    mask_garbage = df["home_team"].apply(is_garbage_row) | df["away_team"].apply(is_garbage_row)
    n_garbage = mask_garbage.sum()
    df = df[~mask_garbage].copy()
    print(f"-> {n_garbage} lignes supprimées.")

    # 3. NETTOYAGE STANDARD
    df["home_result"] = pd.to_numeric(df["home_result"], errors="coerce")
    df["away_result"] = pd.to_numeric(df["away_result"], errors="coerce")
    df["round"] = df["round"].map(clean_round)
    df["city"] = df["city"].map(lambda x: clean_name(x, keep_accents=False))
    df["edition"] = df["edition"].astype(str)
    df["edition_year"] = extract_year_from_edition_label(df["edition"])

    df["home_team_raw"] = df["home_team"].astype(str)
    df["away_team_raw"] = df["away_team"].astype(str)
    
    # C'est ici que la fonction corrigée clean_team_raw est appelée
    df["home_team_clean"] = df["home_team_raw"].map(clean_team_raw)
    df["away_team_clean"] = df["away_team_raw"].map(clean_team_raw)

    invalid_team = (df["home_team_clean"] == "") | (df["away_team_clean"] == "")
    df = df.loc[~invalid_team].copy()
    ghost = df["date"].isna() & df["home_result"].isna() & df["away_result"].isna()
    df = df.loc[~ghost].copy()
    df["is_placeholder_date"] = df["date"].astype(str).map(is_placeholder_date)

    # 4. CONSTRUCTION DIM_TEAMS
    resolve = build_country_resolver()
    raw_teams = pd.concat([df["home_team_raw"], df["away_team_raw"]]).dropna().astype(str).unique().tolist()

    alias_rows = []
    for raw in raw_teams:
        clean = clean_team_raw(raw)
        if not clean: continue
        alias_rows.append({"team_raw": raw, "team_clean": clean, "alias_key": alias_key(clean)})

    aliases_df = pd.DataFrame(alias_rows).drop_duplicates(subset=["team_raw"]).reset_index(drop=True)

    tmp_dim_rows = []
    for clean in sorted(aliases_df["team_clean"].dropna().unique()):
        canonical, iso2, iso3 = resolve(clean)
        
        # FORCE FINALE : Si le resolveur nous redonne une ancienne version
        if "Cote d" in canonical or "Côte d" in canonical:
             canonical = "Cote d Ivoire"
        
        canonical_key = alias_key(canonical) if canonical else alias_key(clean)
        tmp_dim_rows.append({
            "team_canonical": canonical, "team_clean_example": clean,
            "canonical_key": canonical_key, "iso2": iso2, "iso3": iso3,
        })

    tmp_dim = pd.DataFrame(tmp_dim_rows)
    tmp_dim["_iso_score"] = tmp_dim["iso3"].notna().astype(int) + tmp_dim["iso2"].notna().astype(int)
    
    dim = (
        tmp_dim.sort_values(["canonical_key", "_iso_score", "team_canonical"], ascending=[True, False, True])
        .drop_duplicates(subset=["canonical_key"], keep="first")
        .drop(columns=["_iso_score"])
        .sort_values("team_canonical").reset_index(drop=True)
    )
    dim["team_id"] = range(1, len(dim) + 1)

    clean_to_canonical_key = {row["team_clean_example"]: row["canonical_key"] for _, row in tmp_dim.iterrows()}
    aliases_df["canonical_key"] = aliases_df["team_clean"].map(clean_to_canonical_key)
    canonical_key_to_id = dict(zip(dim["canonical_key"], dim["team_id"]))
    aliases_df["team_id"] = aliases_df["canonical_key"].map(canonical_key_to_id)

    unknown = aliases_df.loc[aliases_df["team_id"].isna(), ["team_raw", "team_clean", "alias_key"]].copy()
    
    qa = (
        aliases_df.groupby(["team_id"], dropna=False)
        .agg(n_variants=("team_raw", "nunique"), variants=("team_clean", lambda s: ", ".join(sorted(set(s))[:30])))
        .reset_index().sort_values("n_variants", ascending=False)
    )

    # 5. MAPPING FINAL & EXPORT
    raw_to_id = dict(zip(aliases_df["team_raw"], aliases_df["team_id"]))
    id_to_canonical = dict(zip(dim["team_id"], dim["team_canonical"]))

    df["home_team_id"] = df["home_team_raw"].map(raw_to_id)
    df["away_team_id"] = df["away_team_raw"].map(raw_to_id)
    df["home_team_canonical"] = df["home_team_id"].map(id_to_canonical)
    df["away_team_canonical"] = df["away_team_id"].map(id_to_canonical)

    df = df.loc[df["home_team_id"].notna() & df["away_team_id"].notna()].copy()

    match_key = ["edition_year", "date", "home_team_id", "away_team_id", "home_result", "away_result", "round"]
    df = df.drop_duplicates(subset=match_key, keep="first").copy()
    df = df.reset_index(drop=True)
    df["id_match"] = range(1, len(df) + 1)
    df["result"] = df.apply(compute_result, axis=1)

    after = len(df)

    dim.to_csv(OUT_DIM, index=False, encoding="utf-8")
    aliases_df.to_csv(OUT_ALIASES, index=False, encoding="utf-8")
    unknown.to_csv(OUT_UNKNOWN, index=False, encoding="utf-8")
    qa.to_csv(OUT_QA, index=False, encoding="utf-8")

    cols_out = [
        "id_match", "edition", "edition_year", "date", "round", "city",
        "home_team_id", "away_team_id", "home_team_canonical", "away_team_canonical",
        "home_result", "away_result", "result", "is_placeholder_date"
    ]
    cols_out = [c for c in cols_out if c in df.columns]
    df[cols_out].to_csv(OUT_V3, index=False, encoding="utf-8")

    print(f"\n[SUCCÈS] CSV Clean généré : {OUT_V3}")
    print(f"Lignes avant nettoyage: {before}")
    print(f"Lignes après nettoyage: {after}")
    print(f"Pays uniques: {len(dim)}")

if __name__ == "__main__":
    main()