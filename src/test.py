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

IN_MATCHES = DATA / "processed" / "matches_unified_v2.csv"

OUT_FACT = DATA / "processed" / "matches_unified_v3_ref.csv"
OUT_DIM = DATA / "reference" / "dim_teams.csv"
OUT_ALIASES = DATA / "reference" / "team_aliases.csv"
OUT_UNKNOWN = DATA / "reference" / "unknown_teams.csv"
OUT_QA = DATA / "reference" / "qa_team_collisions.csv"

OUT_DIM.parent.mkdir(parents=True, exist_ok=True)
OUT_FACT.parent.mkdir(parents=True, exist_ok=True)


# -----------------------
# Normalization helpers
# -----------------------
CONTROL_CHARS_RE = re.compile(r"[\u0000-\u001F\u007F-\u009F]+")
PAREN_RE = re.compile(r"\s*\(([^)]*)\)\s*")

# Cas genre ""rn"">Bosnia and Herzegovina",266
LEADING_GARBAGE_RE = re.compile(r'^\s*["\']*(?:\\r\\n|\\n|\\r|rn|r?n)?["\']*\s*>\s*', re.IGNORECASE)
TRAILING_ID_RE = re.compile(r'"\s*,\s*\d+\s*$|,\s*\d+\s*$')

# Faux teams type A1..H2
GROUP_LABEL_RE = re.compile(r"^[A-H]\s*[12]$", re.IGNORECASE)  # A1 A2 ... H1 H2
# Optionnel: élimine aussi "Group A", "Group B" si jamais ça apparaît
GROUP_WORD_RE = re.compile(r"^group\s*[a-h]$", re.IGNORECASE)


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def fix_mojibake_basic(s: str) -> str:
    """
    Fix simple des cas comme 'C�te' / 'CÃ´te'.
    - supprime le caractère de remplacement �
    - tentative latin1->utf8 si ça ressemble à du mojibake
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


def clean_team_raw(x: object) -> str:
    """
    Nettoyage "léger mais robuste" :
    - enlève artefacts CSV, ids, guillemets, espaces
    - retire parenthèses (langues locales, etc.)
    - enlève groupes A1..H2 + 'A'
    - ne supprime PAS les accents ici (on le fait dans alias_key)
    """
    if x is None or pd.isna(x):
        return ""

    t = str(x)
    t = CONTROL_CHARS_RE.sub(" ", t)
    t = normalize_spaces(t)
    t = t.strip().strip('"').strip("'").strip()

    t = fix_mojibake_basic(t)

    # retire préfixes cassés
    t = LEADING_GARBAGE_RE.sub("", t).strip()
    t = TRAILING_ID_RE.sub("", t).strip()
    t = t.lstrip(">").strip()

    # apostrophes typographiques
    t = t.replace("’", "'").replace("`", "'")

    # parenthèses : si non-latin, on garde la partie "anglais"
    m = PAREN_RE.search(t)
    if m:
        inside = m.group(1)
        if contains_non_latin(inside) or True:
            t = PAREN_RE.sub("", t).strip()

    t = normalize_spaces(t)

    # élimine artefacts connus
    if t.upper() == "A":
        return ""
    if GROUP_LABEL_RE.fullmatch(t.replace(" ", "")):  # "A1", "A 1" -> group
        return ""
    if GROUP_WORD_RE.fullmatch(t):
        return ""

    # trop court / pas de lettres
    if len(t) < 2 or not any(ch.isalpha() for ch in t):
        return ""

    return t


def alias_key(s: str) -> str:
    """
    Clé stable pour matcher (agressive) :
    - minuscules
    - sans accents
    - alphanum only
    """
    s = strip_accents(s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


# -----------------------
# Country resolver
# -----------------------
def build_country_resolver():
    """
    Retourne resolve(name)->(canonical_name, iso2, iso3)
    - Overrides pour les cas football/historiques
    - pycountry si dispo (fuzzy)
    """
    try:
        import pycountry  # pip install pycountry
    except Exception:
        pycountry = None

    overrides = {
        # alias fréquents
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
        # historiques / pas dans iso moderne
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
            # fallback: conserve le nom nettoyé (mais pas canonique ISO)
            return (strip_accents(name), None, None)

        try:
            # on tente fuzzy sur version sans accents
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
def main():
    df = pd.read_csv(IN_MATCHES)

    required_cols = {"home_team", "away_team"}
    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing columns in matches file: {missing}")

    # 1) Extract raw teams
    raw_teams = (
        pd.concat([df["home_team"], df["away_team"]])
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    # 2) Clean raw -> team_clean
    alias_rows = []
    for raw in raw_teams:
        clean = clean_team_raw(raw)
        if not clean:
            continue
        alias_rows.append(
            {
                "team_raw": raw,
                "team_clean": clean,
                "alias_key": alias_key(clean),
            }
        )

    aliases_df = pd.DataFrame(alias_rows).drop_duplicates(subset=["team_raw"]).reset_index(drop=True)

    # 3) Resolve canonical, then build dim with 1 row per canonical_key
    resolve = build_country_resolver()

    tmp_dim_rows = []
    for clean in sorted(aliases_df["team_clean"].dropna().unique()):
        canonical, iso2, iso3 = resolve(clean)
        canonical_key = alias_key(canonical) if canonical else alias_key(clean)
        tmp_dim_rows.append(
            {
                "team_canonical": canonical,
                "team_clean_example": clean,  # pour debug/traçabilité
                "canonical_key": canonical_key,
                "iso2": iso2,
                "iso3": iso3,
            }
        )

    tmp_dim = pd.DataFrame(tmp_dim_rows)

    # dédoublonne PAR canonical_key => 1 seule ligne par équipe canonique
    # garde la ligne la plus "riche" (celle qui a iso3 si dispo)
    tmp_dim["_iso_score"] = tmp_dim["iso3"].notna().astype(int) + tmp_dim["iso2"].notna().astype(int)
    dim = (
        tmp_dim.sort_values(["canonical_key", "_iso_score", "team_canonical"], ascending=[True, False, True])
        .drop_duplicates(subset=["canonical_key"], keep="first")
        .drop(columns=["_iso_score"])
        .sort_values("team_canonical")
        .reset_index(drop=True)
    )
    dim["team_id"] = range(1, len(dim) + 1)

    # 4) Build aliases table: map alias_key -> canonical_key -> team_id
    # On associe chaque alias_key (issu de team_clean) au canonical_key (issu du resolver)
    clean_to_canonical_key = {
        row["team_clean_example"]: row["canonical_key"] for _, row in tmp_dim.iterrows()
    }
    aliases_df["canonical_key"] = aliases_df["team_clean"].map(clean_to_canonical_key)

    canonical_key_to_id = dict(zip(dim["canonical_key"], dim["team_id"]))
    aliases_df["team_id"] = aliases_df["canonical_key"].map(canonical_key_to_id)

    # Unknown list (problèmes restants)
    unknown = aliases_df.loc[aliases_df["team_id"].isna(), ["team_raw", "team_clean", "alias_key"]].copy()
    unknown = unknown.sort_values(["team_clean", "team_raw"]).reset_index(drop=True)

    # QA collisions: plusieurs team_clean -> même canonical (normal), mais on veut le voir
    qa = (
        aliases_df.groupby(["team_id"], dropna=False)
        .agg(
            n_variants=("team_raw", "nunique"),
            variants=("team_clean", lambda s: ", ".join(sorted(set(s))[:30])),
        )
        .reset_index()
        .sort_values("n_variants", ascending=False)
    )

    # 5) Apply mapping to matches (RAW -> team_id)
    raw_to_id = dict(zip(aliases_df["team_raw"], aliases_df["team_id"]))

    df["home_team_id"] = df["home_team"].map(raw_to_id)
    df["away_team_id"] = df["away_team"].map(raw_to_id)

    id_to_canonical = dict(zip(dim["team_id"], dim["team_canonical"]))
    df["home_team_canonical"] = df["home_team_id"].map(id_to_canonical)
    df["away_team_canonical"] = df["away_team_id"].map(id_to_canonical)

    # 6) Remove invalid lines (team_id missing)
    before = len(df)
    df = df.loc[df["home_team_id"].notna() & df["away_team_id"].notna()].copy()
    after = len(df)

    # 7) Save outputs
    dim.to_csv(OUT_DIM, index=False, encoding="utf-8")
    # aliases final: garde colonnes utiles
    aliases_final = aliases_df[["team_raw", "team_clean", "alias_key", "team_id"]].copy()
    aliases_final.to_csv(OUT_ALIASES, index=False, encoding="utf-8")
    unknown.to_csv(OUT_UNKNOWN, index=False, encoding="utf-8")
    qa.to_csv(OUT_QA, index=False, encoding="utf-8")

    df.to_csv(OUT_FACT, index=False, encoding="utf-8")

    print("✅ dim_teams:", OUT_DIM, f"({len(dim)} rows)")
    print("✅ team_aliases:", OUT_ALIASES, f"({len(aliases_final)} rows)")
    print("⚠️ unknown_teams:", OUT_UNKNOWN, f"({len(unknown)} rows)")
    print("🧪 qa_team_collisions:", OUT_QA, f"({len(qa)} rows)")
    print("✅ matches_unified_v3_ref:", OUT_FACT)
    print(f"Rows kept: {before} -> {after} (dropped {before-after} because team_id missing)")


if __name__ == "__main__":
    main()
