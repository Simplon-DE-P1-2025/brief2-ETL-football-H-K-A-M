from pathlib import Path
import re
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "processed"
OUT.mkdir(parents=True, exist_ok=True)

CUP_TXT = RAW / "cup.txt"
FINALS_TXT = RAW / "cup_finals.txt"

# Exemple lignes :
# (1) Sun Nov/20 19:00      Qatar   0-2 (0-2)   Ecuador    @ Al Bayt Stadium, Al Khor
# (53) Mon Dec/5 18:00     Japan  1-3 pen. 1-1 a.e.t (1-1, 1-0)  Croatia  @ Al Janoub Stadium, Al Wakrah
MATCH_RE = re.compile(
    r"^\(\d+\)\s+"                       # (1)
    r"(?P<dow>\w+)\s+"                   # Sun
    r"(?P<mon>[A-Za-z]{3})/(?P<day>\d{1,2})\s+"  # Nov/20
    r"(?P<time>\d{1,2}:\d{2})\s+"
    r"(?P<rest>.+?)\s+@\s+(?P<stad_city>.+)$"
)

GROUP_HEADER_RE = re.compile(r"^Group\s+([A-H])\s*$", re.IGNORECASE)
FINALS_HEADER_RE = re.compile(
    r"^(Round of 16|Quarter-finals|Semi-finals|Match for third place|Final)\s*$",
    re.IGNORECASE
)

def parse_match_score(rest: str) -> tuple[int | None, int | None]:
    """
    Renvoie (home_goals, away_goals) pour le score du match.
    Si tirs au but, on prend le score 'a.e.t' (ex: 1-1), pas le score TAB.
    """
    # Cas tirs au but : "... 1-3 pen. 1-1 a.e.t ..."
    m = re.search(r"(\d+)\s*-\s*(\d+)\s*a\.e\.t", rest)
    if m:
        return int(m.group(1)), int(m.group(2))

    # Cas normal : "... Qatar 0-2 (0-2) Ecuador ..."
    m = re.search(r"(\d+)\s*-\s*(\d+)", rest)
    if m:
        return int(m.group(1)), int(m.group(2))

    return None, None

def extract_teams(rest: str) -> tuple[str, str]:
    """
    Extrait home_team et away_team depuis 'rest' (texte entre l'heure et '@').
    On identifie la première occurrence de score X-Y, puis on coupe autour.
    """
    # Si tirs au but, le premier score peut être le TAB; on préfère couper sur "a.e.t" si présent
    if "a.e.t" in rest:
        m = re.search(r"(.+?)\s+(\d+\s*-\s*\d+)\s+.*?a\.e\.t.*?\s+(.+)$", rest)
        if m:
            home = m.group(1).strip()
            away = m.group(3).strip()
            return home, away

    # Sinon, couper sur le premier X-Y
    m = re.search(r"(.+?)\s+(\d+\s*-\s*\d+)\s+(.+)$", rest)
    if not m:
        return rest.strip(), ""

    home = m.group(1).strip()
    away = m.group(3).strip()

    # Nettoyage léger : enlever les parenthèses de mi-temps à gauche/droite
    away = re.sub(r"\s*\(.*?\)\s*", " ", away).strip()
    return home, away

def parse_stadium_city(stad_city: str) -> tuple[str, str]:
    """
    'Al Bayt Stadium, Al Khor' -> (stadium, city)
    """
    parts = [p.strip() for p in stad_city.split(",")]
    if len(parts) >= 2:
        stadium = ", ".join(parts[:-1]).strip()
        city = parts[-1].strip().rstrip(".")
        return stadium, city
    return stad_city.strip(), ""

def parse_file(path: Path, default_round: str = "Group") -> list[dict]:
    current_round = default_round
    rows: list[dict] = []

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Détecter headers (Group A / Round of 16 / etc.)
        g = GROUP_HEADER_RE.match(line)
        if g:
            current_round = "Group"  # on garde simple (vous pouvez changer en f"Group {g.group(1)}" si vous voulez)
            continue

        f = FINALS_HEADER_RE.match(line)
        if f:
            current_round = f.group(1)
            continue

        # Match line
        m = MATCH_RE.match(line)
        if not m:
            continue

        mon = m.group("mon")
        day = int(m.group("day"))
        rest = m.group("rest").strip()
        stad_city = m.group("stad_city").strip()

        home_team, away_team = extract_teams(rest)
        home_goals, away_goals = parse_match_score(rest)

        _, city = parse_stadium_city(stad_city)

        # Date ISO (on ignore l'heure, on fixe 2022)
        # Exemple: "Nov/20" -> "2022-11-20"
        dt = pd.to_datetime(f"2022 {mon} {day}", format="%Y %b %d", errors="coerce")
        date_iso = dt.strftime("%Y-%m-%d") if pd.notna(dt) else None

        rows.append({
            "home_team": home_team,
            "away_team": away_team,
            "home_result": home_goals,
            "away_result": away_goals,
            "date": date_iso,
            "round": current_round,
            "city": city,
            "edition": "2022"
        })

    return rows

def main():
    if not CUP_TXT.exists():
        raise FileNotFoundError(f"Fichier introuvable : {CUP_TXT}")
    if not FINALS_TXT.exists():
        raise FileNotFoundError(f"Fichier introuvable : {FINALS_TXT}")

    rows = []
    rows += parse_file(CUP_TXT, default_round="Group")
    rows += parse_file(FINALS_TXT, default_round="Round of 16")

    df = pd.DataFrame(rows)

    # Check rapide
    print("2022 df shape:", df.shape)
    print("Columns:", df.columns.tolist())
    print(df.head(5))

    # Sauvegarde CSV
    out_csv = OUT / "matches_2022.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8")
    print("Saved:", out_csv)

if __name__ == "__main__":
    main()