"""
Load FIFA World Cup cleaned CSVs into PostgreSQL (Render)

Tables loaded:
- dim_teams         (from dim_teams.csv)
- fact_matches      (from matches_final_kpi.csv)
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Text, Date, Boolean


# -------------------------------------------------
# PATHS
# -------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data" / "clean"

TEAMS_FILE = CLEAN / "dim_teams.csv"
MATCHES_FILE = CLEAN / "matches_final_kpi.csv"


# -------------------------------------------------
# LOAD ENV
# -------------------------------------------------
load_dotenv()

DATABASE_URL = os.getenv("RENDER_DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("RENDER_DATABASE_URL not found in .env")


# -------------------------------------------------
# ENGINE
# -------------------------------------------------
engine = create_engine(DATABASE_URL, future=True)


# -------------------------------------------------
# MAIN LOAD
# -------------------------------------------------
def main() -> None:
    print("📦 Connexion à la base Render…")

    df_teams = pd.read_csv(TEAMS_FILE)
    df_matches = pd.read_csv(MATCHES_FILE)

    with engine.begin() as conn:
        # Schema dédié (propre pour l'exo)
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS worldcup_khalid"))

        # -----------------------------
        # DIM_TEAMS
        # -----------------------------
        df_teams.to_sql(
            "dim_teams",
            conn,
            schema="worldcup_khalid",
            if_exists="replace",
            index=False,
            dtype={
                "team_id": Integer(),
                "team": Text(),
            },
        )

        # -----------------------------
        # FACT_MATCHES
        # -----------------------------
        df_matches.to_sql(
            "fact_matches",
            conn,
            schema="worldcup_khalid",
            if_exists="replace",
            index=False,
            dtype={
                "match_id": Integer(),
                "edition": Text(),
                "match_date": Date(),
                "home_team": Text(),
                "away_team": Text(),
                "home_score": Integer(),
                "away_score": Integer(),
                "round": Text(),
                "is_final": Boolean(),
            },
        )

    print("✅ Chargement terminé avec succès")
    print("- worldcup_khalid.dim_teams :", len(df_teams), "lignes")
    print("- worldcup_khalid.fact_matches :", len(df_matches), "lignes")


if __name__ == "__main__":
    main()
