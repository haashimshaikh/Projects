import os
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")
OUT_DIR = DATA_DIR / "cleaned"
PER_TEAM_DIR = DATA_DIR / "teams"  # optional per-team outputs
OUT_DIR.mkdir(parents=True, exist_ok=True)
PER_TEAM_DIR.mkdir(parents=True, exist_ok=True)

def normalize_team_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Make team identifiers reliable for sorting/grouping
    if "TEAM_ABBREVIATION" in df.columns:
        df["TEAM_ABBREVIATION"] = (
            df["TEAM_ABBREVIATION"].astype(str).str.strip().str.upper()
        )
    if "TEAM_ID" in df.columns:
        df["TEAM_ID"] = pd.to_numeric(df["TEAM_ID"], errors="coerce").astype("Int64")
    return df

def load_csv(path: Path, parse_dates=None) -> pd.DataFrame:
    parse_dates = parse_dates or []
    df = pd.read_csv(path, parse_dates=parse_dates)
    return df

def save_sorted(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved: {path}")

def clean_games():
    src = DATA_DIR / "gsw_games.csv"
    if not src.exists():
        print(f"Skipping games (not found): {src}")
        return

    games = load_csv(src, parse_dates=["GAME_DATE"])
    games = normalize_team_cols(games)

    # Build robust sort keys if present
    sort_cols = []
    if "TEAM_ID" in games.columns:
        sort_cols.append("TEAM_ID")
    if "TEAM_ABBREVIATION" in games.columns:
        sort_cols.append("TEAM_ABBREVIATION")
    if "GAME_DATE" in games.columns:
        sort_cols.append("GAME_DATE")
    if "OPPONENT_TEAM_ABBREVIATION" in games.columns:
        sort_cols.append("OPPONENT_TEAM_ABBREVIATION")
    elif "MATCHUP" in games.columns:
        sort_cols.append("MATCHUP")

    if sort_cols:
        games_sorted = games.sort_values(sort_cols, kind="mergesort")  # stable sort keeps natural blocks
    else:
        games_sorted = games.copy()

    save_sorted(games_sorted, OUT_DIR / "games_sorted.csv")

    # Optional: write per-team games CSVs (only if it’s multi-team; safe if one team)
    if "TEAM_ABBREVIATION" in games_sorted.columns:
        for abbr, sub in games_sorted.groupby("TEAM_ABBREVIATION"):
            save_sorted(sub, PER_TEAM_DIR / f"{abbr}_games.csv")

def clean_boxscores():
    src = DATA_DIR / "gsw_boxscores.csv"
    if not src.exists():
        print(f"Skipping boxscores (not found): {src}")
        return

    box = load_csv(src)
    box = normalize_team_cols(box)

    # Normalize names a touch for consistent grouping (no changes to saved columns)
    # (We won't overwrite names; just build a temporary sort key)
    if "PLAYER_NAME" in box.columns:
        box["_PLAYER_NAME_KEY"] = box["PLAYER_NAME"].astype(str).str.strip().str.upper()
    else:
        box["_PLAYER_NAME_KEY"] = ""

    # Also sort consistently by game id/date if available
    # If GAME_DATE isn't in the box file, this still works fine
    if "GAME_ID" in box.columns:
        box["GAME_ID"] = box["GAME_ID"].astype(str)
        box["_GID_NORM"] = box["GAME_ID"].str.lstrip("0")
    else:
        box["_GID_NORM"] = ""

    # Preferred sort order for grouping teammates together and keeping seasons coherent
    sort_cols = []
    if "TEAM_ID" in box.columns:
        sort_cols.append("TEAM_ID")
    if "TEAM_ABBREVIATION" in box.columns:
        sort_cols.append("TEAM_ABBREVIATION")
    if "_PLAYER_NAME_KEY" in box.columns:
        sort_cols.append("_PLAYER_NAME_KEY")
    # keep same game blocks together
    sort_cols.append("_GID_NORM")

    box_sorted = box.sort_values(sort_cols, kind="mergesort").drop(columns=["_PLAYER_NAME_KEY", "_GID_NORM"], errors="ignore")

    save_sorted(box_sorted, OUT_DIR / "boxscores_sorted.csv")

    # Optional: write per-team boxscores CSVs
    if "TEAM_ABBREVIATION" in box_sorted.columns:
        for abbr, sub in box_sorted.groupby("TEAM_ABBREVIATION"):
            save_sorted(sub, PER_TEAM_DIR / f"{abbr}_boxscores.csv")

if __name__ == "__main__":
    clean_games()
    clean_boxscores()
    print("\nDone.")