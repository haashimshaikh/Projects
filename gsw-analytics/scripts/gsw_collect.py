from dotenv import load_dotenv
load_dotenv()

import os, time
from typing import List
import pandas as pd
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from requests_cache import install_cache

# ---- Caching: patch requests globally so nba_api uses it ----
# Cache for 24h; change to 3600 (1h) if you prefer
install_cache("nba_cache", expire_after=86400)

# ---- Config ----
SEASON = os.getenv("SEASON", "2024-25")
TEAM_ABBR = os.getenv("TEAM_ABBR", "GSW")

def get_team_id(team_abbr: str) -> int:
    all_teams = teams.get_teams()
    for t in all_teams:
        if t["abbreviation"].upper() == team_abbr.upper():
            return t["id"]
    raise ValueError(f"Team {team_abbr} not found")

def get_games_for_team(team_id: int, season: str) -> pd.DataFrame:
    # Pylance wants str for team_id_nullable; API accepts it
    res = leaguegamefinder.LeagueGameFinder(team_id_nullable=str(team_id), timeout=90)
    df = res.get_data_frames()[0]

    # Normalize season like '22024' -> '2024-25'
    if "SEASON_ID" in df.columns:
        df["SEASON_DISPLAY"] = df["SEASON_ID"].astype(str).apply(
            lambda s: f"{int(s[-4:])}-{str(int(s[-4:]) + 1)[-2:]}" if s.isdigit() else s
        )
        df = df[df["SEASON_DISPLAY"] == season]

    return df.sort_values("GAME_DATE")

def fetch_boxscore_safe(game_id: str, retries: int = 5, base_sleep: float = 1.2) -> pd.DataFrame:
    """
    Robust boxscore fetch: longer timeout, retry with backoff, polite delay.
    Returns a DataFrame (may be empty if the endpoint returns none).
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            # Slight delay to avoid rate limits
            time.sleep(base_sleep)
            bs = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=90)
            frames = bs.get_data_frames()
            if not frames:
                return pd.DataFrame()
            part = frames[0]
            part["GAME_ID"] = game_id
            return part
        except Exception as e:
            last_err = e
            # Exponential backoff
            time.sleep(base_sleep * attempt * 1.5)
    # If still failing after retries, raise so we can see which GAME_ID caused it
    raise RuntimeError(f"Failed to fetch boxscore for GAME_ID={game_id}") from last_err

def get_boxscores(game_ids: list[str]) -> pd.DataFrame:
    parts = []
    for i, gid in enumerate(game_ids, 1):
        df_part = fetch_boxscore_safe(gid)
        if df_part is not None and not df_part.empty:
            parts.append(df_part)
        # Progress in console
        if i % 10 == 0:
            print(f"Fetched {i}/{len(game_ids)} boxscores…")
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

def main():
    os.makedirs("data", exist_ok=True)

    team_id = get_team_id(TEAM_ABBR)
    games = get_games_for_team(team_id, SEASON)
    games.to_csv("data/gsw_games.csv", index=False)
    print(f"Saved {len(games)} games to data/gsw_games.csv")

    # Resume-safe: skip GAME_IDs we already pulled into data/gsw_boxscores.csv
    game_ids = games["GAME_ID"].astype(str).unique().tolist()
    out_path = "data/gsw_boxscores.csv"

    already = set()
    if os.path.exists(out_path):
        old = pd.read_csv(out_path, dtype={"GAME_ID": str})
        if not old.empty:
            already = set(old["GAME_ID"].unique())
            print(f"Resuming: found {len(already)} boxscores already on disk")

    remaining = [g for g in game_ids if g not in already]
    print(f"Fetching {len(remaining)} remaining boxscores…")

    if remaining:
        new_box = get_boxscores(remaining)
        if os.path.exists(out_path) and len(already) > 0:
            # Append and deduplicate
            merged = pd.concat([pd.read_csv(out_path, dtype={"GAME_ID": str}), new_box], ignore_index=True)
            merged = merged.drop_duplicates(subset=["GAME_ID", "PLAYER_ID"], keep="last")
            merged.to_csv(out_path, index=False)
        else:
            new_box.to_csv(out_path, index=False)

    box = pd.read_csv(out_path, dtype={"GAME_ID": str})
    print(f"Saved {len(box)} boxscore rows to {out_path}")

if __name__ == "__main__":
    main()
