# scripts/common_center_utils.py
import os
from typing import Set, Dict, Any, Tuple, Optional
import pandas as pd

# ------------------------
# Constants (can be overridden via env)
# ------------------------
GSW_TEAM_ID_DEFAULT = 1610612744  # Golden State Warriors
EXCLUDE_SMALL_BALL_DEFAULT: Set[str] = {"DRAYMOND GREEN"}
INCLUDE_TRADITIONAL_DEFAULT: Set[str] = {"KEVON LOONEY", "TRAYCE JACKSON-DAVIS"}  # add more if needed

# ------------------------
# Helpers
# ------------------------
def _safe_numeric_series(series_like, length: int, index) -> pd.Series:
    """Return a Series (never None) suitable for pd.to_numeric."""
    if isinstance(series_like, pd.Series):
        return series_like
    # fallback: Series of NA (float dtype to allow to_numeric)
    return pd.Series([pd.NA] * length, index=index, dtype="float64")

def normalize_team_id(box: pd.DataFrame) -> pd.DataFrame:
    s = _safe_numeric_series(box.get("TEAM_ID"), len(box), box.index)
    box["TEAM_ID"] = pd.to_numeric(s, errors="coerce").astype("Int64")
    return box

def add_pts_opp_if_missing(games: pd.DataFrame) -> pd.DataFrame:
    if "PTS_OPP" not in games.columns and "PLUS_MINUS" in games.columns:
        games["PTS_OPP"] = games["PTS"] - games["PLUS_MINUS"]
    elif "PTS_OPP" not in games.columns:
        games["PTS_OPP"] = pd.NA
    return games

def normalize_game_ids(games: pd.DataFrame, box: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    games["GAME_ID"] = games["GAME_ID"].astype(str)
    box["GAME_ID"]   = box["GAME_ID"].astype(str)
    games["GID_NORM"] = games["GAME_ID"].str.lstrip("0")
    box["GID_NORM"]   = box["GAME_ID"].str.lstrip("0")
    return games, box

def filter_regular_season_and_playoffs(games: pd.DataFrame) -> pd.DataFrame:
    # Prefer explicit SEASON_TYPE if present
    if "SEASON_TYPE" in games.columns:
        mask = games["SEASON_TYPE"].isin(["Regular Season", "Playoffs"])
        return games.loc[mask].copy()
    # Fallback by GAME_ID prefix (use normalized)
    mask = games["GID_NORM"].str.startswith(("2", "4"))  # 2=RS, 4=PO
    return games.loc[mask].copy()

def ensure_start_position(box: pd.DataFrame) -> pd.DataFrame:
    if "START_POSITION" not in box.columns:
        box["START_POSITION"] = ""
    box["START_POSITION"] = box["START_POSITION"].fillna("").astype(str)
    return box

def build_box_gsw(box: pd.DataFrame, games: pd.DataFrame, team_id: int) -> pd.DataFrame:
    valid_gid_norm = set(games["GID_NORM"].unique())
    box_gsw = box[
        (box["TEAM_ID"] == team_id) &
        (box["GID_NORM"].isin(valid_gid_norm))
    ].copy()
    return box_gsw

def detect_c_starters(box_gsw: pd.DataFrame) -> pd.DataFrame:
    # Normalize START_POSITION and treat any slot that includes "C" as center
    pos = box_gsw["START_POSITION"].astype(str).str.upper().str.strip()
    is_center = pos.str.contains(r"\bC\b") | pos.str.startswith("C") | pos.str.endswith("C")
    starters_c = box_gsw.loc[is_center, ["GID_NORM", "PLAYER_NAME"]].copy()
    starters_c["PLAYER_NAME_NORM"] = starters_c["PLAYER_NAME"].astype(str).str.upper().str.strip()
    return starters_c

def build_allowed_traditional_set(
    starters_c: pd.DataFrame,
    exclude_small_ball: Set[str],
    include_traditional: Set[str],
) -> Set[str]:
    detected = set(starters_c["PLAYER_NAME_NORM"].unique())
    return (detected - {n.upper() for n in exclude_small_ball}) | {n.upper() for n in include_traditional}

def label_has_traditional_center(games: pd.DataFrame, starters_c: pd.DataFrame, allowed_trad: Set[str]) -> pd.Series:
    # Create a set of game IDs where an allowed name started at C
    allowed_games = set(
        starters_c.loc[starters_c["PLAYER_NAME_NORM"].isin(allowed_trad), "GID_NORM"].unique()
    )
    return games["GID_NORM"].isin(allowed_games)

def summarize_games(games: pd.DataFrame) -> pd.DataFrame:
    summary = (
        games.groupby("HAS_CENTER").agg(
            games_played=("GAME_DATE", "count"),
            wins=("WL", lambda x: (x == "W").sum()),
            avg_pts_scored=("PTS", "mean"),
            avg_pts_allowed=("PTS_OPP", "mean"),
        ).reset_index()
    )
    summary["win_rate"]  = (summary["wins"] / summary["games_played"]) * 100
    summary["net_rating"] = summary["avg_pts_scored"] - summary["avg_pts_allowed"]
    summary["label"] = summary["HAS_CENTER"].map({True: "With Traditional Center", False: "Without Traditional Center"})
    return summary

def team_eff(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {"avg_pts_scored": None, "avg_pts_allowed": None, "net_rating": None}
    avg_pts = df["PTS"].mean()
    avg_opp = df["PTS_OPP"].mean()
    return {
        "avg_pts_scored": int(round(avg_pts)),
        "avg_pts_allowed": int(round(avg_opp)),
        "net_rating": int(round(avg_pts - avg_opp)),
    }

def win_rate(df: pd.DataFrame) -> float:
    if df.empty:
        return float("nan")
    return (df["WL"] == "W").mean()

def fmt_pct(p: float) -> str:
    import pandas as _pd  # ✅ correct local alias to avoid linter noise
    return f"{int(round(p * 100))}%" if _pd.notna(p) else "N/A"

def pipeline(
    games: pd.DataFrame,
    box: pd.DataFrame,
    team_id: Optional[int] = None,
    exclude_small_ball: Optional[Set[str]] = None,
    include_traditional: Optional[Set[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Set[str]]:
    """Run the full data-prep pipeline and return (games, box_gsw, starters_c, allowed_trad)."""
    # Defaults and env overrides
    env_team = os.getenv("TEAM_ID")
    team_id_final: int = int(env_team) if env_team is not None else int(team_id or GSW_TEAM_ID_DEFAULT)

    exclude_small_ball_final: Set[str] = (exclude_small_ball or EXCLUDE_SMALL_BALL_DEFAULT)
    include_traditional_final: Set[str] = (include_traditional or INCLUDE_TRADITIONAL_DEFAULT)

    # Normalize, filter, build
    games, box = normalize_game_ids(games, box)
    games = add_pts_opp_if_missing(games)
    box = normalize_team_id(box)
    games = filter_regular_season_and_playoffs(games)
    box = ensure_start_position(box)
    box_gsw = build_box_gsw(box, games, team_id_final)

    starters_c = detect_c_starters(box_gsw)
    allowed_trad = build_allowed_traditional_set(starters_c, exclude_small_ball_final, include_traditional_final)

    # Label
    games = games.copy()
    games["HAS_CENTER"] = label_has_traditional_center(games, starters_c, allowed_trad)

    return games, box_gsw, starters_c, allowed_trad