import pandas as pd
from scripts.common_center_utils import (
    pipeline, summarize_games, win_rate, team_eff, fmt_pct
)

# ---- Load ----
games = pd.read_csv("data/gsw_games.csv", parse_dates=["GAME_DATE"])
box   = pd.read_csv("data/gsw_boxscores.csv", dtype={"GAME_ID": str})

# ---- Run shared pipeline (TEAM_ID from env or defaults to GSW) ----
games, box_gsw, starters_c, allowed_trad = pipeline(games, box)

# ---- Splits ----
with_center = games[games["HAS_CENTER"]]
without_center = games[~games["HAS_CENTER"]]

# ---- Output ----
print("Recognized traditional centers from data:", sorted(allowed_trad) if allowed_trad else "None")
print(f"Games WITH a traditional center: {len(with_center)}")
print(f"Games WITHOUT a traditional center: {len(without_center)}")

wr_with = win_rate(with_center)
wr_without = win_rate(without_center)
print(f"Win rate WITH a traditional center: {fmt_pct(wr_with)}")
print(f"Win rate WITHOUT a traditional center: {fmt_pct(wr_without)}")

eff_with = team_eff(with_center)
eff_without = team_eff(without_center)

print("\nEfficiency WITH traditional center:")
print(f"  Avg Points Scored : {eff_with['avg_pts_scored'] if eff_with['avg_pts_scored'] is not None else 'N/A'}")
print(f"  Avg Points Allowed: {eff_with['avg_pts_allowed'] if eff_with['avg_pts_allowed'] is not None else 'N/A'}")
print(f"  Net Rating        : {eff_with['net_rating'] if eff_with['net_rating'] is not None else 'N/A'}")

print("\nEfficiency WITHOUT traditional center:")
print(f"  Avg Points Scored : {eff_without['avg_pts_scored'] if eff_without['avg_pts_scored'] is not None else 'N/A'}")
print(f"  Avg Points Allowed: {eff_without['avg_pts_allowed'] if eff_without['avg_pts_allowed'] is not None else 'N/A'}")
print(f"  Net Rating        : {eff_without['net_rating'] if eff_without['net_rating'] is not None else 'N/A'}")

# ---- Save breakdown used by notebooks/dashboards ----
out_cols = ["GAME_DATE", "MATCHUP", "WL", "PTS", "PTS_OPP", "PLUS_MINUS", "HAS_CENTER"]
games[out_cols].to_csv("data/center_impact_summary.csv", index=False)
print("\nSaved detailed breakdown to data/center_impact_summary.csv")