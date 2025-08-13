import pandas as pd

games = pd.read_csv("data/gsw_games.csv", parse_dates=["GAME_DATE"])
box = pd.read_csv("data/gsw_boxscores.csv")

# 1) Home vs Away Performance
home = games[games["MATCHUP"].str.contains(" vs. ")]
away = games[games["MATCHUP"].str.contains(" @ ")]

print("Home Games:", len(home), "Away Games:", len(away))

# 2) Curry games with >= 5 made threes
curry5 = box[(box["PLAYER_NAME"] == "Stephen Curry") & (box["FG3M"] >= 5)]
print("Curry games with >=5 threes:", curry5["GAME_ID"].nunique())
curry5.to_csv("data/curry_5plus_threes.csv", index=False)

# 3) Minutes Filter: bench players with 15-24 minutes
bench = box[(box["START_POSITION"].isna()) & (box["MINUTES"].between(15,24, inclusive= "both"))] \
    if "MINUTES" in box.columns else box[box["START_POSITION"].isna()]
bench.to_csv("data/bench_15to24.csv", index=False)
print("Saved filtered files in data/")