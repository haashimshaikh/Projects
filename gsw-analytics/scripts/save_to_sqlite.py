import os
import sqlite3
import pandas as pd

os.makedirs("data", exist_ok=True)

# Load your CSVs
games = pd.read_csv("data/gsw_games.csv", parse_dates=["GAME_DATE"])
box = pd.read_csv("data/gsw_boxscores.csv", dtype={"GAME_ID": str})

# Optional: Clean minutes to numeric for easier queries later
def mmss_to_min(s):
    try:
        m, s = str(s).split(":")
        return int(m) + int(s) / 60
    except Exception:
        return 0.0
    
box["MIN_float"] = box["MIN"].apply(mmss_to_min)

#Create / open SQLite database

con = sqlite3.connect("data/gsw.db")

#Write Tables
games.to_sql("games", con, if_exists="replace", index=False)
box.to_sql("boxscores", con, if_exists="replace", index=False)

#QOL simple index for faster queries
with con:
    con.execute("CREATE INDEX IF NOT EXISTS idx_box_game ON boxscores (GAME_ID);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_box_player ON boxscores (PLAYER_NAME);")
    
con.close()

print("Wrote data/g 'gsw.db' with tables: games, boxscores")