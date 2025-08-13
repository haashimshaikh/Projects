import sqlite3
import pandas as pd

con = sqlite3.connect("data/gsw.db")

# Example 1: Top 10 GSW scorers by total points
q1 = """
SELECT PLAYER_NAME, SUM(PTS) AS total_pts
FROM boxscores
GROUP BY PLAYER_NAME
ORDER BY total_pts DESC
LIMIT 10;
"""
print(pd.read_sql(q1, con))

# Example 2: Curry games with >= 5 made threes
q2 = """
SELECT DISTINCT GAME_ID, PLAYER_NAME, FG3M, PTS
FROM boxscores
WHERE PLAYER_NAME='Stephen Curry' AND FG3M >= 5
ORDER BY GAME_ID;
"""
print(pd.read_sql(q2, con))

# Example 3: Average minutes for bench (no START_POSITION) vs starters
q3 = """
SELECT CASE WHEN START_POSITION IS NULL THEN 'Bench' ELSE 'Starter' END AS role,
       ROUND(AVG(MIN_float),2) AS avg_min
FROM boxscores
GROUP BY role;
"""
print(pd.read_sql(q3, con))

con.close()