import re
import pandas as pd
df = pd.read_sql("SELECT excerpt FROM hero_progression_rows", __import__("sqlite3").connect("data/processed/grandchase.db"))
matches = df[df["excerpt"].str.contains(r"(not stack|does not overlap|not overlap|intersect|not apply|not applied|cannot stack|cannot overlap|overwrite|override)", case=False, na=False)]
print(f"Num matches: {len(matches)}")
for _, row in matches.head(10).iterrows():
    print("MATCH:", row["excerpt"])
