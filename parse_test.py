import re
import pandas as pd

df = pd.read_csv("data/raw/namuwiki_notes.csv")

pattern = re.compile(r"does not overlap|does not stack|overwrite|not applied simultaneously", re.IGNORECASE)

matches = df[df["content"].str.contains(pattern, na=False)]
for _, row in matches.iterrows():
    print(f"[{row['source_page']}] {row['content']}")

