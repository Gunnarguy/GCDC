import re
import pandas as pd
df1 = pd.read_csv("data/raw/namuwiki_heroes.csv")
pattern = re.compile(r"stack|overlap|invalid|duplicat|simultaneous", re.IGNORECASE)
for col in df1.columns:
    if df1[col].dtype == 'object':
        matches = df1[df1[col].str.contains(pattern, na=False)]
        for _, row in matches.head(10).iterrows():
            print(f"[{col}] {row[col]}")
