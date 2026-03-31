import re
import pandas as pd

try:
    df1 = pd.read_csv("data/raw/namuwiki_heroes.csv")
    df2 = pd.read_csv("data/raw/fandom_skills.csv")
except Exception as e:
    print(e)
    exit(1)

pattern = re.compile(r"does not overlap|does not stack|overwrite|not applied simultaneously", re.IGNORECASE)

for df, title in [(df1, 'namuwiki_heroes.csv'), (df2, 'fandom_skills.csv')]:
    print(f"--- {title} ---")
    for col in df.columns:
        if df[col].dtype == 'object':
            matches = df[df[col].str.contains(pattern, na=False)]
            if len(matches) > 0:
                print(f"Column {col} has {len(matches)} matches")
                for _, row in matches.head(5).iterrows():
                    print(f"   Excerpt: {row[col]}")
