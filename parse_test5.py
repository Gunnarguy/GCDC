import re
text = "The effect of 'Power' does not stack with 'Strength'. Overwrites 'Buff'."
patterns = re.compile(r"([^\.]*(?:does not stack|not stack|does not overlap|not overlap|overwrite|override|cannot stack)[^\.]*\.)", re.IGNORECASE)
print(patterns.findall(text))
