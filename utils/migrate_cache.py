import pickle
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from utils.extraction_helpers import normalize_text, text_hash

OLD_PKL = Path(r"C:\Users\SREE\data-cleansing-framework\data\outputs\20251231_160609\20251231_160609_lx_cache.pkl")  # change
NEW_PKL = Path(r"C:\Users\SREE\data-cleansing-framework\data\outputs\20251231_160609\20251231_160609_lx_cache.pkl")  # change

def get_input_text(annot) -> str | None:
    """
    Try common attribute names where input text might live.
    Adjust if inspect_cache shows a different attribute.
    """
    for name in ["input_text", "text", "document", "content", "raw_text", "source_text"]:
        if hasattr(annot, name):
            val = getattr(annot, name)
            if isinstance(val, str) and val.strip():
                return val

    # Sometimes it's stored in a dict-like attribute
    if hasattr(annot, "attributes") and isinstance(getattr(annot, "attributes"), dict):
        d = getattr(annot, "attributes")
        for k in ["input_text", "text", "source_text"]:
            v = d.get(k)
            if isinstance(v, str) and v.strip():
                return v

    return None

with OLD_PKL.open("rb") as f:
    old_cache = pickle.load(f)

new_cache = {}
missing_text = 0
collisions = 0

for old_key, annot in old_cache.items():
    t = get_input_text(annot)
    if not t:
        missing_text += 1
        continue

    t_norm = normalize_text(t)
    if t_norm is None:
        missing_text += 1
        continue

    h_new = text_hash(t_norm)

    # If multiple old entries map to same new hash, keep first (they should be equivalent)
    if h_new in new_cache:
        collisions += 1
        continue

    new_cache[h_new] = annot

NEW_PKL.parent.mkdir(parents=True, exist_ok=True)
with NEW_PKL.open("wb") as f:
    pickle.dump(new_cache, f)

print("Old cache entries     :", len(old_cache))
print("New cache entries     :", len(new_cache))
print("Missing input text    :", missing_text)
print("Hash collisions merged:", collisions)
print("Wrote:", NEW_PKL)
