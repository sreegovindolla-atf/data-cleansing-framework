import pickle
from pathlib import Path

PKL_PATH = Path("C:\\Users\\SREE\\data-cleansing-framework\\data\\outputs\\full-run\\full-run_lx_cache.pkl")
DOC_ID_TO_DELETE = "doc_787fe011"

with PKL_PATH.open("rb") as f:
    cache = pickle.load(f)

keys_to_delete = []
for k, doc in cache.items():
    doc_id = getattr(doc, "_document_id", None) or getattr(doc, "document_id", None)
    if doc_id == DOC_ID_TO_DELETE:
        keys_to_delete.append(k)

for k in keys_to_delete:
    del cache[k]

with PKL_PATH.open("wb") as f:
    pickle.dump(cache, f)

print(f"Deleted {len(keys_to_delete)} cached entries for document_id={DOC_ID_TO_DELETE}")

