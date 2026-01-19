import pickle
from pathlib import Path

PKL_PATH = Path("C:\\Users\\SREE\\data-cleansing-framework\\data\\outputs\\full-run\\full-run_lx_cache.pkl")

with PKL_PATH.open("rb") as f:
    cache = pickle.load(f)

print("Cache entries:", len(cache))

# Peek one object to see what fields exist
any_doc = next(iter(cache.values()))
print("Doc type:", type(any_doc))
print("Has text:", hasattr(any_doc, "text"))
print("Has _document_id:", hasattr(any_doc, "_document_id"))
print("Has extractions:", hasattr(any_doc, "extractions"))

# Print first few extraction classes
if hasattr(any_doc, "extractions"):
    print([getattr(e, "extraction_text", None) for e in any_doc.extractions[:5]])
