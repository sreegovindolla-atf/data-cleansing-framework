import json
import re
import pandas as pd
from pathlib import Path
import argparse
from sqlalchemy import create_engine
import urllib

# -----------------------
# helpers
# -----------------------

STOP_WORDS = {"of", "with", "at", "in", "on", "for", "to", "and", "or", "the", "a", "an", "by", "from"}

def smart_title_case(text):
    if text is None:
        return None
    words = str(text).strip().lower().split()
    if not words:
        return None
    out = []
    for i, w in enumerate(words):
        out.append(w.capitalize() if i == 0 or w not in STOP_WORDS else w)
    return " ".join(out)

def normalize_class(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", "_", str(name).strip().lower())

def to_int_or_none(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group(0)) if m else None

def to_float_or_none(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    s = s.replace(",", "")
    m = re.search(r"-?\d+(\.\d+)?", s)
    return float(m.group(0)) if m else None

def is_missing_or_bad(x):
    if x is None or pd.isna(x):
        return True
    try:
        return float(x) <= 0.0
    except Exception:
        return True

def fmt_mp(n: int) -> str:
    return f"MP-{n:06d}"

def fmt_prj(mp_code: str, n: int) -> str:
    return f"PRJ-{mp_code}-{n:03d}"

# -----------------------
# IMPORTANT: reconstruct extractions from split schema
# -----------------------
def parse_langextract_grouped_pairs(doc: dict):
    raw = doc.get("extractions", [])
    if not isinstance(raw, list) or not raw:
        return []

    looks_split = any(
        isinstance(e, dict) and e.get("extraction_class") in ("extraction_class", "extraction_text", "extraction_index")
        for e in raw
    )

    if not looks_split:
        out = []
        for e in raw:
            if not isinstance(e, dict):
                continue
            out.append({
                "extraction_class": e.get("extraction_class"),
                "extraction_text": e.get("extraction_text"),
                "extraction_index": e.get("extraction_index", 10**9),
            })
        out.sort(key=lambda x: x.get("extraction_index", 10**9))
        return out

    groups = {}
    for e in raw:
        if not isinstance(e, dict):
            continue

        g = e.get("group_index")
        if g is None:
            g = f"idx_{e.get('extraction_index', 10**9)}"

        groups.setdefault(g, {"cls": None, "val": None, "idx": None, "order": e.get("extraction_index", 10**9)})

        kind = e.get("extraction_class")
        txt  = e.get("extraction_text")
        groups[g]["order"] = min(groups[g]["order"], e.get("extraction_index", 10**9))

        if kind == "extraction_class":
            groups[g]["cls"] = txt
        elif kind == "extraction_text":
            groups[g]["val"] = txt
        elif kind == "extraction_index":
            try:
                groups[g]["idx"] = int(str(txt).strip())
            except Exception:
                groups[g]["idx"] = None

    out = []
    for _, v in groups.items():
        if v["cls"] is None and v["val"] is None:
            continue
        out.append({
            "extraction_class": v["cls"],
            "extraction_text": v["val"],
            "extraction_index": v["idx"] if v["idx"] is not None else v["order"],
        })

    out.sort(key=lambda x: x.get("extraction_index", 10**9))
    return out