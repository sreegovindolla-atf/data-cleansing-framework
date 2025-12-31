import sys
import os
import json
import argparse
import hashlib
import pickle
import re
from pathlib import Path
from sqlalchemy import create_engine
import urllib

import pandas as pd
import langextract as lx

def normalize_text(t: str) -> str:
    t = t.strip()
    t = re.sub(r"\s+", " ", t)
    return t

def text_hash(t: str) -> str:
    t_norm = normalize_text(t)
    return hashlib.sha256(t_norm.encode("utf-8")).hexdigest()

def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with open(path, "rb") as f:
            obj = pickle.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

def save_cache(cache: dict, path: Path):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        pickle.dump(cache, f)
    tmp.replace(path)

def _enum_to_value(x):
    # Converts Enum-like objects to their value, else returns x
    try:
        return x.value
    except Exception:
        return x

def extraction_to_dict(ex):
    """
    Convert langextract Extraction object -> plain dict
    """
    if isinstance(ex, dict):
        return ex

    # Most langextract Extraction objects have attributes like these:
    d = {}
    for k in [
        "extraction_class",
        "extraction_text",
        "char_interval",
        "alignment_status",
        "extraction_index",
        "group_index",
        "description",
        "attributes",
    ]:
        if hasattr(ex, k):
            v = getattr(ex, k)

            # char_interval is an object with start_pos/end_pos
            if k == "char_interval" and v is not None:
                if isinstance(v, dict):
                    d[k] = v
                else:
                    start = getattr(v, "start_pos", None)
                    end = getattr(v, "end_pos", None)
                    d[k] = {"start_pos": start, "end_pos": end}
                continue

            # alignment_status may be Enum-like
            if k == "alignment_status" and v is not None:
                d[k] = _enum_to_value(v)
                continue

            # attributes might be non-serializable sometimes; keep only dict
            if k == "attributes" and v is not None and not isinstance(v, dict):
                d[k] = None
                continue

            d[k] = v

    return d

def annotated_to_dict(res):
    """
    Robust serializer for langextract AnnotatedDocument.
    Handles:
      - AnnotatedDocument object
      - dict
      - cached JSON dict
      - cached JSON string
    """
    if isinstance(res, dict):
        return res

    # If cache stored JSON as a string
    if isinstance(res, str):
        s = res.strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        # Not valid JSON -> treat as text-only
        return {"text": s, "extractions": []}

    # Try known attributes on AnnotatedDocument
    text = getattr(res, "text", None)
    exs  = getattr(res, "extractions", None)
    doc_id = getattr(res, "document_id", None)

    if text is not None or exs is not None:
        out = {
            "extractions": [extraction_to_dict(e) for e in (exs or [])],
            "text": text if text is not None else "",
        }
        if doc_id is not None:
            out["document_id"] = doc_id
        return out

    # last resort
    return {"text": str(res), "extractions": []}

def save_results_with_master_project_amount(results_with_amount, jsonl_path: Path, json_path: Path):
    """
    Writes JSONL and JSON where each doc includes:
      - extractions
      - text
      - master_project_amount_actual   (right after text)
      - document_id (if present)
    """
    docs = []

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for result, index, master_project_amount_actual, master_project_oda_amount, master_project_ge_amount, master_project_off_amount in results_with_amount:
            d = annotated_to_dict(result)
            # Build ordered output explicitly
            out = {}
            out["extractions"] = d.get("extractions", [])
            out["text"] = d.get("text", "")
            out["index"] = index
            out["master_project_amount_actual"] = master_project_amount_actual
            out["master_project_oda_amount"] = master_project_oda_amount
            out["master_project_ge_amount"] = master_project_ge_amount
            out["master_project_off_amount"] = master_project_off_amount

            if "document_id" in d:
                out["document_id"] = d.get("document_id")

            # If you want to keep other keys from d (optional)
            for k, v in d.items():
                if k in ("extractions", "text", "document_id"):
                    continue
                out[k] = v

            f.write(json.dumps(out, ensure_ascii=False) + "\n")
            docs.append(out)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)