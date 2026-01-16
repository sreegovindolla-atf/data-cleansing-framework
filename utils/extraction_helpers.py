import os
import json
import hashlib
import pickle
import re
from pathlib import Path

import pandas as pd


# -----------------------
# JSONL helpers
# -----------------------
def load_processed_indexes_from_jsonl(path: Path) -> set:
    """Return set of indexes already present in the JSONL (best-effort)."""
    if not path.exists():
        return set()

    done = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if obj.get("index") is not None:
                    done.add(obj["index"])
            except Exception:
                continue
    return done


def _jsonl_remove_index(jsonl_path: Path, idx, index_key: str = "index") -> bool:
    """
    Rewrite JSONL excluding records where record[index_key] == idx.
    Returns True if any record was removed.
    """
    if not jsonl_path.exists():
        return False

    tmp_path = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
    removed = False

    with jsonl_path.open("r", encoding="utf-8") as src, tmp_path.open("w", encoding="utf-8") as dst:
        for line in src:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                dst.write(line)
                continue

            if obj.get(index_key) == idx:
                removed = True
                continue

            dst.write(json.dumps(obj, ensure_ascii=False) + "\n")

    if removed:
        os.replace(tmp_path, jsonl_path)
    else:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

    return removed


def _jsonl_append(jsonl_path: Path, record: dict):
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def jsonl_upsert_by_index(path: Path, record: dict, index_key: str = "index"):
    """
    Upsert a record into JSONL:
      - if index doesn't exist -> append
      - if index exists -> rewrite file excluding that index, then append record
    """
    idx = record.get(index_key)
    if idx is None:
        _jsonl_append(path, record)
        return

    # Remove existing occurrences (only does real rewrite if found)
    _jsonl_remove_index(path, idx, index_key=index_key)
    _jsonl_append(path, record)


def jsonl_to_json_snapshot(jsonl_path: Path, json_path: Path):
    """
    Build a VALID JSON array file from JSONL. This overwrites json_path.
    JSON cannot be safely appended as an array without loading.
    """
    docs = []
    if jsonl_path.exists():
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    docs.append(json.loads(s))
                except Exception:
                    continue

    json_path.parent.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)


# -----------------------
# basic text helpers
# -----------------------
def safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)


def normalize_text(text):
    if text is None:
        return ""
    try:
        if pd.isna(text):
            return ""
    except Exception:
        pass

    text = str(text)
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def text_hash(t: str) -> str:
    t_norm = normalize_text(t)
    return hashlib.sha256(t_norm.encode("utf-8")).hexdigest()


# -----------------------
# cache helpers
# -----------------------
def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            obj = pickle.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def save_cache(cache: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        pickle.dump(cache, f)
    os.replace(tmp, path)


# -----------------------
# langextract serialization helpers
# -----------------------
def _enum_to_value(x):
    try:
        return x.value
    except Exception:
        return x


def extraction_to_dict(ex):
    """
    Convert langextract Extraction object -> plain dict.
    """
    if isinstance(ex, dict):
        return ex

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
        if not hasattr(ex, k):
            continue

        v = getattr(ex, k)

        if k == "char_interval" and v is not None:
            if isinstance(v, dict):
                d[k] = v
            else:
                start = getattr(v, "start_pos", None)
                end = getattr(v, "end_pos", None)
                d[k] = {"start_pos": start, "end_pos": end}
            continue

        if k == "alignment_status" and v is not None:
            d[k] = _enum_to_value(v)
            continue

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

    if isinstance(res, str):
        s = res.strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        return {"text": s, "extractions": []}

    text = getattr(res, "text", None)
    exs = getattr(res, "extractions", None)

    doc_id = getattr(res, "document_id", None)
    if doc_id is None:
        doc_id = getattr(res, "_document_id", None)

    if text is not None or exs is not None:
        out = {
            "extractions": [extraction_to_dict(e) for e in (exs or [])],
            "text": text if text is not None else "",
        }
        if doc_id is not None:
            out["document_id"] = doc_id
        return out

    return {"text": str(res), "extractions": []}


# -----------------------
# incremental-safe writer
# -----------------------
def save_results_with_master_project_amount(results_with_amount, jsonl_path: Path, json_path: Path):
    """
    Incremental-safe writing:
      - JSONL: upsert by index (rewrite only if same index appears again)
      - JSON: valid snapshot rebuilt from JSONL (overwrites json_path)

    NOTE: JSON cannot be safely appended as an array; rebuilding is the simplest reliable option.
    """
    for (
        result,
        index,
        master_project_amount_actual,
        master_project_oda_amount,
        master_project_ge_amount,
        master_project_off_amount,
    ) in results_with_amount:
        d = annotated_to_dict(result)

        out = {}
        out["extractions"] = d.get("extractions", [])
        out["text"] = d.get("text", "")
        out["index"] = index
        out["master_project_amount_actual"] = master_project_amount_actual
        out["master_project_oda_amount"] = master_project_oda_amount
        out["master_project_ge_amount"] = master_project_ge_amount
        out["master_project_off_amount"] = master_project_off_amount

        if d.get("document_id"):
            out["document_id"] = d.get("document_id")

        for k, v in d.items():
            if k in ("extractions", "text", "document_id"):
                continue
            out[k] = v

        jsonl_upsert_by_index(jsonl_path, out, index_key="index")

    # rebuild JSON snapshot after the batch
    jsonl_to_json_snapshot(jsonl_path, json_path)


# -----------------------
# input builder
# -----------------------
def build_labeled_bilingual_input(
    title_en: str,
    desc_en: str,
    title_ar: str,
    desc_ar: str,
) -> str:
    """
    Build the labeled bilingual text expected by the prompt.
    Always emits all four labels (even if some are empty) to make the structure stable.
    """
    title_en = safe_str(title_en).strip()
    desc_en = safe_str(desc_en).strip()
    title_ar = safe_str(title_ar).strip()
    desc_ar = safe_str(desc_ar).strip()

    text = (
        f"TITLE_EN: {title_en}\n"
        f"DESC_EN: {desc_en}\n\n"
        f"TITLE_AR: {title_ar}\n"
        f"DESC_AR: {desc_ar}"
    )

    # whitespace normalization only
    text = normalize_text(text)
    return str(text).strip()