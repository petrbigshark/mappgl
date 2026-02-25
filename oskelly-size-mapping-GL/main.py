#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Pattern, Tuple

import pandas as pd
import yaml
from unidecode import unidecode

from llm_client import LLMConfig, LLMError, SizeCategoryClassifier


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        die(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_space(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())


def normalize_for_contains(s: Any) -> str:
    return normalize_space(s).casefold()


def normalize_brand(s: Any) -> str:
    x = normalize_space(s)
    if not x:
        return ""
    x = x.replace("’", "'").replace("`", "'")
    x = x.replace("&", " and ")
    x = x.replace("@", "a")
    x = re.sub(r"[™®©]", "", x)
    x = unidecode(x)
    x = x.casefold()
    x = re.sub(r"[^a-z0-9\s'/\-]", " ", x)
    x = re.sub(r"[\-_/]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x


def normalize_text_for_keywords(s: Any) -> str:
    x = normalize_space(s)
    if not x:
        return ""
    x = unidecode(x).casefold()
    x = re.sub(r"[^a-z0-9а-яё\s\-]", " ", x)
    x = re.sub(r"[\-_/]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x


def resolve_column(df: pd.DataFrame, wanted: str) -> Optional[str]:
    w = normalize_space(wanted).casefold()
    if not w:
        return None

    for c in df.columns:
        if normalize_space(c).casefold() == w:
            return c
    for c in df.columns:
        if w in normalize_space(c).casefold():
            return c
    return None


def resolve_reference_path(
    cli_value: Optional[str],
    config_filename: str,
    script_dir: Path,
    input_path: Path,
) -> Path:
    if cli_value:
        return Path(cli_value)

    candidates = [
        script_dir / config_filename,
        input_path.parent / config_filename,
        Path(config_filename),
    ]
    for c in candidates:
        if c.exists():
            return c

    # Return first candidate for clear error message downstream.
    return candidates[0]


def compile_reason_patterns(patterns: List[str]) -> List[Pattern[str]]:
    out: List[Pattern[str]] = []
    for p in patterns:
        try:
            out.append(re.compile(p, flags=re.IGNORECASE))
        except re.error as e:
            die(f"Bad regex in filters.reason_start_regex: {p}. Error: {e}")
    return out


def reason_starts_with_target(reason: Any, patterns: List[Pattern[str]]) -> bool:
    s = str(reason or "")
    return any(p.search(s) is not None for p in patterns)


def contains_any_marker(text: Any, markers: List[str]) -> bool:
    s = normalize_for_contains(text)
    if not s:
        return False
    return any(normalize_for_contains(m) in s for m in markers if normalize_space(m))


def parent_is_allowed(parentcategory: Any, include_markers: List[str], exclude_markers: List[str]) -> bool:
    if contains_any_marker(parentcategory, exclude_markers):
        return False
    if not include_markers:
        # Exclude-only mode: everything non-kids is allowed.
        return True
    return contains_any_marker(parentcategory, include_markers)


def heuristic_category_label(
    category: str,
    shoes_keywords: List[str],
    clothing_keywords: List[str],
    default_label: str,
) -> str:
    cat = normalize_text_for_keywords(category)
    if not cat:
        return default_label

    for kw in shoes_keywords:
        nkw = normalize_text_for_keywords(kw)
        if nkw and nkw in cat:
            return "SHOES"

    for kw in clothing_keywords:
        nkw = normalize_text_for_keywords(kw)
        if nkw and nkw in cat:
            return "CLOTHING"

    return default_label


def load_size_reference(path: Path) -> Tuple[Dict[str, str], List[str]]:
    if not path.exists():
        die(f"Reference file not found: {path}")

    df = pd.read_excel(path)
    c_brand = resolve_column(df, "brand")
    c_sizetype = resolve_column(df, "sizetype")
    if not c_brand or not c_sizetype:
        die(f"{path.name} must contain columns brand and sizetype. Found: {list(df.columns)}")

    mapping: Dict[str, str] = {}
    conflicts: List[str] = []

    for _, r in df.iterrows():
        brand_raw = normalize_space(r[c_brand])
        sizetype_raw = normalize_space(r[c_sizetype])
        if not brand_raw:
            continue

        key = normalize_brand(brand_raw)
        if not key:
            continue

        prev = mapping.get(key)
        if prev is None:
            mapping[key] = sizetype_raw
            continue

        if prev != sizetype_raw:
            conflicts.append(f"{brand_raw}: '{prev}' vs '{sizetype_raw}'")

    return mapping, conflicts


def ensure_output_run_dir(output_root: Path, input_stem: str) -> Tuple[Path, str, int]:
    output_root.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%d.%m.%Y")
    base = f"{input_stem} {date_str}"

    versions: List[int] = []
    for p in output_root.iterdir():
        if not p.is_dir():
            continue
        if not p.name.startswith(base):
            continue
        m = re.search(r"\sv(\d+)$", p.name)
        if m:
            versions.append(int(m.group(1)))

    v = (max(versions) + 1) if versions else 1
    run_dir = output_root / f"{base} v{v}"
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir, date_str, v


def classify_categories(
    categories: List[str],
    cfg: Dict[str, Any],
    use_llm: bool,
) -> Tuple[Dict[str, str], Dict[str, str], Optional[str]]:
    cls_cfg = cfg["classification"]
    allowed_labels = {str(x).upper().strip() for x in cls_cfg.get("allowed_labels", [])}
    if not allowed_labels:
        allowed_labels = {"CLOTHING", "SHOES", "OTHER"}

    default_label = str(cls_cfg.get("default_label", "OTHER")).upper().strip()
    if default_label not in allowed_labels:
        default_label = "OTHER"

    shoes_keywords = cls_cfg.get("shoes_keywords", [])
    clothing_keywords = cls_cfg.get("clothing_keywords", [])

    llm_map: Dict[str, str] = {}
    llm_error: Optional[str] = None

    if use_llm and categories:
        llm_cfg = LLMConfig(
            model=str(cls_cfg.get("llm_model", "gpt-5-mini")),
            timeout_sec=int(cls_cfg.get("llm_timeout_sec", 90)),
            max_retries=int(cls_cfg.get("llm_max_retries", 3)),
        )
        classifier = SizeCategoryClassifier(llm_cfg)
        try:
            llm_map = classifier.classify(categories)
        except LLMError as e:
            llm_error = str(e)

    out_label: Dict[str, str] = {}
    out_source: Dict[str, str] = {}
    for cat in categories:
        label = str(llm_map.get(cat, "")).upper().strip()
        if label in allowed_labels:
            out_label[cat] = label
            out_source[cat] = "llm"
            continue

        h = heuristic_category_label(cat, shoes_keywords, clothing_keywords, default_label)
        if h not in allowed_labels:
            h = default_label
        out_label[cat] = h
        out_source[cat] = "heuristic"

    return out_label, out_source, llm_error


def main() -> None:
    parser = argparse.ArgumentParser(description="Oskelly Size Mapping")
    parser.add_argument("--input", required=True, help="Input Excel file with columns reason/brand/parentcategory/category")
    parser.add_argument("--clothing-dict", default=None, help="Excel dictionary for clothing: columns brand,sizetype")
    parser.add_argument("--shoes-dict", default=None, help="Excel dictionary for shoes: columns brand,sizetype")
    parser.add_argument("--config", default="config.yml", help="Path to config.yml")
    parser.add_argument("--outdir", default=None, help="Output root folder (overrides config)")
    parser.add_argument("--sheet-name", default=None, help="Input sheet name override")
    parser.add_argument("--dry-run", action="store_true", help="Do not call LLM, use heuristic category classification only")
    parser.add_argument("--disable-llm", action="store_true", help="Force-disable LLM and use heuristic category classification")
    args = parser.parse_args()

    cfg = load_yaml(Path(args.config))

    in_path = Path(args.input)
    if not in_path.exists():
        die(f"Input file not found: {in_path}")

    script_dir = Path(__file__).resolve().parent
    refs_cfg = cfg.get("references", {})
    clothing_dict_name = str(refs_cfg.get("clothing_dict_xlsx", "Справочник одежда.xlsx"))
    shoes_dict_name = str(refs_cfg.get("shoes_dict_xlsx", "Справочник обувь.xlsx"))
    clothing_dict_path = resolve_reference_path(args.clothing_dict, clothing_dict_name, script_dir, in_path)
    shoes_dict_path = resolve_reference_path(args.shoes_dict, shoes_dict_name, script_dir, in_path)

    sheet_name: Any = args.sheet_name if args.sheet_name is not None else cfg["input"].get("sheet_name", 0)
    df_in = pd.read_excel(in_path, sheet_name=sheet_name)

    input_cols = cfg["input"]["columns"]
    c_reason = resolve_column(df_in, input_cols["reason"])
    c_brand = resolve_column(df_in, input_cols["brand"])
    c_parent = resolve_column(df_in, input_cols["parentcategory"])
    c_category = resolve_column(df_in, input_cols["category"])

    missing = [x for x in [("reason", c_reason), ("brand", c_brand), ("parentcategory", c_parent), ("category", c_category)] if not x[1]]
    if missing:
        miss = ", ".join(k for k, _ in missing)
        die(f"Could not resolve columns: {miss}. Found columns: {list(df_in.columns)}")

    reason_patterns = compile_reason_patterns(cfg["filters"]["reason_start_regex"])
    include_parent_markers = cfg["filters"].get("include_parent_markers", [])
    exclude_parent_markers = cfg["filters"].get("exclude_parent_markers", [])

    mask_reason = df_in[c_reason].astype(str).apply(lambda x: reason_starts_with_target(x, reason_patterns))
    df_reason = df_in[mask_reason].copy()

    mask_parent = df_reason[c_parent].astype(str).apply(
        lambda x: parent_is_allowed(x, include_parent_markers, exclude_parent_markers)
    )
    df_filtered = df_reason[mask_parent].copy()
    df_filtered["_category_clean"] = df_filtered[c_category].astype(str).map(normalize_space)

    unique_categories = sorted([x for x in df_filtered["_category_clean"].dropna().unique().tolist() if normalize_space(x)])

    can_use_llm = bool(
        cfg["classification"].get("llm_enabled", True)
        and not args.dry_run
        and not args.disable_llm
        and os.getenv("OPENAI_API_KEY")
    )

    llm_skip_reason: Optional[str] = None
    if not can_use_llm:
        if args.dry_run:
            llm_skip_reason = "dry_run"
        elif args.disable_llm:
            llm_skip_reason = "disabled_by_flag"
        elif not cfg["classification"].get("llm_enabled", True):
            llm_skip_reason = "disabled_in_config"
        elif not os.getenv("OPENAI_API_KEY"):
            llm_skip_reason = "OPENAI_API_KEY is not set"
        else:
            llm_skip_reason = "unknown"

    category_label_map, category_source_map, llm_error = classify_categories(unique_categories, cfg, can_use_llm)

    out_cols_cfg = cfg["output"]["add_columns"]
    col_size_category = str(out_cols_cfg.get("size_category", "sizeCategory"))
    col_size_type_mapped = str(out_cols_cfg.get("size_type_mapped", "sizeTypeMapped"))

    default_label = str(cfg["classification"].get("default_label", "OTHER")).upper().strip()
    df_filtered[col_size_category] = df_filtered["_category_clean"].map(category_label_map).fillna(default_label)

    keep_labels = {"CLOTHING", "SHOES"}
    df_target = df_filtered[df_filtered[col_size_category].isin(keep_labels)].copy()

    clothing_map, clothing_conflicts = load_size_reference(clothing_dict_path)
    shoes_map, shoes_conflicts = load_size_reference(shoes_dict_path)

    df_target["_brand_key"] = df_target[c_brand].astype(str).map(normalize_brand)
    df_target[col_size_type_mapped] = ""

    mask_clothing = df_target[col_size_category] == "CLOTHING"
    mask_shoes = df_target[col_size_category] == "SHOES"
    df_target.loc[mask_clothing, col_size_type_mapped] = (
        df_target.loc[mask_clothing, "_brand_key"].map(clothing_map).fillna("")
    )
    df_target.loc[mask_shoes, col_size_type_mapped] = (
        df_target.loc[mask_shoes, "_brand_key"].map(shoes_map).fillna("")
    )

    if bool(cfg["output"].get("keep_only_mapped_size_type", False)):
        df_target = df_target[df_target[col_size_type_mapped].astype(str).str.strip() != ""].copy()

    unmatched = df_target[df_target[col_size_type_mapped].astype(str).str.strip() == ""].copy()
    unmatched_unique = (
        unmatched[[c_brand, c_category, col_size_category]]
        .astype(str)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    df_out = df_target.drop(columns=["_category_clean", "_brand_key"], errors="ignore")

    out_root = Path(args.outdir) if args.outdir else Path(cfg["output"].get("outdir", "output"))
    run_dir, date_str, version = ensure_output_run_dir(out_root, in_path.stem)

    result_name_tmpl = str(cfg["output"].get("result_filename_template", "Размеры для маппинга {input_stem} {date}.xlsx"))
    result_name = result_name_tmpl.format(input_stem=in_path.stem, date=date_str)
    result_path = run_dir / result_name
    df_out.to_excel(result_path, index=False)

    category_map_name = str(cfg["output"].get("category_map_filename", "category_mapping.xlsx"))
    df_cat_map = pd.DataFrame(
        [
            {"category": cat, "label": category_label_map.get(cat, default_label), "source": category_source_map.get(cat, "heuristic")}
            for cat in unique_categories
        ]
    )
    df_cat_map.to_excel(run_dir / category_map_name, index=False)

    if not unmatched_unique.empty:
        unmatched_name = str(cfg["output"].get("unmatched_brands_filename", "unmatched_brands.xlsx"))
        unmatched_unique.to_excel(run_dir / unmatched_name, index=False)

    source_counts: Dict[str, int] = {}
    for v in category_source_map.values():
        source_counts[v] = source_counts.get(v, 0) + 1

    label_counts: Dict[str, int] = {}
    for v in category_label_map.values():
        label_counts[v] = label_counts.get(v, 0) + 1

    report = {
        "args": {
            "input": str(in_path),
            "clothing_dict": str(clothing_dict_path),
            "shoes_dict": str(shoes_dict_path),
            "sheet_name": sheet_name,
            "dry_run": bool(args.dry_run),
            "disable_llm": bool(args.disable_llm),
        },
        "run": {
            "run_dir": str(run_dir),
            "date": date_str,
            "version": version,
            "result_file": str(result_path),
        },
        "counts": {
            "input_rows": int(len(df_in)),
            "rows_after_reason_filter": int(len(df_reason)),
            "rows_after_parent_filter": int(len(df_filtered)),
            "unique_categories_for_classification": int(len(unique_categories)),
            "rows_after_category_filter": int(len(df_target)),
            "rows_with_empty_size_type": int(len(unmatched)),
            "rows_final_output": int(len(df_out)),
        },
        "classification": {
            "llm_used": bool(can_use_llm),
            "llm_skip_reason": llm_skip_reason,
            "llm_error": llm_error,
            "category_label_counts": label_counts,
            "category_source_counts": source_counts,
        },
        "references": {
            "clothing_brands": len(clothing_map),
            "shoes_brands": len(shoes_map),
            "clothing_conflicts": clothing_conflicts[:50],
            "shoes_conflicts": shoes_conflicts[:50],
        },
    }

    report_name = str(cfg["output"].get("report_filename", "run_report.json"))
    (run_dir / report_name).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK")
    print(f"Saved result: {result_path}")
    print(f"Saved report: {run_dir / report_name}")
    print(f"Rows: {len(df_in)} -> {len(df_out)}")
    if llm_error:
        print(f"LLM warning: {llm_error}")


if __name__ == "__main__":
    main()
