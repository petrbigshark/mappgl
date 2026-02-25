#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd
import yaml
from zoneinfo import ZoneInfo


ROOT_DIR = Path(__file__).resolve().parents[1]
ROW_ID_COL = "_unified_row_id"

MODULE_DIRS = {
    "brand": ROOT_DIR / "oskelly-brand-mapping-GL",
    "color": ROOT_DIR / "oskelly-color-mapping-GL",
    "material": ROOT_DIR / "oskelly-material-mapping-GL",
    "category": ROOT_DIR / "oskelly-category-mapping-GL",
    "season": ROOT_DIR / "oskelly-season-mapping-GL",
    "size": ROOT_DIR / "oskelly-size-mapping-GL",
}

MODULE_ORDER = ["brand", "color", "material", "category", "season", "size"]
MODULE_OUTPUT_DIRS = {
    "brand": "brands",
    "color": "colors",
    "material": "materials",
    "category": "categories",
    "season": "seasons",
    "size": "sizes",
}

PREFILTER_DROP_PREFIXES = [
    "Product with PLU",
    "Ошибка при обновлении товара: 400 BAD_REQUEST",
    "Ошибка при публикации товара",
]

PREFILTER_DROP_EQUALS_OR_PREFIX = [
    "Найдены ошибки при валидации товара: Не указана цена товара",
]

PREFILTER_KEEP_IF_CONTAINS = ["Цвет", "Материал", "color", "material"]

BRAND_REASON_NEEDLE = "Не найден бренд с названием"
SEASON_REASON_NEEDLE = "Отсутствует конфигурация для сезона с типом"


class PipelineError(RuntimeError):
    pass


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_column(df: pd.DataFrame, wanted: str) -> Optional[str]:
    want = str(wanted or "").strip().casefold()
    if not want:
        return None

    for col in df.columns:
        if str(col).strip().casefold() == want:
            return col

    for col in df.columns:
        if want in str(col).strip().casefold():
            return col

    return None


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def contains_any_casefold(text: str, markers: Sequence[str]) -> bool:
    s = normalize_text(text).casefold()
    return any(normalize_text(m).casefold() in s for m in markers if normalize_text(m))


def row_ids_from_mask(df: pd.DataFrame, mask: pd.Series) -> List[int]:
    if ROW_ID_COL in df.columns:
        ids = df.loc[mask, ROW_ID_COL].tolist()
        return sorted({int(x) for x in ids})
    return sorted({int(i) + 1 for i in df.index[mask]})


def should_drop_reason(reason: Any) -> bool:
    s = normalize_text(reason)
    if not s:
        return False

    s_cf = s.casefold()

    for needle in PREFILTER_DROP_EQUALS_OR_PREFIX:
        needle_cf = needle.casefold()
        if s_cf == needle_cf or s_cf.startswith(needle_cf):
            return True

    for prefix in PREFILTER_DROP_PREFIXES:
        if s_cf.startswith(prefix.casefold()):
            if contains_any_casefold(s, PREFILTER_KEEP_IF_CONTAINS):
                return False
            return True

    return False


def next_versioned_run_dir(base_dir: Path, email: str, timezone: str) -> Path:
    tz = ZoneInfo(timezone)
    date_str = datetime.now(tz=tz).strftime("%d.%m.%Y")
    base = f"{email} {date_str}"

    base_dir.mkdir(parents=True, exist_ok=True)
    version = 1
    while True:
        run_dir = base_dir / f"{base} v{version}"
        if not run_dir.exists():
            run_dir.mkdir(parents=True, exist_ok=False)
            return run_dir
        version += 1


def prefilter_workbook(input_path: Path, output_path: Path) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    xls = pd.ExcelFile(input_path)
    sheet_names = list(xls.sheet_names)
    if not sheet_names:
        raise PipelineError(f"В файле нет листов: {input_path}")

    per_sheet: Dict[str, Any] = {}
    main_sheet_name: Optional[str] = None
    main_df: Optional[pd.DataFrame] = None

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name in sheet_names:
            df = pd.read_excel(input_path, sheet_name=sheet_name)
            reason_col = resolve_column(df, "reason")

            if reason_col is None:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                per_sheet[sheet_name] = {
                    "reason_col_found": False,
                    "rows_in": int(len(df)),
                    "rows_out": int(len(df)),
                    "rows_removed": 0,
                }
                continue

            df = df.copy()
            df[ROW_ID_COL] = range(1, len(df) + 1)
            drop_mask = df[reason_col].map(should_drop_reason)
            df_out = df.loc[~drop_mask].copy()
            df_out.to_excel(writer, sheet_name=sheet_name, index=False)

            per_sheet[sheet_name] = {
                "reason_col_found": True,
                "reason_col": str(reason_col),
                "rows_in": int(len(df)),
                "rows_out": int(len(df_out)),
                "rows_removed": int(drop_mask.sum()),
            }

            if main_df is None:
                main_df = df_out.copy()
                main_sheet_name = sheet_name

    if main_df is None or main_sheet_name is None:
        raise PipelineError("Не найден лист с колонкой reason после prefilter.")

    return main_df, main_sheet_name, per_sheet


def match_brand_rows(df: pd.DataFrame) -> List[int]:
    reason_col = resolve_column(df, "reason")
    if not reason_col:
        return []
    mask = df[reason_col].astype(str).str.contains(BRAND_REASON_NEEDLE, case=False, na=False)
    return row_ids_from_mask(df, mask)


def match_color_rows(df: pd.DataFrame) -> List[int]:
    cfg = load_yaml(MODULE_DIRS["color"] / "config.yml")
    reason_col = resolve_column(df, cfg["input"].get("reason_column", "reason"))
    if not reason_col:
        return []

    pattern = str(cfg["input"].get("reason_filter_substring", "")).strip()
    if not pattern:
        return []

    mask = df[reason_col].astype(str).str.contains(pattern, case=False, regex=True, na=False)
    return row_ids_from_mask(df, mask)


def match_material_rows(df: pd.DataFrame) -> List[int]:
    cfg = load_yaml(MODULE_DIRS["material"] / "config.yml")
    reason_col = resolve_column(df, cfg.get("input", {}).get("reason_column", "reason"))
    if not reason_col:
        return []

    pattern = str(cfg.get("filter", {}).get("reason_contains", "")).strip()
    if not pattern:
        return []

    mask = df[reason_col].astype(str).str.contains(pattern, case=False, regex=True, na=False)
    return row_ids_from_mask(df, mask)


def match_category_rows(df: pd.DataFrame) -> List[int]:
    cfg = load_yaml(MODULE_DIRS["category"] / "config.yml")
    in_cols = cfg.get("input", {}).get("columns", {})
    filters = cfg.get("filters", {})

    reason_col = resolve_column(df, in_cols.get("reason", "reason"))
    cat_col = resolve_column(df, in_cols.get("category", "category"))
    parent_col = resolve_column(df, in_cols.get("parentcategory", "parentcategory"))
    if not reason_col or not cat_col or not parent_col:
        return []

    reasons = filters.get("reasons", [])
    if not reasons:
        return []
    reason_a = str(reasons[0])
    reason_b = str(reasons[1]) if len(reasons) > 1 else ""

    brand_exclude_prefixes = filters.get("brand_exclude_prefixes", ["Не найден бренд с названием"])
    kids_markers = filters.get("kids_parent_markers", [])
    skirts_markers = filters.get("skirts_category_markers", [])

    tmp = df.copy()
    tmp["_reason"] = tmp[reason_col].astype(str)
    tmp["_cat"] = tmp[cat_col].astype(str)
    tmp["_parent"] = tmp[parent_col].astype(str)

    mask_brand_excluded = tmp["_reason"].map(
        lambda s: any(normalize_text(s).startswith(str(p)) for p in brand_exclude_prefixes)
    )
    tmp = tmp.loc[~mask_brand_excluded].copy()

    mask_reason = tmp["_reason"].map(lambda s: (reason_a in s) or (reason_b in s if reason_b else False))
    tmp = tmp.loc[mask_reason].copy()
    if tmp.empty:
        return []

    tmp["_reason_type"] = tmp["_reason"].map(lambda s: "A" if reason_a in s else ("B" if reason_b and reason_b in s else "OTHER"))
    mask_a_kids = (tmp["_reason_type"] == "A") & tmp["_parent"].map(lambda s: contains_any_casefold(s, kids_markers))
    mask_b_skirts = (tmp["_reason_type"] == "B") & tmp["_cat"].map(lambda s: contains_any_casefold(s, skirts_markers))
    tmp = tmp.loc[~(mask_a_kids | mask_b_skirts)].copy()

    if tmp.empty:
        return []

    if ROW_ID_COL in tmp.columns:
        return sorted({int(v) for v in tmp[ROW_ID_COL].tolist()})
    return sorted({int(i) + 1 for i in tmp.index})


def match_season_rows(df: pd.DataFrame) -> List[int]:
    reason_col = resolve_column(df, "reason")
    if not reason_col:
        return []
    mask = df[reason_col].astype(str).str.contains(SEASON_REASON_NEEDLE, case=False, na=False)
    return row_ids_from_mask(df, mask)


def match_size_rows(df: pd.DataFrame) -> List[int]:
    cfg = load_yaml(MODULE_DIRS["size"] / "config.yml")
    in_cols = cfg.get("input", {}).get("columns", {})
    filters = cfg.get("filters", {})

    reason_col = resolve_column(df, in_cols.get("reason", "reason"))
    parent_col = resolve_column(df, in_cols.get("parentcategory", "parentcategory"))
    if not reason_col or not parent_col:
        return []

    regex_list = filters.get("reason_start_regex", [])
    patterns = [re.compile(p, flags=re.IGNORECASE) for p in regex_list]
    include_markers = filters.get("include_parent_markers", [])
    exclude_markers = filters.get("exclude_parent_markers", [])

    def reason_ok(s: Any) -> bool:
        t = str(s or "")
        return any(p.search(t) is not None for p in patterns)

    def parent_ok(s: Any) -> bool:
        text = str(s or "")
        if contains_any_casefold(text, exclude_markers):
            return False
        if not include_markers:
            return True
        return contains_any_casefold(text, include_markers)

    mask = df[reason_col].map(reason_ok) & df[parent_col].map(parent_ok)
    return row_ids_from_mask(df, mask)


def build_env(request: Dict[str, Any]) -> Dict[str, str]:
    env = os.environ.copy()

    base_url = request.get("openai_base_url")
    if base_url is not None:
        if str(base_url).strip():
            env["OPENAI_BASE_URL"] = str(base_url).strip()
        else:
            env.pop("OPENAI_BASE_URL", None)

    return env


def run_command(cmd: List[str], cwd: Path, env: Dict[str, str], log_file: Path) -> Tuple[int, str, str]:
    started = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    duration = round(time.time() - started, 3)

    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w", encoding="utf-8") as f:
        f.write(f"$ (cwd={cwd}) {' '.join(cmd)}\n")
        f.write(f"exit_code={proc.returncode} duration_sec={duration}\n\n")
        f.write("STDOUT:\n")
        f.write(proc.stdout or "")
        f.write("\n\nSTDERR:\n")
        f.write(proc.stderr or "")

    return proc.returncode, proc.stdout or "", proc.stderr or ""


def list_child_dirs(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    return {p.name for p in path.iterdir() if p.is_dir()}


def ensure_unique_file_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    i = 2
    while True:
        candidate = parent / f"{stem}__{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def copy_new_dirs_flat(source_root: Path, before_names: Set[str], dest_root: Path) -> Tuple[List[Path], List[Path]]:
    if not source_root.exists():
        return [], []

    after_dirs = [p for p in source_root.iterdir() if p.is_dir()]
    new_dirs = [p for p in after_dirs if p.name not in before_names]
    if not new_dirs and after_dirs:
        new_dirs = [max(after_dirs, key=lambda p: p.stat().st_mtime)]

    copied_files: List[Path] = []
    dest_root.mkdir(parents=True, exist_ok=True)

    for src in new_dirs:
        for file_path in src.rglob("*"):
            if not file_path.is_file():
                continue
            dst = ensure_unique_file_path(dest_root / file_path.name)
            shutil.copy2(file_path, dst)
            copied_files.append(dst)

    return new_dirs, copied_files


def flatten_output_tree(dest_root: Path) -> List[Path]:
    if not dest_root.exists():
        return []

    moved_files: List[Path] = []
    for file_path in sorted(dest_root.rglob("*")):
        if not file_path.is_file():
            continue
        if file_path.parent == dest_root:
            continue
        dst = ensure_unique_file_path(dest_root / file_path.name)
        shutil.move(str(file_path), str(dst))
        moved_files.append(dst)

    # cleanup empty directories after flatten
    for p in sorted(dest_root.rglob("*"), key=lambda x: len(x.parts), reverse=True):
        if p.is_dir():
            try:
                p.rmdir()
            except OSError:
                pass

    return moved_files


def find_latest_file(root: Path, pattern: str) -> Optional[Path]:
    files = [p for p in root.rglob(pattern) if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def read_json_file(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def run_brand(
    request: Dict[str, Any],
    prefiltered_input: Path,
    module_out: Path,
    env: Dict[str, str],
) -> Dict[str, Any]:
    module_dir = MODULE_DIRS["brand"]
    output_root = module_dir / "output"
    before = list_child_dirs(output_root)

    cmd = [
        sys.executable,
        "main.py",
        "--input",
        str(prefiltered_input),
        "--index-start",
        str(request["index_starts"]["brand"]),
        "--email",
        str(request["email"]),
    ]

    if bool(request.get("use_llm_brand", True)):
        cmd.append("--use-llm")
        if request.get("openai_base_url"):
            cmd.extend(["--llm-base-url", str(request["openai_base_url"])])

    log_file = module_out / "execution.log"
    rc, stdout, err = run_command(cmd, module_dir, env, log_file)
    if rc != 0:
        raise PipelineError(f"brand mapping завершился с ошибкой: {err.strip() or 'unknown error'}")

    copied_dirs, copied_files = copy_new_dirs_flat(output_root, before, module_out)
    debug_file = find_latest_file(module_out, "debug_all_rows.xlsx")

    llm_stats: Dict[str, Any] = {}
    if debug_file:
        try:
            dbg = pd.read_excel(debug_file)
            llm_stats["debug_rows"] = int(len(dbg))
            if "rule" in dbg.columns:
                llm_stats["mapped_by_llm_or_cache"] = int(dbg["rule"].astype(str).isin(["llm", "llm_cache"]).sum())
            if "status" in dbg.columns:
                llm_stats["mapped_total"] = int((dbg["status"].astype(str) == "mapped").sum())
        except Exception:
            llm_stats["debug_parse_error"] = True

    cache_file = find_latest_file(module_out, "llm_cache.json")
    if cache_file and cache_file.exists():
        cache_json = read_json_file(cache_file) or {}
        llm_stats["llm_sent_after_dedup"] = int(len(cache_json))
    else:
        llm_stats["llm_sent_after_dedup"] = 0 if bool(request.get("use_llm_brand", True)) is False else None

    llm_stats["llm_enabled_for_module"] = bool(request.get("use_llm_brand", True))
    if "⚠️ DRY-RUN MODE" in (stdout or ""):
        llm_stats["llm_sent_after_dedup"] = 0

    return {
        "status": "OK",
        "output_dir": str(module_out),
        "source_output_dirs": [str(p) for p in copied_dirs],
        "output_files": [str(p) for p in copied_files],
        "log_file": str(log_file),
        "llm": llm_stats,
    }


def run_color(
    request: Dict[str, Any],
    prefiltered_input: Path,
    module_out: Path,
    env: Dict[str, str],
) -> Dict[str, Any]:
    module_dir = MODULE_DIRS["color"]
    module_out.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "main.py",
        "--input",
        str(prefiltered_input),
        "--index-start",
        str(request["index_starts"]["color"]),
        "--email",
        str(request["email"]),
        "--outdir",
        str(module_out),
    ]

    log_file = module_out / "execution.log"
    rc, stdout, err = run_command(cmd, module_dir, env, log_file)
    if rc != 0:
        raise PipelineError(f"color mapping завершился с ошибкой: {err.strip() or 'unknown error'}")

    flatten_output_tree(module_out)
    report_path = find_latest_file(module_out, "run_report.json") or (module_out / "run_report.json")
    report = read_json_file(report_path)

    llm_sent_after_dedup: Optional[int] = None
    if "DRY-RUN MODE" in (stdout or ""):
        llm_sent_after_dedup = 0
    else:
        m = re.search(r"ONE-SHOT GPT call for\s+(\d+)\s+unique values", stdout or "", flags=re.IGNORECASE)
        if m:
            llm_sent_after_dedup = int(m.group(1))

    return {
        "status": "OK",
        "output_dir": str(module_out),
        "report_file": str(report_path) if report_path.exists() else None,
        "report": report,
        "log_file": str(log_file),
        "llm": {
            "llm_sent_after_dedup": llm_sent_after_dedup,
            "llm_enabled_for_module": bool((llm_sent_after_dedup or 0) > 0),
        },
    }


def run_material(
    request: Dict[str, Any],
    prefiltered_input: Path,
    module_out: Path,
    env: Dict[str, str],
    temp_dir: Path,
) -> Dict[str, Any]:
    module_dir = MODULE_DIRS["material"]
    module_out.mkdir(parents=True, exist_ok=True)

    cfg_path = module_dir / "config.yml"
    cfg = load_yaml(cfg_path)
    cfg.setdefault("app", {})
    cfg["app"]["output_dir"] = str(module_out)
    cfg["app"]["report_name"] = "run_report.json"

    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_cfg_path = temp_dir / "material.config.yml"
    with temp_cfg_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)

    cmd = [
        sys.executable,
        "main.py",
        "--input",
        str(prefiltered_input),
        "--index-start",
        str(request["index_starts"]["material"]),
        "--email",
        str(request["email"]),
        "--config",
        str(temp_cfg_path),
    ]

    log_file = module_out / "execution.log"
    rc, stdout, err = run_command(cmd, module_dir, env, log_file)
    if rc != 0:
        raise PipelineError(f"material mapping завершился с ошибкой: {err.strip() or 'unknown error'}")

    flatten_output_tree(module_out)
    report_path = find_latest_file(module_out, "run_report.json") or (module_out / "run_report.json")
    report = read_json_file(report_path)

    return {
        "status": "OK",
        "output_dir": str(module_out),
        "report_file": str(report_path) if report_path.exists() else None,
        "report": report,
        "log_file": str(log_file),
        "llm": (report or {}).get("llm", {"llm_sent_after_dedup": 0, "enabled": "DRY-RUN MODE" not in (stdout or "")}),
    }


def run_category(
    request: Dict[str, Any],
    prefiltered_input: Path,
    module_out: Path,
    env: Dict[str, str],
) -> Dict[str, Any]:
    module_dir = MODULE_DIRS["category"]
    output_root = module_dir / "output"
    before = list_child_dirs(output_root)

    cmd = [
        sys.executable,
        "main.py",
        "--input",
        str(prefiltered_input),
        "--index-start",
        str(request["index_starts"]["category"]),
        "--email",
        str(request["email"]),
    ]

    log_file = module_out / "execution.log"
    rc, _, err = run_command(cmd, module_dir, env, log_file)
    if rc != 0:
        raise PipelineError(f"category mapping завершился с ошибкой: {err.strip() or 'unknown error'}")

    copied_dirs, copied_files = copy_new_dirs_flat(output_root, before, module_out)
    report_path = find_latest_file(module_out, "run_report.json")
    report = read_json_file(report_path)
    cache_path = find_latest_file(module_out, "llm_cache.jsonl")
    llm_sent_after_dedup = None
    if cache_path and cache_path.exists():
        try:
            llm_sent_after_dedup = int(len(cache_path.read_text(encoding="utf-8").splitlines()))
        except Exception:
            llm_sent_after_dedup = None

    return {
        "status": "OK",
        "output_dir": str(module_out),
        "source_output_dirs": [str(p) for p in copied_dirs],
        "output_files": [str(p) for p in copied_files],
        "report_file": str(report_path) if report_path else None,
        "report": report,
        "log_file": str(log_file),
        "llm": {
            "llm_sent_after_dedup": llm_sent_after_dedup,
            "llm_enabled_for_module": True,
        },
    }


def run_season(
    request: Dict[str, Any],
    prefiltered_input: Path,
    module_out: Path,
    env: Dict[str, str],
) -> Dict[str, Any]:
    module_dir = MODULE_DIRS["season"]
    module_out.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "main.py",
        "--input",
        str(prefiltered_input),
        "--index-start",
        str(request["index_starts"]["season"]),
        "--email",
        str(request["email"]),
        "--output-dir",
        str(module_out),
    ]

    if bool(request.get("use_llm_season", False)):
        cmd.append("--use-llm")
        if request.get("season_llm_model"):
            cmd.extend(["--llm-model", str(request["season_llm_model"])])

    log_file = module_out / "execution.log"
    rc, _, err = run_command(cmd, module_dir, env, log_file)
    if rc != 0:
        raise PipelineError(f"season mapping завершился с ошибкой: {err.strip() or 'unknown error'}")

    flatten_output_tree(module_out)
    out_files = [str(p) for p in module_out.rglob("*.xlsx")]
    return {
        "status": "OK",
        "output_dir": str(module_out),
        "xlsx_files": out_files,
        "log_file": str(log_file),
        "llm": {
            "llm_sent_after_dedup": None if bool(request.get("use_llm_season", False)) else 0,
            "llm_enabled_for_module": bool(request.get("use_llm_season", False)),
            "note": "Season module currently does not expose exact LLM dedup metric.",
        },
    }


def run_size(
    request: Dict[str, Any],
    prefiltered_input: Path,
    module_out: Path,
    env: Dict[str, str],
) -> Dict[str, Any]:
    module_dir = MODULE_DIRS["size"]
    module_out.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "main.py",
        "--input",
        str(prefiltered_input),
        "--outdir",
        str(module_out),
    ]

    log_file = module_out / "execution.log"
    rc, _, err = run_command(cmd, module_dir, env, log_file)
    if rc != 0:
        raise PipelineError(f"size mapping завершился с ошибкой: {err.strip() or 'unknown error'}")

    flatten_output_tree(module_out)
    report_path = find_latest_file(module_out, "run_report.json")
    report = read_json_file(report_path)

    if bool(request.get("require_llm_success", True)):
        llm_used = bool((report or {}).get("classification", {}).get("llm_used"))
        llm_error = (report or {}).get("classification", {}).get("llm_error")
        if not llm_used:
            reason = (report or {}).get("classification", {}).get("llm_skip_reason", "unknown")
            raise PipelineError(f"size mapping: LLM не был использован ({reason})")
        if llm_error:
            raise PipelineError(f"size mapping: ошибка LLM ({llm_error})")

    return {
        "status": "OK",
        "output_dir": str(module_out),
        "report_file": str(report_path) if report_path else None,
        "report": report,
        "log_file": str(log_file),
    }


def ensure_llm_env_or_fail(request: Dict[str, Any], matched_rows: Dict[str, List[int]], env: Dict[str, str]) -> None:
    # Kept for backward compatibility; module-level LLM checks are handled per module
    # so one failing module does not stop the whole 6-module pipeline.
    return


def collect_match_stats(main_df: pd.DataFrame) -> Dict[str, List[int]]:
    return {
        "brand": match_brand_rows(main_df),
        "color": match_color_rows(main_df),
        "material": match_material_rows(main_df),
        "category": match_category_rows(main_df),
        "season": match_season_rows(main_df),
        "size": match_size_rows(main_df),
    }


def summarize_llm_metrics(module_name: str, module_result: Dict[str, Any]) -> Dict[str, Any]:
    report = module_result.get("report") or {}
    rows_in_scope = module_result.get("matched_rows")
    base = {
        "rows_in_scope_input": rows_in_scope,
        "llm_sent_after_dedup": None,
        "module_rows_output": None,
        "note": None,
    }

    if module_name == "category":
        llm = report.get("llm", {})
        base["llm_sent_after_dedup"] = (module_result.get("llm") or {}).get("llm_sent_after_dedup")
        base["module_rows_output"] = (report.get("counts") or {}).get("output_rows")
        base["note"] = {
            "rows_need_llm": llm.get("rows_need_llm"),
            "unique_stage1": llm.get("unique_stage1"),
            "unique_force_desc": llm.get("unique_force_desc"),
            "rows_anchor_llm": llm.get("rows_anchor_llm"),
            "rows_depth_fallback": llm.get("rows_depth_fallback"),
        }
        return base
    if module_name == "size":
        classification = report.get("classification", {})
        counts = report.get("counts", {})
        base["llm_sent_after_dedup"] = (
            counts.get("unique_categories_for_classification")
            if classification.get("llm_used")
            else 0
        )
        base["module_rows_output"] = counts.get("rows_final_output")
        base["note"] = {
            "llm_used": classification.get("llm_used"),
            "llm_error": classification.get("llm_error"),
            "llm_skip_reason": classification.get("llm_skip_reason"),
            "unique_categories_for_classification": counts.get("unique_categories_for_classification"),
        }
        return base
    if module_name == "color":
        base["llm_sent_after_dedup"] = (module_result.get("llm") or {}).get("llm_sent_after_dedup")
        base["module_rows_output"] = (report or {}).get("rows_processed")
        base["note"] = {"model": (report or {}).get("model")}
        return base
    if module_name == "material":
        llm = (module_result.get("llm") or {})
        base["llm_sent_after_dedup"] = llm.get("llm_sent_after_dedup")
        base["module_rows_output"] = (report or {}).get("rows_final_output")
        base["note"] = {
            "rows_after_filter": (report or {}).get("rows_after_filter"),
            "warnings_count": len((report or {}).get("warnings", [])),
            "llm_enabled": llm.get("enabled"),
            "unique_categories": llm.get("unique_categories"),
            "unique_pairs": llm.get("unique_pairs"),
        }
        return base
    if module_name == "brand":
        llm = module_result.get("llm", {})
        base["llm_sent_after_dedup"] = llm.get("llm_sent_after_dedup")
        base["module_rows_output"] = llm.get("mapped_total")
        base["note"] = {
            "mapped_by_llm_or_cache": llm.get("mapped_by_llm_or_cache"),
            "debug_rows": llm.get("debug_rows"),
        }
        return base
    if module_name == "season":
        llm = module_result.get("llm", {})
        base["llm_sent_after_dedup"] = llm.get("llm_sent_after_dedup")
        base["module_rows_output"] = None
        base["note"] = llm.get("note")
        return base
    return base


def run_pipeline(request: Dict[str, Any], log: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    logger = log or (lambda msg: None)
    started_at = datetime.now().isoformat()
    started_ts = time.time()
    timings: Dict[str, Any] = {
        "prefilter_sec": None,
        "matching_sec": None,
        "coverage_sec": None,
        "modules_sec": {},
    }

    input_file = Path(str(request["input_file"])).expanduser().resolve()
    if not input_file.exists():
        raise PipelineError(f"Входной файл не найден: {input_file}")

    if not input_file.suffix.lower().endswith("xlsx"):
        raise PipelineError("Поддерживается только .xlsx")

    required_index_keys = {"brand", "color", "material", "category", "season"}
    idx = request.get("index_starts", {})
    missing_idx = sorted(required_index_keys - set(idx.keys()))
    if missing_idx:
        raise PipelineError(f"Не заданы index_starts для: {', '.join(missing_idx)}")

    timezone = str(request.get("timezone", "Europe/Tallinn"))
    output_root = Path(str(request.get("output_root") or (ROOT_DIR / "unified-output"))).expanduser().resolve()
    run_dir = next_versioned_run_dir(output_root, str(request["email"]), timezone)
    temp_root = run_dir / "tmp"
    temp_root.mkdir(parents=True, exist_ok=True)

    logger(f"🚀 [start] Unified run for: {input_file}")
    logger(f"🧹 [prefilter] input={input_file}")
    t0 = time.time()
    prefiltered_input = run_dir / "prefiltered_input.xlsx"
    main_df, main_sheet_name, prefilter_stats = prefilter_workbook(input_file, prefiltered_input)
    timings["prefilter_sec"] = round(time.time() - t0, 3)
    logger(f"✅ [prefilter] done rows={len(main_df)} sheet={main_sheet_name} in {timings['prefilter_sec']}s")

    t0 = time.time()
    matched_rows = collect_match_stats(main_df)
    matched_counts = {m: len(ids) for m, ids in matched_rows.items()}
    timings["matching_sec"] = round(time.time() - t0, 3)
    logger("🧭 [matching] " + ", ".join(f"{m}={matched_counts[m]}" for m in MODULE_ORDER) + f" in {timings['matching_sec']}s")

    t0 = time.time()
    coverage_union: Set[int] = set()
    for ids in matched_rows.values():
        coverage_union.update(ids)

    without_mapping_file = None
    if ROW_ID_COL in main_df.columns:
        mask_without = ~main_df[ROW_ID_COL].astype(int).isin(coverage_union)
        df_without = main_df.loc[mask_without].copy()
        if not df_without.empty:
            without_mapping_file = run_dir / "rows_without_any_mapping.xlsx"
            df_without.to_excel(without_mapping_file, index=False)
            logger(f"📭 [coverage] rows_without_any_mapping={len(df_without)}")
    timings["coverage_sec"] = round(time.time() - t0, 3)
    logger(f"⏱️ [coverage] done in {timings['coverage_sec']}s")

    env = build_env(request)

    module_results: Dict[str, Any] = {}
    for module_name in MODULE_ORDER:
        module_t0 = time.time()
        module_out = run_dir / MODULE_OUTPUT_DIRS[module_name]
        row_ids = matched_rows[module_name]
        matched_count = len(row_ids)

        if matched_count == 0:
            module_results[module_name] = {
                "status": "NO_MAPPING",
                "matched_rows": 0,
                "matched_row_ids": [],
                "message": "В prefiltered-файле нет релевантных строк для этого модуля.",
            }
            module_results[module_name]["llm_metrics"] = {
                "rows_in_scope_input": 0,
                "llm_sent_after_dedup": 0,
                "module_rows_output": 0,
                "note": "NO_MAPPING",
            }
            module_results[module_name]["duration_sec"] = 0.0
            timings["modules_sec"][module_name] = 0.0
            logger(f"😴 [{module_name}] NO_MAPPING in 0.0s")
            continue

        logger(f"🔄 [{module_name}] start rows={matched_count}")
        try:
            result: Dict[str, Any]
            if module_name == "brand":
                result = run_brand(request, prefiltered_input, module_out, env)
            elif module_name == "color":
                result = run_color(request, prefiltered_input, module_out, env)
            elif module_name == "material":
                result = run_material(request, prefiltered_input, module_out, env, temp_root)
            elif module_name == "category":
                result = run_category(request, prefiltered_input, module_out, env)
            elif module_name == "season":
                result = run_season(request, prefiltered_input, module_out, env)
                result["llm_enabled"] = bool(request.get("use_llm_season", False))
            elif module_name == "size":
                result = run_size(request, prefiltered_input, module_out, env)
            else:
                raise PipelineError(f"Неизвестный модуль: {module_name}")

            result["matched_rows"] = matched_count
            result["matched_row_ids"] = row_ids
            result["llm_metrics"] = summarize_llm_metrics(module_name, result)
            module_dur = round(time.time() - module_t0, 3)
            result["duration_sec"] = module_dur
            timings["modules_sec"][module_name] = module_dur
            module_results[module_name] = result
            if module_name == "material":
                warnings = ((result.get("report") or {}).get("warnings") or [])
                if warnings:
                    logger(f"⚠️ [material] warnings={len(warnings)}; first='{warnings[0]}'")
            logger(f"✅ [{module_name}] done in {module_dur}s")
        except Exception as e:
            module_dur = round(time.time() - module_t0, 3)
            module_results[module_name] = {
                "status": "FAILED",
                "matched_rows": matched_count,
                "matched_row_ids": row_ids,
                "error": str(e),
                "duration_sec": module_dur,
                "llm_metrics": {
                    "rows_in_scope_input": matched_count,
                    "llm_sent_after_dedup": None,
                    "module_rows_output": None,
                    "note": "FAILED before metrics collection",
                },
            }
            timings["modules_sec"][module_name] = module_dur
            logger(f"❌ [{module_name}] FAILED in {module_dur}s: {e}")

    ended_at = datetime.now().isoformat()
    duration_sec = round(time.time() - started_ts, 3)

    # Temporary configs are needed only during execution.
    if temp_root.exists():
        shutil.rmtree(temp_root, ignore_errors=True)

    status_counts = {"OK": 0, "NO_MAPPING": 0, "FAILED": 0}
    for item in module_results.values():
        st = str(item.get("status", "FAILED"))
        if st not in status_counts:
            st = "FAILED"
        status_counts[st] += 1

    report = {
        "run": {
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_sec": duration_sec,
            "run_dir": str(run_dir),
            "input_file": str(input_file),
            "prefiltered_input": str(prefiltered_input),
            "main_sheet": main_sheet_name,
        },
        "request": {
            "email": request["email"],
            "index_starts": request["index_starts"],
            "timezone": timezone,
            "output_root": str(output_root),
            "use_llm_brand": bool(request.get("use_llm_brand", True)),
            "use_llm_season": bool(request.get("use_llm_season", False)),
            "require_llm_success": bool(request.get("require_llm_success", True)),
            "openai_base_url": request.get("openai_base_url"),
            "openai_api_key_source": "env",
            "openai_api_key_in_env": bool(env.get("OPENAI_API_KEY")),
        },
        "prefilter": prefilter_stats,
        "matching": {
            "counts_by_module": {k: len(v) for k, v in matched_rows.items()},
            "rows_in_prefiltered_sheet": int(len(main_df)),
            "rows_covered_by_any_module": int(len(coverage_union)),
            "rows_without_any_mapping": int(len(main_df) - len(coverage_union)),
            "rows_without_any_mapping_file": str(without_mapping_file) if without_mapping_file else None,
        },
        "modules": module_results,
        "summary": {
            "status_counts": status_counts,
            "has_failures": bool(status_counts["FAILED"] > 0),
            "failed_modules": [name for name, item in module_results.items() if item.get("status") == "FAILED"],
        },
        "timings": {
            **timings,
            "modules_total_sec": round(sum(float(v) for v in timings["modules_sec"].values()), 3),
            "total_sec": duration_sec,
        },
    }

    report_path = run_dir / "run_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["report_file"] = str(report_path)

    logger(f"🎉 [done] report={report_path} total={duration_sec}s")
    return report


def parse_request_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Unified Oskelly orchestrator (CLI mode)")
    parser.add_argument("--request-json", required=True, help="Path to JSON file with request payload")
    args = parser.parse_args()

    req = parse_request_json(Path(args.request_json))
    result = run_pipeline(req, log=lambda m: print(m, flush=True))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
