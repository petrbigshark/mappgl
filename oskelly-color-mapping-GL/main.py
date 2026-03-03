from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from llm_client import ColorLLMBatchClient


TEMPLATE_COLUMNS = [
    "id",
    "objectType",
    "expr",
    "outputValue",
    "matchType",
    "repeatMatching",
    "params",
    "indexNumber",
    "clientEmail",
    "oskellyId",
]


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _sheet_key(name: Any) -> str:
    return "".join(str(name or "").split()).casefold()


def resolve_input_sheet(path: Path, preferred_sheet: Any = None) -> str:
    with pd.ExcelFile(path) as xls:
        sheet_names = list(xls.sheet_names)
    if not sheet_names:
        raise ValueError(f"No sheets in input file: {path}")

    by_key = {_sheet_key(name): name for name in sheet_names}
    if isinstance(preferred_sheet, str) and preferred_sheet.strip():
        hit = by_key.get(_sheet_key(preferred_sheet))
        if hit:
            return hit

    hit = by_key.get(_sheet_key("Result 1"))
    if hit:
        return hit

    if isinstance(preferred_sheet, int) and 0 <= preferred_sheet < len(sheet_names):
        return sheet_names[preferred_sheet]
    return sheet_names[0]


def next_versioned_output_dir(base_dir: Path, email: str, date_str: str) -> tuple[Path, int]:
    ensure_dir(base_dir)
    version = 1
    while True:
        run_dir = base_dir / f"{email} {date_str} v{version}"
        if not run_dir.exists():
            run_dir.mkdir(parents=True, exist_ok=False)
            return run_dir, version
        version += 1


def consolidate_final_by_expr(df: pd.DataFrame, expr_col: str) -> pd.DataFrame:
    """
    Build final one-row-per-expr mapping after all LLM batches are merged.
    If one expr has multiple outputValue variants, pick the most frequent one.
    Tie-break: prefer non-'Other', then earliest appearance.
    """
    working = df[[expr_col, "outputValue"]].copy()
    working["_row_order"] = range(len(working))

    stats = (
        working.groupby([expr_col, "outputValue"], as_index=False)
        .agg(cnt=("outputValue", "size"), first_order=("_row_order", "min"))
    )
    stats["_is_other"] = (stats["outputValue"] == "Other").astype(int)

    best = (
        stats.sort_values(
            by=[expr_col, "cnt", "_is_other", "first_order"],
            ascending=[True, False, True, True],
        )
        .drop_duplicates(subset=[expr_col], keep="first")
    )

    best_map = dict(zip(best[expr_col], best["outputValue"]))
    final_df = working.drop_duplicates(subset=[expr_col], keep="first")[[expr_col]].copy()
    final_df["outputValue"] = final_df[expr_col].map(best_map).fillna("Other")
    return final_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Excel → ONE-SHOT GPT color normalization → template Excel")

    parser.add_argument("--input", required=True, help="Path to input Excel (.xlsx)")
    parser.add_argument("--config", default="config.yml", help="Path to config.yml")
    parser.add_argument("--outdir", default="output", help="Output directory")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call OpenAI; write 'Other' for all output values",
    )

    parser.add_argument("--index-start", type=int, required=True, help="indexNumber start value")
    parser.add_argument("--email", required=True, help="clientEmail value")
    parser.add_argument("--oskelly-id", default="", help="oskellyId (optional)")
    parser.add_argument(
        "--no-versioned-output",
        action="store_true",
        help="Write output directly to --outdir without date vN subfolder",
    )

    args = parser.parse_args()
    cfg = load_config(args.config)

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    outdir_base = Path(args.outdir)
    ensure_dir(outdir_base)

    date_short = datetime.now().strftime("%d.%m")
    date_full = datetime.now().strftime("%d.%m.%Y")
    if args.no_versioned_output:
        run_outdir = outdir_base
        output_version = None
    else:
        run_outdir, output_version = next_versioned_output_dir(outdir_base, args.email, date_full)

    # -------------------------
    # READ EXCEL
    # -------------------------
    preferred_sheet = cfg["input"].get("sheet_name", "Result 1")
    sheet = resolve_input_sheet(input_path, preferred_sheet)
    df = pd.read_excel(input_path, sheet_name=sheet)

    reason_col = cfg["input"]["reason_column"]
    brand_col = cfg["input"]["brand_column"]
    color_col = cfg["input"]["color_column"]
    reason_substr = cfg["input"]["reason_filter_substring"]

    # -------------------------
    # FILTER
    # -------------------------
    df = df[df[reason_col].astype(str).str.contains(reason_substr, na=False)].copy()

    # -------------------------
    # BUILD QUERY
    # -------------------------
    df[brand_col] = df[brand_col].astype(str).fillna("").str.strip()
    df[color_col] = df[color_col].astype(str).fillna("").str.strip()
    df["brand_color_query"] = df[brand_col] + " color " + df[color_col]

    # -------------------------
    # DEDUPLICATION
    # -------------------------
    df = df.drop_duplicates(subset=["brand_color_query"], keep="first").copy()

    allowed_colors = cfg["allowed_colors"]

    # -------------------------
    # NORMALIZATION
    # -------------------------
    llm_calls = 0
    llm_batches = 0
    llm_error = None
    rows_sent_to_llm = 0
    rows_other = 0

    if args.dry_run:
        print("⚠️ DRY-RUN MODE: outputValue='Other' for all rows, no OpenAI calls")
        df["outputValue"] = "Other"
    else:
        llm_cfg = cfg.get("llm", {})
        debug_enabled = bool(llm_cfg.get("debug_enabled", False))
        debug_dir = (run_outdir / "_llm_debug") if debug_enabled else None
        rows_sent_to_llm = int(len(df))

        print(f"🚀 LLM for all values: {rows_sent_to_llm} / {len(df)}")

        mapping: Dict[str, str] = {}
        if rows_sent_to_llm > 0:
            client = ColorLLMBatchClient(
                allowed_colors=allowed_colors,
                model=llm_cfg.get("model", "gpt-5-mini"),
                max_retries=int(llm_cfg.get("max_retries", 1)),
                timeout_sec=int(llm_cfg.get("timeout_sec", 300)),
                batch_size=int(llm_cfg.get("batch_size", 500)),
                max_total_calls=int(llm_cfg.get("max_total_calls", 6)),
                debug_dir=debug_dir,
            )
            try:
                queries = df["brand_color_query"].tolist()
                mapping = client.classify_all(queries)
                llm_calls = int((client.last_stats or {}).get("calls_made", 0))
                llm_batches = int((client.last_stats or {}).get("batches_total", 0))
            except Exception as e:
                llm_error = str(e)
                raise RuntimeError(
                    "LLM mapping failed; stopped without fallback to 'Other'. "
                    f"Details: {llm_error}"
                ) from e

        df["outputValue"] = df["brand_color_query"].map(mapping)
        missing_after_map = int(df["outputValue"].isna().sum())
        if missing_after_map > 0:
            raise RuntimeError(
                f"LLM mapping incomplete: missing {missing_after_map}/{len(df)} keys after merge. "
                "Stopped without fallback to 'Other'."
            )

        rows_other = int((df["outputValue"] == "Other").sum())
        other_ratio = (rows_other / len(df)) if len(df) else 0.0
        max_other_ratio = float(llm_cfg.get("max_other_ratio", 0.98))
        if other_ratio > max_other_ratio:
            raise RuntimeError(
                f"Suspicious LLM output: 'Other' ratio is {other_ratio:.1%}, "
                f"above llm.max_other_ratio={max_other_ratio:.0%}. "
                "Stopped to prevent bad export."
            )

    # -------------------------------
    # FINAL CONSOLIDATION (ONE ROW PER EXPR)
    # -------------------------------
    before = len(df)
    final_df = consolidate_final_by_expr(df, color_col)
    after = len(final_df)
    print(f"🧹 Final dedupe by expr: {before} → {after}")

    rows_other = int((final_df["outputValue"] == "Other").sum())

    # -------------------------
    # BUILD TEMPLATE OUTPUT
    # -------------------------
    n = len(final_df)
    index_start = args.index_start

    out_df = pd.DataFrame({
        "id": list(range(1, n + 1)),
        "objectType": ["COLOR"] * n,
        "expr": final_df[color_col].tolist(),          # WHAT WAS
        "outputValue": final_df["outputValue"].tolist(),  # WHAT BECAME
        "matchType": ["EQUALS"] * n,
        "repeatMatching": ["false"] * n,
        "params": ["{}"] * n,
        "indexNumber": list(range(index_start, index_start + n)),
        "clientEmail": [args.email] * n,
        "oskellyId": [args.oskelly_id] * n,
    })[TEMPLATE_COLUMNS]

    # -------------------------
    # OUTPUT FILE NAME
    # -------------------------
    filename = f"Цвета для загрузки {args.email} {date_short}.xlsx"
    out_path = run_outdir / filename

    out_df.to_excel(out_path, index=False)

    # -------------------------
    # REPORT
    # -------------------------
    report = {
        "input": str(input_path),
        "sheet_name": sheet,
        "output": str(out_path),
        "rows_processed": n,
        "dry_run": bool(args.dry_run),
        "model": cfg.get("llm", {}).get("model", "gpt-5-mini"),
        "rows_total_unique_before_output_dedup": int(before),
        "rows_sent_to_llm": rows_sent_to_llm,
        "rows_other": rows_other,
        "llm_calls": llm_calls,
        "llm_batches": llm_batches,
        "llm_error": llm_error,
    }

    (run_outdir / "run_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("✅ DONE")
    print(f"📄 Output file: {out_path}")
    print(
        f"📊 rows={n}, sent_to_llm={rows_sent_to_llm}, "
        f"other={rows_other}, llm_batches={llm_batches}"
    )


if __name__ == "__main__":
    main()
