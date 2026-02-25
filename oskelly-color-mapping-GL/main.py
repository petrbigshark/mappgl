# main.py
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from llm_client import ColorLLMBatchClient
from heuristic import normalize_color_heuristic


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Excel → ONE-SHOT GPT color normalization → template Excel")

    parser.add_argument("--input", required=True, help="Path to input Excel (.xlsx)")
    parser.add_argument("--config", default="config.yml", help="Path to config.yml")
    parser.add_argument("--outdir", default="output", help="Output directory")
    parser.add_argument("--dry-run", action="store_true", help="Do not call OpenAI, use heuristic")

    parser.add_argument("--index-start", type=int, required=True, help="indexNumber start value")
    parser.add_argument("--email", required=True, help="clientEmail value")
    parser.add_argument("--oskelly-id", default="", help="oskellyId (optional)")

    args = parser.parse_args()
    cfg = load_config(args.config)

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    outdir = Path(args.outdir)
    ensure_dir(outdir)

    # -------------------------
    # READ EXCEL
    # -------------------------
    sheet = cfg["input"].get("sheet_name", 0)
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
    if args.dry_run:
        print("⚠️ DRY-RUN MODE: using heuristic, no OpenAI calls")
        df["outputValue"] = df[color_col].apply(normalize_color_heuristic)
    else:
        print(f"🚀 ONE-SHOT GPT call for {len(df)} unique values")
        client = ColorLLMBatchClient(
            allowed_colors=allowed_colors,
            model=cfg["llm"]["model"],
        )

        queries = df["brand_color_query"].tolist()
        mapping = client.classify_all(queries)

        df["outputValue"] = df["brand_color_query"].map(mapping).fillna("Other")

    # -------------------------------
    # DEDUPE BY (expr + outputValue)
    # -------------------------------
    before = len(df)
    df = df.drop_duplicates(subset=[color_col, "outputValue"], keep="first").copy()
    after = len(df)

    print(f"🧹 Deduplicated by (expr + outputValue): {before} → {after}")

    # -------------------------
    # BUILD TEMPLATE OUTPUT
    # -------------------------
    n = len(df)
    index_start = args.index_start

    out_df = pd.DataFrame({
        "id": list(range(1, n + 1)),
        "objectType": ["COLOR"] * n,
        "expr": df[color_col].tolist(),          # WHAT WAS
        "outputValue": df["outputValue"].tolist(),  # WHAT BECAME
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
    date_str = datetime.now().strftime("%d.%m")
    filename = f"Цвета для загрузки {args.email} {date_str}.xlsx"
    out_path = outdir / filename

    out_df.to_excel(out_path, index=False)

    # -------------------------
    # REPORT
    # -------------------------
    report = {
        "input": str(input_path),
        "output": str(out_path),
        "rows_processed": n,
        "dry_run": bool(args.dry_run),
        "model": cfg["llm"]["model"],
    }

    (outdir / "run_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("✅ DONE")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

