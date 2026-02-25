import argparse
from datetime import date
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any

import pandas as pd
import yaml

from llm_client import LLMClient, LLMConfig, LLMError


OUTPUT_COLUMNS = [
    "id",
    "objectType",
    "expr",
    "outputValue",
    "matchType",
    "repeatMatching",
    "params",
    "indexNumber",
    "clientEmail",
]


def load_config(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_material_type_ids(path: Path) -> Dict[str, int]:
    df = pd.read_excel(path)
    if not {"id", "name"}.issubset(df.columns):
        raise SystemExit("id_materials.xlsx должен содержать колонки 'id' и 'name'")
    mapping: Dict[str, int] = {}
    for _, row in df.iterrows():
        name = str(row["name"]).strip()
        if not name:
            continue
        mapping[name] = int(row["id"])
    return mapping


def load_material_catalog(path: Path) -> Dict[str, List[str]]:
    df = pd.read_excel(path)
    required_cols = {"attribute_name", "attribute_value"}
    if not required_cols.issubset(df.columns):
        raise SystemExit(
            "material_types.xlsx должен содержать колонки 'attribute_name' и 'attribute_value'"
        )
    catalog: Dict[str, List[str]] = {}
    for _, row in df.iterrows():
        attr_name = str(row["attribute_name"]).strip()
        attr_val = str(row["attribute_value"]).strip()
        if not attr_name or not attr_val:
            continue
        catalog.setdefault(attr_name, []).append(attr_val)
    for k, values in list(catalog.items()):
        seen = set()
        uniq: List[str] = []
        for v in values:
            if v not in seen:
                seen.add(v)
                uniq.append(v)
        catalog[k] = uniq
    return catalog

def llm_map_categories(
    client: LLMClient,
    df: pd.DataFrame,
    category_col: str,
    material_type_names: List[str],
) -> Dict[str, str]:
    cats = sorted((str(x).strip() for x in df[category_col].dropna().unique()))
    mapping: Dict[str, str] = {}
    system_prompt = (
        "You help an online marketplace to determine the MATERIAL TYPE of a product.\n"
        "You are given only the product category (in English, as it is in the feed).\n"
        "You must choose exactly ONE material type name from the list.\n"
        "Return ONLY the chosen option, nothing else.\n"
    )
    for cat in cats:
        desc = f"Product category: {cat}"
        material_type = client.choose_from_list(
            system_prompt=system_prompt,
            question=desc,
            options=material_type_names,
        )
        mapping[cat] = material_type or "UNKNOWN"
    return mapping

def llm_map_materials(
    client: LLMClient,
    pairs: List[Tuple[str, str]],
    material_catalog: Dict[str, List[str]],
    warnings: List[str],
) -> Dict[Tuple[str, str], str]:
    mapping: Dict[Tuple[str, str], str] = {}
    warned_missing_types = set()
    system_prompt = (
        "You help an online marketplace normalize product materials into a clean catalog.\n"
        "You are given:\n"
        "1) A MATERIAL TYPE (for example: bag material, shoe material, clothing material).\n"
        "2) A RAW material string from the shop feed.\n"
        "You must choose exactly ONE value from the allowed list for this material type.\n"
        "If the raw string contains several materials with percentages, choose the one "
        "with the highest percentage. If there are no percentages, choose the FIRST "
        "material mentioned.\n"
        "If nothing from the list can reasonably match, you MUST choose 'Other' "
        "if it exists. Use 'UNKNOWN' only when mapping is completely impossible.\n"
        "Answer with ONLY the chosen catalog value, nothing else.\n"
    )

    for mat_type, raw_material in pairs:
        mat_type_clean = str(mat_type).strip()
        raw_clean = str(raw_material).strip()
        allowed = material_catalog.get(mat_type_clean)

        if not allowed:
            if mat_type_clean not in warned_missing_types:
                warnings.append(
                    f"No catalog of materials for type '{mat_type_clean}'. "
                    "All such materials will be marked as UNKNOWN."
                )
                warned_missing_types.add(mat_type_clean)
            mapping[(mat_type, raw_material)] = "UNKNOWN"
            continue

        desc = (
            f"Material type: {mat_type_clean}\n"
            f"Raw material from shop: {raw_clean}"
        )

        choice = client.choose_from_list(
            system_prompt=system_prompt,
            question=desc,
            options=allowed,
        )

        mapping[(mat_type, raw_material)] = choice or "UNKNOWN"

    return mapping


def build_output_rows(
    df: pd.DataFrame,
    material_type_ids: Dict[str, int],
    material_mapping: Dict[Tuple[str, str], str],
    index_start: int,
    client_email: str,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    current_index = index_start
    for _, row in df.iterrows():
        material_type_name = str(row["material_type_name"]).strip()
        raw_material = str(row["material"]).strip()
        key = (material_type_name, raw_material)
        mapped_material = material_mapping.get(key, "UNKNOWN")
        mat_type_id = material_type_ids.get(material_type_name)
        if mat_type_id is None:
            mat_type_id = 0
        params = json.dumps({"materialTypes": [mat_type_id]}, ensure_ascii=False)
        rows.append(
            {
                "id": len(rows) + 1,
                "objectType": "MATERIAL",
                "outputValue": mapped_material,
                "expr": raw_material,
                "matchType": "EQUALS",
                "repeatMatching": "false",
                "params": params,
                "indexNumber": current_index,
                "clientEmail": client_email,
            }
        )
        current_index += 1
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def main() -> None:
    parser = argparse.ArgumentParser(description="Маппинг материалов в формат OSkelly")
    parser.add_argument("--input", required=True)
    parser.add_argument("--index-start", required=True, type=int)
    parser.add_argument("--email", required=True)
    parser.add_argument("--config", default="config.yml")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    input_cfg = cfg.get("input", {})
    sheet_name = input_cfg.get("sheet_name") or 0
    reason_col = input_cfg.get("reason_column", "reason")
    category_col = input_cfg.get("category_column", "category")
    material_col = input_cfg.get("material_column", "material")
    filter_cfg = cfg.get("filter", {})
    reason_contains = filter_cfg.get("reason_contains", "")
    refs_cfg = cfg.get("references", {})
    id_xlsx = Path(refs_cfg.get("material_type_ids_xlsx", "id_materials.xlsx"))
    catalog_xlsx = Path(refs_cfg.get("material_catalog_xlsx", "material_types.xlsx"))
    llm_cfg = cfg.get("llm", {})
    llm_config = LLMConfig(
        model=llm_cfg.get("model", "gpt-5-mini"),
        max_output_tokens=int(llm_cfg.get("max_output_tokens", 300)),
    )

    client = LLMClient(llm_config) 

    app_cfg = cfg.get("app", {})
    out_dir = Path(app_cfg.get("output_dir", "output"))
    out_dir.mkdir(parents=True, exist_ok=True)

    df_input = pd.read_excel(args.input, sheet_name=sheet_name)
    if reason_contains:
        mask = (
            df_input[reason_col]
            .astype(str)
            .str.contains(reason_contains, case=False, na=False)
        )
        df_filtered = df_input.loc[mask].copy()
    else:
        df_filtered = df_input.copy()
    if df_filtered.empty:
        raise SystemExit("После фильтрации не осталось строк для обработки.")

    material_type_ids = load_material_type_ids(id_xlsx)
    material_catalog = load_material_catalog(catalog_xlsx)
    material_type_names = list(material_type_ids.keys())

    warnings: List[str] = []

    llm_metrics: Dict[str, Any] = {
        "enabled": False,
        "unique_categories": 0,
        "unique_pairs": 0,
        "llm_sent_after_dedup": 0,
    }

    if args.dry_run:
        df_filtered["material_type_name"] = "UNKNOWN"
        material_mapping = {
            (str(r["material_type_name"]).strip(), str(r[material_col]).strip()): "UNKNOWN"
            for _, r in df_filtered.iterrows()
        }
    else:
        client = LLMClient(llm_config)
        cat_to_type = llm_map_categories(client, df_filtered, category_col, material_type_names)
        llm_metrics["enabled"] = True
        llm_metrics["unique_categories"] = int(len(cat_to_type))
        df_filtered["material_type_name"] = (
            df_filtered[category_col].astype(str).map(lambda x: cat_to_type.get(str(x).strip(), "UNKNOWN"))
        )
        pairs_set = {
            (str(r["material_type_name"]).strip(), str(r[material_col]).strip())
            for _, r in df_filtered.iterrows()
        }
        pairs = sorted(pairs_set)
        llm_metrics["unique_pairs"] = int(len(pairs))
        llm_metrics["llm_sent_after_dedup"] = int(len(cat_to_type) + len(pairs))
        material_mapping = llm_map_materials(
            client,
            pairs,
            material_catalog,
            warnings,
        )

    df_filtered.rename(columns={material_col: "material"}, inplace=True)

    out = build_output_rows(
        df_filtered,
        material_type_ids,
        material_mapping,
        args.index_start,
        client_email=args.email,
    )

        # ---------- УБИРАЕМ ДУБЛИКАТЫ МАППИНГОВ ----------
    # 1. Чуть отсортируем, чтобы выбор "первого" был предсказуем
    out = (
        out.sort_values(["objectType", "expr", "params"])
           .drop_duplicates(subset=["expr", "params"], keep="first")
           .reset_index(drop=True)
    )

    # 2. Пересчитываем id и indexNumber, чтобы не было дырок
    out["id"] = range(1, len(out) + 1)
    out["indexNumber"] = range(args.index_start, args.index_start + len(out))

    # 3. Пишем Excel с материалами
    date_str = date.today().isoformat()
    out_path = out_dir / f"Материалы для загрузки {args.email} {date_str}.xlsx"
    out.to_excel(out_path, index=False)

    # ---------- ОТЧЁТ ----------
    report = {
        "input_file": Path(args.input).name,
        "rows_total": int(len(df_input)),
        "rows_after_filter": int(len(df_filtered)),
        "rows_final_output": int(len(out)),
        "warnings": warnings,
        "errors": [],
        "llm": llm_metrics,
    }
    report_name = app_cfg.get("report_name", "run_report.json")
    with (out_dir / report_name).open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"OK: wrote {out_path}")
    print(f"Report: {out_dir / report_name}")


if __name__ == "__main__":
    main()
