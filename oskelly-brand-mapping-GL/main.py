#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Oskelly Brand Mapping (Global)

Pipeline:
1) Read input Excel (L2L export).
2) Filter rows where reason contains "Не найден бренд с названием".
3) Take unique values from column "brand".
4) Map to reference brands (brands_id_name.xlsx) using:
   - special overrides
   - KIDS -> parent
   - collab: first brand else second brand (exact only)
   - fuzzy fallback for multi-token (rapidfuzz)
   - (optional) LLM fallback for remaining unmapped (safe: must pick from provided candidates)
5) Output:
   - "Бренды для загрузки {email} {dd.mm.yyyy}.xlsx" in Global upload template format (10 cols)
   - "Незамапленные бренды {email} {dd.mm.yyyy}.xlsx" single column
Both files are saved into a versioned folder: "{email} {dd.mm.yyyy} vN"
"""

import argparse
import json
import os
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
from unidecode import unidecode
from rapidfuzz import fuzz, process


PHRASE_NOT_FOUND = "Не найден бренд с названием"
RESULT_COLUMNS = ["incoming", "mapped_to", "status", "rule", "score"]

# --- Overrides (extend as you learn) ---
SPECIAL_MAP = {
    "100%": "100% EYEWEAR",
    "BOSS": "HUGO BOSS",
}

# --- Rules ---
TAIL_WORDS = {"kids", "kid", "junior", "jr", "baby", "children", "child"}
GENERIC_BLACKLIST = {"studio", "collection", "official", "classic", "original", "label"}
COLLAB_RE = re.compile(r"\s(?:x|×|loves)\s", flags=re.IGNORECASE)


def normalize(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = s.replace("’", "'").replace("`", "'")
    s = s.replace("&", " and ")
    s = re.sub(r"[™®©]", "", s)
    s = unidecode(s)
    s = s.casefold()
    s = re.sub(r"[^a-z0-9\s'/\-]", " ", s)
    s = re.sub(r"[\-_/]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def next_version_folder(base_dir: Path, email: str, date_str: str) -> Path:
    v = 1
    while True:
        folder = base_dir / f"{email} {date_str} v{v}"
        if not folder.exists():
            return folder
        v += 1


def _sheet_key(name: str) -> str:
    return "".join(str(name or "").split()).casefold()


def resolve_input_sheet(path: Path, preferred_sheet: str = "Result 1") -> str:
    with pd.ExcelFile(path) as xls:
        sheet_names = list(xls.sheet_names)
    if not sheet_names:
        raise SystemExit(f"Во входном файле нет листов: {path}")

    by_key = {_sheet_key(name): name for name in sheet_names}
    hit = by_key.get(_sheet_key(preferred_sheet))
    if hit:
        return hit

    hit = by_key.get(_sheet_key("Result 1"))
    if hit:
        return hit
    return sheet_names[0]


def build_reference(brands_id_path: Path):
    brands_df = pd.read_excel(brands_id_path)
    if "id" not in brands_df.columns or "name" not in brands_df.columns:
        raise SystemExit("brands_id_name.xlsx должен содержать колонки: id, name")

    brands_df["name"] = brands_df["name"].astype(str).str.strip()
    name_to_id = dict(zip(brands_df["name"], brands_df["id"]))

    norm_to_ref = {}
    for name in brands_df["name"].tolist():
        n = normalize(name)
        if n and n not in norm_to_ref:
            norm_to_ref[n] = name

    ref_choices_norm = list(norm_to_ref.keys())
    return brands_df, name_to_id, norm_to_ref, ref_choices_norm


def top_candidates(norm_query: str, norm_to_ref: dict, ref_choices_norm: list, k: int = 10):
    # Returns list of reference brand names ordered by similarity
    if not norm_query:
        return []
    hits = process.extract(norm_query, ref_choices_norm, scorer=fuzz.token_set_ratio, limit=k)
    # hits: (choice_norm, score, idx)
    return [(norm_to_ref[h[0]], float(h[1])) for h in hits]


def map_rules_only(raw_incoming: str, name_to_id, norm_to_ref, ref_choices_norm):
    raw = str(raw_incoming).strip()

    # 0) overrides
    if raw in SPECIAL_MAP:
        mapped = SPECIAL_MAP[raw]
        if mapped in name_to_id:
            return {"incoming": raw, "mapped_to": mapped, "status": "mapped", "rule": "special_map", "score": 100.0}

    n = normalize(raw)
    if not n:
        return {"incoming": raw, "mapped_to": "", "status": "unmapped", "rule": "empty", "score": None}

    # 1) exact normalized
    if n in norm_to_ref:
        return {"incoming": raw, "mapped_to": norm_to_ref[n], "status": "mapped", "rule": "exact_norm", "score": 100.0}

    tokens = n.split()

    # 2) generic one-token -> unmapped
    if len(tokens) == 1 and tokens[0] in GENERIC_BLACKLIST:
        return {"incoming": raw, "mapped_to": "", "status": "unmapped", "rule": "generic_blacklist", "score": None}

    # 3) kids -> parent
    n_for_fuzzy = n
    if tokens and tokens[-1] in TAIL_WORDS:
        parent_norm = " ".join(tokens[:-1]).strip()
        if parent_norm in norm_to_ref:
            return {"incoming": raw, "mapped_to": norm_to_ref[parent_norm], "status": "mapped", "rule": "kids_to_parent", "score": 100.0}
        if parent_norm:
            n_for_fuzzy = parent_norm

    # 4) collab -> first else second (exact only)
    if COLLAB_RE.search(raw) or COLLAB_RE.search(n):
        parts = [p.strip() for p in COLLAB_RE.split(n) if p.strip()]
        if parts:
            first = parts[0]
            second = parts[1] if len(parts) > 1 else ""
            if first in norm_to_ref:
                return {"incoming": raw, "mapped_to": norm_to_ref[first], "status": "mapped", "rule": "collab_first_brand", "score": 100.0}
            if second and second in norm_to_ref:
                return {"incoming": raw, "mapped_to": norm_to_ref[second], "status": "mapped", "rule": "collab_second_brand", "score": 100.0}
            return {"incoming": raw, "mapped_to": "", "status": "unmapped", "rule": "collab_no_exact", "score": None}

    # 5) single-token strict: do not fuzzy (avoid TEKLA->TELA etc.)
    if len(n_for_fuzzy.split()) == 1:
        return {"incoming": raw, "mapped_to": "", "status": "unmapped", "rule": "single_token_no_match", "score": None}

    # 6) multi-token fuzzy high threshold
    cand = process.extractOne(n_for_fuzzy, ref_choices_norm, scorer=fuzz.token_set_ratio)
    if not cand:
        return {"incoming": raw, "mapped_to": "", "status": "unmapped", "rule": "no_match", "score": None}

    choice_norm, score, _ = cand
    mapped = norm_to_ref.get(choice_norm, "")
    score = float(score)

    # Guard (refined): avoid subset false-positives like "MA.STRUM" -> "MA+",
    # but allow legitimate short-brand mappings like "ON RUNNING" -> "ON" and "WEB EYEWEAR" -> "WEB".
    incoming_tokens = n_for_fuzzy.split()
    candidate_tokens = choice_norm.split()
    candidate_compact = choice_norm.replace(" ", "")
    incoming_compact = n_for_fuzzy.replace(" ", "")

    if (
        len(incoming_tokens) >= 2
        and len(candidate_tokens) == 1
        and len(candidate_compact) <= 3
        and not re.search(r"\d", incoming_compact)
    ):
        candidate_original = norm_to_ref.get(choice_norm, "")

        # Allow if incoming starts with the short token (e.g., "on running", "web eyewear")
        starts_with_short = incoming_tokens[0] == candidate_compact

        # Extra safety: if the candidate brand uses "+" (e.g., "MA+"), but incoming uses "." (e.g., "MA.STRUM")
        # and does NOT contain "+", then reject (common false positive).
        dot_plus_conflict = ("+" in candidate_original) and ("." in raw) and ("+" not in raw)

        if (not starts_with_short) or dot_plus_conflict:
            return {
                "incoming": raw,
                "mapped_to": "",
                "status": "unmapped",
                "rule": "fuzzy_rejected_short_candidate",
                "score": score,
            }


    if score >= 92:
        return {"incoming": raw, "mapped_to": mapped, "status": "mapped", "rule": "fuzzy_high", "score": score}

    return {"incoming": raw, "mapped_to": "", "status": "unmapped", "rule": "no_match", "score": score}


# ---------------- LLM fallback (optional) ----------------
def llm_decide_map(
    incoming: str,
    candidates: list,
    model: str,
    api_key: str | None,
    base_url: str | None
):
    """
    candidates: list of tuples (brand_name, fuzzy_score)
    Safe contract: LLM must choose mapped_to from candidates OR return UNIQUE.
    Returns dict: {decision, mapped_to, confidence, note}
    """
    if not candidates:
        return {"decision": "UNIQUE", "mapped_to": "", "confidence": 0.0, "note": "no_candidates"}

    # Minimal dependency: use OpenAI compatible chat.completions endpoint via requests
    import requests

    url = (base_url.rstrip("/") if base_url else "https://api.openai.com") + "/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    cand_lines = "\n".join([f"- {name} (fuzzy={score:.1f})" for name, score in candidates])

    system = (
        "You are a strict brand-matching assistant. "
        "You MUST choose mapped_to ONLY from the provided candidates list, or return UNIQUE. "
        "Do not invent brands. If unsure, return UNIQUE."
    )
    user = (
        f"Incoming brand: {incoming}\n\n"
        f"Candidates (choose ONLY from this list):\n{cand_lines}\n\n"
        "Rules:\n"
        "- If incoming is a collaboration (X/×/LOVES), map to the first brand if it exists in candidates; "
        "otherwise map to the second.\n"
        "- If incoming is a sub-line like 'Kids', map to the parent brand.\n\n"
        "Return JSON ONLY with keys: decision, mapped_to, confidence, note.\n"
        "decision must be MAP or UNIQUE.\n"
        "confidence: number 0..1.\n"
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "response_format": {"type": "json_object"},
    }

    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    r.raise_for_status()
    data = r.json()
    content = data["choices"][0]["message"]["content"]

    try:
        obj = json.loads(content)
    except Exception:
        return {"decision": "UNIQUE", "mapped_to": "", "confidence": 0.0, "note": "bad_json"}

    decision = str(obj.get("decision", "UNIQUE")).upper().strip()
    mapped_to = str(obj.get("mapped_to", "")).strip()
    confidence = obj.get("confidence", 0.0)
    note = str(obj.get("note", "")).strip()

    # Validate
    cand_names = {c[0] for c in candidates}
    if decision == "MAP" and mapped_to in cand_names:
        try:
            confidence = float(confidence)
        except Exception:
            confidence = 0.0
        return {"decision": "MAP", "mapped_to": mapped_to, "confidence": confidence, "note": note}

    return {"decision": "UNIQUE", "mapped_to": "", "confidence": 0.0, "note": "not_allowed_or_unknown"}


def load_cache(cache_path: Path) -> dict:
    if not cache_path.exists():
        return {}
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache_path: Path, cache: dict):
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def build_upload_df(mapped_df: pd.DataFrame, template_path: Path, name_to_id: dict, index_start: int, email: str) -> pd.DataFrame:
    template_cols = pd.read_excel(template_path).columns.tolist()

    rows = []
    index_num = index_start
    for i, r in enumerate(mapped_df.sort_values("incoming").itertuples(index=False), start=1):
        mapped_to = r.mapped_to
        osk_id = name_to_id.get(mapped_to, None)

        rows.append({
            "id": i,
            "objectType": "BRAND",
            "expr": r.incoming,
            "outputValue": mapped_to,
            "matchType": "EQUALS",
            "repeatMatching": "false",  # IMPORTANT: lowercase string
            "params": "{}",
            "indexNumber": index_num,
            "clientEmail": email,
            "oskellyId": osk_id
        })
        index_num += 1

    out = pd.DataFrame(rows).reindex(columns=template_cols)
    return out




def map_hidden_unmapped(unmapped_list, hidden_path: Path):
    """
    Прогоняем незамапленные бренды через brands_hidden.xlsx.
    Возвращает dict: incoming -> hidden_brand_name (если нашли).
    Если hidden-файл отсутствует — вернёт пустой dict.
    """
    hidden_path = Path(hidden_path)
    if not hidden_path.exists():
        return {}

    _hdf, h_name_to_id, h_norm_to_ref, h_ref_choices_norm = build_reference(hidden_path)

    out = {}
    for incoming in unmapped_list:
        r = map_rules_only(incoming, h_name_to_id, h_norm_to_ref, h_ref_choices_norm)
        if r.get("status") == "mapped" and r.get("mapped_to"):
            out[incoming] = r["mapped_to"]
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Входной xlsx (L2L.xlsx)")
    ap.add_argument("--index-start", required=True, type=int, help="С какого indexNumber начинать в файле загрузки Global")
    ap.add_argument("--email", required=True, help="clientEmail для файла загрузки Global")

    ap.add_argument("--brands-id-name", default="brands_id_name.xlsx", help="Справочник брендов (id,name)")
    ap.add_argument("--template", default="Бренды для загрузки Глобал.xlsx", help="Шаблон 10-колоночного файла загрузки")
    ap.add_argument("--outdir", default=".", help="Куда класть папку с результатами (по умолчанию текущая директория)")
    ap.add_argument("--brands-hidden", default="brands_hidden.xlsx", help="Справочник скрытых брендов (id,name) — опционально")


    # LLM options
    ap.add_argument("--use-llm", action="store_true", help="Включить LLM fallback для незамапленных")
    ap.add_argument("--llm-model", default="gpt-5.1", help="Модель для /v1/chat/completions (OpenAI-compatible)")
    ap.add_argument("--llm-candidates", type=int, default=10, help="Сколько кандидатов отдавать LLM")
    ap.add_argument("--llm-min-confidence", type=float, default=0.75, help="Минимальная уверенность, чтобы принять MAP")
    ap.add_argument("--llm-base-url", default=None, help="Базовый URL (если не OpenAI). Пример: https://api.openai.com")
    ap.add_argument("--llm-cache", default="llm_cache.json", help="Файл кеша решений LLM (JSON)")

    args = ap.parse_args()

    input_path = Path(args.input)
    brands_id_path = Path(args.brands_id_name)
    template_path = Path(args.template)
    # Всегда кладём в папку "output" рядом со скриптом
    script_dir = Path(__file__).resolve().parent
    out_base = script_dir / "output"

    # Создаём папку, если её нет (не пересоздаёт!)
    out_base.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise SystemExit(f"Не найден input-файл: {input_path}")
    if not brands_id_path.exists():
        raise SystemExit(f"Не найден brands-id-name файл: {brands_id_path}")
    if not template_path.exists():
        raise SystemExit(f"Не найден template файл: {template_path}")

    date_str = datetime.now().strftime("%d.%m.%Y")
    out_folder = next_version_folder(out_base, args.email, date_str)
    out_folder.mkdir(parents=True, exist_ok=False)

    sheet_name = resolve_input_sheet(input_path, preferred_sheet="Result 1")
    df = pd.read_excel(input_path, sheet_name=sheet_name)
    if "reason" not in df.columns or "brand" not in df.columns:
        raise SystemExit("В input-файле должны быть колонки reason и brand")

    mask = df["reason"].astype(str).str.contains(PHRASE_NOT_FOUND, case=False, na=False)
    incoming = df.loc[mask, "brand"].dropna().astype(str).str.strip()
    unique_incoming = pd.Series(incoming.unique()).tolist()

    # reference
    _brands_df, name_to_id, norm_to_ref, ref_choices_norm = build_reference(brands_id_path)

    # pass 1: rules+fuzzy
    results = []
    for b in unique_incoming:
        results.append(map_rules_only(b, name_to_id, norm_to_ref, ref_choices_norm))
    results_df = pd.DataFrame(results, columns=RESULT_COLUMNS)

    # pass 2: LLM fallback for unmapped (safe)
    if args.use_llm:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise SystemExit("Нужен OPENAI_API_KEY в переменной окружения для --use-llm")

        cache_path = out_folder / args.llm_cache
        cache = load_cache(cache_path)

        unmapped_mask = results_df["status"] != "mapped"
        for idx, row in results_df[unmapped_mask].iterrows():
            incoming_brand = row["incoming"]
            if incoming_brand in cache:
                mapped_to = cache[incoming_brand]
                if mapped_to and mapped_to in name_to_id:
                    results_df.loc[idx, ["mapped_to", "status", "rule", "score"]] = [mapped_to, "mapped", "llm_cache", 100.0]
                continue

            n = normalize(incoming_brand)

            # For collabs, we still feed full incoming but candidates computed from:
            # - first part, then second part
            cand_list = top_candidates(n, norm_to_ref, ref_choices_norm, k=args.llm_candidates)

            decision = llm_decide_map(
                incoming=incoming_brand,
                candidates=cand_list,
                model=args.llm_model,
                api_key=api_key,
                base_url=args.llm_base_url,
            )

            if decision["decision"] == "MAP" and decision["confidence"] >= args.llm_min_confidence:
                mapped_to = decision["mapped_to"]
                if mapped_to in name_to_id:
                    results_df.loc[idx, ["mapped_to", "status", "rule", "score"]] = [
                        mapped_to, "mapped", "llm", float(decision["confidence"])
                    ]
                    cache[incoming_brand] = mapped_to
                else:
                    cache[incoming_brand] = ""
            else:
                cache[incoming_brand] = ""

        save_cache(cache_path, cache)

    mapped = results_df[results_df["status"] == "mapped"][["incoming", "mapped_to", "rule", "score"]].copy()
    unmapped = results_df[results_df["status"] != "mapped"][["incoming"]].copy()

    upload_df = build_upload_df(mapped, template_path, name_to_id, args.index_start, args.email)

    out_upload_name = f"Бренды для загрузки {args.email} {date_str}.xlsx"
    out_unmapped_name = f"Незамапленные бренды {args.email} {date_str}.xlsx"

    out_upload_path = out_folder / out_upload_name
    out_unmapped_path = out_folder / out_unmapped_name

    upload_df.to_excel(out_upload_path, index=False)

    unmapped_list = sorted(unmapped["incoming"].astype(str).tolist())

    # Прогоняем незамапленные бренды через скрытые (brands_hidden.xlsx), если файл есть
    hidden_map = map_hidden_unmapped(unmapped_list, Path(args.brands_hidden))

    # 2 колонки: "Незамапленные бренды" (всегда incoming) и "Скрытые бренды" (если нашли в справочнике hidden)
    out_rows = []
    for b in unmapped_list:
        out_rows.append({
            "Незамапленные бренды": b,
            "Скрытые бренды": hidden_map.get(b, "")
        })
    pd.DataFrame(out_rows).to_excel(out_unmapped_path, index=False)

    # Optional debug
    results_df.to_excel(out_folder / "debug_all_rows.xlsx", index=False)

    print("OK")
    print(f"Saved: {out_upload_path}")
    print(f"Saved: {out_unmapped_path}")
    print(f"Folder: {out_folder}")


if __name__ == "__main__":
    main()
