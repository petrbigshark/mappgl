\
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import yaml
from rapidfuzz import fuzz
from zoneinfo import ZoneInfo

from llm_client import LLMConfig, ResponsesFullDictMapper


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def read_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _sheet_key(name: Any) -> str:
    return "".join(str(name or "").split()).casefold()


def resolve_input_sheet(path: Path, preferred_sheet: Any = None) -> str:
    with pd.ExcelFile(path) as xls:
        sheet_names = list(xls.sheet_names)
    if not sheet_names:
        die(f"No sheets in input file: {path}")

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


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())


def norm_key_for_lookup(s: str) -> str:
    return normalize_space(s).casefold()


def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def path_depth(full_path: str) -> int:
    if not full_path:
        return 0
    if ">" in full_path:
        return len([p.strip() for p in full_path.split(">") if p.strip()])
    if "/" in full_path:
        return len([p.strip() for p in full_path.split("/") if p.strip()])
    return 1


def contains_any(hay: str, needles: List[str]) -> bool:
    h = normalize_space(hay).casefold()
    return any(n.casefold() in h for n in needles)


def detect_group(parentcategory: str, cfg: Dict[str, Any]) -> str:
    s = normalize_space(parentcategory).casefold()
    gcfg = cfg["grouping"]

    def any_marker(markers: List[str]) -> bool:
        for marker in markers:
            marker_n = normalize_space(marker).casefold()
            if not marker_n:
                continue
            pattern = r"(?<!\w)" + r"\s+".join(re.escape(part) for part in marker_n.split()) + r"(?!\w)"
            if re.search(pattern, s):
                return True
        return False

    if any_marker(gcfg["lifestyle_markers"]):
        return "LIFESTYLE"
    if any_marker(gcfg["men_markers"]):
        return "MEN"
    if any_marker(gcfg["women_markers"]):
        return "WOMEN"
    if any_marker(gcfg["unisex_markers"]):
        return "WOMEN"
    return gcfg.get("default_group", "WOMEN")


def ensure_output_run_dir(output_root: Path, email: str, tz_name: str) -> Tuple[Path, str, int]:
    tz = ZoneInfo(tz_name)
    date_str = datetime.now(tz=tz).strftime("%d.%m.%Y")
    base = f"{email} {date_str}"
    existing = []
    if output_root.exists():
        for p in output_root.iterdir():
            if p.is_dir() and p.name.startswith(base) and re.search(r"\sv(\d+)$", p.name):
                m = re.search(r"\sv(\d+)$", p.name)
                if m:
                    existing.append(int(m.group(1)))
    v = (max(existing) + 1) if existing else 1
    run_dir = output_root / f"{base} v{v}"
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "cache").mkdir(parents=True, exist_ok=True)
    return run_dir, date_str, v


@dataclass(frozen=True)
class SeedEntry:
    output_value: str
    confidence: float
    is_conflict: bool


def load_seed(csv_path: Path) -> Dict[str, SeedEntry]:
    if not csv_path.exists():
        return {}
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig").fillna("")
    if {"expr", "outputValue"}.issubset(df.columns) and not {"confidence", "is_conflict"}.issubset(df.columns):
        out = {}
        for _, r in df.iterrows():
            out[norm_key_for_lookup(r["expr"])] = SeedEntry(str(r["outputValue"]), 1.0, False)
        return out
    required = {"expr", "outputValue", "confidence", "is_conflict"}
    if not required.issubset(df.columns):
        raise ValueError(f"seed CSV must contain columns {required} or at least expr,outputValue")
    out = {}
    for _, r in df.iterrows():
        key = norm_key_for_lookup(r["expr"])
        conf = float(r.get("confidence", "0") or 0)
        is_conf = str(r.get("is_conflict", "")).strip().lower() in ("true", "1", "yes")
        out[key] = SeedEntry(str(r["outputValue"]), conf, is_conf)
    return out


def reduce_description(description: str, full_path: str, output_value: str, cfg: Dict[str, Any]) -> str:
    if not cfg["desc_reduction"].get("enabled", True):
        return normalize_space(description)
    desc = normalize_space(description)
    if not desc:
        return ""
    tokens = desc.split(" ")
    target_text = f"{full_path} {output_value}".strip()
    target_words = [w for w in re.split(r"[\s>/]+", target_text) if w]
    target_words_n = [w.casefold() for w in target_words]
    thr = float(cfg["desc_reduction"].get("token_score_threshold", 78))
    scored = []
    for i, tok in enumerate(tokens):
        tok_n = tok.casefold()
        best = 0.0
        for tw in target_words_n:
            sc = fuzz.WRatio(tok_n, tw)
            if sc > best:
                best = sc
        if best >= thr:
            scored.append((i, best))
    if not scored:
        return desc
    max_tokens = int(cfg["desc_reduction"].get("max_tokens", 6))
    scored = sorted(scored, key=lambda x: (-x[1], x[0]))[:max_tokens]
    keep_idx = sorted({i for i, _ in scored})
    return " ".join([tokens[i] for i in keep_idx])


DEFAULT_DESC_EXTRACT_STOP_MARKERS = [
    " composed of ",
    " characterized by ",
    " characterised by ",
    " featuring ",
    " made of ",
    " crafted from ",
    " produced from ",
    " from ",
    " with ",
    " in ",
    " by ",
]

DEFAULT_DESC_EXTRACT_PHRASES = [
    "hand and shoulder bag",
    "one-piece swimsuit",
    "two-piece swimsuit",
    "two-piece set",
    "crossbody bag",
    "changing bag",
    "shopping bag",
    "shoulder bag",
    "crew neck sweatshirt",
    "crewneck sweatshirt",
    "crew neck dress",
    "crewneck dress",
    "crew neck sweater",
    "crewneck sweater",
    "low-top sneakers",
    "low-top sneaker",
    "low sneakers",
    "low sneaker",
    "high-top sneakers",
    "high-top sneaker",
    "swim shorts",
    "tote bag",
    "slides",
    "slide",
    "sneakers",
    "sneaker",
    "trainers",
    "trainer",
    "sandals",
    "sandal",
    "ballerinas",
    "ballerina",
    "espadrilles",
    "moccasins",
    "moccasin",
    "loafer",
    "loafers",
    "slippers",
    "slipper",
    "swimsuit",
    "swimwear",
    "bikini",
    "boots",
    "boot",
    "dress",
    "shirt",
    "blouse",
    "bodysuit",
    "romper",
    "onesie",
    "leggings",
    "joggers",
    "jacket",
    "shorts",
    "jeans",
    "skirt",
    "scarf",
    "gloves",
    "mittens",
    "beanie",
    "hat",
    "cap",
    "bag",
    "set",
    "outfit",
]

DESC_EXTRACT_CONNECTORS = {"and", "&", "de", "di", "da", "du", "of", "the", "a", "an"}

TOKEN_RE = re.compile(r"[0-9A-Za-zÀ-ÖØ-öø-ÿА-Яа-яЁё]+(?:[-'][0-9A-Za-zÀ-ÖØ-öø-ÿА-Яа-яЁё]+)*")


def description_headline(description: str) -> str:
    text = str(description or "").replace("\r", "\n")
    parts = [p.strip() for p in text.split("\n") if p and p.strip()]
    if not parts:
        return ""
    return normalize_space(parts[0].split("•", 1)[0])


def load_description_phrase_dict(path: Path) -> List[str]:
    if not path.exists():
        return []
    phrases: List[str] = []
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = normalize_space(raw)
            if not line or line.startswith("#"):
                continue
            phrases.append(line)
    return phrases


def description_phrase_patterns(cfg: Dict[str, Any]) -> List[Tuple[re.Pattern[str], str]]:
    section = cfg.get("description_extraction", {})
    phrases = list(DEFAULT_DESC_EXTRACT_PHRASES)
    dict_path = normalize_space(section.get("phrase_dict", ""))
    if dict_path:
        phrases.extend(load_description_phrase_dict(Path(dict_path)))
    phrases.extend(section.get("extra_phrases", []) or [])

    seen = set()
    out: List[Tuple[re.Pattern[str], str]] = []
    for phrase in sorted(
        phrases,
        key=lambda x: (-len(normalize_space(str(x)).split(" ")), -len(normalize_space(str(x)))),
    ):
        value = normalize_space(str(phrase))
        if not value:
            continue
        key = norm_key_for_lookup(value)
        if key in seen:
            continue
        seen.add(key)
        pattern = r"(?<!\w)" + r"\s+".join(re.escape(part) for part in value.split()) + r"(?!\w)"
        out.append((re.compile(pattern, flags=re.IGNORECASE), value))
    return out


def probable_brand_token(token: str) -> bool:
    if not token:
        return False
    letters = [ch for ch in token if ch.isalpha()]
    if not letters:
        return False
    return letters[0].isupper() and token.casefold() not in DESC_EXTRACT_CONNECTORS


def fallback_description_fragment(headline: str, cfg: Dict[str, Any]) -> str:
    section = cfg.get("description_extraction", {})
    markers = section.get("stop_markers") or DEFAULT_DESC_EXTRACT_STOP_MARKERS
    headline_n = f" {normalize_space(headline)} "
    cut = len(headline_n)
    for marker in markers:
        marker_base = normalize_space(str(marker)).casefold()
        if not marker_base:
            continue
        marker_n = f" {marker_base} "
        hit = headline_n.casefold().find(marker_n)
        if hit != -1:
            cut = min(cut, hit)
    prefix = normalize_space(headline_n[:cut])
    if not prefix:
        prefix = normalize_space(headline)
    tokens = [m.group(0) for m in TOKEN_RE.finditer(prefix)]
    if not tokens:
        return ""

    tail: List[str] = []
    for token in reversed(tokens):
        if tail and probable_brand_token(token):
            break
        if probable_brand_token(token) and not tail:
            break
        tail.append(token)
    if not tail:
        tail = [tokens[-1]]

    fragment_tokens = list(reversed(tail))
    while fragment_tokens and norm_key_for_lookup(fragment_tokens[0]) in DESC_EXTRACT_CONNECTORS:
        fragment_tokens.pop(0)
    if not fragment_tokens:
        return ""

    max_tail_words = int(section.get("max_tail_words", 4))
    if max_tail_words > 0 and len(fragment_tokens) > max_tail_words:
        fragment_tokens = fragment_tokens[-max_tail_words:]
    return normalize_space(" ".join(fragment_tokens))


def extract_description_fragment(
    description: str,
    cfg: Dict[str, Any],
    phrase_patterns: Sequence[Tuple[re.Pattern[str], str]],
) -> str:
    section = cfg.get("description_extraction", {})
    if not section.get("enabled", True):
        return ""
    headline = description_headline(description)
    if not headline:
        return ""

    for pattern, _phrase in phrase_patterns:
        hit = pattern.search(headline)
        if hit:
            return normalize_space(hit.group(0))
    return fallback_description_fragment(headline, cfg)


@dataclass(frozen=True)
class DictRow:
    oskelly_id: int
    output_value: str
    full_path: str
    group: str


def load_category_dict(path: Path) -> List[DictRow]:
    if not path.exists():
        die(f"Category dictionary not found: {path}")

    df = pd.read_excel(path)

    col_id = next((c for c in df.columns if str(c).strip() in ("id", "ID", "Id")), None)
    col_out = next((c for c in df.columns if str(c).strip() == "Конечная категория"), None)
    col_path = next((c for c in df.columns if str(c).strip() == "Полный путь категории"), None)

    if not (col_id and col_out and col_path):
        die(
            "Category dictionary must have columns: id, Конечная категория, Полный путь категории. "
            f"Found: {list(df.columns)}"
        )

    rows: List[DictRow] = []

    def detect_group_from_path(fp: str) -> str:
        s = str(fp or "").strip()
        if not s:
            return "OTHER"

        # берём первый сегмент пути (работает и для "/" и для ">" и для "\")
        root = s.replace(">", "/").replace("\\", "/").split("/")[0].strip().casefold()

        if root in ("women", "женское", "donna"):
            return "WOMEN"
        if root in ("men", "мужское", "uomo"):
            return "MEN"
        if root in ("lifestyle", "home", "casa"):
            return "LIFESTYLE"
        return "OTHER"

    for _, r in df.iterrows():
        fp = str(r[col_path])
        ov = str(r[col_out])

        try:
            oid = int(r[col_id])
        except Exception:
            continue

        grp = detect_group_from_path(fp)
        if grp != "OTHER":
            rows.append(DictRow(oskelly_id=oid, output_value=ov, full_path=fp, group=grp))

    return rows


def load_parentcategory_dict(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        die(f"Parentcategory dictionary not found: {path}")
    df = pd.read_excel(path, header=None)
    out = {}
    for _, r in df.iterrows():
        k = normalize_space(r.iloc[0])
        v = normalize_space(r.iloc[1])
        if not k or not v or k.lower() == "nan" or v.lower() == "nan":
            continue
        try:
            params = json.loads(v)
            if not isinstance(params, dict):
                raise ValueError()
        except Exception:
            if re.fullmatch(r"\d+", v):
                params = {"parents": [int(v)]}
            else:
                params = {"parentsRaw": v}
        out[k] = params
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Oskelly category mapping (Flexify export)")
    ap.add_argument("--input", required=True)
    ap.add_argument("--index-start", required=True, type=int)
    ap.add_argument("--email", required=True)
    ap.add_argument("--config", default="config.yml")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = read_yaml(Path(args.config))
    run_dir, date_str, version = ensure_output_run_dir(Path("output"), args.email, cfg["app"]["timezone"])

    report: Dict[str, Any] = {
        "args": {"input": args.input, "index_start": args.index_start, "email": args.email, "dry_run": args.dry_run},
        "run": {"date": date_str, "version": version, "run_dir": str(run_dir)},
        "counts": {},
        "seed": {},
        "llm": {},
        "dedup": {},
        "errors": [],
    }
    desc_phrase_patterns = description_phrase_patterns(cfg)

    category_rows = load_category_dict(Path(cfg["dictionaries"]["category_xlsx"]))
    parent_params = load_parentcategory_dict(Path(cfg["dictionaries"]["parentcategory_xlsx"]))

    # Full candidate lists per group
    candidates_by_group: Dict[str, List[str]] = {"WOMEN": [], "MEN": [], "LIFESTYLE": []}
    for r in category_rows:
        if r.group in candidates_by_group:
            candidates_by_group[r.group].append(r.full_path)

    # Deterministic ordering
    for g in candidates_by_group:
        candidates_by_group[g] = sorted(candidates_by_group[g])

    # outputValue/id lookup by (group, full_path)
    fp_lookup: Dict[Tuple[str, str], DictRow] = {(r.group, r.full_path): r for r in category_rows}


    def find_anchor_candidates(group: str, anchor: str) -> List[str]:
        a = normalize_space(anchor)
        if not a:
            return []
        # match as substring of the path; prefer segment-like matches using separators
        a_cf = a.casefold()
        out = []
        for fp in candidates_by_group.get(group, []):
            fp_cf = fp.casefold()
            if a_cf in fp_cf:
                out.append(fp)
        return out

    # Seed
    seed_enabled = bool(cfg["dictionaries"].get("seed_enabled", False))
    seed_path = Path(cfg["dictionaries"].get("seed_csv", ""))
    seed_map = load_seed(seed_path) if (seed_enabled and str(seed_path)) else {}
    seed_thr = float(cfg["dictionaries"].get("seed_confidence_threshold", 0.65))
    conflict_policy = str(cfg["dictionaries"].get("seed_conflict_policy", "llm")).lower()

    report["seed"] = {"enabled": seed_enabled and bool(seed_map), "seed_path": str(seed_path), "seed_entries": len(seed_map),
                      "seed_thr": seed_thr, "conflict_policy": conflict_policy}

    # Read input
    in_path = Path(args.input)
    if not in_path.exists():
        die(f"Input not found: {in_path}")
    preferred_sheet = cfg.get("input", {}).get("sheet_name", "Result 1")
    sheet_name = resolve_input_sheet(in_path, preferred_sheet)
    df_in = pd.read_excel(in_path, sheet_name=sheet_name)
    report["args"]["sheet_name"] = sheet_name

    def col(name: str) -> Optional[str]:
        want = cfg["input"]["columns"][name]
        for c in df_in.columns:
            if str(c).strip().casefold() == str(want).strip().casefold():
                return c
        for c in df_in.columns:
            if str(want).strip().casefold() in str(c).strip().casefold():
                return c
        return None

    c_reason, c_cat, c_parent, c_desc = col("reason"), col("category"), col("parentcategory"), col("description")
    if not c_reason or not c_cat or not c_parent:
        die(f"Could not resolve required columns. Found={list(df_in.columns)} Resolved reason={c_reason} category={c_cat} parent={c_parent}")
    if not c_desc:
        df_in["Description"] = ""
        c_desc = "Description"

    brand_exclude_prefixes = cfg["filters"].get("brand_exclude_prefixes", ["Не найден бренд с названием"])

    reasons = cfg["filters"]["reasons"]
    reason_a, reason_b = reasons[0], reasons[1]

    df0 = df_in.copy()
    # Exclude brand-related errors early: if Reason starts with any excluded prefix, drop the row.
    def _is_brand_excluded(val: str) -> bool:
        s = str(val or "").strip()
        for p in brand_exclude_prefixes:
            if s.startswith(p):
                return True
        return False
    df0 = df0[~df0[c_reason].astype(str).apply(_is_brand_excluded)].copy()

    df = df0[df0[c_reason].astype(str).apply(lambda x: (reason_a in str(x)) or (reason_b in str(x)))].copy()
    df["_reason_type"] = df[c_reason].astype(str).apply(lambda x: "A" if reason_a in str(x) else ("B" if reason_b in str(x) else "OTHER"))

    kids_markers = cfg["filters"]["kids_parent_markers"]
    skirts_markers = cfg["filters"]["skirts_category_markers"]
    df = df[~((df["_reason_type"] == "A") & df[c_parent].astype(str).apply(lambda x: contains_any(x, kids_markers)))].copy()
    df = df[~((df["_reason_type"] == "B") & df[c_cat].astype(str).apply(lambda x: contains_any(x, skirts_markers)))].copy()

    df["_group"] = df[c_parent].astype(str).apply(lambda x: detect_group(x, cfg))
    df["expr"] = df[c_cat].astype(str).map(normalize_space)
    df["description"] = df[c_desc].astype(str).map(normalize_space)
    df["_desc_fragment"] = df["description"].apply(lambda value: extract_description_fragment(value, cfg, desc_phrase_patterns))
    df["_desc_for_llm"] = df["_desc_fragment"].where(df["_desc_fragment"].astype(str).str.strip().ne(""), df["description"])

    report["counts"]["input_rows"] = int(len(df_in))
    report["counts"]["rows_after_filters"] = int(len(df))

    label_by_group = {"MEN": "Мужское", "WOMEN": "Женское", "LIFESTYLE": "Lifestyle"}

    def base_params(g: str) -> Dict[str, Any]:
        label = label_by_group.get(g, "")
        if label and label in parent_params:
            return dict(parent_params[label])
        if g == "MEN":
            return {"parents": [105]}
        if g == "WOMEN":
            return {"parents": [2]}
        return {"parents": []}

    df["_base_params"] = df["_group"].apply(base_params)

    def seed_entry(expr: str) -> Optional[SeedEntry]:
        return seed_map.get(norm_key_for_lookup(expr)) if seed_map else None

    df["_seed_entry"] = df["expr"].apply(seed_entry)

    def seed_usable(se: Optional[SeedEntry]) -> bool:
        if se is None:
            return False
        if se.is_conflict and conflict_policy == "llm":
            return False
        return se.confidence >= seed_thr

    df["_use_seed"] = df["_seed_entry"].apply(seed_usable)
    df["_seed_output"] = df["_seed_entry"].apply(lambda x: x.output_value if x else "")

    report["seed"]["rows_use_seed"] = int(df["_use_seed"].sum())

    df["_outputValue"] = ""
    df["_oskellyId"] = None
    df["_full_path"] = ""
    df["_used_desc"] = False
    df["_seed_anchor"] = ""
    df["_needs_anchor_llm"] = False

    # Resolve seed outputValue -> pick a matching DictRow by group+outputValue (deterministic first by sorted full_path)
    ov_to_best: Dict[Tuple[str, str], DictRow] = {}
    for r in sorted(category_rows, key=lambda x: x.full_path):
        key = (r.group, r.output_value)
        if key not in ov_to_best:
            ov_to_best[key] = r

    if df["_use_seed"].any():
        for idx, row in df[df["_use_seed"]].iterrows():
            g = row["_group"]
            ov = row["_seed_output"]
            rec = ov_to_best.get((g, ov))
            if rec:
                # Seed provided a leaf конечная категория
                df.at[idx, "_outputValue"] = rec.output_value
                df.at[idx, "_oskellyId"] = rec.oskelly_id
                df.at[idx, "_full_path"] = rec.full_path
            else:
                # Seed output is not a leaf in current dictionary. Use it as an anchor to narrow candidates and continue with LLM.
                anchor_cands = find_anchor_candidates(g, ov)
                if anchor_cands:
                    df.at[idx, "_seed_anchor"] = ov
                    df.at[idx, "_needs_anchor_llm"] = True
                    # mark seed as not final, so row will be handled by LLM
                    df.at[idx, "_use_seed"] = False
                else:
                    # Anchor not found either; fall back to LLM normally
                    df.at[idx, "_use_seed"] = False

    # LLM mapping for remaining
    need_llm = df[~df["_use_seed"]].copy()
    report["llm"]["rows_need_llm"] = int(len(need_llm))

    force_desc_markers = cfg["mapping"]["force_desc_markers"]
    depth_thr = int(cfg["mapping"]["depth_fallback_threshold"])

    # Cache
    cache_path = run_dir / "cache" / "llm_cache.jsonl"
    cache: Dict[str, str] = {}
    if cache_path.exists():
        for line in cache_path.read_text(encoding="utf-8").splitlines():
            try:
                obj = json.loads(line)
                cache[obj["cache_key"]] = obj["full_path"]
            except Exception:
                pass

    def cache_key(group: str, key_text: str, prompt_version: str, model: str) -> str:
        # Candidate lists are fixed per prompt version; no need to include them in key.
        return sha1_text(json.dumps({"v": prompt_version, "m": model, "g": group, "k": key_text}, ensure_ascii=False))

    llm_cfg = LLMConfig(
        model=cfg["llm"]["model"],
        max_items_per_request=int(cfg["llm"]["max_items_per_request"]),
        request_timeout_sec=int(cfg["llm"]["request_timeout_sec"]),
        prompt_version=str(cfg["llm"]["prompt_version"]),
        debug_log_path=str(run_dir / "llm_debug.jsonl"),
    )
    mapper = ResponsesFullDictMapper(llm_cfg)
    report["llm"]["debug_log_file"] = str(run_dir / "llm_debug.jsonl")
    print(f"LLM debug log: {run_dir / 'llm_debug.jsonl'}")

    def make_llm_item(
        group: str,
        expr: str,
        text: str,
        used_desc: bool,
        match_kind: str,
        desc_value: str = "",
        anchor_hint: str = "",
    ) -> Dict[str, Any]:
        payload = {
            "group": normalize_space(group),
            "expr": normalize_space(expr),
            "text": normalize_space(text),
            "match_kind": normalize_space(match_kind),
            "desc_value": normalize_space(desc_value),
            "anchor_hint": normalize_space(anchor_hint),
        }
        return {
            **payload,
            "payload_key": sha1_text(json.dumps(payload, ensure_ascii=False, sort_keys=True)),
            "used_desc": bool(used_desc),
        }

    def run_items(
        items_meta: List[Dict[str, Any]],
        stage: str,
        candidates_override: Optional[Dict[str, List[str]]] = None,
        stage_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        if not items_meta:
            return {}

        uniq = {item["payload_key"]: dict(item) for item in items_meta}
        payload = []
        meta = []
        for item in uniq.values():
            ck = cache_key(item["group"], item["text"], llm_cfg.prompt_version, llm_cfg.model)
            if ck in cache:
                continue
            payload.append({"key": item["payload_key"], "group": item["group"], "text": item["text"]})
            meta.append((ck, item["payload_key"], item["group"], item["expr"], item["text"], item["used_desc"]))

        if payload and args.dry_run:
            return {}

        if payload:
            # Batch split
            results_all = []
            chunk = max(1, llm_cfg.max_items_per_request)
            total_chunks = (len(payload) + chunk - 1) // chunk
            cache_hits_before_stage = len(uniq) - len(payload)
            for batch_no, i in enumerate(range(0, len(payload), chunk), start=1):
                part = payload[i:i+chunk]
                results_all.extend(
                    mapper.map(
                        candidates_override or candidates_by_group,
                        part,
                        debug_context={
                            "stage": stage,
                            "chunk_index": batch_no,
                            "chunk_total": total_chunks,
                            "payload_items_total": len(payload),
                            "cache_hits_before_stage": cache_hits_before_stage,
                            **(stage_meta or {}),
                        },
                    )
                )

            by_key = {r.key: r.full_path for r in results_all}
            with cache_path.open("a", encoding="utf-8") as f:
                for ck, payload_key, group, expr, text, used_desc in meta:
                    fp = by_key.get(payload_key, "")
                    if fp:
                        cache[ck] = fp
                        f.write(json.dumps({"cache_key": ck, "payload_key": payload_key, "group": group, "expr": expr, "text": text,
                                            "full_path": fp, "prompt_version": llm_cfg.prompt_version,
                                            "model": llm_cfg.model, "used_desc": used_desc},
                                           ensure_ascii=False) + "\n")

        out = {}
        for item in uniq.values():
            ck = cache_key(item["group"], item["text"], llm_cfg.prompt_version, llm_cfg.model)
            if ck in cache:
                out[item["payload_key"]] = cache[ck]
        return out

    # Stage 1: category-only unless force-desc
    items_stage1 = []
    items_force = []
    for _, r in need_llm.iterrows():
        g, expr = r["_group"], r["expr"]
        desc_for_llm = normalize_space(r.get("_desc_for_llm", ""))
        if contains_any(expr, force_desc_markers):
            items_force.append(make_llm_item(g, expr, f"{expr} | {desc_for_llm}".strip(), True, "desc", desc_value=desc_for_llm))
        else:
            items_stage1.append(make_llm_item(g, expr, expr, False, "expr"))

    report["llm"]["unique_stage1"] = len({item["payload_key"] for item in items_stage1})
    report["llm"]["unique_force_desc"] = len({item["payload_key"] for item in items_force})

    fp_stage1 = run_items(items_stage1, stage="stage1")
    fp_force = run_items(items_force, stage="force_desc")

    # Anchor-based LLM: when seed returned a non-leaf value that exists inside dictionary paths,
    # we narrow candidates to only those paths that contain that anchor segment.
    df_anchor = df[(df["_needs_anchor_llm"] == True)].copy()
    report["llm"]["rows_anchor_llm"] = int(len(df_anchor))
    anchor_maps: List[Dict[str, str]] = []
    anchor_items: List[Dict[str, Any]] = []
    if len(df_anchor):
        # group items by (group, anchor)
        buckets: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for _, r in df_anchor.iterrows():
            g = r["_group"]
            expr = r["expr"]
            anchor = r.get("_seed_anchor", "")
            text = f"{expr} ANCHOR:{anchor}" if anchor else expr
            item = make_llm_item(g, expr, text, False, "anchor", anchor_hint=anchor)
            buckets.setdefault((g, anchor), []).append(item)
            anchor_items.append(item)
        for (g, anchor), items in buckets.items():
            sub = find_anchor_candidates(g, anchor)
            cand_override = {"WOMEN": candidates_by_group["WOMEN"], "MEN": candidates_by_group["MEN"], "LIFESTYLE": candidates_by_group["LIFESTYLE"]}
            cand_override[g] = sorted(sub)
            anchor_maps.append(
                run_items(
                    items,
                    stage="anchor",
                    candidates_override=cand_override,
                    stage_meta={
                        "group": g,
                        "anchor": anchor,
                        "override_candidates": len(cand_override.get(g, [])),
                    },
                )
            )
    fp_anchor = {}
    for m in anchor_maps:
        fp_anchor.update(m)

    def apply_fp(fp_map: Dict[str, str], items_meta: List[Dict[str, Any]], used_desc_flag: bool):
        uniq_items = {item["payload_key"]: item for item in items_meta}
        for item in uniq_items.values():
            fp = fp_map.get(item["payload_key"], "")
            if not fp:
                continue
            rec = fp_lookup.get((item["group"], fp))
            if not rec:
                die(f"Internal: full path not found for group={item['group']}: {fp}")
            mask = (df["_group"] == item["group"]) & (df["expr"] == item["expr"]) & (~df["_use_seed"])
            if item["match_kind"] == "desc":
                mask = mask & (df["_desc_for_llm"].fillna("").map(normalize_space) == item["desc_value"])
            elif item["match_kind"] == "anchor":
                mask = mask & (df["_seed_anchor"].fillna("").map(normalize_space) == item["anchor_hint"])
            else:
                mask = mask & (~df["expr"].astype(str).apply(lambda x: contains_any(x, force_desc_markers)))
            df.loc[mask, "_full_path"] = rec.full_path
            df.loc[mask, "_outputValue"] = rec.output_value
            df.loc[mask, "_oskellyId"] = rec.oskelly_id
            df.loc[mask, "_used_desc"] = used_desc_flag

    apply_fp(fp_stage1, items_stage1, False)
    apply_fp(fp_force, items_force, True)
    if 'fp_anchor' in locals():
        apply_fp(fp_anchor, anchor_items, False)

    # Depth fallback for those not using desc yet
    need_fb = df[(~df["_use_seed"]) & (df["_used_desc"] == False)].copy()
    need_fb = need_fb[need_fb["_full_path"].astype(str).apply(lambda x: path_depth(x) <= depth_thr)].copy()
    report["llm"]["rows_depth_fallback"] = int(len(need_fb))

    items_fb = []
    for _, r in need_fb.iterrows():
        g, expr = r["_group"], r["expr"]
        desc_for_llm = normalize_space(r.get("_desc_for_llm", ""))
        items_fb.append(make_llm_item(g, expr, f"{expr} | {desc_for_llm}".strip(), True, "desc", desc_value=desc_for_llm))

    fp_fb = run_items(items_fb, stage="depth_fallback")
    apply_fp(fp_fb, items_fb, True)

    # params with descContains if used desc
    def build_params(row) -> str:
        base = dict(row["_base_params"]) if isinstance(row["_base_params"], dict) else {"parents": []}
        if bool(row.get("_used_desc", False)):
            extracted = normalize_space(row.get("_desc_fragment", ""))
            if extracted:
                base["descContains"] = extracted
            else:
                rd = reduce_description(row["description"], str(row["_full_path"]), str(row["_outputValue"]), cfg)
                if rd:
                    base["descContains"] = rd
        return json.dumps(base, ensure_ascii=False, separators=(",", ":"))

    df["params"] = df.apply(build_params, axis=1)

    # strict unmapped
    missing = df[df["_outputValue"].astype(str).str.strip() == ""]
    if len(missing) and not args.dry_run:
        miss_path = run_dir / "unmapped_rows.xlsx"
        missing.to_excel(miss_path, index=False)
        die(f"Unmapped rows after LLM: {len(missing)}. See {miss_path}")

    out_cols = cfg["output"]["columns"]
    out_df = pd.DataFrame({
        "id": range(1, len(df) + 1),
        "objectType": [cfg["output"]["object_type_value"]] * len(df),
        "expr": df["expr"].tolist(),
        "outputValue": df["_outputValue"].astype(str).tolist(),
        "matchType": [cfg["output"]["match_type_value"]] * len(df),
        "repeatMatching": [cfg["output"]["repeat_matching_value"]] * len(df),
        "params": df["params"].tolist(),
        "oskellyId": df["_oskellyId"].astype("Int64").astype(str).replace("<NA>", "").tolist(),
        "indexNumber": range(args.index_start, args.index_start + len(df)),
        "clientEmail": [args.email] * len(df),
    })[out_cols]

    before = len(out_df)
    out_df = out_df.drop_duplicates(subset=cfg["output"]["dedup_subset"], keep="first").reset_index(drop=True)
    after = len(out_df)
    out_df["id"] = range(1, after + 1)
    out_df["indexNumber"] = range(args.index_start, args.index_start + after)
    report["dedup"] = {"before": before, "after": after, "removed": before - after}

    out_name = f"Категории для загрузки {args.email} {date_str}.xlsx"
    out_path = run_dir / out_name
    out_df.to_excel(out_path, index=False)

    report["counts"]["output_rows"] = int(after)
    (run_dir / "run_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK: {out_path}")


if __name__ == "__main__":
    main()
