\
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI


class LLMError(RuntimeError):
    pass


@dataclass(frozen=True)
class LLMConfig:
    model: str
    max_items_per_request: int
    request_timeout_sec: int
    prompt_version: str
    debug_log_path: Optional[str] = None


@dataclass
class LLMResult:
    key: str
    full_path: str
    raw: Dict[str, Any]


class ResponsesFullDictMapper:
    """
    Batch mapper using OpenAI Responses API, where the FULL candidate lists are sent once per prompt.
    Items only contain (group, key_text). Model returns choice_index into the group's list.

    Guarantees:
    - Batch only (no per-row calls).
    - No temperature/top_p.
    - Strict JSON output.
    """

    def __init__(self, cfg: LLMConfig):
        self.cfg = cfg
        self.client = OpenAI()
        self.debug_log_path = Path(cfg.debug_log_path) if cfg.debug_log_path else None
        if self.debug_log_path:
            self.debug_log_path.parent.mkdir(parents=True, exist_ok=True)

    def _write_debug(self, payload: Dict[str, Any]) -> None:
        if not self.debug_log_path:
            return
        with self.debug_log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _build_prompt(self, candidates_by_group: Dict[str, List[str]], items: List[Dict[str, Any]]) -> str:
        """
        candidates_by_group:
          {"WOMEN":[...], "MEN":[...], "LIFESTYLE":[...]}
        items:
          [{"key":"WOMEN||Maglie", "group":"WOMEN", "text":"Maglie"}, ...]
        Output JSON array:
          [{"key":"WOMEN||Maglie","choice_index":17}]
        where choice_index is 0-based index into candidates_by_group[group].
        """
        rules = f"""
You are a strict classifier. For each item, choose exactly ONE category path by returning its 0-based index.
Rules:
- Output MUST be valid JSON only (no markdown, no commentary).
- Output MUST be a JSON array of objects: {{ "key": "...", "choice_index": <int> }}.
- choice_index MUST be an integer within the bounds of the group's candidates list.
- Choose the most semantically correct category path for the item's text, considering the group.
- Do NOT invent categories or modify candidate strings.
- Prompt version: {self.cfg.prompt_version}
"""
        payload = {
            "candidates_by_group": candidates_by_group,
            "items": [{"key": it["key"], "group": it["group"], "text": it["text"]} for it in items],
        }
        return rules.strip() + "\n\n" + json.dumps(payload, ensure_ascii=False)

    def map(
        self,
        candidates_by_group: Dict[str, List[str]],
        items: List[Dict[str, Any]],
        debug_context: Optional[Dict[str, Any]] = None,
    ) -> List[LLMResult]:
        if not items:
            return []

        prompt = self._build_prompt(candidates_by_group, items)
        involved_groups = sorted({str(it.get("group", "")) for it in items if it.get("group")})
        debug_base = {
            "stage": (debug_context or {}).get("stage"),
            "chunk_index": (debug_context or {}).get("chunk_index"),
            "chunk_total": (debug_context or {}).get("chunk_total"),
            "payload_items_total": (debug_context or {}).get("payload_items_total"),
            "chunk_item_count": len(items),
            "cache_hits_before_stage": (debug_context or {}).get("cache_hits_before_stage"),
            "group": (debug_context or {}).get("group"),
            "anchor": (debug_context or {}).get("anchor"),
            "override_candidates": (debug_context or {}).get("override_candidates"),
            "groups_in_chunk": involved_groups,
            "candidate_counts": {g: len(candidates_by_group.get(g, [])) for g in involved_groups},
            "sample_keys": [str(it.get("key", "")) for it in items[:5]],
            "model": self.cfg.model,
            "request_timeout_sec": self.cfg.request_timeout_sec,
            "prompt_version": self.cfg.prompt_version,
            "prompt_chars": len(prompt),
        }

        # retry w/ backoff
        last_err: Optional[Exception] = None
        for attempt in range(1, 4):
            started_at = time.time()
            text = ""
            try:
                resp = self.client.responses.create(
                    model=self.cfg.model,
                    input=prompt,
                    timeout=self.cfg.request_timeout_sec,
                )
                text = resp.output_text or ""
                data = json.loads(text)
                if not isinstance(data, list):
                    raise LLMError(f"Expected JSON list, got {type(data)}")

                out: List[LLMResult] = []
                by_key = {it["key"]: it for it in items}
                for obj in data:
                    if not isinstance(obj, dict):
                        continue
                    key = obj.get("key")
                    idx = obj.get("choice_index")
                    if key not in by_key:
                        continue
                    group = by_key[key]["group"]
                    cands = candidates_by_group.get(group, [])
                    if not isinstance(idx, int) or idx < 0 or idx >= len(cands):
                        raise LLMError(f"Invalid choice_index for key={key}: {idx}")
                    out.append(LLMResult(key=key, full_path=cands[idx], raw=obj))

                got = {r.key for r in out}
                missing = [it["key"] for it in items if it["key"] not in got]
                if missing:
                    raise LLMError(f"Missing keys in response: {missing[:5]} (and {len(missing)-5} more)" if len(missing)>5 else f"Missing keys in response: {missing}")
                self._write_debug(
                    {
                        **debug_base,
                        "event": "success",
                        "attempt": attempt,
                        "duration_sec": round(time.time() - started_at, 3),
                        "response_chars": len(text),
                    }
                )
                return out
            except Exception as e:
                last_err = e
                self._write_debug(
                    {
                        **debug_base,
                        "event": "error",
                        "attempt": attempt,
                        "duration_sec": round(time.time() - started_at, 3),
                        "error_type": type(e).__name__,
                        "error": str(e),
                        "response_preview": text[:500] if text else "",
                    }
                )
                time.sleep(0.7 * attempt)

        raise LLMError(f"Responses API call failed after retries: {last_err}")
