from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI


_JSON_RE = re.compile(r"\{.*\}", re.S)


class LLMResponseError(RuntimeError):
    """Raised when the model response is malformed or incomplete."""


class ColorLLMBatchClient:
    """
    Resource-aware color classifier:
    - Splits large inputs into controlled batches.
    - Hard-limits total API calls.
    - Uses strict completeness checks per batch.
    - Normalizes returned labels before validating against allowed colors.
    """

    def __init__(
        self,
        allowed_colors: List[str],
        model: str = "gpt-5-mini",
        max_retries: int = 1,
        timeout_sec: int = 300,
        batch_size: int = 500,
        max_total_calls: int = 6,
        debug_dir: Optional[str | Path] = None,
    ):
        if "Other" not in allowed_colors:
            raise ValueError("allowed_colors must include 'Other'")

        if not os.getenv("OPENAI_API_KEY"):
            raise RuntimeError("OPENAI_API_KEY is not set")

        self.allowed_colors = allowed_colors
        self.model = model
        self.max_retries = max(1, int(max_retries))
        self.timeout_sec = max(1, int(timeout_sec))
        self.batch_size = max(1, int(batch_size))
        self.max_total_calls = max(1, int(max_total_calls))
        self.debug_dir = Path(debug_dir) if debug_dir else None

        # Disable SDK auto-retries to keep request count predictable.
        self.client = OpenAI(max_retries=0)

        self.allowed_by_norm = {
            self._normalize_label(color): color
            for color in self.allowed_colors
            if self._normalize_label(color)
        }
        self.alias_to_allowed = self._build_alias_map()
        self.last_stats: Dict[str, Any] = {}

    def classify_all(self, queries: List[str]) -> Dict[str, str]:
        """
        Input: list of brand_color_query
        Output: dict {query: normalized_color}
        """
        if not queries:
            self.last_stats = {
                "query_count": 0,
                "batches_total": 0,
                "calls_made": 0,
                "effective_batch_size": 0,
            }
            return {}

        effective_batch_size = self.batch_size
        batches: List[List[str]] = [
            queries[i : i + effective_batch_size]
            for i in range(0, len(queries), effective_batch_size)
        ]
        if len(batches) > self.max_total_calls:
            raise LLMResponseError(
                f"Too many LLM batches required: {len(batches)} > max_total_calls={self.max_total_calls}. "
                "Increase llm.batch_size or max_total_calls."
            )

        result: Dict[str, str] = {}
        calls_made = 0
        for batch_idx, batch_queries in enumerate(batches, start=1):
            batch_mapping = self._classify_batch(
                batch_queries,
                batch_idx=batch_idx,
                total_batches=len(batches),
            )
            result.update(batch_mapping)
            calls_made += 1

        self.last_stats = {
            "query_count": len(queries),
            "batches_total": len(batches),
            "calls_made": calls_made,
            "effective_batch_size": effective_batch_size,
        }
        return result

    def _classify_batch(self, queries: List[str], *, batch_idx: int, total_batches: int) -> Dict[str, str]:
        batch_items = [
            {"id": str(i + 1), "query": q}
            for i, q in enumerate(queries)
        ]
        expected_ids = [item["id"] for item in batch_items]

        system = (
            "You are a color classifier.\n"
            "Return STRICTLY a JSON object where:\n"
            "- key = id from values list\n"
            "- value = ONE color from allowed_colors\n"
            "If unsure, use 'Other'.\n"
            "Return ALL ids without omissions.\n"
            "No text outside JSON."
        )

        user = (
            "allowed_colors:\n"
            + "\n".join(self.allowed_colors)
            + "\n\nvalues:\n"
            + "\n".join(f"- {item['id']}: {item['query']}" for item in batch_items)
            + "\n\nResponse format example:\n"
            + '{"1":"Black","2":"White"}'
        )

        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            raw_text = ""
            try:
                resp = self.client.responses.create(
                    model=self.model,
                    input=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    timeout=self.timeout_sec,
                )

                raw_text = (resp.output_text or "").strip()
                parsed = self._parse_json(raw_text)
                data = self._align_response_ids(parsed, expected_ids)

                missing_ids = [id_ for id_ in expected_ids if id_ not in data]
                self._write_debug(
                    attempt=attempt,
                    batch_idx=batch_idx,
                    total_batches=total_batches,
                    batch_items=batch_items,
                    raw_response=raw_text,
                    parsed_data=data,
                    missing_ids=missing_ids,
                    error=None,
                )

                if missing_ids:
                    raise LLMResponseError(
                        f"Incomplete JSON from model: got {len(expected_ids) - len(missing_ids)}/{len(expected_ids)} keys"
                    )

                return {
                    item["query"]: self._to_allowed_color(data.get(item["id"]))
                    for item in batch_items
                }
            except Exception as e:
                last_error = e
                self._write_debug(
                    attempt=attempt,
                    batch_idx=batch_idx,
                    total_batches=total_batches,
                    batch_items=batch_items,
                    raw_response=raw_text,
                    parsed_data={},
                    missing_ids=expected_ids,
                    error=str(e),
                )
                if attempt < self.max_retries:
                    time.sleep(0.7 * attempt)

        raise LLMResponseError(
            f"Failed to get complete color mapping for batch {batch_idx}/{total_batches} "
            f"after {self.max_retries} attempts: {last_error}"
        )

    @staticmethod
    def _normalize_label(value: Any) -> str:
        s = str(value or "").strip().strip("'\"")
        s = s.replace("Ё", "Е").replace("ё", "е")
        s = s.replace("–", "-").replace("—", "-")
        s = re.sub(r"\s+", " ", s).strip()
        s = s.casefold()
        s = s.replace("-", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _build_alias_map(self) -> Dict[str, str]:
        aliases_raw = {
            "black": "Black",
            "white": "White",
            "beige": "Beige",
            "gray": "Grey",
            "grey": "Grey",
            "dark blue": "Deep blue",
            "deep blue": "Deep blue",
            "light blue": "Light Blue",
            "red": "Red",
            "green": "Green",
            "yellow": "Yellow",
            "pink": "Pink",
            "orange": "Orange",
            "purple": "Purple",
            "brown": "Brown",
            "gold": "Gold",
            "silver": "Silver",
            "khaki": "Khaki",
            "anthracite": "Anthracite",
            "turquoise": "Turquoise",
            "multi": "Multicolour",
            "multicolor": "Multicolour",
            "multi color": "Multicolour",
            "multicolour": "Multicolour",
            "multi colour": "Multicolour",
            "other": "Other",
        }
        out: Dict[str, str] = {}
        for alias, canonical in aliases_raw.items():
            if canonical in self.allowed_colors:
                out[self._normalize_label(alias)] = canonical
        return out

    def _to_allowed_color(self, value: Any) -> str:
        norm = self._normalize_label(value)
        if not norm:
            return "Other"

        if norm in self.allowed_by_norm:
            return self.allowed_by_norm[norm]

        if norm in self.alias_to_allowed:
            return self.alias_to_allowed[norm]

        return "Other"

    def _parse_json(self, text: str) -> Dict[str, Any]:
        try:
            obj = json.loads(text)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            match = _JSON_RE.search(text)
            if match:
                try:
                    obj = json.loads(match.group(0))
                    return obj if isinstance(obj, dict) else {}
                except Exception:
                    pass
        return {}

    @staticmethod
    def _normalize_id(value: Any) -> str:
        s = str(value or "").strip().casefold()
        if not s:
            return ""
        s = s.replace("id", "")
        s = s.replace("#", "")
        s = re.sub(r"\s+", "", s)
        digits = re.sub(r"\D+", "", s)
        if digits:
            return digits
        return s

    def _align_response_ids(self, data: Dict[str, Any], expected_ids: List[str]) -> Dict[str, Any]:
        if not isinstance(data, dict):
            return {}

        out: Dict[str, Any] = {}
        exact = {id_: id_ for id_ in expected_ids}
        norm_to_id = {
            self._normalize_id(id_): id_
            for id_ in expected_ids
            if self._normalize_id(id_)
        }

        for raw_key, value in data.items():
            key = str(raw_key or "").strip()
            if not key:
                continue

            if key in exact and key not in out:
                out[key] = value
                continue

            nk = self._normalize_id(key)
            resolved = norm_to_id.get(nk)
            if resolved and resolved not in out:
                out[resolved] = value

        return out

    def _write_debug(
        self,
        *,
        attempt: int,
        batch_idx: int,
        total_batches: int,
        batch_items: List[Dict[str, str]],
        raw_response: str,
        parsed_data: Dict[str, Any],
        missing_ids: List[str],
        error: Optional[str],
    ) -> None:
        if not self.debug_dir:
            return

        self.debug_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        debug_path = self.debug_dir / f"llm_batch_{batch_idx:03d}_attempt_{attempt}_{ts}.json"
        payload = {
            "batch_index": batch_idx,
            "total_batches": total_batches,
            "attempt": attempt,
            "model": self.model,
            "batch_size": self.batch_size,
            "query_count": len(batch_items),
            "parsed_key_count": len(parsed_data),
            "missing_key_count": len(missing_ids),
            "missing_ids_preview": missing_ids[:25],
            "batch_items_preview": batch_items[:25],
            "error": error,
            "raw_response": raw_response,
        }
        debug_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
