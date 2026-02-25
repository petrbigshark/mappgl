from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from openai import OpenAI


class LLMError(RuntimeError):
    pass


@dataclass(frozen=True)
class LLMConfig:
    model: str
    timeout_sec: int
    max_retries: int


_JSON_RE = re.compile(r"(\[.*\]|\{.*\})", flags=re.S)


class SizeCategoryClassifier:
    """
    Classifies category names into CLOTHING/SHOES/OTHER in a single batch call.
    """

    def __init__(self, cfg: LLMConfig):
        self.cfg = cfg
        self.client = OpenAI()

    def classify(self, categories: List[str]) -> Dict[str, str]:
        if not categories:
            return {}

        system = (
            "You classify fashion product categories.\n"
            "For each category, return exactly one label from: CLOTHING, SHOES, OTHER.\n"
            "Rules:\n"
            "- CLOTHING: apparel (dresses, tops, shirts, pants, jeans, skirts, jackets, coats, knitwear, underwear, swimwear).\n"
            "- SHOES: sneakers, boots, sandals, loafers, heels, flats, slippers and any footwear.\n"
            "- OTHER: accessories/jewelry/bags/hats/scarves/belts/sunglasses/home and everything else.\n"
            "Return JSON only. No markdown, no explanation."
        )

        payload = [{"idx": i, "category": c} for i, c in enumerate(categories)]
        user = (
            "Classify each item.\n"
            "Input JSON:\n"
            f"{json.dumps(payload, ensure_ascii=False)}\n\n"
            "Output MUST be a JSON array of objects with this exact schema:\n"
            '[{"idx": 0, "label": "CLOTHING"}]\n'
            "Where label is strictly one of: CLOTHING, SHOES, OTHER."
        )

        last_error: Optional[Exception] = None
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                resp = self.client.responses.create(
                    model=self.cfg.model,
                    input=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    timeout=self.cfg.timeout_sec,
                )
                text = (resp.output_text or "").strip()
                data = self._parse_json(text)
                return self._decode(categories, data)
            except Exception as e:
                last_error = e
                time.sleep(0.7 * attempt)

        raise LLMError(f"LLM classification failed after retries: {last_error}")

    def _parse_json(self, text: str):
        try:
            return json.loads(text)
        except Exception:
            match = _JSON_RE.search(text)
            if not match:
                raise LLMError("Model response is not valid JSON")
            try:
                return json.loads(match.group(1))
            except Exception as e:
                raise LLMError(f"Could not parse JSON from model response: {e}") from e

    def _decode(self, categories: List[str], data) -> Dict[str, str]:
        if not isinstance(data, list):
            raise LLMError(f"Expected JSON list, got {type(data)}")

        out: Dict[str, str] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            idx = item.get("idx")
            label = str(item.get("label", "")).strip().upper()
            if not isinstance(idx, int):
                continue
            if idx < 0 or idx >= len(categories):
                continue
            if label not in {"CLOTHING", "SHOES", "OTHER"}:
                continue

            out[categories[idx]] = label

        return out
