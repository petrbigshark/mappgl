# llm_client.py
from __future__ import annotations

import json
import os
import re
from typing import Dict, List

from openai import OpenAI


_JSON_RE = re.compile(r"\{.*\}", re.S)


class ColorLLMBatchClient:
    def __init__(self, allowed_colors: List[str], model: str = "gpt-5-mini"):
        if "Other" not in allowed_colors:
            raise ValueError("allowed_colors must include 'Other'")

        if not os.getenv("OPENAI_API_KEY"):
            raise RuntimeError("OPENAI_API_KEY is not set")

        self.allowed_colors = allowed_colors
        self.model = model
        self.client = OpenAI()

    def classify_all(self, queries: List[str]) -> Dict[str, str]:
        """
        One-shot classification.
        Input: list of brand_color_query
        Output: dict {query: normalized_color}
        """

        system = (
            "Ты классификатор цветов.\n"
            "Верни JSON-объект, где:\n"
            "- ключ = исходная строка\n"
            "- значение = ОДИН цвет из allowed_colors\n"
            "Если сомневаешься — 'Other'.\n"
            "Ответ СТРОГО JSON, без пояснений."
        )

        user = (
            "allowed_colors:\n"
            + "\n".join(self.allowed_colors)
            + "\n\nvalues:\n"
            + "\n".join(f"- {q}" for q in queries)
        )

        resp = self.client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )

        text = resp.output_text.strip()
        data = self._parse_json(text)

        result = {}
        for q in queries:
            val = data.get(q, "Other")
            if val not in self.allowed_colors:
                val = "Other"
            result[q] = val

        return result

    def _parse_json(self, text: str) -> Dict[str, str]:
        try:
            return json.loads(text)
        except Exception:
            match = _JSON_RE.search(text)
            if match:
                try:
                    return json.loads(match.group(0))
                except Exception:
                    pass
        return {}

