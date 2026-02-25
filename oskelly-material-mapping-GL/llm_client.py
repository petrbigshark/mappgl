from dataclasses import dataclass
from typing import List

from openai import OpenAI


class LLMError(Exception):
    pass


@dataclass
class LLMConfig:
    model: str
    max_output_tokens: int = 256
    max_retries: int = 4


class LLMClient:
    def __init__(self, cfg: LLMConfig) -> None:
        self.cfg = cfg
        self.client = OpenAI()

    def _chat(self, system_prompt: str, user_prompt: str) -> str:
        last_err = None
        for _ in range(self.cfg.max_retries):
            try:
                resp = self.client.chat.completions.create(
                    model=self.cfg.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    max_completion_tokens=self.cfg.max_output_tokens,
                    # temperature можно убрать или поставить 0/1 – как хочешь
                )
                return resp.choices[0].message.content.strip()
            except Exception as e:
                last_err = e
        raise LLMError(
            f"LLM chat failed after {self.cfg.max_retries} attempts: {last_err}"
        )

    def choose_from_list(
        self,
        system_prompt: str,
        question: str,
        options: List[str],
    ) -> str:
        if not options:
            return ""

        opts_text = "\n".join(f"- {opt}" for opt in options)

        user_prompt = (
            f"{question}\n\n"
            f"Allowed options:\n{opts_text}\n\n"
            "Choose EXACTLY ONE option from the list above and answer with that "
            "option only. Do not add any extra words or explanations."
        )

        raw_answer = self._chat(system_prompt, user_prompt).strip()
        answer = raw_answer.strip().strip('"').strip("'")

        for opt in options:
            if answer == opt:
                return opt
        ans_low = answer.lower()
        for opt in options:
            if opt.lower() == ans_low:
                return opt

        raise LLMError(
            "Model returned value outside allowed options. "
            f"answer={raw_answer!r}"
        )
