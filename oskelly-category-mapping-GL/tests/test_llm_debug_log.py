from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import llm_client  # noqa: E402


class _StubResponses:
    def __init__(self, sequence):
        self.sequence = list(sequence)
        self.calls = 0

    def create(self, **_kwargs):
        item = self.sequence[self.calls]
        self.calls += 1
        if isinstance(item, Exception):
            raise item
        return type("Resp", (), {"output_text": item})()


class _StubClient:
    def __init__(self, sequence):
        self.responses = _StubResponses(sequence)


def test_debug_log_writes_success_entry(tmp_path: Path, monkeypatch) -> None:
    debug_path = tmp_path / "llm_debug.jsonl"
    monkeypatch.setattr(llm_client, "OpenAI", lambda: _StubClient(['[{"key":"WOMEN||Dress","choice_index":0}]']))

    mapper = llm_client.ResponsesFullDictMapper(
        llm_client.LLMConfig(
            model="gpt-5-mini",
            max_items_per_request=60,
            request_timeout_sec=120,
            prompt_version="test",
            debug_log_path=str(debug_path),
        )
    )

    result = mapper.map(
        {"WOMEN": ["Женское / Одежда / Платья"]},
        [{"key": "WOMEN||Dress", "group": "WOMEN", "text": "Dress"}],
        debug_context={"stage": "stage1", "chunk_index": 1, "chunk_total": 1},
    )

    assert len(result) == 1
    lines = [json.loads(line) for line in debug_path.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 1
    assert lines[0]["event"] == "success"
    assert lines[0]["stage"] == "stage1"
    assert lines[0]["attempt"] == 1
    assert lines[0]["chunk_item_count"] == 1


def test_debug_log_writes_each_retry_error(tmp_path: Path, monkeypatch) -> None:
    debug_path = tmp_path / "llm_debug.jsonl"
    monkeypatch.setattr(
        llm_client,
        "OpenAI",
        lambda: _StubClient([TimeoutError("Request timed out.")] * 3),
    )
    monkeypatch.setattr(llm_client.time, "sleep", lambda _seconds: None)

    mapper = llm_client.ResponsesFullDictMapper(
        llm_client.LLMConfig(
            model="gpt-5-mini",
            max_items_per_request=60,
            request_timeout_sec=120,
            prompt_version="test",
            debug_log_path=str(debug_path),
        )
    )

    with pytest.raises(llm_client.LLMError, match="Request timed out"):
        mapper.map(
            {"WOMEN": ["Женское / Одежда / Платья"]},
            [{"key": "WOMEN||Dress", "group": "WOMEN", "text": "Dress"}],
            debug_context={"stage": "force_desc", "chunk_index": 2, "chunk_total": 5},
        )

    lines = [json.loads(line) for line in debug_path.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 3
    assert [line["attempt"] for line in lines] == [1, 2, 3]
    assert all(line["event"] == "error" for line in lines)
    assert all(line["stage"] == "force_desc" for line in lines)
    assert all("Request timed out" in line["error"] for line in lines)
