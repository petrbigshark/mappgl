from __future__ import annotations

import sys
from pathlib import Path

import openpyxl


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as season_main  # noqa: E402


def test_build_records_deduplicates_same_reason_before_llm(monkeypatch) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["storecode", "reason"])
    ws.append(["100", "Отсутствует конфигурация для сезона с типом '25SS' и годом 'null'"])
    ws.append(["101", "  Отсутствует   конфигурация  для сезона с типом '25SS' и годом 'null'  "])

    llm_calls: list[str] = []

    def fake_map_by_llm(token: str, model: str = "gpt-5") -> str:
        llm_calls.append(token)
        return "SS"

    monkeypatch.setattr(season_main, "map_by_llm", fake_map_by_llm)

    records, errors = season_main.build_records_from_input(
        ws=ws,
        header_map={"storecode": 1, "reason": 2},
        ref_map=[],
        index_start=45,
        email="test@example.com",
        use_llm=True,
        llm_model="gpt-5",
    )

    assert llm_calls == ["25SS"]
    assert len(records) == 1
    assert len(errors) == 0
    assert records[0][2] == "25SS"
    assert records[0][3] == "SS"
