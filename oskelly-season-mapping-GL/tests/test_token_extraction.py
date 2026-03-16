from __future__ import annotations

import sys
from pathlib import Path

import openpyxl


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as season_main  # noqa: E402


def test_extract_season_token_handles_split_fw_year() -> None:
    reason = (
        "Отсутствует конфигурация для сезона с типом 'FW'24' и годом 'FW'24'; "
        "Не найден размер '31' с количеством '1' для типа 'US'"
    )

    assert season_main.extract_season_tokens_from_reason(reason) == ["FW'24"]


def test_extract_season_token_handles_split_ss_year() -> None:
    reason = "Отсутствует конфигурация для сезона с типом 'SS'25' и годом 'SS'25'"

    assert season_main.extract_season_tokens_from_reason(reason) == ["SS'25"]


def test_extract_attr_28_token_handles_split_fw_year() -> None:
    reason = 'Ошибка при обновлении товара: 400 BAD_REQUEST "Не задано значение обязательного атрибута 28: \'FW\'24\'"; x'

    assert season_main.extract_season_tokens_from_reason(reason) == ["FW'24"]


def test_extract_season_token_stops_before_year_block() -> None:
    reason = "Отсутствует конфигурация для сезона с типом 'FW24' и годом 'FW25'"

    assert season_main.extract_season_tokens_from_reason(reason) == ["FW24"]


def test_extract_season_token_preserves_long_phrase() -> None:
    reason = "Отсутствует конфигурация для сезона с типом 'Fall/Winter 2024/25' и годом 'null'"

    assert season_main.extract_season_tokens_from_reason(reason) == ["Fall/Winter 2024/25"]


def test_normalize_lookup_token_removes_inner_quote_for_mapping() -> None:
    assert season_main.normalize_lookup_token("FW'24") == "FW24"


def test_build_records_preserves_expr_but_maps_by_normalized_token() -> None:
    reason = "Отсутствует конфигурация для сезона с типом 'FW'24' и годом 'FW'24'"

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["storecode", "reason"])
    ws.append(["100", reason])

    records, errors = season_main.build_records_from_input(
        ws=ws,
        header_map={"storecode": 1, "reason": 2},
        ref_map=[(season_main.re.compile(r"^FW24$", season_main.re.IGNORECASE), "AW")],
        index_start=1,
        email="test@example.com",
        use_llm=False,
        llm_model="gpt-5",
    )

    assert errors == []
    assert len(records) == 1
    assert records[0][2] == "FW'24"
    assert records[0][3] == "AW"
