from __future__ import annotations

from pathlib import Path

import pandas as pd

import orchestrator


def test_e2e_pipeline_no_mapping(tmp_path: Path) -> None:
    input_path = tmp_path / "input.xlsx"
    df = pd.DataFrame(
        {
            "storecode": ["s1", "s2"],
            "reason": [
                "Найдены ошибки при валидации товара: Не указана цена товара",
                "Найдены ошибки при валидации товара: Не указано описание товара",
            ],
            "brand": ["A", "B"],
            "parentcategory": ["Women", "Women"],
            "category": ["Unknown", "Unknown"],
            "color": ["x", "y"],
            "material": ["m1", "m2"],
        }
    )
    with pd.ExcelWriter(input_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Result 1", index=False)

    request = {
        "input_file": str(input_path),
        "email": "e2e@example.com",
        "index_starts": {
            "brand": 100,
            "color": 200,
            "material": 300,
            "category": 400,
            "season": 500,
        },
        "output_root": str(tmp_path / "runs"),
        "timezone": "Europe/Tallinn",
        "require_llm_success": True,
    }

    result = orchestrator.run_pipeline(request)

    assert Path(result["report_file"]).exists()
    assert result["matching"]["rows_in_prefiltered_sheet"] == 1
    assert result["summary"]["status_counts"]["NO_MAPPING"] == 6
    assert result["summary"]["status_counts"]["FAILED"] == 0
    assert result["modules"]["brand"]["llm_metrics"]["llm_sent_after_dedup"] == 0
    assert result["modules"]["color"]["llm_metrics"]["llm_sent_after_dedup"] == 0

