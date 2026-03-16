from __future__ import annotations

from pathlib import Path

import pandas as pd

import orchestrator


def test_should_drop_reason_rules() -> None:
    assert orchestrator.should_drop_reason(" Product with PLU 123 ")
    assert orchestrator.should_drop_reason("Найдены ошибки при валидации товара: Не указана цена товара")
    assert orchestrator.should_drop_reason(
        "Найдены ошибки при валидации товара: Не указана цена товара; Дополнительная ошибка"
    )

    assert orchestrator.should_drop_reason("Ошибка при публикации товара: random")
    assert orchestrator.should_drop_reason("Ошибка при обновлении товара: 400 BAD_REQUEST x")
    assert orchestrator.should_drop_reason(
        'Ошибка при обновлении товара: 500 INTERNAL_SERVER_ERROR "could not extract ResultSet"'
    )
    assert orchestrator.should_drop_reason(
        "Для категории 'Браслеты' не найден тип размера 'INT'; "
        'Ошибка при обновлении товара: 500 INTERNAL_SERVER_ERROR "could not extract ResultSet"'
    )

    # Keep BAD_REQUEST/publish rows if color/material is present.
    assert not orchestrator.should_drop_reason("Ошибка при публикации товара: не найден Цвет")
    assert not orchestrator.should_drop_reason("Ошибка при обновлении товара: 400 BAD_REQUEST Material missing")
    assert not orchestrator.should_drop_reason(
        "Ошибка при обновлении товара: 400 BAD_REQUEST "
        "\"Не задано значение обязательного атрибута 24: Shirt fabric\""
    )
    assert not orchestrator.should_drop_reason(
        "Ошибка при обновлении товара: 400 BAD_REQUEST "
        "\"Не задано значение обязательного атрибута 10: Color\""
    )
    assert not orchestrator.should_drop_reason(
        "Ошибка при обновлении товара: 400 BAD_REQUEST "
        "\"Не задано значение обязательного атрибута 28: Some value\""
    )
    assert orchestrator.should_drop_reason(
        "Ошибка при обновлении товара: 400 BAD_REQUEST "
        "\"Не задано значение обязательного атрибута 27: Some value\""
    )

    # Unrelated reasons are kept.
    assert not orchestrator.should_drop_reason("Не найден бренд с названием 'X'")


def test_prefilter_workbook_removes_rows(tmp_path: Path) -> None:
    in_path = tmp_path / "input.xlsx"
    out_path = tmp_path / "prefiltered.xlsx"

    df = pd.DataFrame(
        {
            "reason": [
                "Product with PLU 123",
                "Найдены ошибки при валидации товара: Не указана цена товара",
                'Ошибка при обновлении товара: 500 INTERNAL_SERVER_ERROR "could not extract ResultSet"',
                "Ошибка при публикации товара: не найден Цвет",
                "Не найден бренд с названием 'Test'",
            ],
            "brand": ["a", "b", "c", "d", "e"],
        }
    )

    with pd.ExcelWriter(in_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Result 1", index=False)

    main_df, main_sheet, stats = orchestrator.prefilter_workbook(in_path, out_path)

    assert main_sheet == "Result 1"
    assert len(main_df) == 2
    assert stats["Result 1"]["rows_removed"] == 3
    assert out_path.exists()


def test_prefilter_deleted_rows_aggregates_only_by_reason(tmp_path: Path) -> None:
    in_path = tmp_path / "input.xlsx"
    out_path = tmp_path / "prefiltered.xlsx"

    repeated_500 = 'Ошибка при обновлении товара: 500 INTERNAL_SERVER_ERROR "could not extract ResultSet"'
    df = pd.DataFrame(
        {
            "storecode": ["100", "101", "102", "103"],
            "reason": [
                repeated_500,
                repeated_500,  # same reason, different row
                "Product with PLU 123",
                "Не найден бренд с названием 'Test'",
            ],
            "brand": ["a", "b", "c", "d"],
        }
    )

    with pd.ExcelWriter(in_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Result 1", index=False)

    _main_df, _main_sheet, _stats = orchestrator.prefilter_workbook(in_path, out_path)
    deleted_rows_path = out_path.parent / "deleted_rows.xlsx"
    deleted = pd.read_excel(deleted_rows_path)

    assert deleted_rows_path.exists()
    assert "reason" in deleted.columns
    assert "Число удалённых строк" in deleted.columns
    assert len(deleted) == 2

    row_500 = deleted.loc[deleted["reason"] == repeated_500]
    assert len(row_500) == 1
    assert int(row_500.iloc[0]["Число удалённых строк"]) == 2


def test_match_material_rows_by_ids_and_material_words() -> None:
    df = pd.DataFrame(
        {
            "reason": [
                "Ошибка при обновлении товара: 400 BAD_REQUEST "
                "\"Не задано значение обязательного атрибута 24: Shirt fabric\"",
                "Ошибка при обновлении товара: 400 BAD_REQUEST "
                "\"Не задано значение обязательного атрибута 10: Color\"",
                "Не найден материал",
                "Ошибка при обновлении товара: 400 BAD_REQUEST "
                "\"Не задано значение обязательного атрибута 28: Some value\"",
                "Ошибка при публикации товара: "
                "\"Не задано значение обязательного атрибута 17: Fabric\"",
                "Ошибка при обновлении товара: 400 BAD_REQUEST "
                "\"Не задано значение обязательного атрибута 10: Material\"",
                "Ошибка при обновлении товара: 400 BAD_REQUEST "
                "\"Не задано значение обязательного атрибута 28: материал\"",
            ]
        }
    )

    matched = orchestrator.match_material_rows(df)
    assert matched == [1, 3, 5, 6, 7]


def test_match_season_rows_includes_required_attr_28() -> None:
    df = pd.DataFrame(
        {
            "reason": [
                "Отсутствует конфигурация для сезона с типом '25SS' и годом 'null'",
                "Ошибка при обновлении товара: 400 BAD_REQUEST "
                "\"Не задано значение обязательного атрибута 28: 24AW\"",
                "Ошибка при обновлении товара: 400 BAD_REQUEST "
                "\"Не задано значение обязательного атрибута 24: Shirt fabric\"",
            ]
        }
    )

    matched = orchestrator.match_season_rows(df)
    assert matched == [1, 2]


def test_run_pipeline_continues_when_one_module_fails(tmp_path: Path, monkeypatch) -> None:
    input_path = tmp_path / "input.xlsx"
    pd.DataFrame({"reason": ["x"]}).to_excel(input_path, index=False)

    def fake_prefilter(_in: Path, out: Path):
        df = pd.DataFrame(
            {
                "reason": ["need brand", "need color"],
                orchestrator.ROW_ID_COL: [1, 2],
            }
        )
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Result 1", index=False)
        return df, "Result 1", {"Result 1": {"rows_in": 2, "rows_out": 2, "rows_removed": 0}}

    monkeypatch.setattr(orchestrator, "prefilter_workbook", fake_prefilter)
    monkeypatch.setattr(
        orchestrator,
        "collect_match_stats",
        lambda _df: {"brand": [1], "color": [2], "material": [], "category": [], "season": [], "size": []},
    )

    def fail_brand(*_args, **_kwargs):
        raise orchestrator.PipelineError("brand llm failed")

    monkeypatch.setattr(orchestrator, "run_brand", fail_brand)
    monkeypatch.setattr(
        orchestrator,
        "run_color",
        lambda *_args, **_kwargs: {"status": "OK", "report": {"rows_processed": 1}},
    )

    request = {
        "input_file": str(input_path),
        "email": "test@example.com",
        "index_starts": {
            "brand": 1,
            "color": 2,
            "material": 3,
            "category": 4,
            "season": 5,
        },
        "output_root": str(tmp_path / "runs"),
        "timezone": "Europe/Tallinn",
    }

    result = orchestrator.run_pipeline(request)

    assert result["modules"]["brand"]["status"] == "FAILED"
    assert result["modules"]["color"]["status"] == "OK"
    assert result["modules"]["material"]["status"] == "NO_MAPPING"
    assert result["summary"]["has_failures"] is True
    assert result["summary"]["failed_modules"] == ["brand"]
    assert "timings" in result
    assert result["timings"]["total_sec"] is not None
    assert "modules_sec" in result["timings"]
    assert set(result["timings"]["modules_sec"].keys()) == set(orchestrator.MODULE_ORDER)
    assert result["modules"]["brand"]["duration_sec"] is not None
    assert result["modules"]["color"]["duration_sec"] is not None
    assert Path(result["report_file"]).exists()


def test_match_category_rows_excludes_bambina_for_reason_a() -> None:
    cfg = orchestrator.load_yaml(orchestrator.MODULE_DIRS["category"] / "config.yml")
    reason_a = cfg["filters"]["reasons"][0]

    df = pd.DataFrame(
        {
            orchestrator.ROW_ID_COL: [1, 2],
            "reason": [reason_a, reason_a],
            "category": ["Any", "Any"],
            "parentcategory": ["Bambina", "Donna"],
        }
    )
    assert orchestrator.match_category_rows(df) == [2]


def test_match_category_rows_handles_nan_reason_without_crash() -> None:
    cfg = orchestrator.load_yaml(orchestrator.MODULE_DIRS["category"] / "config.yml")
    reason_a = cfg["filters"]["reasons"][0]

    df = pd.DataFrame(
        {
            orchestrator.ROW_ID_COL: [1, 2],
            "reason": [float("nan"), reason_a],
            "category": ["Any", "Any"],
            "parentcategory": ["Donna", "Donna"],
        }
    )
    assert orchestrator.match_category_rows(df) == [2]
