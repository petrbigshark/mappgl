from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as brand_main  # noqa: E402


def test_main_handles_rows_with_missing_brand_values(tmp_path: Path, monkeypatch) -> None:
    input_path = tmp_path / "input.xlsx"
    brands_id_path = tmp_path / "brands_id_name.xlsx"
    template_path = tmp_path / "template.xlsx"
    outdir = tmp_path / "output"

    pd.DataFrame(
        {
            "reason": [brand_main.PHRASE_NOT_FOUND],
            "brand": [None],
        }
    ).to_excel(input_path, index=False)

    pd.DataFrame({"id": [1], "name": ["TEST BRAND"]}).to_excel(brands_id_path, index=False)
    pd.DataFrame(
        columns=[
            "id",
            "objectType",
            "expr",
            "outputValue",
            "matchType",
            "repeatMatching",
            "params",
            "indexNumber",
            "clientEmail",
            "oskellyId",
        ]
    ).to_excel(template_path, index=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "--input",
            str(input_path),
            "--index-start",
            "1000",
            "--email",
            "test@example.com",
            "--brands-id-name",
            str(brands_id_path),
            "--template",
            str(template_path),
            "--outdir",
            str(outdir),
        ],
    )
    monkeypatch.setattr(brand_main, "__file__", str(tmp_path / "main.py"))

    brand_main.main()

    debug_files = list(outdir.rglob("debug_all_rows.xlsx"))
    upload_files = list(outdir.rglob("Бренды для загрузки *.xlsx"))
    unmapped_files = list(outdir.rglob("Незамапленные бренды *.xlsx"))

    assert len(debug_files) == 1
    assert len(upload_files) == 1
    assert len(unmapped_files) == 1

    debug_df = pd.read_excel(debug_files[0])
    upload_df = pd.read_excel(upload_files[0])
    unmapped_df = pd.read_excel(unmapped_files[0])

    assert list(debug_df.columns) == brand_main.RESULT_COLUMNS
    assert debug_df.empty
    assert upload_df.empty
    assert unmapped_df.empty
