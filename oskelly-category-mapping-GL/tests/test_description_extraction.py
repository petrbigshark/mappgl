from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as category_main  # noqa: E402


CFG = {
    "description_extraction": {
        "enabled": True,
        "stop_markers": [
            "composed of",
            "characterized by",
            "characterised by",
            "featuring",
            "made of",
            "crafted from",
            "produced from",
            "from",
            "with",
            "in",
            "by",
        ],
        "max_tail_words": 4,
        "extra_phrases": [],
    }
}


def test_extract_description_fragment_prefers_exact_phrase_matches() -> None:
    patterns = category_main.description_phrase_patterns(CFG)

    assert (
        category_main.extract_description_fragment(
            "Sneakers by Golden Goose Deluxe Brand in suede leather",
            CFG,
            patterns,
        )
        == "Sneakers"
    )
    assert (
        category_main.extract_description_fragment(
            "Polo Ralph Lauren dress in cotton denim",
            CFG,
            patterns,
        )
        == "dress"
    )
    assert (
        category_main.extract_description_fragment(
            "Two-piece set by Givenchy in cotton",
            CFG,
            patterns,
        )
        == "Two-piece set"
    )


class _StubResult:
    def __init__(self, key: str, full_path: str):
        self.key = key
        self.full_path = full_path


class _RecordingMapper:
    seen_items: list[list[dict[str, str]]] = []

    def __init__(self, _cfg):
        pass

    def map(self, candidates_by_group, items, debug_context=None):  # noqa: ARG002
        self.__class__.seen_items.append([dict(item) for item in items])
        return [
            _StubResult(item["key"], candidates_by_group[item["group"]][0])
            for item in items
        ]


def test_main_uses_desc_fragment_and_dedups_identical_desc_matches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    pd.DataFrame(
        [
            {"id": 1, "Конечная категория": "Кеды", "Полный путь категории": "Women / Shoes / Sneakers"},
        ]
    ).to_excel(data_dir / "Справочник category.xlsx", index=False)
    pd.DataFrame(
        [
            ["Женское", '{"parents": [2]}'],
            ["Мужское", '{"parents": [105]}'],
            ["Lifestyle", '{"parents": [999]}'],
        ]
    ).to_excel(data_dir / "Справочник parentcategory.xlsx", index=False, header=False)
    (data_dir / "description_fragments.txt").write_text("sneakers\nsneaker\n", encoding="utf-8")

    config = """
app:
  timezone: "Europe/Tallinn"
input:
  columns:
    reason: "Reason"
    category: "Category"
    parentcategory: "ParentCategory"
    description: "Description"
filters:
  brand_exclude_prefixes:
    - "Не найден бренд с названием"
  reasons:
    - "Не найдена категория с названием"
    - "Для категории 'Юбки миди' не найден тип размера"
  kids_parent_markers: ["Kids", "Girl", "Boy"]
  skirts_category_markers: ["юбк", "skirt", "skirts"]
grouping:
  men_markers: ["Uomo", "Men", "Man", "Male", "Homme", "Herr", "Herren"]
  women_markers: ["Donna", "Women", "Woman", "Female", "Femme", "Damen"]
  unisex_markers: ["Unisex"]
  lifestyle_markers: ["Lifestyle", "Home", "Casa", "Maison"]
  default_group: "WOMEN"
dictionaries:
  category_xlsx: "data/Справочник category.xlsx"
  parentcategory_xlsx: "data/Справочник parentcategory.xlsx"
  seed_csv: "data/seed_expr_to_final.csv"
  seed_enabled: false
  seed_confidence_threshold: 0.65
  seed_conflict_policy: "llm"
mapping:
  use_full_dictionary_candidates: true
  depth_fallback_threshold: 2
  force_desc_markers: ["Other", "Другое", "Accessories", "Apparel"]
desc_reduction:
  enabled: true
  token_score_threshold: 78
  max_tokens: 6
description_extraction:
  enabled: true
  phrase_dict: "data/description_fragments.txt"
  stop_markers: ["composed of", "featuring", "from", "with", "in", "by"]
  max_tail_words: 4
  extra_phrases: []
llm:
  model: "gpt-5-mini"
  max_items_per_request: 60
  request_timeout_sec: 120
  prompt_version: "test"
output:
  columns:
    - "id"
    - "objectType"
    - "expr"
    - "outputValue"
    - "matchType"
    - "repeatMatching"
    - "params"
    - "oskellyId"
    - "indexNumber"
    - "clientEmail"
  object_type_value: "CATEGORY"
  match_type_value: "EQUALS"
  repeat_matching_value: 'false'
  dedup_subset: ["expr", "outputValue", "params"]
"""
    (tmp_path / "config.yml").write_text(config, encoding="utf-8")

    pd.DataFrame(
        [
            {
                "Reason": "Не найдена категория с названием",
                "Category": "Apparel & Accessories > Shoes",
                "ParentCategory": "Women",
                "Description": "Sneakers by Golden Goose Deluxe Brand in suede leather",
            },
            {
                "Reason": "Не найдена категория с названием",
                "Category": "Apparel & Accessories > Shoes",
                "ParentCategory": "Women",
                "Description": "Sneakers by New Balance with mesh upper",
            },
        ]
    ).to_excel(tmp_path / "input.xlsx", index=False)

    _RecordingMapper.seen_items = []
    monkeypatch.setattr(category_main, "ResponsesFullDictMapper", _RecordingMapper)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "--input",
            str(tmp_path / "input.xlsx"),
            "--index-start",
            "100",
            "--email",
            "global@example.com",
        ],
    )

    category_main.main()

    assert len(_RecordingMapper.seen_items) == 1
    assert len(_RecordingMapper.seen_items[0]) == 1
    assert _RecordingMapper.seen_items[0][0]["group"] == "WOMEN"
    assert _RecordingMapper.seen_items[0][0]["text"] == "Apparel & Accessories > Shoes | Sneakers"

    out_files = list((tmp_path / "output").glob("*/*.xlsx"))
    assert len(out_files) == 1
    out_df = pd.read_excel(out_files[0])

    assert len(out_df) == 1
    assert out_df["outputValue"].tolist() == ["Кеды"]
    params = [json.loads(value) for value in out_df["params"]]
    assert [item["descContains"] for item in params] == ["Sneakers"]
