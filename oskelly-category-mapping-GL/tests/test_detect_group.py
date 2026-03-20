from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as category_main  # noqa: E402


CFG = {
    "grouping": {
        "men_markers": ["Uomo", "Men", "Man", "Male", "Homme", "Herr", "Herren"],
        "women_markers": ["Donna", "Women", "Woman", "Female", "Femme", "Damen"],
        "unisex_markers": ["Unisex"],
        "lifestyle_markers": ["Lifestyle", "Home", "Casa", "Maison"],
        "default_group": "WOMEN",
    }
}


def test_detect_group_keeps_woman_and_women_in_women_group() -> None:
    assert category_main.detect_group("Woman", CFG) == "WOMEN"
    assert category_main.detect_group("Women", CFG) == "WOMEN"
    assert category_main.detect_group("Women > Clothing", CFG) == "WOMEN"


def test_detect_group_respects_other_marker_groups() -> None:
    assert category_main.detect_group("Man", CFG) == "MEN"
    assert category_main.detect_group("Men > Clothing", CFG) == "MEN"
    assert category_main.detect_group("Unisex > Tops", CFG) == "WOMEN"
    assert category_main.detect_group("Home > Women > Accessories", CFG) == "LIFESTYLE"
