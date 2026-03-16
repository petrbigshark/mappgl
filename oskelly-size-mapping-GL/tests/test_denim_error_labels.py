from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as size_main  # noqa: E402


def test_slash_size_is_marked_only_for_jeans_category() -> None:
    reason = "не найден размер '7 1/8' с количеством '2' для типа 'INT'"
    assert (
        size_main.infer_denim_size_error_reason_label(
            reason=reason,
            category="Cappelli",
            parentcategory="Uomo",
        )
        is None
    )
    assert (
        size_main.infer_denim_size_error_reason_label(
            reason=reason,
            category="Jeans",
            parentcategory="Donna",
        )
        == size_main.DENIM_SLASH_LABEL
    )


def test_pants_and_skirts_require_default_size_phrase_with_20_to_33() -> None:
    assert (
        size_main.infer_denim_size_error_reason_label(
            reason=(
                "Для категории 'Прямые брюки' не найден тип размера 'INT'; "
                "Вместо размера '25' выставлен размер по умолчанию '3XS'"
            ),
            category="Camicie",
            parentcategory="Donna",
        )
        == size_main.DENIM_PANTS_LABEL
    )
    assert (
        size_main.infer_denim_size_error_reason_label(
            reason=(
                "Для категории 'Юбки мини' не найден тип размера 'INT'; "
                "Вместо размера '32' выставлен размер по умолчанию '3XS'"
            ),
            category="Abiti",
            parentcategory="Donna",
        )
        == size_main.DENIM_SKIRTS_LABEL
    )
    assert (
        size_main.infer_denim_size_error_reason_label(
            reason=(
                "Для категории 'Юбки мини' не найден тип размера 'INT'; "
                "Вместо размера '19' выставлен размер по умолчанию '3XS'"
            ),
            category="Abiti",
            parentcategory="Donna",
        )
        is None
    )
    assert (
        size_main.infer_denim_size_error_reason_label(
            reason=(
                "Для категории 'Юбки мини' не найден тип размера 'INT'; "
                "Вместо размера '34' выставлен размер по умолчанию '3XS'"
            ),
            category="Abiti",
            parentcategory="Donna",
        )
        is None
    )

