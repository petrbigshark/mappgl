import re
from typing import Optional


def normalize_color_heuristic(raw: Optional[str]) -> str:
    """Cheap baseline (offline) normalizer. Anything unclear -> 'Другое'."""
    s = (raw or "").strip().lower()
    s = re.sub(r"[_\-/]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    if any(k in s for k in ["multi", "multicolor", "multi color", "assorted", "various"]):
        return "Multicolour"
    if any(k in s for k in ["anthracite", "antracite", "anthra", "antra"]):
        return "Anthracite"

    rules = [
        (["pure black", "black", "nero", "noir", "schwarz", "zwart"], "Black"),
        (["white", "bianco", "blanc", "weiss"], "White"),
        (["beige", "tan", "nude", "sabbia", "sand"], "Beige"),
        (["grey", "gray", "grigio", "gris"], "Grey"),
        (["navy", "dark blue", "blu scuro", "midnight", "deep blue"], "Deep blue"),
        (["light blue", "sky", "azzurro", "baby blue"], "Light Blue"),
        (["blue", "blu", "azul"], "Blue"),
        (["burgundy", "bordeaux", "bordo"], "Burgundy"),
        (["coral", "corallo"], "Coral"),
        (["mustard"], "Mustard"),
        (["light green", "mint", "pistachio"], "Light Green"),
        (["green", "verde"], "Green"),
        (["khaki"], "Khaki"),
        (["red", "rosso", "rouge"], "Red"),
        (["yellow", "giallo"], "Yellow"),
        (["pink", "rosa", "rose"], "Pink"),
        (["orange", "arancione"], "Orange"),
        (["purple", "violet", "viola"], "Purple"),
        (["fuchsia", "fucsia"], "Fuchsia"),
        (["brown", "marrone", "brun"], "Brown"),
        (["gold", "oro", "dorato"], "Gold"),
        (["silver", "argento"], "Silver"),
        (["turquoise", "turchese", "teal"], "Turquoise"),
    ]
    for keys, out in rules:
        if any(k in s for k in keys):
            return out

    compact = re.sub(r"\s+", "", s)
    if re.fullmatch(r"[0-9a-z]{2,}", compact):
        return "Other"

    return "Other"

