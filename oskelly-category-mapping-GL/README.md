# Oskelly Category Mapping (FULL dictionary candidates)

Changes in this version:
- Seed confidence threshold set to **0.65**.
- Fuzzy shortlist is **disabled**. LLM receives the **full list** of "Полный путь категории" per group (WOMEN/MEN/LIFESTYLE) once per prompt,
  and returns `choice_index` into that list.

Key constraints still satisfied:
- OpenAI Responses API.
- No temperature/top_p.
- Batch only (no per-row calls).
- Dedup before LLM (by unique (group, expr)).
- `--dry-run` supported.
- Output only under `output/`.
- `run_report.json` created.
- Final dedup by (expr, outputValue, params).

## Install
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## API key
export OPENAI_API_KEY="sk-..."

## Build seed (optional but recommended)
python3 build_seed.py --input "data/Для llm прод.xlsx" --out "data/seed_expr_to_final.csv"

## Run
python3 main.py \
  --input "cuccini.xlsx" \
  --index-start 679 \
  --email "cuccuinioskelly@gmail.com"


### Notes
- Rows where Reason starts with `Не найден бренд с названием` are excluded from processing.
- If seed returns a non-leaf value that exists inside `Полный путь категории`, it is treated as an **anchor** and the row is mapped via LLM within that anchored subset.
- Jewelry preference: when both exist, choose paths under `Украшения` rather than `Ювелирные изделия`.
