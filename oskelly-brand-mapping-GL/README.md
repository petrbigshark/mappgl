# oskelly-brand-mapping

## Install
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run (без LLM)
```bash
python3 main.py --input l2l.xlsx --index-start 261 --email "3kicksshop@gmail.com"
```

## Run (c LLM)
1) Set API key:
```bash
export OPENAI_API_KEY="YOUR_KEY"
```

2) Run:
```bash
python3 main.py --input l2l.xlsx --index-start 261 --email "3kicksshop@gmail.com" --use-llm
```

### Notes
- LLM используется только для строк, которые не удалось замапить правилами.
- LLM НЕ может "придумать" бренд: она обязана выбрать `mapped_to` только из списка кандидатов (top-N из справочника), иначе вернёт UNIQUE.
- `repeatMatching` пишется как строка `"false"` (lowercase), как требуется для импорта.
