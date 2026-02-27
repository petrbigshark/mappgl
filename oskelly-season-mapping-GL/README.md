# Season Mapper (AW/SS)

## Что делает
- Берёт входящий Excel (как твой файл с колонками `storecode` и `reason`)
- В `reason` ищет строки вида: **Отсутствует конфигурация для сезона с типом 'XXXX'**
- Также обрабатывает строки вида: **Не задано значение обязательного атрибута 28: XXXX**
- Достаёт `XXXX` и маппит в `AW` или `SS` по справочнику (Excel с колонками `expr`, `outputValue`)
- Формирует output-папку: `output/<email> <dd.mm.yyyy> vN/`
  - `Сезоны для загрузки <email> <dd.mm.yyyy>.xlsx`
  - `ошибки сезонов.xlsx` (только если есть незамаппленные токены)

## Установка
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск
```bash
python3 main.py \
  --input "cuccini.xlsx" \
  --index-start 3653 \
  --email "cuccuinioskelly@gmail.com"
```

Справочник должен лежать рядом с `main.py` и называться **`Справочник.xlsx`**.
Если нужно — можно переопределить путь через `--mapping`.

### LLM (опционально)
Если хочешь включить fallback через модель:
```bash
export OPENAI_API_KEY="..."
python3 main.py --input "cuccini.xlsx" --index-start 3653 --email "..." --use-llm --llm-model "gpt-5"
```
