# OSkelly material mapping (gpt‑5.1‑mini)

Скрипт берёт Excel партнёра, фильтрует строки с ошибкой «не найден материал»,
через LLM определяет тип материала (LLM №1), затем замапливает сырой материал
в справочник OSkelly (LLM №2) и отдаёт Excel в формате примера Ebonucci.

Запуск:

```bash
pip install -r requirements.txt

export OPENAI_API_KEY="..."
python main.py --input "tessabit18dec.xlsx" --index-start 1222 --email "tessabit@mail.ru"
```

Итоговый файл появится в папке `output` с именем:

`Материалы для загрузки {email} YYYY-MM-DD.xlsx`.
