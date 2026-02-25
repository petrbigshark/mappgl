# Color Normalization Pipeline (Excel -> OpenAI -> Template Excel)

## Что делает
1) Берёт входной Excel  
2) Оставляет только строки, где `reason` содержит `Не найден цвет`  
3) Создаёт `brand_color_query = brand + " color " + color`  
4) Делает дедупликацию по `brand_color_query` (чтобы не было повторов)  
5) Для каждой уникальной строки просит OpenAI вернуть один цвет строго из списка  
6) Сохраняет результат в Excel **в формате как в шаблоне**:

Колонки:
- `id` (1..N)
- `objectType` (константа: `COLOR`)
- `expr` (что было: сырое значение `color`)
- `outputValue` (что стало: нормализованный цвет)
- `matchType` (константа: `EQUALS`)
- `repeatMatching` (константа: `false`)
- `params` (константа: `{}`)
- `indexNumber` (последовательность от заданного стартового значения)
- `clientEmail` (одна почта на все строки)
- `oskellyId` (по умолчанию пусто)

## Установка
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

## Ключ OpenAI
Экспортируй переменную окружения:
```bash
export OPENAI_API_KEY="sk-..."
```
(или используй свой менеджер секретов)

## Запуск
### Обычный запуск
```bash
python main.py --input meyer.xlsx --index-start 1000 --email you@mail.com
```

### Без OpenAI (заглушка)
```bash
python main.py --input meyer.xlsx --dry-run --index-start 1000 --email you@mail.com
```

### Если нужно заполнить oskellyId константой
```bash
python main.py --input meyer.xlsx --index-start 1000 --email you@mail.com --oskelly-id 555
```

Результат: `output/template_output.xlsx`
