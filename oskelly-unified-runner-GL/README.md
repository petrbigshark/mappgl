Oskelly Unified Runner

Короткий запуск: поднять локальный API и отправить один запрос на прогон.

0) Первичная настройка shared venv (один раз)

cd "/Users/petr/Documents/Automatisation Oskelly"
python3 -m venv GLOBAL/.venv
source GLOBAL/.venv/bin/activate
pip install -r GLOBAL/requirements.shared.txt

1) Запуск сервера

export OPENAI_API_KEY="sk-..."
cd "/Users/petr/Documents/Automatisation Oskelly/GLOBAL/oskelly-unified-runner-GL"
source ../.venv/bin/activate
uvicorn server:app --host 127.0.0.1 --port 8000

Сервер держи в отдельной вкладке терминала.

2) Отправка запроса

cat > /tmp/run.json <<'JSON'
{
  "input_file": "/Users/petr/Documents/Automatisation Oskelly/GLOBAL/oskelly-unified-runner-GL/ccc.xlsx",
  "email": "cuccuinioskelly@gmail.com",
  "index_starts": {
    "brand": 1000,
    "color": 2000,
    "material": 3000,
    "category": 4000,
    "season": 5000
  },
  "output_root": "/Users/petr/Documents/Automatisation Oskelly/GLOBAL/unified-output",
  "timezone": "Europe/Moscow",
  "require_llm_success": true,
  "use_llm_brand": true,
  "use_llm_season": false
}
JSON

curl -sS -o /tmp/resp.json -w "HTTP %{http_code}\n" \
  -X POST "http://127.0.0.1:8000/run" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/run.json

cat /tmp/resp.json

3) Где результат

- Общий отчёт: .../unified-output/<email> <date> vN/run_report.json
- Файлы модулей: brands/, colors/, materials/, categories/, seasons/, sizes/ внутри того же run_dir
- Строки без маппинга: rows_without_any_mapping.xlsx (если есть)

4) Остановка сервера

В терминале с uvicorn нажми Ctrl+C.
