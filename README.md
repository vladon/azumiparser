# azfilter.jp catalogue exporter

Скрипт выгружает **все запчасти** из `azfilter.jp/catalogue/catalogue` и их **применимость** (нормализовано по авто) в **XLSX**.

## Установка

```bash
cd /Users/vladon/projects/test1
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск

Полный прогон (с чекпоинтом/кешем):

```bash
python tools/azfilter_scrape.py run
```

Только собрать список `pid` (без парсинга карточек):

```bash
python tools/azfilter_scrape.py collect-pids
```

Только распарсить карточки по уже собранным `pid`:

```bash
python tools/azfilter_scrape.py scrape-pages
```

Экспортировать в Excel:

```bash
python tools/azfilter_scrape.py export-xlsx
```

## Результаты и кеш

- Итоговый файл: `out/azfilter_parts.xlsx`
- SQLite чекпоинт/кеш: `out/cache/azfilter.sqlite3`
- HTML карточек (кеш): `out/cache/html/{pid}.html`

## Примечания

- В среде без системных CA сертификатов можно включить небезопасный режим TLS:

```bash
python tools/azfilter_scrape.py run --insecure
```

