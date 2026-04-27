### Настройка интеграции Telegram-бота (ООО «Фрукт Сервис»)

#### 1) Переменные окружения
В корне проекта уже создан файл `.env`:

`/home/ubuntu/bot_project/assistant-bot-main/.env`

Заполните реальные значения:

- `TELEGRAM_TOKEN` — токен бота от @BotFather
- `OPENAI_API_KEY` — ключ OpenAI
- `DADATA_API_KEY` — ключ DaData
- `GOOGLE_SCRIPT_URL` — URL веб-приложения Google Apps Script

Текущий `GOOGLE_SCRIPT_URL` уже установлен:

`https://script.google.com/macros/s/AKfycbxAvRM5GrKnSYJ0GIG-0k9U7HLJhRLsUQfRcbsWlk_W1YdPl9NN0vhJxB4tajvXslTu/exec`

> Примечание: код поддерживает и `DADATA_API_KEY`, и `DADATA_TOKEN` для совместимости.

#### 2) Установка зависимостей
Из папки проекта выполните:

```bash
pip install -r requirements.txt
```

В `requirements.txt` добавлен `python-dotenv`, чтобы бот автоматически читал `.env`.

#### 3) Запуск бота
Из корня проекта:

```bash
python3 main.py
```

Если всё настроено корректно, бот начнёт polling Telegram.

#### 4) Что было исправлено в Google Drive
Папка шаблонов: `18oAFBeUo9qx1gXqIjs0UWq1ZKXg4vWcZ` (`/CRM_ЛОГИСТИКА/Шаблоны/`)

Исправления:

1. Переименован файл:
   - Было: `Договор_ЗАЯВКА_ШАБЛОН ` (с пробелом в конце)
   - Стало: `Договор_ЗАЯВКА_ШАБЛОН`

2. Создан недостающий документ:
   - `Шаблон доверенность на водителя` (Google Docs)

Это устраняет падение Apps Script при поиске шаблонов по имени.

#### 5) Быстрая проверка после запуска
1. Отправьте боту команду `/start`
2. Отправьте тест на создание нового перевозчика
3. Убедитесь, что бот доходит до вызова Google Script без ошибки `GOOGLE_SCRIPT_URL не задан`
4. Проверьте, что сценарии, использующие шаблоны, больше не падают из-за отсутствующих/неверно названных файлов
