import os
import json
import requests
import telebot

# =========================
# ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# =========================

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
DADATA_TOKEN = os.environ.get("DADATA_TOKEN")

OPENAI_MODEL = "gpt-4o-mini"

if not TELEGRAM_TOKEN:
    print("❌ Нет TELEGRAM_TOKEN")

if not OPENAI_API_KEY:
    print("❌ Нет OPENAI_API_KEY")

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# =========================
# DADATA
# =========================

def get_company_by_inn(inn: str):
    if not DADATA_TOKEN:
        return None

    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {DADATA_TOKEN}"
    }

    data = {
        "query": inn
    }

    try:
        response = requests.post(url, json=data, headers=headers, timeout=30)

        if response.status_code != 200:
            return None

        result = response.json()
        suggestions = result.get("suggestions", [])

        if not suggestions:
            return None

        company = suggestions[0]["data"]

        return {
            "name": company.get("name", {}).get("full_with_opf"),
            "address": company.get("address", {}).get("value"),
            "ogrn": company.get("ogrn"),
            "inn": company.get("inn")
        }

    except Exception:
        return None

# =========================
# OPENAI PROMPT
# =========================

SYSTEM_PROMPT = """
Ты AI-роутер логистической системы.

ВСЕГДА отвечай ТОЛЬКО на русском языке.
Никакого английского текста.
Ответ возвращай СТРОГО в формате JSON.

Твоя задача:
1. Понять, что хочет пользователь.
2. Определить сценарий.
3. Выделить известные данные.
4. Указать, каких данных не хватает.

Возможные scenario:
- new_carrier_contract
- existing_carrier_trip_request
- create_waybill
- driver_free
- driver_issue
- driver_expense
- logistics_report
- unknown

Возможные role:
- manager
- driver
- owner
- unknown

Ключевые поля, которые нужно извлекать, если они есть:
- customer_name
- carrier_name
- carrier_type
- inn
- phone
- email
- registration_address
- tax_mode
- vat_mode
- ogrn
- route_from
- route_to
- route_name
- price
- date
- loading_time
- driver_name
- vehicle_number
- pallets
- temperature_mode
- requires_medbook
- issue_text
- expense_type
- expense_amount
- fuel_amount
- fuel_liters

Правила определения сценария:

1) new_carrier_contract
Если пользователь пишет:
- новый перевозчик
- сделай договор
- оформи нового перевозчика
- есть ИНН
то это new_carrier_contract

Обязательные поля:
- customer_name
- inn ИЛИ carrier_name
- phone
- email
- registration_address
- tax_mode

2) existing_carrier_trip_request
Если пользователь пишет:
- сделай договор-заявку
- рейс
- заявка на рейс
то это existing_carrier_trip_request

Обязательные поля:
- customer_name
- carrier_name
- route_from ИЛИ route_name
- route_to
- price

3) create_waybill
Если пользователь пишет:
- путевой лист
то это create_waybill

Обязательные поля:
- vehicle_number
- driver_name
- date

4) driver_free
Если пользователь пишет:
- свободен
- товар сдал
- разгрузился
- документы позже
то это driver_free

Обязательные поля:
- факт сдачи груза
- driver_name или vehicle_number, если указаны

5) driver_issue
Если пользователь пишет:
- замечание по машине
- поломка
- неисправность
то это driver_issue

Обязательные поля:
- vehicle_number
- issue_text

6) driver_expense
Если пользователь пишет:
- заправка
- купил масло
- расход
- потратил
то это driver_expense

Обязательные поля:
- vehicle_number
- expense_type
- expense_amount

7) logistics_report
Если пользователь пишет:
- расход за сегодня
- логистика за 28 марта
- сколько потратили
то это logistics_report

Верни JSON строго такого вида:

{
  "role": "manager",
  "scenario": "new_carrier_contract",
  "known": {
    "inn": "381234567890"
  },
  "missing": ["customer_name", "phone", "email", "registration_address", "tax_mode"],
  "next_question": "Укажите заказчика, телефон, email, адрес регистрации и налоговый режим."
}

Если сценарий неясен, верни:

{
  "role": "unknown",
  "scenario": "unknown",
  "known": {},
  "missing": [],
  "next_question": "Уточните, что нужно сделать: договор, договор-заявку, путевой лист, замечание по машине или расход."
}
"""

# =========================
# OPENAI REQUEST
# =========================

def ask_openai_router(user_text: str) -> dict:
    url = "https://api.openai.com/v1/responses"

    payload = {
        "model": OPENAI_MODEL,
        "input": [
            {
                "role": "system",
                "content": SYSTEM_PROMPT
            },
            {
                "role": "user",
                "content": user_text
            }
        ],
        "store": False
    }

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=payload, timeout=90)
    response.raise_for_status()
    data = response.json()

    output_text = ""

    if "output_text" in data and data["output_text"]:
        output_text = data["output_text"]
    else:
        output = data.get("output", [])
        for item in output:
            if item.get("type") == "message":
                content = item.get("content", [])
                for c in content:
                    if c.get("type") in ("output_text", "text"):
                        output_text += c.get("text", "")

    output_text = output_text.strip()

    if output_text.startswith("```"):
        output_text = output_text.strip("`")
        output_text = output_text.replace("json", "", 1).strip()

    return json.loads(output_text)

# =========================
# DADATA ENRICHMENT
# =========================

def enrich_result_with_dadata(result: dict) -> dict:
    known = result.get("known", {}) or {}
    missing = result.get("missing", []) or []

    inn = known.get("inn")
    if not inn:
        return result

    company = get_company_by_inn(inn)
    if not company:
        return result

    if company.get("name") and not known.get("carrier_name"):
        known["carrier_name"] = company["name"]

    if company.get("address") and not known.get("registration_address"):
        known["registration_address"] = company["address"]

    if company.get("ogrn") and not known.get("ogrn"):
        known["ogrn"] = company["ogrn"]

    new_missing = []
    for field in missing:
        if field == "registration_address" and known.get("registration_address"):
            continue
        new_missing.append(field)

    result["known"] = known
    result["missing"] = new_missing

    if result.get("scenario") == "new_carrier_contract":
        if known.get("carrier_name") and known.get("registration_address"):
            result["next_question"] = (
                "Нашёл данные по ИНН. "
                "Укажите заказчика, телефон, email и налоговый режим."
            )
        elif known.get("carrier_name"):
            result["next_question"] = (
                "Нашёл название по ИНН. "
                "Укажите заказчика, телефон, email, адрес регистрации и налоговый режим."
            )

    return result

# =========================
# FORMATTING
# =========================

def format_router_result(result: dict) -> str:
    scenario = result.get("scenario", "unknown")
    role = result.get("role", "unknown")
    known = result.get("known", {})
    missing = result.get("missing", [])
    next_question = result.get("next_question", "")

    scenario_labels = {
        "new_carrier_contract": "Новый перевозчик → договор",
        "existing_carrier_trip_request": "Существующий перевозчик → договор-заявка",
        "create_waybill": "Путевой лист",
        "driver_free": "Водитель свободен / сдал груз",
        "driver_issue": "Замечание по машине",
        "driver_expense": "Расход водителя / машины",
        "logistics_report": "Отчёт по логистике",
        "unknown": "Не определено"
    }

    role_labels = {
        "manager": "Менеджер",
        "driver": "Водитель",
        "owner": "Руководитель",
        "unknown": "Не определено"
    }

    field_labels = {
        "customer_name": "заказчик",
        "carrier_name": "название перевозчика",
        "carrier_type": "тип перевозчика",
        "inn": "ИНН",
        "phone": "телефон",
        "email": "email",
        "registration_address": "адрес регистрации",
        "tax_mode": "налоговый режим",
        "vat_mode": "ставка НДС",
        "ogrn": "ОГРН / ОГРНИП",
        "route_from": "откуда",
        "route_to": "куда",
        "route_name": "маршрут",
        "price": "цена",
        "date": "дата",
        "loading_time": "время загрузки",
        "driver_name": "водитель",
        "vehicle_number": "номер машины",
        "pallets": "палеты",
        "temperature_mode": "температурный режим",
        "requires_medbook": "нужна медкнижка",
        "issue_text": "замечание",
        "expense_type": "тип расхода",
        "expense_amount": "сумма расхода",
        "fuel_amount": "сумма топлива",
        "fuel_liters": "литры топлива"
    }

    lines = []
    lines.append(f"Роль: {role_labels.get(role, role)}")
    lines.append(f"Сценарий: {scenario_labels.get(scenario, scenario)}")
    lines.append("")

    if known:
        lines.append("Что я уже понял:")
        for k, v in known.items():
            label = field_labels.get(k, k)
            lines.append(f"• {label}: {v}")
        lines.append("")

    if missing:
        lines.append("Чего не хватает:")
        for item in missing:
            label = field_labels.get(item, item)
            lines.append(f"• {label}")
        lines.append("")

    if next_question:
        lines.append("Следующий вопрос:")
        lines.append(next_question)

    return "\n".join(lines)

# =========================
# TELEGRAM HANDLERS
# =========================

@bot.message_handler(commands=["start"])
def handle_start(message):
    bot.send_message(
        message.chat.id,
        "Бот запущен.\n\n"
        "Сейчас он умеет:\n"
        "— понимать задачу\n"
        "— определять сценарий\n"
        "— подтягивать реквизиты по ИНН через DaData\n\n"
        "Пример:\n"
        "Сделай договор новый перевозчик ИНН 381234567890"
    )

@bot.message_handler(content_types=["text"])
def handle_text(message):
    user_text = message.text.strip()

    if not user_text:
        bot.send_message(message.chat.id, "Пустое сообщение.")
        return

    try:
        result = ask_openai_router(user_text)
        result = enrich_result_with_dadata(result)
        reply = format_router_result(result)
        bot.send_message(message.chat.id, reply)
    except Exception as e:
        bot.send_message(message.chat.id, f"Ошибка:\n{str(e)}")

# =========================
# START
# =========================

if __name__ == "__main__":
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
