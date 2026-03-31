import os
import json
import requests
import telebot

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
DADATA_TOKEN = os.environ.get("DADATA_TOKEN")
GOOGLE_SCRIPT_URL = os.environ.get("GOOGLE_SCRIPT_URL")

OPENAI_MODEL = "gpt-4o-mini"

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
    data = {"query": inn}

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
# OPENAI
# =========================

def ask_openai_router(user_text: str) -> dict:
    url = "https://api.openai.com/v1/responses"

    payload = {
        "model": OPENAI_MODEL,
        "input": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_text}
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
                for c in item.get("content", []):
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

    return result

# =========================
# GOOGLE SCRIPT
# =========================

def call_google_script(payload: dict):
    if not GOOGLE_SCRIPT_URL:
        raise RuntimeError("Не задан GOOGLE_SCRIPT_URL")

    response = requests.post(
        GOOGLE_SCRIPT_URL,
        json=payload,
        timeout=120
    )
    response.raise_for_status()
    return response.json()

# =========================
# FORMAT
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
# SESSION MEMORY
# =========================

SESSION_STORE = {}

def get_session(chat_id: int):
    return SESSION_STORE.get(chat_id, {})

def save_session(chat_id: int, data: dict):
    SESSION_STORE[chat_id] = data

def clear_session(chat_id: int):
    if chat_id in SESSION_STORE:
        del SESSION_STORE[chat_id]

# =========================
# TELEGRAM
# =========================

@bot.message_handler(commands=["start"])
def handle_start(message):
    bot.send_message(
        message.chat.id,
        "Бот запущен.\n\n"
        "Сейчас он умеет:\n"
        "— понимать задачу\n"
        "— определять сценарий\n"
        "— подтягивать реквизиты по ИНН через DaData\n"
        "— создавать договор через Google Script после сбора данных\n\n"
        "Пример:\n"
        "Сделай договор новый перевозчик ИНН 381234567890"
    )

@bot.message_handler(content_types=["text"])
def handle_text(message):
    chat_id = message.chat.id
    user_text = message.text.strip()

    if not user_text:
        bot.send_message(chat_id, "Пустое сообщение.")
        return

    try:
        session = get_session(chat_id)

        # Если уже есть незавершённый сценарий нового перевозчика
        if session.get("scenario") == "new_carrier_contract" and session.get("awaiting_more_data"):
            text_lower = user_text.lower()

            if not session.get("customer_name"):
                session["customer_name"] = user_text.strip()
                save_session(chat_id, session)
                bot.send_message(chat_id, "Укажите телефон перевозчика.")
                return

            if not session.get("phone"):
                session["phone"] = user_text.strip()
                save_session(chat_id, session)
                bot.send_message(chat_id, "Укажите email перевозчика.")
                return

            if not session.get("email"):
                session["email"] = user_text.strip()
                save_session(chat_id, session)
                bot.send_message(chat_id, "Укажите налоговый режим. Например: НДС 5%, НДС 20%, без НДС, патент.")
                return

            if not session.get("tax_mode"):
                session["tax_mode"] = user_text.strip()

                payload = {
                    "action": "create_carrier_and_contract",
                    "customer_name": session.get("customer_name", ""),
                    "name": session.get("carrier_name", ""),
                    "inn": session.get("inn", ""),
                    "ogrn": session.get("ogrn", ""),
                    "address": session.get("registration_address", ""),
                    "phone": session.get("phone", ""),
                    "email": session.get("email", ""),
                    "tax_mode": session.get("tax_mode", "")
                }

                gs_result = call_google_script(payload)

                clear_session(chat_id)

                if gs_result.get("ok"):
                    result = gs_result.get("result", {})
                    bot.send_message(
                        chat_id,
                        "Готово.\n\n"
                        f"Перевозчик занесён в базу.\n"
                        f"Договор создан.\n\n"
                        f"Номер договора: {result.get('contractNumber', '-')}\n"
                        f"Документ: {result.get('docUrl', '-')}\n"
                        f"PDF: {result.get('pdfUrl', '-')}"
                    )
                else:
                    bot.send_message(chat_id, f"Ошибка Google Script:\n{gs_result.get('error', 'Неизвестная ошибка')}")
                return

        # Новый входящий запрос
        result = ask_openai_router(user_text)
        result = enrich_result_with_dadata(result)
        reply = format_router_result(result)
        bot.send_message(chat_id, reply)

        if result.get("scenario") == "new_carrier_contract":
            known = result.get("known", {})
            missing = result.get("missing", [])

            if missing:
                session_data = {
                    "scenario": "new_carrier_contract",
                    "awaiting_more_data": True,
                    "customer_name": known.get("customer_name", ""),
                    "carrier_name": known.get("carrier_name", ""),
                    "inn": known.get("inn", ""),
                    "ogrn": known.get("ogrn", ""),
                    "registration_address": known.get("registration_address", ""),
                    "phone": known.get("phone", ""),
                    "email": known.get("email", ""),
                    "tax_mode": known.get("tax_mode", "")
                }
                save_session(chat_id, session_data)

    except Exception as e:
        bot.send_message(chat_id, f"Ошибка:\n{str(e)}")

if __name__ == "__main__":
    bot.infinity_polling(skip_pending=True)
