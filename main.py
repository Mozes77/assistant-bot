import os
import json
import requests
import telebot

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN",)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY",)
OPENAI_MODEL = "gpt-5.4-mini"

bot = telebot.TeleBot(TELEGRAM_TOKEN)

SYSTEM_PROMPT = """
Ты AI-роутер логистической системы.

Твоя задача: разобрать сообщение пользователя и вернуть СТРОГО JSON.

Возможные scenario:
- new_carrier_contract
- existing_carrier_trip_request
- create_waybill
- driver_free
- driver_issue
- driver_expense
- logistics_report
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

Правила:

1) new_carrier_contract
если пользователь пишет:
- новый перевозчик
- сделай договор
- оформи нового перевозчика
- заказчик ИП Галанина / ООО Фрукт Сервис
- есть ИНН
то это new_carrier_contract

обязательные поля:
- customer_name
- inn ИЛИ carrier_name
- phone
- email
- registration_address
- tax_mode

2) existing_carrier_trip_request
если пользователь пишет:
- сделай договор-заявку
- рейс
- заявка на рейс
то это existing_carrier_trip_request

обязательные поля:
- customer_name
- carrier_name
- route_from ИЛИ route_name
- route_to
- price

3) create_waybill
если пользователь пишет:
- путевой лист
то это create_waybill

обязательные поля:
- vehicle_number
- driver_name
- date

4) driver_free
если пользователь пишет:
- свободен
- товар сдал
- разгрузился
- документы позже
то это driver_free

обязательные поля:
- driver_name если есть
- vehicle_number если есть
- факт сдачи груза

5) driver_issue
если пользователь пишет:
- замечание по машине
- поломка
- неисправность
то это driver_issue

обязательные поля:
- vehicle_number
- issue_text

6) driver_expense
если пользователь пишет:
- заправка
- купил масло
- расход
- потратил
то это driver_expense

обязательные поля:
- vehicle_number
- expense_type
- expense_amount

7) logistics_report
если пользователь пишет:
- расход за сегодня
- логистика за 28 марта
- сколько потратили
то это logistics_report

Ответ возвращай ТОЛЬКО в JSON, без пояснений вне JSON.

Формат ответа:

{
  "role": "manager",
  "scenario": "new_carrier_contract",
  "known": {
    "customer_name": "ИП Галанина",
    "inn": "381234567890"
  },
  "missing": ["phone", "email", "registration_address", "tax_mode"],
  "next_question": "Укажите телефон, email, адрес регистрации и налоговый режим."
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

    lines = []
    lines.append(f"Роль: {role_labels.get(role, role)}")
    lines.append(f"Сценарий: {scenario_labels.get(scenario, scenario)}")
    lines.append("")

    if known:
        lines.append("Что я уже понял:")
        for k, v in known.items():
            lines.append(f"• {k}: {v}")
        lines.append("")

    if missing:
        lines.append("Чего не хватает:")
        for item in missing:
            lines.append(f"• {item}")
        lines.append("")

    if next_question:
        lines.append("Следующий вопрос:")
        lines.append(next_question)

    return "\n".join(lines)


@bot.message_handler(commands=["start"])
def handle_start(message):
    bot.send_message(
        message.chat.id,
        "Бот запущен.\n\n"
        "Сейчас он работает как AI-роутер:\n"
        "понимает задачу, выделяет что уже известно и чего не хватает.\n\n"
        "Примеры:\n"
        "1) Сделай договор заказчик ИП Галанина с новым перевозчиком ИНН 381234567890\n"
        "2) Сделай договор-заявку от ИП Галанина с ИП Тропиным рейс ИВИ - Слата ФРОВ цена 42000\n"
        "3) Выписать путевой лист на 456 водитель Иванов дата 31.03.2026\n"
        "4) Машина 456 свободен, товар сдал, документы позже\n"
        "5) Замечание по машине 456: спустило колесо\n"
        "6) Заправка 456 на 3200 рублей"
    )


@bot.message_handler(content_types=["text"])
def handle_text(message):
    user_text = message.text.strip()

    if not user_text:
        bot.send_message(message.chat.id, "Пустое сообщение.")
        return

    try:
        result = ask_openai_router(user_text)
        reply = format_router_result(result)
        bot.send_message(message.chat.id, reply)
    except Exception as e:
        bot.send_message(message.chat.id, f"Ошибка:\n{str(e)}")


if __name__ == "__main__":
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
