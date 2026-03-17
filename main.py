import os
import telebot
import requests

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
GOOGLE_SCRIPT_URL = os.environ["GOOGLE_SCRIPT_URL"]

bot = telebot.TeleBot(TELEGRAM_TOKEN)

REQUIRED_FIELDS = [
    "form",
    "name",
    "inn",
    "phone",
    "email",
    "bank",
    "rs",
    "bik",
    "ks",
    "address",
]

FIELD_LABELS = {
    "form": "Тип",
    "name": "Название",
    "inn": "ИНН",
    "ogrn": "ОГРН / ОГРНИП",
    "director": "Директор",
    "address": "Адрес",
    "phone": "Телефон",
    "email": "Email",
    "bank": "Банк",
    "rs": "Расчетный счет",
    "bik": "БИК",
    "ks": "Корр счет",
}


def get_template_by_text(text: str):
    t = text.strip().lower()

    if t == "договор с новым перевозчиком ип":
        return (
            "Заполните и отправьте одним сообщением:\n\n"
            "Новый перевозчик\n"
            "Тип: ИП\n"
            "Название:\n"
            "ИНН:\n"
            "ОГРНИП:\n"
            "Телефон:\n"
            "Email:\n"
            "Банк:\n"
            "Расчетный счет:\n"
            "БИК:\n"
            "Корр счет:\n"
            "Адрес:"
        )

    if t == "договор с новым перевозчиком ооо":
        return (
            "Заполните и отправьте одним сообщением:\n\n"
            "Новый перевозчик\n"
            "Тип: ООО\n"
            "Название:\n"
            "ИНН:\n"
            "ОГРН:\n"
            "Директор:\n"
            "Телефон:\n"
            "Email:\n"
            "Банк:\n"
            "Расчетный счет:\n"
            "БИК:\n"
            "Корр счет:\n"
            "Адрес:"
        )

    if t == "договор с новым перевозчиком самозанятый":
        return (
            "Заполните и отправьте одним сообщением:\n\n"
            "Новый перевозчик\n"
            "Тип: САМОЗАНЯТЫЙ\n"
            "Название:\n"
            "ИНН:\n"
            "Телефон:\n"
            "Email:\n"
            "Банк:\n"
            "Расчетный счет:\n"
            "БИК:\n"
            "Корр счет:\n"
            "Адрес:"
        )

    return None


def parse_carrier_message(text: str) -> dict:
    data = {}

    for line in text.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue

        key, value = line.split(":", 1)
        data[key.strip().lower()] = value.strip()

    return {
        "action": "create_carrier_contract",
        "form": data.get("тип", ""),
        "name": data.get("название", ""),
        "inn": data.get("инн", ""),
        "ogrn": data.get("огрн", data.get("огрнип", "")),
        "director": data.get("директор", ""),
        "address": data.get("адрес", ""),
        "phone": data.get("телефон", ""),
        "email": data.get("email", ""),
        "bank": data.get("банк", ""),
        "rs": data.get("расчетный счет", ""),
        "bik": data.get("бик", ""),
        "ks": data.get("корр счет", ""),
    }


def validate_payload(payload: dict):
    return [field for field in REQUIRED_FIELDS if not payload.get(field)]


def format_missing_fields(missing_fields):
    return "\n".join(f"{FIELD_LABELS.get(field, field)}:" for field in missing_fields)


def create_contract(payload: dict) -> dict:
    response = requests.post(GOOGLE_SCRIPT_URL, json=payload, timeout=120)
    response.raise_for_status()
    return response.json()


@bot.message_handler(commands=["start"])
def start(message):
    bot.reply_to(
        message,
        "Бот готов.\n\n"
        "Команды:\n"
        "договор с новым перевозчиком ИП\n"
        "договор с новым перевозчиком ООО\n"
        "договор с новым перевозчиком самозанятый\n\n"
        "Или отправьте заполненную карточку, начиная с:\n"
        "Новый перевозчик"
    )


@bot.message_handler(func=lambda message: True)
def handle(message):
    text = (message.text or "").strip()

    template = get_template_by_text(text)
    if template:
        bot.reply_to(message, template)
        return

    if text.lower().startswith("новый перевозчик"):
        try:
            payload = parse_carrier_message(text)
            missing = validate_payload(payload)

            if missing:
                bot.reply_to(
                    message,
                    "Не хватает следующих полей:\n\n" + format_missing_fields(missing)
                )
                return

            bot.reply_to(message, "Создаю договор...")

            result = create_contract(payload)

            if not result.get("ok"):
                bot.reply_to(
                    message,
                    f"Ошибка создания договора: {result.get('error', 'неизвестная ошибка')}"
                )
                return

            data = result.get("result", {})

            contract_number = data.get("contractNumber", "")
            document_url = data.get("docUrl", "")
            pdf_url = data.get("pdfUrl", "")
            folder_url = data.get("folderUrl", "")

            parts = ["Договор создан."]

            if contract_number:
                parts.append(f"Номер договора: {contract_number}")

            if document_url:
                parts.append(f"Документ:\n{document_url}")

            if pdf_url:
                parts.append(f"PDF:\n{pdf_url}")

            if folder_url:
                parts.append(f"Папка:\n{folder_url}")

            bot.reply_to(message, "\n\n".join(parts))

        except requests.HTTPError as e:
            bot.reply_to(message, f"Ошибка HTTP при обращении к Google Script: {e}")
        except Exception as e:
            bot.reply_to(message, f"Ошибка: {e}")
        return

    bot.reply_to(
        message,
        "Не понял команду.\n\n"
        "Напишите:\n"
        "договор с новым перевозчиком ИП\n"
        "или отправьте заполненную карточку с заголовком:\n"
        "Новый перевозчик"
    )


print("Бот запущен...")
bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=30)
