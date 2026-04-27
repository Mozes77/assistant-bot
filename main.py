import os
import json
import re
import base64
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Any, List, Tuple

import requests
import telebot
from dotenv import load_dotenv

# =========================
# КОНСТАНТЫ И ЛОГИРОВАНИЕ
# =========================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "bot.log"
CONFIG_PATH = BASE_DIR / "config.json"

logger = logging.getLogger("assistant_bot")
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# =========================
# ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# =========================

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
DADATA_TOKEN = os.environ.get("DADATA_TOKEN") or os.environ.get("DADATA_API_KEY")
GOOGLE_SCRIPT_URL = os.environ.get("GOOGLE_SCRIPT_URL")

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

DADATA_TIMEOUT = 30
OPENAI_ROUTER_TIMEOUT = 90
OPENAI_VISION_TIMEOUT = 120
GOOGLE_SCRIPT_TIMEOUT = 120
TELEGRAM_FILE_TIMEOUT = 60

if not TELEGRAM_TOKEN:
    raise RuntimeError("Не задан TELEGRAM_TOKEN")

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# =========================
# КОНФИГ ЗАКАЗЧИКОВ
# =========================

DEFAULT_CONFIG = {
    "customers": [
        {
            "name": "ООО Фрукт Сервис",
            "code": "FRUKT_SERVICE",
            "aliases": ["фрукт сервис", "ооо фрукт сервис", "фруктсервис"],
        },
        {
            "name": "ИП Галанина",
            "code": "GALANINA_IP",
            "aliases": ["галанин", "галина", "ип галанина"],
        },
    ]
}


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        logger.warning("config.json не найден, использую дефолтный конфиг")
        return DEFAULT_CONFIG

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)

        if not isinstance(config, dict) or "customers" not in config:
            logger.warning("config.json имеет неверный формат, использую дефолтный конфиг")
            return DEFAULT_CONFIG

        logger.info("Конфиг заказчиков загружен из %s", CONFIG_PATH)
        return config
    except Exception as e:
        logger.exception("Ошибка чтения config.json: %s", e)
        return DEFAULT_CONFIG


CONFIG = load_config()
CUSTOMERS = CONFIG.get("customers", [])

# =========================
# ПРОСТОЕ ХРАНЕНИЕ СЕССИЙ
# =========================

SESSION_STORE: Dict[int, Dict[str, Any]] = {}


def get_session(chat_id: int) -> Dict[str, Any]:
    return SESSION_STORE.get(chat_id, {})


def save_session(chat_id: int, data: Dict[str, Any]):
    SESSION_STORE[chat_id] = data


def clear_session(chat_id: int):
    if chat_id in SESSION_STORE:
        del SESSION_STORE[chat_id]


# =========================
# ВАЛИДАЦИЯ
# =========================


def clean_digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def normalize_phone(value: str) -> str:
    digits = clean_digits(value)
    if len(digits) != 11 or digits[0] not in ("7", "8"):
        return ""
    return "+7" + digits[1:]


def validate_phone(value: str) -> bool:
    return bool(normalize_phone(value))


def validate_inn(value: str) -> bool:
    digits = clean_digits(value)
    return len(digits) in (10, 12)


def validate_bik(value: str) -> bool:
    return bool(re.fullmatch(r"\d{9}", clean_digits(value)))


def validate_email(value: str) -> bool:
    return bool(re.fullmatch(r"[\w\.-]+@[\w\.-]+\.\w+", (value or "").strip()))


def validate_account_20(value: str) -> bool:
    return bool(re.fullmatch(r"\d{20}", clean_digits(value)))


def validate_session_fields(session: Dict[str, Any]) -> Dict[str, str]:
    errors = {}

    inn = session.get("inn", "")
    if inn and not validate_inn(inn):
        errors["inn"] = "ИНН должен содержать 10 или 12 цифр"

    phone = session.get("phone", "")
    if phone and not validate_phone(phone):
        errors["phone"] = "Телефон должен быть в российском формате (+7XXXXXXXXXX или 8XXXXXXXXXX)"

    email = session.get("email", "")
    if email and not validate_email(email):
        errors["email"] = "Некорректный email"

    bik = session.get("bik", "")
    if bik and not validate_bik(bik):
        errors["bik"] = "БИК должен содержать 9 цифр"

    rs = session.get("rs", "")
    if rs and not validate_account_20(rs):
        errors["rs"] = "Расчетный счет должен содержать 20 цифр"

    ks = session.get("ks", "")
    if ks and not validate_account_20(ks):
        errors["ks"] = "Корреспондентский счет должен содержать 20 цифр"

    return errors


def format_validation_errors_for_user(errors: Dict[str, str]) -> str:
    labels = {
        "inn": "ИНН",
        "phone": "телефон",
        "email": "email",
        "bik": "БИК",
        "rs": "расчетный счет",
        "ks": "корр. счет",
    }

    lines = ["Проверьте, пожалуйста, данные:"]
    for field, error in errors.items():
        lines.append(f"• {labels.get(field, field)}: {error}")

    return "\n".join(lines)


# =========================
# HTTP-ОБВЯЗКА
# =========================


def post_json_with_handling(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int,
    source: str,
) -> Tuple[Dict[str, Any], str]:
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)
    except requests.exceptions.Timeout:
        logger.error("%s: timeout after %s sec", source, timeout)
        return {}, f"Сервис {source} не ответил вовремя. Попробуйте позже."
    except requests.exceptions.RequestException as e:
        logger.exception("%s: request error: %s", source, e)
        return {}, f"Ошибка соединения с сервисом {source}."

    if not (200 <= response.status_code < 300):
        body_preview = (response.text or "")[:500]
        logger.error(
            "%s: bad status=%s body=%s",
            source,
            response.status_code,
            body_preview,
        )
        return {}, f"Сервис {source} вернул ошибку (код {response.status_code})."

    try:
        return response.json(), ""
    except ValueError:
        logger.error("%s: invalid JSON response: %s", source, (response.text or "")[:500])
        return {}, f"Сервис {source} вернул некорректный ответ."


# =========================
# DADATA
# =========================


def get_company_by_inn(inn: str) -> Tuple[Dict[str, Any], str]:
    if not DADATA_TOKEN:
        logger.warning("DADATA_TOKEN не задан")
        return {}, "Сервис DaData не настроен (нет токена)."

    if not validate_inn(inn):
        return {}, "Некорректный ИНН. ИНН должен содержать 10 или 12 цифр."

    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {DADATA_TOKEN}",
    }
    payload = {"query": clean_digits(inn)}

    data, error = post_json_with_handling(
        url=url,
        payload=payload,
        headers=headers,
        timeout=DADATA_TIMEOUT,
        source="DaData",
    )
    if error:
        return {}, error

    suggestions = data.get("suggestions", [])
    if not suggestions:
        return {}, "По указанному ИНН не удалось найти компанию в DaData."

    company = suggestions[0].get("data", {})
    name_data = company.get("name", {}) or {}
    full_name = (
        name_data.get("full_with_opf")
        or name_data.get("full")
        or name_data.get("short_with_opf")
        or name_data.get("short")
        or ""
    )

    return {
        "name": full_name,
        "address": company.get("address", {}).get("value") or "",
        "ogrn": company.get("ogrn") or "",
        "inn": company.get("inn") or "",
        "director": "",
        "carrier_type": detect_carrier_type_from_dadata(company),
    }, ""


# =========================
# OPENAI ROUTER
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
- ogrn
- director
- bank
- rs
- bik
- ks
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
- bank
- rs
- bik
- ks
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
- логистика за дату
- сколько потратили
то это logistics_report

Верни JSON строго такого вида:

{
  "role": "manager",
  "scenario": "new_carrier_contract",
  "known": {
    "inn": "381234567890"
  },
  "missing": ["customer_name", "phone", "email", "bank", "rs", "bik", "ks", "tax_mode"],
  "next_question": "Укажите заказчика, телефон, email, банк, расчетный счет, БИК, корр. счет и налогообложение."
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


def ask_openai_router(user_text: str) -> Tuple[Dict[str, Any], str]:
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY не задан")
        return {}, "Сервис OpenAI не настроен (нет API-ключа)."

    payload = {
        "model": OPENAI_MODEL,
        "input": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ],
        "store": False,
    }

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    data, error = post_json_with_handling(
        url="https://api.openai.com/v1/responses",
        payload=payload,
        headers=headers,
        timeout=OPENAI_ROUTER_TIMEOUT,
        source="OpenAI",
    )
    if error:
        return {}, error

    output_text = extract_output_text(data)
    parsed, parse_error = safe_json_loads(output_text)
    if parse_error:
        logger.error("OpenAI router: ошибка парсинга JSON: %s", parse_error)
        return {}, "Сервис OpenAI вернул неожиданный формат ответа."

    return parsed, ""


# =========================
# OPENAI VISION ДЛЯ КАРТОЧКИ
# =========================


def download_telegram_file(file_id: str) -> Tuple[bytes, str]:
    try:
        file_info = bot.get_file(file_id)
    except Exception as e:
        logger.exception("Telegram get_file error: %s", e)
        return b"", "Не удалось получить файл из Telegram."

    file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_info.file_path}"
    try:
        response = requests.get(file_url, timeout=TELEGRAM_FILE_TIMEOUT)
    except requests.exceptions.Timeout:
        logger.error("Telegram file download timeout")
        return b"", "Скачивание фото заняло слишком много времени."
    except requests.exceptions.RequestException as e:
        logger.exception("Telegram file download request error: %s", e)
        return b"", "Ошибка при скачивании фото."

    if not (200 <= response.status_code < 300):
        logger.error("Telegram file download bad status=%s", response.status_code)
        return b"", f"Telegram вернул ошибку при скачивании фото (код {response.status_code})."

    return response.content, ""


def extract_card_data_from_image(image_bytes: bytes) -> Tuple[Dict[str, Any], str]:
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY не задан")
        return {}, "Сервис OpenAI не настроен (нет API-ключа)."

    base64_image = base64.b64encode(image_bytes).decode("utf-8")

    prompt = """
Ты извлекаешь реквизиты перевозчика с фото карточки предприятия.
Нужно вернуть ТОЛЬКО JSON без пояснений.

Если поле не найдено — верни пустую строку.

Поля:
- carrier_name
- carrier_short_name
- carrier_type
- inn
- ogrn
- registration_address
- phone
- email
- bank
- bank_city
- rs
- ks
- bik
- director

Правила:
- Если это ИП, carrier_type = "ИП"
- Если это ООО, carrier_type = "ООО"
- Если это самозанятый, carrier_type = "САМОЗАНЯТЫЙ"
- ОГРНИП записывай в поле ogrn
- Если видишь ФИО ИП, director можно продублировать этим же ФИО
- Верни строго JSON
"""

    payload = {
        "model": OPENAI_MODEL,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {
                        "type": "input_image",
                        "image_url": f"data:image/jpeg;base64,{base64_image}",
                        "detail": "high",
                    },
                ],
            }
        ],
        "store": False,
    }

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    data, error = post_json_with_handling(
        url="https://api.openai.com/v1/responses",
        payload=payload,
        headers=headers,
        timeout=OPENAI_VISION_TIMEOUT,
        source="OpenAI Vision",
    )
    if error:
        return {}, error

    output_text = extract_output_text(data)
    parsed, parse_error = safe_json_loads(output_text)
    if parse_error:
        logger.error("OpenAI vision: ошибка парсинга JSON: %s", parse_error)
        return {}, "Не удалось корректно распознать карточку. Попробуйте более четкое фото."

    return parsed, ""


# =========================
# ОБОГАЩЕНИЕ РЕЗУЛЬТАТА DADATA
# =========================


def enrich_result_with_dadata(result: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    known = result.get("known", {}) or {}
    missing = result.get("missing", []) or []

    inn = known.get("inn")
    if not inn:
        return result, ""

    company, error = get_company_by_inn(inn)
    if error:
        logger.info("DaData enrichment skipped: %s", error)
        return result, error

    if company.get("name") and not known.get("carrier_name"):
        known["carrier_name"] = company["name"]

    if company.get("address") and not known.get("registration_address"):
        known["registration_address"] = company["address"]

    if company.get("ogrn") and not known.get("ogrn"):
        known["ogrn"] = company["ogrn"]

    if company.get("carrier_type") and not known.get("carrier_type"):
        known["carrier_type"] = company["carrier_type"]

    if company.get("director") and not known.get("director"):
        known["director"] = company["director"]

    new_missing = []
    for field in missing:
        if field == "registration_address" and known.get("registration_address"):
            continue
        new_missing.append(field)

    result["known"] = known
    result["missing"] = new_missing

    if result.get("scenario") == "new_carrier_contract":
        result["next_question"] = (
            "Нашёл данные по ИНН.\n"
            "Пришлите одним сообщением:\n"
            "заказчик, телефон, email, банк, расчетный счет, БИК, корр. счет, налогообложение."
        )

    return result, ""


# =========================
# GOOGLE SCRIPT
# =========================


def call_google_script(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    if not GOOGLE_SCRIPT_URL:
        logger.warning("GOOGLE_SCRIPT_URL не задан")
        return {}, "Не настроена интеграция с Google Script (нет URL)."

    data, error = post_json_with_handling(
        url=GOOGLE_SCRIPT_URL,
        payload=payload,
        headers={"Content-Type": "application/json"},
        timeout=GOOGLE_SCRIPT_TIMEOUT,
        source="Google Script",
    )
    if error:
        return {}, error

    if not isinstance(data, dict):
        logger.error("Google Script: response is not dict")
        return {}, "Google Script вернул неожиданный формат ответа."

    return data, ""


# =========================
# ФОРМАТ ОТВЕТА
# =========================


def format_router_result(result: Dict[str, Any]) -> str:
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
        "unknown": "Не определено",
    }

    role_labels = {
        "manager": "Менеджер",
        "driver": "Водитель",
        "owner": "Руководитель",
        "unknown": "Не определено",
    }

    field_labels = {
        "customer_name": "заказчик",
        "carrier_name": "название перевозчика",
        "carrier_type": "тип перевозчика",
        "inn": "ИНН",
        "phone": "телефон",
        "email": "email",
        "registration_address": "адрес регистрации",
        "tax_mode": "налогообложение",
        "ogrn": "ОГРН / ОГРНИП",
        "director": "директор / подписант",
        "bank": "банк",
        "rs": "расчетный счет",
        "bik": "БИК",
        "ks": "корр. счет",
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
        "fuel_liters": "литры топлива",
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
# ПАРСИНГ ВХОДЯЩИХ ДАННЫХ
# =========================


def normalize_tax_mode(text: str) -> str:
    t = (text or "").lower()

    if "без ндс" in t:
        return "без НДС"
    if "с ндс" in t:
        return "с НДС"
    if "самозан" in t:
        return "самозанятый"
    if "патент" in t:
        return "патент"
    if "нпд" in t:
        return "НПД"

    return ""


def extract_email(text: str) -> str:
    match = re.search(r"[\w\.-]+@[\w\.-]+\.\w+", text or "")
    value = match.group(0) if match else ""
    return value if not value or validate_email(value) else ""


def extract_phone(text: str) -> str:
    match = re.search(r"(\+7|8)[\d\-\s\(\)]{9,}", text or "")
    if not match:
        return ""
    return normalize_phone(match.group(0).strip())


def extract_bik(text: str) -> str:
    match = re.search(r"\b\d{9}\b", text or "")
    if not match:
        return ""
    bik = match.group(0)
    return bik if validate_bik(bik) else ""


def detect_customer_name(text: str) -> str:
    t = (text or "").lower()
    for customer in CUSTOMERS:
        aliases = customer.get("aliases", []) or []
        name = customer.get("name", "")
        normalized_aliases = [a.lower() for a in aliases]

        if name and name.lower() in t:
            return name

        for alias in normalized_aliases:
            if alias and alias in t:
                return name

    return ""


def detect_customer_code(customer_name: str) -> str:
    t = (customer_name or "").lower()
    for customer in CUSTOMERS:
        name = (customer.get("name") or "").lower()
        if name and name == t:
            return customer.get("code", "FRUKT_SERVICE")

    # fallback в случае отсутствия явного совпадения
    return "FRUKT_SERVICE"


def detect_bank_name(text: str) -> str:
    t = text or ""

    known_banks = [
        "Сбербанк",
        "Альфа-Банк",
        "Т-Банк",
        "Тинькофф",
        "ВТБ",
        "Россельхозбанк",
        "Газпромбанк",
        "Совкомбанк",
        "Открытие",
    ]

    for bank in known_banks:
        if bank.lower() in t.lower():
            return bank

    bank_match = re.search(r"банк[:\s\-]+([^\n,]+)", t, re.IGNORECASE)
    if bank_match:
        return bank_match.group(1).strip()

    return ""


def detect_legal_form_from_name(name: str) -> str:
    text = (name or "").upper()

    if "САМОЗАН" in text:
        return "САМОЗАНЯТЫЙ"

    legal_markers = [
        "ОБЩЕСТВО",
        "ООО",
        "ПАО",
        "АО",
        "ОАО",
        "ЗАО",
        "АКЦИОНЕРНОЕ ОБЩЕСТВО",
    ]
    if any(marker in text for marker in legal_markers):
        return "ООО"

    ip_markers = ["ИП", "ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ"]
    if any(marker in text for marker in ip_markers):
        return "ИП"

    return "ИП"


def detect_carrier_type_from_dadata(company: Dict[str, Any]) -> str:
    name_data = company.get("name", {}) or {}
    opf_data = company.get("opf", {}) or {}

    full_name = (
        name_data.get("full_with_opf")
        or name_data.get("full")
        or name_data.get("short_with_opf")
        or name_data.get("short")
        or ""
    )
    opf_short = opf_data.get("short") or ""
    opf_full = opf_data.get("full") or ""
    opf_type = opf_data.get("type") or ""
    dadata_type = company.get("type") or ""

    combined_text = " ".join([full_name, opf_short, opf_full, opf_type]).upper()

    self_employed_flags = [
        company.get("self_employed"),
        company.get("is_self_employed"),
        company.get("is_selfemployed"),
    ]
    has_self_employed_flag = any(flag is True for flag in self_employed_flags)

    if has_self_employed_flag or "САМОЗАН" in combined_text:
        carrier_type = "САМОЗАНЯТЫЙ"
    elif "ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ" in combined_text or re.search(r"(^|\W)ИП(\W|$)", combined_text):
        carrier_type = "ИП"
    elif dadata_type == "INDIVIDUAL":
        carrier_type = "ИП"
    elif detect_legal_form_from_name(combined_text) == "ООО" or dadata_type == "LEGAL":
        carrier_type = "ООО"
    else:
        carrier_type = detect_legal_form_from_name(full_name)

    logger.info(
        "DaData legal form detection | full_name='%s' | opf_short='%s' | opf_full='%s' | dadata_type='%s' | self_employed_flags=%s | carrier_type='%s'",
        full_name,
        opf_short,
        opf_full,
        dadata_type,
        self_employed_flags,
        carrier_type,
    )

    return carrier_type


def extract_all_20_accounts(text: str) -> List[str]:
    accounts = re.findall(r"\b\d{20}\b", text or "")
    return [acc for acc in accounts if validate_account_20(acc)]


def parse_bulk_reply(text: str, session: Dict[str, Any]) -> Dict[str, Any]:
    parsed = dict(session)

    customer_name = detect_customer_name(text)
    if customer_name and not parsed.get("customer_name"):
        parsed["customer_name"] = customer_name

    phone = extract_phone(text)
    if phone and not parsed.get("phone"):
        parsed["phone"] = phone

    email = extract_email(text)
    if email and not parsed.get("email"):
        parsed["email"] = email

    tax_mode = normalize_tax_mode(text)
    if tax_mode and not parsed.get("tax_mode"):
        parsed["tax_mode"] = tax_mode

    bank = detect_bank_name(text)
    if bank and not parsed.get("bank"):
        parsed["bank"] = bank

    all_20 = extract_all_20_accounts(text)
    if all_20:
        if not parsed.get("rs"):
            parsed["rs"] = all_20[0]
        if len(all_20) > 1 and not parsed.get("ks"):
            parsed["ks"] = all_20[1]

    bik = extract_bik(text)
    if bik and not parsed.get("bik"):
        parsed["bik"] = bik

    return parsed


def missing_session_fields(session: Dict[str, Any]) -> List[str]:
    required = ["customer_name", "phone", "email", "bank", "rs", "bik", "ks", "tax_mode"]
    missing = []

    for field in required:
        if not session.get(field):
            missing.append(field)

    return missing


def format_missing_for_user(missing: List[str]) -> str:
    labels = {
        "customer_name": "заказчик",
        "phone": "телефон",
        "email": "email",
        "bank": "банк",
        "rs": "расчетный счет",
        "bik": "БИК",
        "ks": "корр. счет",
        "tax_mode": "налогообложение (с НДС / без НДС / патент / самозанятый)",
    }

    lines = ["Не хватает:"]
    for item in missing:
        lines.append(f"• {labels.get(item, item)}")
    return "\n".join(lines)


# =========================
# СЛУЖЕБНЫЕ
# =========================


def extract_output_text(data: Dict[str, Any]) -> str:
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

    return output_text.strip()


def safe_json_loads(output_text: str) -> Tuple[Dict[str, Any], str]:
    raw = (output_text or "").strip()

    if raw.startswith("```"):
        raw = raw.strip("`")
        raw = raw.replace("json", "", 1).strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        return {}, str(e)

    if not isinstance(data, dict):
        return {}, "JSON не является объектом"

    return data, ""


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
        "— подтягивать реквизиты по ИНН через DaData\n"
        "— считывать карточку предприятия с фото\n"
        "— дозапрашивать недостающие данные\n"
        "— создавать перевозчика и договор через Google Script\n\n"
        "Примеры:\n"
        "1) Сделай договор новый перевозчик ИНН 381250673578\n"
        "2) Отправь фото карточки предприятия",
    )


@bot.message_handler(commands=["reset", "clear"])
def handle_reset(message):
    clear_session(message.chat.id)
    bot.send_message(message.chat.id, "Сессия очищена.")


@bot.message_handler(content_types=["photo"])
def handle_photo(message):
    chat_id = message.chat.id

    try:
        largest_photo = message.photo[-1]
        file_id = largest_photo.file_id

        bot.send_message(chat_id, "Получил фото. Считываю реквизиты с карточки...")

        image_bytes, download_error = download_telegram_file(file_id)
        if download_error:
            bot.send_message(chat_id, f"Не удалось обработать фото: {download_error}")
            return

        extracted, extract_error = extract_card_data_from_image(image_bytes)
        if extract_error:
            bot.send_message(chat_id, f"Ошибка распознавания карточки: {extract_error}")
            return

        session = get_session(chat_id)
        session["scenario"] = "new_carrier_contract"
        session["awaiting_more_data"] = True

        session["carrier_name"] = extracted.get("carrier_name", "")
        session["carrier_type"] = extracted.get("carrier_type", "") or detect_legal_form_from_name(
            extracted.get("carrier_name", "")
        )
        session["inn"] = clean_digits(extracted.get("inn", ""))
        session["ogrn"] = extracted.get("ogrn", "")
        session["registration_address"] = extracted.get("registration_address", "")
        session["phone"] = normalize_phone(extracted.get("phone", ""))
        session["email"] = extracted.get("email", "")
        session["bank"] = extracted.get("bank", "")
        session["rs"] = clean_digits(extracted.get("rs", ""))
        session["ks"] = clean_digits(extracted.get("ks", ""))
        session["bik"] = clean_digits(extracted.get("bik", ""))
        session["director"] = extracted.get("director", "")
        session["customer_name"] = session.get("customer_name", "")
        session["tax_mode"] = session.get("tax_mode", "")

        save_session(chat_id, session)

        lines = ["Нашёл по карточке:"]
        for key, label in [
            ("carrier_name", "Название"),
            ("carrier_type", "Тип"),
            ("inn", "ИНН"),
            ("ogrn", "ОГРН / ОГРНИП"),
            ("registration_address", "Адрес"),
            ("phone", "Телефон"),
            ("email", "Email"),
            ("bank", "Банк"),
            ("rs", "Расчетный счет"),
            ("ks", "Корр. счет"),
            ("bik", "БИК"),
        ]:
            value = session.get(key, "")
            if value:
                lines.append(f"• {label}: {value}")

        validation_errors = validate_session_fields(session)
        if validation_errors:
            lines.append("")
            lines.append(format_validation_errors_for_user(validation_errors))

        missing = missing_session_fields(session)
        if missing:
            lines.append("")
            lines.append(format_missing_for_user(missing))
            lines.append("")
            lines.append("Пришлите недостающие данные одним сообщением.")
        else:
            lines.append("")
            lines.append("Данных достаточно для создания договора.")

        bot.send_message(chat_id, "\n".join(lines))

    except Exception as e:
        logger.exception("Ошибка в handle_photo: %s", e)
        bot.send_message(
            chat_id,
            "Не удалось обработать фото из-за внутренней ошибки. Попробуйте ещё раз через минуту.",
        )


@bot.message_handler(content_types=["text"])
def handle_text(message):
    chat_id = message.chat.id
    user_text = (message.text or "").strip()

    if not user_text:
        bot.send_message(chat_id, "Пустое сообщение.")
        return

    try:
        session = get_session(chat_id)

        # Если уже ждём доп.данные по новому перевозчику
        if session.get("scenario") == "new_carrier_contract" and session.get("awaiting_more_data"):
            session = parse_bulk_reply(user_text, session)

            # Дочищаем и нормализуем поля
            if session.get("inn"):
                session["inn"] = clean_digits(session.get("inn", ""))
            if session.get("bik"):
                session["bik"] = clean_digits(session.get("bik", ""))
            if session.get("rs"):
                session["rs"] = clean_digits(session.get("rs", ""))
            if session.get("ks"):
                session["ks"] = clean_digits(session.get("ks", ""))
            if session.get("phone"):
                session["phone"] = normalize_phone(session.get("phone", ""))

            save_session(chat_id, session)

            still_missing = missing_session_fields(session)
            validation_errors = validate_session_fields(session)

            if still_missing or validation_errors:
                messages = []
                if validation_errors:
                    messages.append(format_validation_errors_for_user(validation_errors))
                if still_missing:
                    messages.append(format_missing_for_user(still_missing))
                messages.append("\nПришлите корректные/недостающие данные одним сообщением.")

                bot.send_message(chat_id, "\n\n".join(messages))
                return

            payload = {
                "action": "create_carrier_and_contract",
                "customer_name": session.get("customer_name", ""),
                "customer_code": detect_customer_code(session.get("customer_name", "")),
                "name": session.get("carrier_name", ""),
                "form": session.get("carrier_type", ""),
                "inn": session.get("inn", ""),
                "ogrn": session.get("ogrn", ""),
                "director": session.get("director", ""),
                "address": session.get("registration_address", ""),
                "phone": session.get("phone", ""),
                "email": session.get("email", ""),
                "bank": session.get("bank", ""),
                "rs": session.get("rs", ""),
                "bik": session.get("bik", ""),
                "ks": session.get("ks", ""),
                "tax_mode": session.get("tax_mode", ""),
            }

            gs_result, gs_error = call_google_script(payload)
            if gs_error:
                bot.send_message(
                    chat_id,
                    f"Не удалось создать договор: {gs_error}\n"
                    "Проверьте данные и попробуйте ещё раз позже.",
                )
                return

            clear_session(chat_id)

            if gs_result.get("ok"):
                result = gs_result.get("result", {})
                bot.send_message(
                    chat_id,
                    "Готово.\n\n"
                    "Перевозчик создан/обновлён в базе.\n"
                    "Договор создан.\n\n"
                    f"Номер договора: {result.get('contractNumber', '-')}\n"
                    f"Документ: {result.get('docUrl', '-')}\n"
                    f"PDF: {result.get('pdfUrl', '-')}",
                )
            else:
                bot.send_message(
                    chat_id,
                    f"Google Script вернул ошибку: {gs_result.get('error', 'Неизвестная ошибка')}",
                )
            return

        # Новый запрос
        result, openai_error = ask_openai_router(user_text)
        if openai_error:
            bot.send_message(chat_id, f"Не удалось обработать запрос: {openai_error}")
            return

        # Локальная валидация ИНН из роутера
        known = result.get("known", {}) or {}
        if known.get("inn"):
            known["inn"] = clean_digits(known["inn"])
            if not validate_inn(known["inn"]):
                bot.send_message(chat_id, "ИНН должен содержать 10 или 12 цифр. Уточните ИНН.")
                return

        result["known"] = known

        result, dadata_error = enrich_result_with_dadata(result)
        reply = format_router_result(result)

        # Не ломаем сценарий из-за DaData, просто предупреждаем
        if dadata_error:
            reply += "\n\n⚠️ Подсказка: автопоиск по DaData сейчас недоступен, можно продолжить вручную."

        bot.send_message(chat_id, reply)

        if result.get("scenario") == "new_carrier_contract":
            known = result.get("known", {})

            session_data = {
                "scenario": "new_carrier_contract",
                "awaiting_more_data": True,
                "customer_name": known.get("customer_name", ""),
                "carrier_name": known.get("carrier_name", ""),
                "carrier_type": known.get("carrier_type", ""),
                "inn": clean_digits(known.get("inn", "")),
                "ogrn": known.get("ogrn", ""),
                "registration_address": known.get("registration_address", ""),
                "phone": normalize_phone(known.get("phone", "")),
                "email": known.get("email", ""),
                "bank": known.get("bank", ""),
                "rs": clean_digits(known.get("rs", "")),
                "bik": clean_digits(known.get("bik", "")),
                "ks": clean_digits(known.get("ks", "")),
                "tax_mode": known.get("tax_mode", ""),
                "director": known.get("director", ""),
            }

            validation_errors = validate_session_fields(session_data)
            if validation_errors:
                bot.send_message(chat_id, format_validation_errors_for_user(validation_errors))

            save_session(chat_id, session_data)

    except Exception as e:
        logger.exception("Ошибка в handle_text: %s", e)
        bot.send_message(
            chat_id,
            "Произошла внутренняя ошибка при обработке сообщения. Попробуйте снова через минуту.",
        )


if __name__ == "__main__":
    logger.info("Бот запущен")
    try:
        bot.infinity_polling(skip_pending=True)
    except Exception as e:
        logger.exception("Критическая ошибка polling: %s", e)
        raise
