import os
import json
import re
import io
import zipfile
import base64
import logging
import time
import tempfile
import subprocess
import shutil
import signal
import sys
from urllib.parse import urlencode, quote
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv

try:
    import openai
except Exception:  # pragma: no cover
    openai = None

try:
    from pydub import AudioSegment
except Exception:  # pragma: no cover
    AudioSegment = None

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover
    PdfReader = None

from card_parser import CardParser, FIELD_LABELS, CARRIER_REQUIRED_FIELDS, CUSTOMER_REQUIRED_FIELDS

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
OPENAI_CARD_MODEL = "gpt-4o-mini"

if openai and OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

DADATA_TIMEOUT = 30
OPENAI_ROUTER_TIMEOUT = 90
OPENAI_VISION_TIMEOUT = 120
GOOGLE_SCRIPT_TIMEOUT = 120
TELEGRAM_FILE_TIMEOUT = 60

if not TELEGRAM_TOKEN:
    raise RuntimeError("Не задан TELEGRAM_TOKEN")

bot = telebot.TeleBot(TELEGRAM_TOKEN)


def get_main_keyboard():
    markup = ReplyKeyboardMarkup(resize_keyboard=True, is_persistent=True)
    markup.row(KeyboardButton("🏠 Главное меню"))
    markup.row(KeyboardButton("🚛 Новый перевозчик"), KeyboardButton("📋 Новый договор"))
    markup.row(KeyboardButton("📦 Новая заявка"), KeyboardButton("📄 Мои заявки"))
    markup.row(KeyboardButton("🚗 Добавить машину"), KeyboardButton("👤 Добавить водителя"))
    markup.row(KeyboardButton("👥 Перевозчики"), KeyboardButton("❓ Помощь"))
    return markup


def get_main_menu_keyboard():
    """Совместимость со старыми вызовами."""
    return get_main_keyboard()


def show_main_menu(chat_id: int):
    """Показать главное меню с командными кнопками"""
    bot.send_message(
        chat_id,
        "🏠 <b>Главное меню</b>\n\nВыберите действие:",
        parse_mode="HTML",
        reply_markup=get_main_keyboard()
    )


# =========================
# КОНФИГ ЗАКАЗЧИКОВ
# =========================

DEFAULT_CONFIG = {
    "customers": []
}


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        logger.warning("config.json не найден, использую дефолтный конфиг")
        return dict(DEFAULT_CONFIG)

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)

        if not isinstance(config, dict):
            logger.warning("config.json имеет неверный формат, использую дефолтный конфиг")
            return dict(DEFAULT_CONFIG)

        config.setdefault("customers", [])
        logger.info("Конфиг заказчиков загружен из %s", CONFIG_PATH)
        return config
    except Exception as e:
        logger.exception("Ошибка чтения config.json: %s", e)
        return dict(DEFAULT_CONFIG)


CONFIG = load_config()
CUSTOMERS_CACHE_TTL_SECONDS = 300
_CUSTOMERS_CACHE: Dict[str, Any] = {"items": [], "updated_at": 0.0}


def _normalize_customers(customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for customer in customers or []:
        if not isinstance(customer, dict):
            continue

        code = str(customer.get("code", "")).strip()
        name = str(customer.get("name", "")).strip()
        if not code or not name:
            continue

        normalized_customer = dict(customer)
        normalized_customer["code"] = code
        normalized_customer["name"] = name
        normalized_customer["aliases"] = normalized_customer.get("aliases") or []
        normalized.append(normalized_customer)

    return normalized


def get_customers_list(force_refresh: bool = False) -> List[Dict[str, Any]]:
    """Получить список заказчиков из Google Sheets через Apps Script."""
    cache_age = time.time() - float(_CUSTOMERS_CACHE.get("updated_at", 0.0) or 0.0)
    if not force_refresh and _CUSTOMERS_CACHE.get("items") and cache_age < CUSTOMERS_CACHE_TTL_SECONDS:
        return _CUSTOMERS_CACHE["items"]

    url = os.getenv("GOOGLE_SCRIPT_URL")
    if not url:
        logger.warning("GOOGLE_SCRIPT_URL не задан: список заказчиков берется из локального config.json")
        fallback_customers = _normalize_customers(CONFIG.get("customers", []))
        _CUSTOMERS_CACHE["items"] = fallback_customers
        _CUSTOMERS_CACHE["updated_at"] = time.time()
        return fallback_customers

    try:
        response = requests.post(
            url,
            json={"action": "get_customers"},
            timeout=GOOGLE_SCRIPT_TIMEOUT,
        )
        response.raise_for_status()

        data = response.json()
        payload = data

        if isinstance(data, dict) and data.get("ok") and isinstance(data.get("result"), dict):
            payload = data.get("result")

        if isinstance(payload, dict) and payload.get("success"):
            customers = _normalize_customers(payload.get("customers", []))
            _CUSTOMERS_CACHE["items"] = customers
            _CUSTOMERS_CACHE["updated_at"] = time.time()
            return customers

        logger.error("Apps Script get_customers: неожиданный ответ: %s", data)
    except Exception as e:
        logger.error("Ошибка получения заказчиков: %s", e)

    fallback_customers = _normalize_customers(CONFIG.get("customers", []))
    _CUSTOMERS_CACHE["items"] = fallback_customers
    _CUSTOMERS_CACHE["updated_at"] = time.time()
    return fallback_customers


def get_customers_from_sheets(force_refresh: bool = False) -> List[Dict[str, Any]]:
    """Совместимое имя функции для получения списка заказчиков из Google Sheets."""
    return get_customers_list(force_refresh=force_refresh)


def get_customer_by_code(code: str) -> dict:
    """Получить полные реквизиты заказчика по коду."""
    for c in get_customers_list():
        if c.get("code", "").upper() == (code or "").upper():
            return c
    return {}


def get_customer_by_inn(inn: str) -> dict:
    """Получить заказчика по ИНН."""
    for c in get_customers_list():
        if c.get("inn") == inn:
            return c
    return {}


def get_customer_by_alias(text: str) -> dict:
    """Найти заказчика по алиасу или имени."""
    t = (text or "").lower().strip()
    for c in get_customers_list():
        name = (c.get("name") or "").lower()
        if name and name in t:
            return c
        for alias in (c.get("aliases") or []):
            if alias and alias.lower() in t:
                return c
    return {}


def format_customer_choice() -> str:
    """Сформировать меню выбора заказчика."""
    customers = get_customers_list()
    if len(customers) == 0:
        return "Заказчиков нет в базе. Нажмите '➕ Добавить нового заказчика'."
    if len(customers) == 1:
        c = customers[0]
        return f"Заказчик: {c.get('name', '?')} (ИНН: {c.get('inn', '?')})"
    lines = ["Выберите заказчика:"]
    for i, c in enumerate(customers, 1):
        inn = c.get("inn", "")
        inn_str = f" (ИНН: {inn})" if inn else ""
        lines.append(f"{i}. {c.get('name', '?')}{inn_str}")
    return "\n".join(lines)


def auto_select_customer(session: dict) -> bool:
    """Автоматически выбрать заказчика если он один в базе.
    Возвращает True если заказчик выбран."""
    if session.get("customer_name"):
        return True

    customers = get_customers_list()
    if len(customers) == 1:
        c = customers[0]
        session["customer_name"] = c.get("name", "")
        session["customer_code"] = c.get("code", "")
        session["customer_data"] = c
        logger.info("Автовыбор заказчика: %s", c.get("name"))
        return True
    return False

VALID_TAX_MODES = ("ОСНО", "УСН", "Патент", "Самозанятый")
TAX_MODE_PROMPT = "налогообложение (ОСНО / УСН / Патент / Самозанятый)"
TAX_MODE_HINTS = (
    "Подсказка по налогообложению:\n"
    "• ОСНО — общая система с НДС\n"
    "• УСН — упрощенная, без НДС\n"
    "• Патент — для ИП\n"
    "• Самозанятый — НПД"
)

# Google Forms URLs
VEHICLE_FORM_URL = "https://docs.google.com/forms/d/1KA-GQGbBGDZCut3y5uvetDRSzn70Gq8M5GlQjo_N7PE/viewform"
CARRIER_FORM_URL = "https://docs.google.com/forms/d/1mJtwAxqExuHHQD96Z9ViXC8MDx5AfkZMa8RJnq2oOFs/viewform"
TRAILER_FORM_URL = "https://docs.google.com/forms/d/199sIbgR6q8TmWHWQ2anS3m2F8QpPrEMaOiZLMjn1f0A/viewform"
DRIVER_FORM_URL = "https://docs.google.com/forms/d/1xTxGCcwgplSXMikhb8FL1mZ_f04dNqoRLE96_8KMGd4/viewform"
CUSTOMER_FORM_URL = "https://docs.google.com/forms/d/1CG9AnZTMN31CqnDekSclmJziEYgUnQ3a9XMoxVXMgKM/viewform"

# Vehicle Form Entry IDs
VEHICLE_FORM_ENTRIES = {
    "carrier": "entry.210466749",
    "brand": "entry.1498851787",
    "model": "entry.1498851787",  # Используем то же поле что и brand (Марка и модель)
    "plate": "entry.587597031",
    "vin": "entry.1187966497",
    "year": "entry.2051882103",
}

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

    tax_mode = session.get("tax_mode", "")
    if tax_mode and tax_mode not in VALID_TAX_MODES:
        errors["tax_mode"] = (
            "Укажите один из вариантов: ОСНО, УСН, Патент или Самозанятый"
        )

    return errors


def format_validation_errors_for_user(errors: Dict[str, str]) -> str:
    labels = {
        "inn": "ИНН",
        "phone": "телефон",
        "email": "email",
        "bik": "БИК",
        "rs": "расчетный счет",
        "ks": "корр. счет",
        "tax_mode": "налогообложение",
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


def get_bank_by_bik(bik: str) -> Tuple[Dict[str, Any], str]:
    """Получить данные банка по БИК через DaData."""
    if not DADATA_TOKEN:
        return {}, "DaData не настроен."
    bik = clean_digits(bik or "")
    if len(bik) != 9:
        return {}, "БИК должен содержать 9 цифр."

    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/bank"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {DADATA_TOKEN}",
    }
    payload = {"query": bik}

    data, error = post_json_with_handling(
        url=url, payload=payload, headers=headers,
        timeout=DADATA_TIMEOUT, source="DaData Bank",
    )
    if error:
        return {}, error

    suggestions = data.get("suggestions", [])
    if not suggestions:
        return {}, "Банк не найден по БИК."

    bank_data = suggestions[0].get("data", {})
    return {
        "bank_name": bank_data.get("name", {}).get("payment") or bank_data.get("name", {}).get("short") or "",
        "ks": bank_data.get("correspondent_account") or "",
        "bik": bank_data.get("bic") or bik,
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

Для поля tax_mode используй ТОЛЬКО один из вариантов:
- ОСНО
- УСН
- Патент
- Самозанятый

Сопоставление синонимов для tax_mode:
- "с НДС" => "ОСНО"
- "без НДС" или "упрощенка" => "УСН"
- "НПД" => "Самозанятый"

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
  "missing": ["phone", "email", "bank", "rs", "bik", "ks", "tax_mode"],
  "next_question": "Укажите телефон, email, банк, расчетный счет, БИК, корр. счет и налогообложение (ОСНО / УСН / Патент / Самозанятый)."
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


def parse_company_card(content: Any, source_type: str = "image") -> Tuple[Dict[str, Any], str]:
    """Распознает карточку предприятия (фото/текст) и возвращает нормализованный JSON."""
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY не задан")
        return {}, "Сервис OpenAI не настроен (нет API-ключа)."

    prompt = (
        "Ты извлекаешь реквизиты перевозчика из карточки предприятия. "
        "Верни строго JSON без markdown и без пояснений. "
        "Если поле не найдено, верни пустую строку.\n\n"
        "Поля JSON: name, carrier_name, carrier_short_name, carrier_type, inn, kpp, ogrn, snils, "
        "address, registration_address, post_address, director, basis, phone, phone2, email, "
        "emails, bank, bank_city, account, rs, corr_account, ks, bik, tax_mode, edo.\n\n"
        "Правила:\n"
        "- carrier_type: только ИП / ООО / САМОЗАНЯТЫЙ\n"
        "- ИНН, КПП, ОГРН/ОГРНИП, БИК, р/с, к/с — возвращай строками из ЦИФР\n"
        "- ВАЖНО: Расчётный счёт (р/с, account, rs) и Корреспондентский счёт (к/с, кор/с, corr_account, ks) — это ДВА РАЗНЫХ поля! "
        "Оба содержат ровно 20 цифр. Р/с обычно начинается на 407 или 408. К/с начинается на 301. "
        "Обязательно найди ОБА счёта!\n"
        "- phone и email — основные, phone2/emails можно заполнить дополнительными значениями\n"
        "- Если найдено ФИО ИП, можно дублировать его в director\n"
        "- Ничего кроме JSON"
    )

    content_parts = [{"type": "input_text", "text": prompt}]
    timeout = OPENAI_ROUTER_TIMEOUT

    if source_type == "image":
        base64_image = base64.b64encode(content).decode("utf-8")
        content_parts.append(
            {
                "type": "input_image",
                "image_url": f"data:image/jpeg;base64,{base64_image}",
                "detail": "high",
            }
        )
        timeout = OPENAI_VISION_TIMEOUT
    else:
        text_content = str(content or "").strip()
        if not text_content:
            return {}, "Не удалось извлечь текст из документа."
        content_parts.append({"type": "input_text", "text": f"Текст карточки:\n{text_content[:15000]}"})

    payload = {
        "model": OPENAI_CARD_MODEL,
        "input": [
            {
                "role": "user",
                "content": content_parts,
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
        timeout=timeout,
        source="OpenAI Company Card Parser",
    )
    if error:
        return {}, error

    output_text = extract_output_text(data)
    parsed, parse_error = safe_json_loads(output_text)
    if parse_error:
        logger.error("parse_company_card: ошибка парсинга JSON: %s", parse_error)
        return {}, "Не удалось корректно распознать карточку. Попробуйте более четкое фото или PDF."

    return parsed, ""


def extract_card_data_from_image(image_bytes: bytes) -> Tuple[Dict[str, Any], str]:
    return parse_company_card(image_bytes, source_type="image")


def extract_card_data_from_text(raw_text: str) -> Tuple[Dict[str, Any], str]:
    return parse_company_card(raw_text, source_type="text")


def extract_text_from_docx_bytes(file_bytes: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            document_xml = zf.read("word/document.xml").decode("utf-8", errors="ignore")
    except Exception as e:
        logger.exception("DOCX parse error: %s", e)
        return ""

    text = re.sub(r"<[^>]+>", " ", document_xml)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_text_from_pdf_bytes(file_bytes: bytes) -> str:
    if PdfReader is None:
        logger.warning("pypdf не установлен, PDF-парсинг недоступен")
        return ""

    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        pages_text = []
        for page in reader.pages[:15]:
            pages_text.append(page.extract_text() or "")
        return "\n".join(pages_text).strip()
    except Exception as e:
        logger.exception("PDF parse error: %s", e)
        return ""


def extract_card_data_from_document(file_bytes: bytes, mime_type: str, file_name: str) -> Tuple[Dict[str, Any], str]:
    mime = (mime_type or "").lower()
    name = (file_name or "").lower()

    raw_text = ""
    if "wordprocessingml.document" in mime or name.endswith(".docx"):
        raw_text = extract_text_from_docx_bytes(file_bytes)
    elif "pdf" in mime or name.endswith(".pdf"):
        raw_text = extract_text_from_pdf_bytes(file_bytes)
    elif name.endswith(".txt"):
        raw_text = file_bytes.decode("utf-8", errors="ignore")

    if not raw_text:
        return {}, "Не удалось извлечь текст из файла. Отправьте фото карточки или DOCX/PDF с текстом."

    return extract_card_data_from_text(raw_text)


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
        # Автовыбор заказчика
        customers = get_customers_list()
        if len(customers) == 1:
            c = customers[0]
            known["customer_name"] = c.get("name", "")
            known["customer_code"] = c.get("code", "")
            known["customer_data"] = c
            result["known"] = known
            if "customer_name" in result.get("missing", []):
                result["missing"].remove("customer_name")
            customer_info = f"Заказчик: {c.get('name', '')} (автоматически)\n"
        elif len(customers) > 1:
            customer_info = "Выберите заказчика кнопкой ниже.\n"
        else:
            customer_info = "Заказчики пока не найдены. Используйте кнопку добавления нового заказчика ниже.\n"
        
        remaining = [f for f in result.get("missing", []) if f != "customer_name"]
        if remaining:
            fields_str = ", ".join([{
                "phone": "телефон",
                "email": "email", 
                "bank": "банк",
                "rs": "расчетный счет",
                "bik": "БИК",
                "ks": "корр. счет",
                "tax_mode": TAX_MODE_PROMPT,
                "customer_name": "заказчик",
            }.get(f, f) for f in remaining])
            result["next_question"] = (
                f"Нашёл данные по ИНН.\n"
                f"{customer_info}"
                f"Пришлите одним сообщением:\n"
                f"{fields_str}.\n\n"
                f"{TAX_MODE_HINTS}"
            )
        else:
            result["next_question"] = (
                f"Нашёл данные по ИНН.\n"
                f"{customer_info}"
                f"Все обязательные данные получены!"
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


def check_carrier_exists_in_sheets(inn: str) -> Tuple[Dict[str, Any], str]:
    payload = {
        "action": "check_carrier_exists",
        "inn": clean_digits(inn),
    }
    data, error = call_google_script(payload)
    if error:
        return {}, error

    if data.get("ok") and isinstance(data.get("result"), dict):
        return data.get("result", {}), ""

    if isinstance(data, dict) and "exists" in data:
        return data, ""

    logger.error("check_carrier_exists: неожиданный ответ Google Script: %s", data)
    return {}, "Google Script вернул неожиданный ответ при проверке дубликата по ИНН."


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
    t = (text or "").strip().lower()

    if not t:
        return ""

    if "самозан" in t or "нпд" in t:
        return "Самозанятый"

    if "патент" in t:
        return "Патент"

    if "осно" in t or "с ндс" in t:
        return "ОСНО"

    if "усн" in t or "упрощ" in t or "без ндс" in t:
        return "УСН"

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
    for customer in get_customers_list():
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
    t = (customer_name or "").lower().strip()
    for customer in get_customers_list():
        name = (customer.get("name") or "").lower().strip()
        if name and name == t:
            return customer.get("code", "")

    return ""


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
    if tax_mode:
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
    # Автовыбор заказчика если один в базе
    auto_select_customer(session)
    
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
        "tax_mode": TAX_MODE_PROMPT,
    }

    lines = ["Не хватает:"]
    for item in missing:
        lines.append(f"• {labels.get(item, item)}")

    if "tax_mode" in missing:
        lines.append("")
        lines.append(TAX_MODE_HINTS)

    return "\n".join(lines)


def extract_number(text: str) -> int:
    text = (text or "").lower()
    match = re.search(r"\d+", text)
    if match:
        return int(match.group(0))

    words_map = {
        "один": 1,
        "два": 2,
        "три": 3,
        "четыре": 4,
        "пять": 5,
        "шесть": 6,
        "семь": 7,
        "восемь": 8,
        "девять": 9,
        "десять": 10,
        "одиннадцать": 11,
        "двенадцать": 12,
        "тринадцать": 13,
        "четырнадцать": 14,
        "пятнадцать": 15,
        "шестнадцать": 16,
        "семнадцать": 17,
        "восемнадцать": 18,
        "девятнадцать": 19,
        "двадцать": 20,
    }
    for word, value in words_map.items():
        if word in text:
            return value

    return 0


def convert_ogg_to_mp3(voice_path: str) -> Tuple[str, str]:
    mp3_path = f"{voice_path}.mp3"

    if AudioSegment is not None:
        try:
            AudioSegment.from_file(voice_path, format="ogg").export(mp3_path, format="mp3")
            return mp3_path, ""
        except Exception as e:
            logger.warning("pydub conversion failed, fallback to ffmpeg: %s", e)

    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        return "", "Не найден ffmpeg для конвертации голосового сообщения."

    try:
        proc = subprocess.run(
            [ffmpeg_path, "-y", "-i", voice_path, mp3_path],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            logger.error("ffmpeg conversion failed: %s", (proc.stderr or "")[:500])
            return "", "Не удалось конвертировать голосовое сообщение в MP3."
        return mp3_path, ""
    except Exception as e:
        logger.exception("ffmpeg conversion exception: %s", e)
        return "", "Ошибка конвертации голосового сообщения."


def transcribe_audio_with_whisper(mp3_path: str) -> Tuple[str, str]:
    if not OPENAI_API_KEY:
        return "", "OPENAI_API_KEY не задан, распознавание голоса недоступно."

    if openai is None:
        return "", "Библиотека openai не установлена на сервере."

    try:
        with open(mp3_path, "rb") as audio:
            transcript = openai.Audio.transcribe("whisper-1", audio)
            text = (transcript or {}).get("text", "").strip()
            if not text:
                return "", "Whisper не вернул текст распознавания."
            return text, ""
    except Exception as e:
        logger.exception("Whisper transcription error: %s", e)
        return "", "Ошибка распознавания голосового сообщения через Whisper."


def build_google_form_url(page: str, **params: Any) -> str:
    base_url = os.getenv("GOOGLE_SCRIPT_URL") or ""
    if not base_url:
        return ""

    query = {"page": page}
    for key, value in params.items():
        if value is not None and str(value).strip() != "":
            query[key] = str(value)

    return f"{base_url}?{urlencode(query)}"


def parse_route_command(message, text: str):
    chat_id = message.chat.id

    result, openai_error = ask_openai_router(text)
    if openai_error:
        bot.send_message(chat_id, f"Не удалось разобрать команду рейса: {openai_error}")
        return

    known = result.get("known", {}) or {}
    session = get_session(chat_id)
    session["scenario"] = "existing_carrier_trip_request"
    session["route_from"] = known.get("route_from", session.get("route_from", ""))
    session["route_to"] = known.get("route_to", session.get("route_to", ""))
    session["route_name"] = known.get("route_name", session.get("route_name", ""))
    session["carrier_name"] = known.get("carrier_name", session.get("carrier_name", ""))
    session["price"] = known.get("price", session.get("price", ""))

    pallets = known.get("pallets") or extract_number(text)
    if pallets:
        session["pallets"] = pallets

    save_session(chat_id, session)

    if not session.get("pallets"):
        bot.send_message(chat_id, "Уточните количество паллет, чтобы подобрать перевозчика.")
        return

    find_suitable_carriers(message, int(session.get("pallets") or 0))


def find_suitable_carriers(message, pallets: int):
    """Подобрать перевозчиков по вместимости."""
    chat_id = message.chat.id

    if pallets <= 0:
        bot.send_message(chat_id, "Укажите количество паллет числом, например: 10 паллет.")
        return

    payload = {
        "action": "get_available_carriers",
        "pallets": pallets,
    }
    data, error = call_google_script(payload)
    if error:
        bot.send_message(chat_id, f"❌ Ошибка подбора перевозчиков: {error}")
        return

    if isinstance(data, dict) and data.get("ok") and isinstance(data.get("result"), dict):
        data = data.get("result", {})

    if not data.get("success"):
        bot.send_message(chat_id, "❌ Ошибка подбора перевозчиков")
        return

    carriers = data.get("carriers", []) or []
    if not carriers:
        bot.send_message(chat_id, f"❌ Нет перевозчиков с машинами на {pallets} паллет")
        return

    session = get_session(chat_id)
    session["scenario"] = "existing_carrier_trip_request"
    session["pallets"] = pallets
    session["auto_carriers_map"] = {str(c.get("id")): c for c in carriers}
    save_session(chat_id, session)

    markup = InlineKeyboardMarkup()
    for carrier in carriers[:5]:
        priority_emoji = {
            1: "🟢",
            2: "🟡",
            3: "🟠",
            4: "🔴",
        }.get(carrier.get("priority"), "⚪")

        btn_text = f"{priority_emoji} {carrier.get('name', 'Без названия')} ({carrier.get('tax_mode', '—')})"
        markup.add(
            InlineKeyboardButton(
                text=btn_text,
                callback_data=f"select_carrier_auto_{carrier.get('id')}",
            )
        )

    bot.send_message(
        chat_id,
        f"🚚 Подобраны перевозчики для {pallets} паллет:\n\n"
        f"🟢 С НДС (приоритет)\n"
        f"🟡 Без НДС\n"
        f"🟠 Самозанятый\n"
        f"🔴 Наличный расчёт",
        reply_markup=markup,
    )


def get_carriers_list() -> List[Dict[str, Any]]:
    """Получить список перевозчиков из Google Sheets."""
    try:
        script_url = os.getenv("GOOGLE_SCRIPT_URL")

        if not script_url:
            logger.error("GOOGLE_SCRIPT_URL не настроен")
            return []

        payload = {
            "action": "list_carriers",
        }

        logger.info("Запрос списка перевозчиков: %s", script_url)

        response = requests.post(
            script_url,
            json=payload,
            timeout=10,
        )

        logger.info("Ответ: %s", response.status_code)

        if response.status_code == 200:
            data = response.json()

            # Поддержка обертки jsonOutput_({ok:true, result:{...}})
            if isinstance(data, dict) and data.get("ok") and isinstance(data.get("result"), dict):
                data = data.get("result", {})

            if isinstance(data, dict) and data.get("success"):
                carriers = data.get("carriers", []) or []
                logger.info("Получено перевозчиков: %s", len(carriers))
                return carriers

            error = data.get("error", "Неизвестная ошибка") if isinstance(data, dict) else "Некорректный ответ"
            logger.error("Ошибка от сервера: %s", error)
            return []

        logger.error("HTTP ошибка: %s", response.status_code)
        logger.error("Тело ответа: %s", response.text)
        return []

    except Exception as e:
        logger.error("Ошибка получения списка перевозчиков: %s", e, exc_info=True)
        return []


def get_carrier_name_by_id(carrier_id: str) -> str:
    """Получить название перевозчика по ID."""
    carriers = get_carriers_list()
    for carrier in carriers:
        if str(carrier.get("id", "")) == str(carrier_id):
            return str(carrier.get("name", "")).strip()
    return ""


def get_customers_for_contract() -> List[Dict[str, Any]]:
    """Получить список активных заказчиков из Google Sheets для договора."""
    try:
        script_url = os.getenv("GOOGLE_SCRIPT_URL")
        if not script_url:
            logger.warning("GOOGLE_SCRIPT_URL не настроен для получения заказчиков")
            return []

        response = requests.post(
            script_url,
            json={"action": "list_customers"},
            timeout=30,
        )

        if response.status_code != 200:
            logger.error("HTTP ошибка при получении заказчиков: %s", response.status_code)
            return []

        data = response.json()

        # Обработка обертки {ok: true, result: [...]}
        if isinstance(data, dict) and data.get("ok") and isinstance(data.get("result"), list):
            return data.get("result", [])

        # Прямой список
        if isinstance(data, list):
            return data

        logger.error("Неожиданный формат ответа list_customers: %s", data)
        return []

    except Exception as e:
        logger.error("Ошибка получения списка заказчиков: %s", e, exc_info=True)
        return []


def generate_carrier_contract(carrier_id: str, customer_id: str = None) -> Dict[str, Any]:
    """Генерировать договор с перевозчиком через Google Apps Script."""
    try:
        from datetime import datetime

        script_url = os.getenv("GOOGLE_SCRIPT_URL")
        if not script_url:
            logger.error("GOOGLE_SCRIPT_URL не настроен")
            return {"success": False, "error": "Сервис недоступен"}

        contract_date = datetime.now().strftime("%d.%m.%Y")

        # Если передан customer_id, используем его, иначе используем дефолтные данные
        if customer_id:
            payload = {
                "action": "create_contract",
                "carrier_id": carrier_id,
                "customer_id": customer_id,
            }
        else:
            # Fallback на старый формат с жестко прописанными данными
            customer_data = {
                "name": "ООО «Фрукт Сервис»",
                "full_name": "Общество с ограниченной ответственностью «Фрукт Сервис»",
                "inn": "3805731231",
                "kpp": "382701001",
                "ogrn": "1173850020960",
                "director": "Минин Роман Николаевич",
                "director_short": "Р.Н. Минин",
                "director_genitive": "Минина Романа Николаевича",
                "address_legal": "664035, Иркутская область, г. Иркутск, Батарейная ул, дом 17, корпус 1, помещение 1",
                "address_post": "664035, Иркутская обл, г. Иркутск, ул. Батарейная, д. 17, корп. 1, пом. 1",
                "bank": "Байкальский Банк ПАО \"Сбербанк\"",
                "rs": "40702810318350026308",
                "ks": "30101810900000000607",
                "bik": "042520607",
                "email": "svetdrus@mail.ru",
                "phone": "8-800-201-26-59",
            }
            payload = {
                "action": "generate_carrier_contract",
                "carrier_id": carrier_id,
                "customer_data": customer_data,
                "contract_date": contract_date,
            }

        logger.info("Отправка запроса на генерацию договора: %s", payload)
        response = requests.post(
            script_url,
            json=payload,
            timeout=30,
        )

        logger.info("Ответ сервера: %s", response.status_code)
        logger.info("Тело ответа: %s", response.text)

        if response.status_code != 200:
            return {"success": False, "error": f"HTTP {response.status_code}"}

        data = response.json()

        if isinstance(data, dict) and data.get("ok") and isinstance(data.get("result"), dict):
            data = data.get("result", {})

        if data.get("success"):
            return {
                "success": True,
                "url": data.get("url"),
                "contract_number": data.get("contract_number"),
                "date": contract_date,
            }

        return {
            "success": False,
            "error": data.get("error", "Неизвестная ошибка"),
        }

    except Exception as e:
        logger.error("Ошибка генерации договора: %s", e, exc_info=True)
        return {
            "success": False,
            "error": str(e),
        }


def parse_sts_document(photo_base64: str) -> Optional[Dict[str, str]]:
    """Распознать СТС через GPT-4 Vision и вернуть JSON-словарь с ключами машины."""
    if not OPENAI_API_KEY:
        logger.warning("parse_sts_document: OPENAI_API_KEY не задан")
        return None

    prompt = (
        "Это фото свидетельства о регистрации транспортного средства (СТС).\n"
        "Извлеки следующие данные:\n"
        "- Государственный регистрационный номер (госномер)\n"
        "- Марка\n"
        "- Модель\n"
        "- VIN (идентификационный номер)\n"
        "- Год выпуска\n\n"
        "Верни строго JSON без markdown и комментариев:\n"
        "{\n"
        '  "plate": "А123БВ199",\n'
        '  "brand": "МАН",\n'
        '  "model": "TGX 18.440",\n'
        '  "vin": "XYZ12345678901234",\n'
        '  "year": "2020"\n'
        "}"
    )

    payload = {
        "model": OPENAI_CARD_MODEL,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {
                        "type": "input_image",
                        "image_url": f"data:image/jpeg;base64,{photo_base64}",
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
        source="OpenAI STS Parser",
    )
    if error:
        logger.error("parse_sts_document: ошибка запроса к OpenAI: %s", error)
        return None

    output_text = extract_output_text(data)
    parsed, parse_error = safe_json_loads(output_text)
    if parse_error:
        logger.error("parse_sts_document: ошибка парсинга JSON: %s | raw=%s", parse_error, output_text)
        return None

    result = {
        "plate": str(parsed.get("plate", "")).strip(),
        "brand": str(parsed.get("brand", "")).strip(),
        "model": str(parsed.get("model", "")).strip(),
        "vin": str(parsed.get("vin", "")).strip(),
        "year": str(parsed.get("year", "")).strip(),
    }

    if not any(result.values()):
        logger.warning("parse_sts_document: OpenAI не вернул распознанных полей")
        return None

    logger.info("СТС распознан: %s", result)
    return result


def parse_driver_license(photo_base64: str) -> dict:
    """
    Парсинг водительского удостоверения через OpenAI Vision.
    Возвращает: full_name, birth_date, license_number, categories, issue_date, expiry_date
    """
    if not OPENAI_API_KEY:
        logger.warning("parse_driver_license: OPENAI_API_KEY не задан")
        return {}

    prompt_text = """Извлеки ВСЕ доступные данные о водителе из этого изображения.
Это может быть:
- Водительское удостоверение РФ
- Паспорт РФ
- Карточка водителя
- Любой документ с данными водителя

Извлеки ВСЕ найденные данные:
- ФИО (полностью, фамилия имя отчество)
- Серия и номер паспорта (формат: ХХХХ ХХХХХХ)
- Номер водительского удостоверения (формат: ХХ ХХ ХХХХХХ, может быть подписан как "ВУ", "В.у.", "Водительское удостоверение")
- Дата выдачи ВУ
- Срок действия ВУ
- Дата рождения
- Место рождения
- Телефон(ы) - ЛЮБЫЕ номера телефонов на изображении в любом формате
- ИНН (если есть)

ВАЖНО:
- Извлекай ВСЕ телефонные номера которые видишь
- Номер ВУ может быть записан как "В.у.:", "ВУ:", "Водительское удостоверение №"
- Если данных нет - оставь поле пустым

Верни ТОЛЬКО валидный JSON без markdown разметки:
{
  "full_name": "",
  "passport_series": "",
  "passport_number": "",
  "license_number": "",
  "license_date": "",
  "license_expiry": "",
  "birth_date": "",
  "birth_place": "",
  "phone": "",
  "phone2": "",
  "inn": "",
  "categories": ""
}"""

    payload = {
        "model": OPENAI_CARD_MODEL,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt_text},
                    {
                        "type": "input_image",
                        "image_url": f"data:image/jpeg;base64,{photo_base64}",
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

    try:
        data, error = post_json_with_handling(
            url="https://api.openai.com/v1/responses",
            payload=payload,
            headers=headers,
            timeout=OPENAI_VISION_TIMEOUT,
            source="OpenAI DL Parser",
        )
        if error:
            logger.error("parse_driver_license: %s", error)
            return {}

        output_text = extract_output_text(data)
        cleaned_text = output_text.replace("```json", "").replace("```", "").strip()

        parsed, parse_error = safe_json_loads(cleaned_text)
        if parse_error:
            logger.error("parse_driver_license: ошибка парсинга JSON: %s | raw=%s", parse_error, output_text)
            return {}

        parsed = parsed if isinstance(parsed, dict) else {}

        # Поддержка нескольких названий полей из OCR
        if not parsed.get("issue_date") and parsed.get("license_date"):
            parsed["issue_date"] = parsed.get("license_date", "")
        if not parsed.get("expiry_date") and parsed.get("license_expiry"):
            parsed["expiry_date"] = parsed.get("license_expiry", "")

        # Склеиваем найденные телефоны в одно поле phone
        phones = []
        for key in ("phone", "phone2"):
            value = str(parsed.get(key, "") or "").strip()
            if value and value not in phones:
                phones.append(value)

        if phones:
            parsed["phone"] = ", ".join(phones)

        return parsed
    except Exception as e:
        logger.exception("Ошибка парсинга ВУ: %s", e)
        return {}


def extract_driver_from_text(text):
    """Извлекает данные водителя из текста в свободной форме"""
    result = {}

    # ФИО - ищем 2-3 слова на кириллице подряд (фамилия имя отчество)
    fio_patterns = [
        r'(?:ФИО|фио|Фамилия)[\s:]*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?)',
        r'([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)',  # Три слова с заглавной
    ]
    for pattern in fio_patterns:
        fio_match = re.search(pattern, text)
        if fio_match:
            result['full_name'] = fio_match.group(1).strip()
            break

    # Паспорт - серия (4 цифры) и номер (6 цифр)
    passport_patterns = [
        r'(?:Паспорт|паспорт|серия|номер)[\s:]*(\d{4})[\s\-]*(\d{6})',
        r'(\d{4})[\s\-]+(\d{6})',  # Просто 4 и 6 цифр подряд
    ]
    for pattern in passport_patterns:
        passport_match = re.search(pattern, text)
        if passport_match:
            result['passport_series'] = passport_match.group(1)
            result['passport_number'] = passport_match.group(2)
            break

    # ВУ - разные форматы
    license_patterns = [
        r'(?:ВУ|ву|Водительское|удостоверение)[\s:]*(\d{2}[\s\-]*\d{2}[\s\-]*\d{6})',
        r'(?:ВУ|ву)[\s:]*(\d{10})',
        r'(?:^|\s)(\d{2}\s*\d{2}\s*\d{6})(?:\s|$)',  # В начале/конце строки
    ]
    for pattern in license_patterns:
        license_match = re.search(pattern, text, re.MULTILINE)
        if license_match:
            result['license_number'] = license_match.group(1).replace(' ', '').replace('-', '')
            break

    # Телефон - извлекаем ДВА телефона если они есть
    phone_patterns = [
        r'(?:Телефон|телефон|тел|моб)[\s:]*([+\d][\d\s\-()]+)',
        r'(\+7[\d\s\-()]{10,})',
        r'(8[\d\s\-()]{10,})',
    ]
    
    # Сначала ищем все телефоны в тексте
    all_phones = []
    for pattern in phone_patterns:
        phone_matches = re.finditer(pattern, text)
        for match in phone_matches:
            phone = match.group(1).strip()
            # Разделяем по запятой если в одном совпадении несколько номеров
            for part in re.split(r'[,;/]', phone):
                part = part.strip()
                if part and len(part) >= 10:  # Минимальная длина телефона
                    all_phones.append(part)
    
    # Записываем первые два телефона
    if len(all_phones) > 0:
        result['phone'] = all_phones[0]
    if len(all_phones) > 1:
        result['phone2'] = all_phones[1]

    return result


def parse_passport(photo_base64: str) -> dict:
    """
    Парсинг паспорта РФ через OpenAI Vision.
    Возвращает: full_name, birth_date, passport_series, passport_number,
                issued_by, issue_date, address
    """
    if not OPENAI_API_KEY:
        logger.warning("parse_passport: OPENAI_API_KEY не задан")
        return {}

    prompt_text = (
        "Распознай паспорт гражданина РФ (разворот 2-3 или 5 страница).\n\n"
        "Верни JSON:\n"
        "{\n"
        '  "full_name": "Фамилия Имя Отчество",\n'
        '  "birth_date": "ДД.ММ.ГГГГ",\n'
        '  "passport_series": "25 08",\n'
        '  "passport_number": "123456",\n'
        '  "issued_by": "Кем выдан",\n'
        '  "issue_date": "ДД.ММ.ГГГГ",\n'
        '  "address": "Адрес регистрации"\n'
        "}\n\n"
        "Если что-то не видно — пустая строка."
    )

    payload = {
        "model": OPENAI_CARD_MODEL,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt_text},
                    {
                        "type": "input_image",
                        "image_url": f"data:image/jpeg;base64,{photo_base64}",
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

    try:
        data, error = post_json_with_handling(
            url="https://api.openai.com/v1/responses",
            payload=payload,
            headers=headers,
            timeout=OPENAI_VISION_TIMEOUT,
            source="OpenAI Passport Parser",
        )
        if error:
            logger.error("parse_passport: %s", error)
            return {}

        output_text = extract_output_text(data)
        parsed, parse_error = safe_json_loads(output_text)
        if parse_error:
            logger.error("parse_passport: ошибка парсинга JSON: %s", parse_error)
            return {}
        return parsed if parsed else {}
    except Exception as e:
        logger.exception("Ошибка парсинга паспорта: %s", e)
        return {}


def generate_vehicle_prefill_url(chat_id: int) -> str:
    session = get_session(chat_id)
    vehicle_data = session.get("vehicle_data", {}) or {}
    carrier_id = session.get("vehicle_carrier_id")
    carrier_name = get_carrier_name_by_id(carrier_id) if carrier_id else ""
    if not carrier_name:
        carrier_name = session.get("selected_carrier_name", "")

    params: Dict[str, str] = {}
    if carrier_name:
        params[VEHICLE_FORM_ENTRIES["carrier"]] = carrier_name
    if vehicle_data.get("brand"):
        params[VEHICLE_FORM_ENTRIES["brand"]] = str(vehicle_data["brand"])
    if vehicle_data.get("model"):
        params[VEHICLE_FORM_ENTRIES["model"]] = str(vehicle_data["model"])
    if vehicle_data.get("plate"):
        params[VEHICLE_FORM_ENTRIES["plate"]] = str(vehicle_data["plate"])
    if vehicle_data.get("vin"):
        params[VEHICLE_FORM_ENTRIES["vin"]] = str(vehicle_data["vin"])
    if vehicle_data.get("year"):
        params[VEHICLE_FORM_ENTRIES["year"]] = str(vehicle_data["year"])

    logger.info("generate_vehicle_prefill_url: chat_id=%s carrier=%s vehicle=%s", chat_id, carrier_name, vehicle_data)
    
    if params:
        return VEHICLE_FORM_URL + "?" + urlencode(params, quote_via=quote)
    return VEHICLE_FORM_URL


def start_add_vehicle_flow(chat_id: int):
    """Запустить сценарий добавления машины с выбором перевозчика."""
    logger.info("start_add_vehicle_flow: chat_id=%s", chat_id)
    carriers = get_carriers_list()

    if not carriers:
        bot.send_message(chat_id, "❌ Сначала добавьте перевозчика!")
        return

    session = get_session(chat_id)
    session["state"] = "waiting_vehicle_carrier_select"
    save_session(chat_id, session)

    markup = InlineKeyboardMarkup()
    for carrier in carriers:
        carrier_id = str(carrier.get("id", "")).strip()
        carrier_name = str(carrier.get("name", "")).strip() or f"ID {carrier_id}"
        if not carrier_id:
            continue
        markup.add(
            InlineKeyboardButton(
                text=carrier_name,
                callback_data=f"vehicle_carrier_{carrier_id}",
            )
        )

    bot.send_message(
        chat_id,
        "🚛 Добавление машины\n\n"
        "Выберите перевозчика:",
        reply_markup=markup,
    )


def show_vehicle_add_options(message):
    """Меню добавления машины (быстрый вход через текст/голос)."""
    start_add_vehicle_flow(message.chat.id)


def show_carrier_vehicles(message, carrier_id, carrier_name):
    """Показать машины перевозчика."""
    chat_id = message.chat.id
    data, error = call_google_script({"action": "get_vehicles", "carrier_id": carrier_id})
    if error:
        bot.send_message(chat_id, f"❌ Ошибка получения машин: {error}")
        return

    if isinstance(data, dict) and data.get("ok") and isinstance(data.get("result"), dict):
        data = data.get("result", {})

    vehicles = data.get("vehicles", []) or []

    session = get_session(chat_id)
    session["selected_carrier_id"] = carrier_id
    session["selected_carrier_name"] = carrier_name

    if not vehicles:
        vehicle_form_url = build_google_form_url("vehicle", carrier_id=carrier_id)
        markup = InlineKeyboardMarkup()
        if vehicle_form_url:
            markup.add(InlineKeyboardButton("➕ Добавить машину", url=vehicle_form_url))
        save_session(chat_id, session)
        bot.send_message(
            chat_id,
            f"❌ У перевозчика {carrier_name} нет машин в базе",
            reply_markup=markup if vehicle_form_url else None,
        )
        return

    session["vehicles_map"] = {str(v.get("id")): v for v in vehicles}
    save_session(chat_id, session)

    markup = InlineKeyboardMarkup()
    for v in vehicles:
        btn_text = (
            f"🚛 {v.get('brand', '')} {v.get('model', '')} {v.get('number', '')} | "
            f"{v.get('capacity_pallets', '?')}п | {v.get('capacity_tons', '?')}т | {v.get('temp_regime', '—')}"
        )
        markup.add(
            InlineKeyboardButton(
                text=btn_text,
                callback_data=f"select_vehicle_{v.get('id')}",
            )
        )

    vehicle_form_url = build_google_form_url("vehicle", carrier_id=carrier_id)
    if vehicle_form_url:
        markup.add(InlineKeyboardButton("➕ Добавить новую машину", url=vehicle_form_url))

    bot.send_message(chat_id, f"Выберите машину ({carrier_name}):", reply_markup=markup)


def show_carrier_drivers(message, carrier_id, vehicle_id):
    """Показать водителей перевозчика."""
    chat_id = message.chat.id
    data, error = call_google_script({"action": "get_drivers", "carrier_id": carrier_id})
    if error:
        bot.send_message(chat_id, f"❌ Ошибка получения водителей: {error}")
        return

    if isinstance(data, dict) and data.get("ok") and isinstance(data.get("result"), dict):
        data = data.get("result", {})

    drivers = data.get("drivers", []) or []

    session = get_session(chat_id)
    session["selected_vehicle_id"] = vehicle_id

    if not drivers:
        driver_form_url = build_google_form_url("driver", carrier_id=carrier_id)
        markup = InlineKeyboardMarkup()
        if driver_form_url:
            markup.add(InlineKeyboardButton("➕ Добавить водителя", url=driver_form_url))
        save_session(chat_id, session)
        bot.send_message(chat_id, "❌ Нет водителей в базе", reply_markup=markup if driver_form_url else None)
        return

    session["drivers_map"] = {str(d.get("id")): d for d in drivers}
    save_session(chat_id, session)

    markup = InlineKeyboardMarkup()
    for d in drivers:
        medbook_status = "✅" if d.get("medbook_valid") else "⚠️"
        medbook_until = d.get("medbook_valid_until") or "—"
        btn_text = f"{medbook_status} {d.get('full_name', 'Без имени')} | Мед до {medbook_until}"
        markup.add(
            InlineKeyboardButton(
                text=btn_text,
                callback_data=f"select_driver_{d.get('id')}_{vehicle_id}",
            )
        )

    driver_form_url = build_google_form_url("driver", carrier_id=carrier_id)
    if driver_form_url:
        markup.add(InlineKeyboardButton("➕ Добавить водителя", url=driver_form_url))

    bot.send_message(chat_id, "Выберите водителя:", reply_markup=markup)




# =========================
# FSM: СОЗДАНИЕ ДОГОВОР-ЗАЯВКИ
# =========================

TRIP_REQUEST_DEFAULT_PAYMENT_TERMS = (
    "Безналичный расчет после предоставления оригиналов ТН, счета и акта"
)

TRIP_REQUEST_FIELD_ORDER = [
    "route_name",
    "loading_datetime",
    "unloading_datetime",
    "loading_address",
    "loading_manager",
    "loading_manager_phone",
    "unloading_address",
    "unloading_manager",
    "unloading_manager_phone",
    "cargo_description",
    "weight",
    "pallets",
    "temperature_mode",
    "price",
    "vat_type",
    "payment_terms",
    "additional_terms",
]

TRIP_REQUEST_FIELD_PROMPTS = {
    "route_name": "1/17. Укажите маршрут (например: Иркутск → Братск)",
    "loading_datetime": "2/17. Укажите дату и время погрузки (например: 12.05.2026 09:30)",
    "unloading_datetime": "3/17. Укажите дату и время выгрузки (например: 13.05.2026 18:00)",
    "loading_address": "4/17. Укажите адрес погрузки",
    "loading_manager": "5/17. Укажите ответственного на погрузке (ФИО)",
    "loading_manager_phone": "6/17. Укажите телефон ответственного на погрузке",
    "unloading_address": "7/17. Укажите адрес выгрузки",
    "unloading_manager": "8/17. Укажите ответственного на выгрузке (ФИО)",
    "unloading_manager_phone": "9/17. Укажите телефон ответственного на выгрузке",
    "cargo_description": "10/17. Укажите описание груза",
    "weight": "11/17. Укажите вес груза (в тоннах)",
    "pallets": "12/17. Укажите количество паллет",
    "temperature_mode": "13/17. Укажите температурный режим",
    "price": "14/17. Укажите ставку (в рублях)",
}

TRIP_REQUEST_STATE_KEYS = [
    "trip_request_data",
    "trip_customers_map",
    "trip_carriers_map",
    "trip_vehicles_map",
    "trip_drivers_map",
]


def _unwrap_google_result(data: Dict[str, Any]) -> Any:
    if isinstance(data, dict) and data.get("ok") and "result" in data:
        return data.get("result")
    return data


def _extract_id(item: Dict[str, Any], candidates: List[str]) -> str:
    for key in candidates:
        value = item.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def _clean_trip_request_state(session: Dict[str, Any]) -> Dict[str, Any]:
    for key in TRIP_REQUEST_STATE_KEYS:
        session.pop(key, None)
    if str(session.get("state", "")).startswith("trip_request_"):
        session["state"] = ""
    return session


def _trip_request_data(session: Dict[str, Any]) -> Dict[str, Any]:
    data = session.get("trip_request_data")
    if not isinstance(data, dict):
        data = {}
        session["trip_request_data"] = data
    return data


def _format_vehicle_title(vehicle: Dict[str, Any]) -> str:
    model = str(vehicle.get("vehicle_model") or vehicle.get("model") or "").strip()
    number = str(vehicle.get("vehicle_number") or vehicle.get("number") or vehicle.get("plate") or "").strip()
    trailer = str(vehicle.get("trailer_number") or vehicle.get("trailer_plate") or "").strip()
    line = f"{model} {number}".strip()
    if trailer:
        line += f" | прицеп: {trailer}"
    return line or "Без машины"


def _trip_validate_datetime(value: str) -> bool:
    has_date = bool(re.search(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b", value or ""))
    has_time = bool(re.search(r"\b\d{1,2}:\d{2}\b", value or ""))
    return has_date and has_time


def _trip_validate_field(field: str, value: str) -> Tuple[bool, str, str]:
    raw = (value or "").strip()
    if not raw:
        return False, "", "Поле не может быть пустым."

    if field in ("loading_datetime", "unloading_datetime"):
        if not _trip_validate_datetime(raw):
            return False, "", "Введите дату и время в формате ДД.ММ.ГГГГ ЧЧ:ММ."
        return True, raw, ""

    if field in ("loading_manager_phone", "unloading_manager_phone"):
        phone = normalize_phone(raw)
        if not phone:
            return False, "", "Телефон должен быть в формате +7XXXXXXXXXX или 8XXXXXXXXXX."
        return True, phone, ""

    if field == "weight":
        try:
            number = float(raw.replace(",", "."))
            if number <= 0:
                return False, "", "Вес должен быть больше 0."
            return True, str(number).rstrip("0").rstrip("."), ""
        except ValueError:
            return False, "", "Вес должен быть числом (например: 20.5)."

    if field == "pallets":
        try:
            pallets = int(clean_digits(raw))
            if pallets <= 0:
                return False, "", "Количество паллет должно быть больше 0."
            return True, str(pallets), ""
        except ValueError:
            return False, "", "Количество паллет должно быть целым числом."

    if field == "price":
        digits = clean_digits(raw)
        if not digits:
            return False, "", "Ставка должна содержать число."
        return True, digits, ""

    return True, raw, ""


def _trip_next_field(current_field: str) -> str:
    try:
        idx = TRIP_REQUEST_FIELD_ORDER.index(current_field)
    except ValueError:
        return ""
    if idx + 1 >= len(TRIP_REQUEST_FIELD_ORDER):
        return ""
    return TRIP_REQUEST_FIELD_ORDER[idx + 1]


def _trip_prompt_field(chat_id: int, field: str):
    session = get_session(chat_id)
    data = _trip_request_data(session)
    session["state"] = f"trip_request_input_{field}"
    save_session(chat_id, session)

    if field == "vat_type":
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("с НДС", callback_data="trip_vat_with"))
        markup.add(InlineKeyboardButton("без НДС", callback_data="trip_vat_without"))
        bot.send_message(chat_id, "15/17. Выберите НДС:", reply_markup=markup)
        return

    if field == "payment_terms":
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Оставить по умолчанию", callback_data="trip_payment_default"))
        markup.add(InlineKeyboardButton("✏️ Ввести вручную", callback_data="trip_payment_custom"))
        bot.send_message(
            chat_id,
            "16/17. Условия оплаты.\n"
            f"По умолчанию: {TRIP_REQUEST_DEFAULT_PAYMENT_TERMS}\n\n"
            "Нажмите кнопку или отправьте свой текст.",
            reply_markup=markup,
        )
        return

    if field == "additional_terms":
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Нет", callback_data="trip_additional_none"))
        markup.add(InlineKeyboardButton("✏️ Ввести текст", callback_data="trip_additional_custom"))
        bot.send_message(chat_id, "17/17. Дополнительные условия:", reply_markup=markup)
        return

    bot.send_message(chat_id, TRIP_REQUEST_FIELD_PROMPTS.get(field, f"Введите {field}:"))


def _trip_show_preview(chat_id: int):
    session = get_session(chat_id)
    data = _trip_request_data(session)

    preview = [
        "📋 Предпросмотр договор-заявки:",
        "",
        f"Заказчик: {data.get('customer_name', '—')}",
        f"Перевозчик: {data.get('carrier_name', '—')} (ИНН: {data.get('carrier_inn', '—')})",
        f"Машина: {_format_vehicle_title(data)}",
        f"Водитель: {data.get('driver_name', 'Без водителя')} ({data.get('driver_phone', '—') or '—'})",
        "",
        f"Маршрут: {data.get('route_name', '—')}",
        f"Погрузка: {data.get('loading_datetime', '—')} | {data.get('loading_address', '—')}",
        f"Ответственный погрузка: {data.get('loading_manager', '—')} ({data.get('loading_manager_phone', '—')})",
        f"Выгрузка: {data.get('unloading_datetime', '—')} | {data.get('unloading_address', '—')}",
        f"Ответственный выгрузка: {data.get('unloading_manager', '—')} ({data.get('unloading_manager_phone', '—')})",
        f"Груз: {data.get('cargo_description', '—')}",
        f"Вес: {data.get('weight', '—')} т",
        f"Паллеты: {data.get('pallets', '—')}",
        f"Температурный режим: {data.get('temperature_mode', '—')}",
        f"Ставка: {data.get('price', '—')} руб.",
        f"НДС: {data.get('vat_type', '—')}",
        f"Условия оплаты: {data.get('payment_terms', '—')}",
        f"Доп. условия: {data.get('additional_terms', '—')}",
    ]

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Создать договор-заявку", callback_data="trip_create_confirm"))
    markup.add(InlineKeyboardButton("❌ Отменить", callback_data="trip_cancel"))

    session["state"] = "trip_request_preview"
    save_session(chat_id, session)
    bot.send_message(chat_id, "\n".join(preview), reply_markup=markup)


def _trip_show_customers(chat_id: int):
    data, error = call_google_script({"action": "list_customers"})
    if error:
        bot.send_message(chat_id, f"❌ Не удалось получить заказчиков: {error}")
        return

    result = _unwrap_google_result(data)
    customers = []
    if isinstance(result, list):
        customers = result
    elif isinstance(result, dict):
        customers = result.get("customers", []) or []

    if not customers:
        bot.send_message(chat_id, "❌ Заказчики не найдены. Сначала добавьте заказчика.")
        return

    session = get_session(chat_id)
    session["state"] = "trip_request_select_customer"
    session["trip_customers_map"] = {}
    markup = InlineKeyboardMarkup()

    for idx, customer in enumerate(customers[:50]):
        cid = _extract_id(customer, ["id", "customer_id", "code"])
        name = str(customer.get("name") or customer.get("customer_name") or cid or "Без названия")
        if not cid:
            cid = f"customer_{idx}"
        session["trip_customers_map"][str(idx)] = {
            "id": cid,
            "name": name,
            "raw": customer,
        }
        markup.add(InlineKeyboardButton(name, callback_data=f"trip_customer_{idx}"))

    save_session(chat_id, session)
    bot.send_message(chat_id, "1/5. Выберите заказчика:", reply_markup=markup)


def _trip_show_carriers(chat_id: int):
    data, error = call_google_script({"action": "list_carriers"})
    if error:
        bot.send_message(chat_id, f"❌ Не удалось получить перевозчиков: {error}")
        return

    result = _unwrap_google_result(data)
    carriers = []
    if isinstance(result, dict):
        carriers = result.get("carriers", []) or []
    elif isinstance(result, list):
        carriers = result

    if not carriers:
        bot.send_message(chat_id, "❌ Перевозчики не найдены. Сначала добавьте перевозчика.")
        return

    session = get_session(chat_id)
    session["state"] = "trip_request_select_carrier"
    session["trip_carriers_map"] = {}
    markup = InlineKeyboardMarkup()

    for idx, carrier in enumerate(carriers[:70]):
        carrier_id = _extract_id(carrier, ["id", "carrier_id"])
        carrier_name = str(carrier.get("name") or carrier.get("carrier_name") or carrier_id or "Без названия")
        carrier_inn = clean_digits(str(carrier.get("inn") or ""))
        if not carrier_id:
            carrier_id = f"carrier_{idx}"
        session["trip_carriers_map"][str(idx)] = {
            "id": carrier_id,
            "name": carrier_name,
            "inn": carrier_inn,
            "raw": carrier,
        }
        title = f"{carrier_name} ({carrier_inn})" if carrier_inn else carrier_name
        markup.add(InlineKeyboardButton(title, callback_data=f"trip_carrier_{idx}"))

    save_session(chat_id, session)
    bot.send_message(chat_id, "2/5. Выберите перевозчика:", reply_markup=markup)


def _trip_show_vehicles(chat_id: int, carrier_id: str):
    data, error = call_google_script({"action": "get_carrier_vehicles", "carrier_id": carrier_id})
    if error:
        bot.send_message(chat_id, f"❌ Не удалось получить машины: {error}")
        return

    result = _unwrap_google_result(data)
    if isinstance(result, dict) and result.get("success") is False:
        bot.send_message(chat_id, f"❌ Ошибка списка машин: {result.get('error', 'неизвестная ошибка')}")
        return

    vehicles = []
    if isinstance(result, dict):
        vehicles = result.get("vehicles", []) or []
    elif isinstance(result, list):
        vehicles = result

    session = get_session(chat_id)
    session["state"] = "trip_request_select_vehicle"
    session["trip_vehicles_map"] = {}

    markup = InlineKeyboardMarkup()
    for idx, vehicle in enumerate(vehicles[:80]):
        vehicle_id = _extract_id(vehicle, ["id", "vehicle_id"])
        number = str(vehicle.get("number") or vehicle.get("plate") or "").strip()
        brand = str(vehicle.get("brand") or "").strip()
        model = str(vehicle.get("model") or "").strip()
        trailer = str(vehicle.get("trailer_number") or vehicle.get("trailer_plate") or "").strip()
        title = " ".join([brand, model, number]).strip() or f"Машина {idx+1}"
        if trailer:
            title += f" | прицеп: {trailer}"
        if not vehicle_id:
            vehicle_id = f"vehicle_{idx}"

        session["trip_vehicles_map"][str(idx)] = {
            "id": vehicle_id,
            "vehicle_number": number,
            "vehicle_model": " ".join([brand, model]).strip(),
            "trailer_number": trailer,
            "raw": vehicle,
        }
        markup.add(InlineKeyboardButton(title, callback_data=f"trip_vehicle_{idx}"))

    markup.add(InlineKeyboardButton("Без машины", callback_data="trip_vehicle_none"))
    markup.add(InlineKeyboardButton("➕ Добавить машину", callback_data="trip_vehicle_add"))

    save_session(chat_id, session)
    bot.send_message(chat_id, "3/5. Выберите машину:", reply_markup=markup)


def _trip_show_drivers(chat_id: int, carrier_id: str):
    print("GET DRIVERS carrier_id =", carrier_id)
    logger.info("GET DRIVERS carrier_id=%s", carrier_id)

    data, error = call_google_script({"action": "get_carrier_drivers", "carrier_id": carrier_id})
    if error:
        bot.send_message(chat_id, f"❌ Не удалось получить водителей: {error}")
        return

    print("API response =", data)
    logger.info("get_carrier_drivers raw response=%s", data)

    # Ожидаемый формат: {"ok": true, "result": {"success": true, "drivers": [...]}}
    result = data.get("result", {}) if isinstance(data, dict) else {}

    # Fallback для обратной совместимости со старыми форматами
    if not isinstance(result, dict):
        result = _unwrap_google_result(data)

    if isinstance(result, dict) and result.get("success") is False:
        bot.send_message(chat_id, f"❌ Ошибка списка водителей: {result.get('error', 'неизвестная ошибка')}")
        return

    drivers = []
    if isinstance(result, dict):
        drivers = result.get("drivers", []) or []
    elif isinstance(result, list):
        drivers = result

    print("Drivers list =", drivers)
    logger.info("get_carrier_drivers parsed drivers count=%s", len(drivers))

    session = get_session(chat_id)
    session["state"] = "trip_request_select_driver"
    session["trip_drivers_map"] = {}

    markup = InlineKeyboardMarkup()
    for idx, driver in enumerate(drivers[:80]):
        driver_id = _extract_id(driver, ["id", "driver_id"])
        name = str(driver.get("full_name") or driver.get("name") or driver.get("driver_name") or "").strip() or f"Водитель {idx+1}"
        phone = str(driver.get("phone") or driver.get("driver_phone") or "").strip()
        if not driver_id:
            driver_id = f"driver_{idx}"

        session["trip_drivers_map"][str(idx)] = {
            "id": driver_id,
            "driver_name": name,
            "driver_phone": phone,
            "raw": driver,
        }
        label = f"{name} ({phone})" if phone else name
        markup.add(InlineKeyboardButton(label, callback_data=f"trip_driver_{idx}"))

    markup.add(InlineKeyboardButton("Без водителя", callback_data="trip_driver_none"))
    markup.add(InlineKeyboardButton("➕ Добавить водителя", callback_data="trip_driver_add"))

    save_session(chat_id, session)
    bot.send_message(chat_id, "4/5. Выберите водителя:", reply_markup=markup)


def start_trip_request_fsm(chat_id: int):
    session = get_session(chat_id)
    session = _clean_trip_request_state(session)
    session["scenario"] = "existing_carrier_trip_request"
    session["trip_request_data"] = {}
    session["state"] = "trip_request_select_customer"
    save_session(chat_id, session)

    bot.send_message(chat_id, "📦 Запускаю пошаговое создание договор-заявки (5 этапов).")
    _trip_show_customers(chat_id)


def start_new_trip_request_fsm(chat_id: int):
    """Явная точка входа для кнопки "📦 Новая заявка" (новый FSM flow)."""
    start_trip_request_fsm(chat_id)


def _trip_build_create_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "action": "create_trip_request",
        "customer_id": data.get("customer_id", ""),
        "customer_name": data.get("customer_name", ""),
        "carrier_id": data.get("carrier_id", ""),
        "carrier_name": data.get("carrier_name", ""),
        "carrier_inn": data.get("carrier_inn", ""),
        "vehicle_id": data.get("vehicle_id", ""),
        "vehicle_number": data.get("vehicle_number", ""),
        "vehicle_model": data.get("vehicle_model", ""),
        "trailer_number": data.get("trailer_number", ""),
        "driver_id": data.get("driver_id", ""),
        "driver_name": data.get("driver_name", ""),
        "driver_phone": data.get("driver_phone", ""),
        "route_name": data.get("route_name", ""),
        "loading_datetime": data.get("loading_datetime", ""),
        "unloading_datetime": data.get("unloading_datetime", ""),
        "loading_address": data.get("loading_address", ""),
        "loading_manager": data.get("loading_manager", ""),
        "loading_manager_phone": data.get("loading_manager_phone", ""),
        "unloading_address": data.get("unloading_address", ""),
        "unloading_manager": data.get("unloading_manager", ""),
        "unloading_manager_phone": data.get("unloading_manager_phone", ""),
        "cargo_description": data.get("cargo_description", ""),
        "weight": data.get("weight", ""),
        "pallets": data.get("pallets", ""),
        "temperature_mode": data.get("temperature_mode", ""),
        "price": data.get("price", ""),
        "vat_type": data.get("vat_type", ""),
        "payment_terms": data.get("payment_terms", TRIP_REQUEST_DEFAULT_PAYMENT_TERMS),
        "additional_terms": data.get("additional_terms", "Нет"),
    }


def process_trip_request_text_input(chat_id: int, text: str) -> bool:
    session = get_session(chat_id)
    state = str(session.get("state", ""))

    if not state.startswith("trip_request_"):
        return False

    if state in {
        "trip_request_select_customer",
        "trip_request_select_carrier",
        "trip_request_select_vehicle",
        "trip_request_select_driver",
        "trip_request_preview",
    }:
        bot.send_message(chat_id, "Пожалуйста, используйте кнопки под сообщением для продолжения.")
        return True

    if not state.startswith("trip_request_input_"):
        return False

    field = state.replace("trip_request_input_", "", 1)

    if field in ("vat_type", "additional_terms"):
        bot.send_message(chat_id, "Используйте кнопки ниже для выбора варианта.")
        return True

    is_ok, cleaned, error = _trip_validate_field(field, text)
    if not is_ok:
        bot.send_message(chat_id, f"⚠️ {error}")
        return True

    data = _trip_request_data(session)
    data[field] = cleaned
    session["trip_request_data"] = data
    save_session(chat_id, session)

    next_field = _trip_next_field(field)
    if not next_field:
        _trip_show_preview(chat_id)
    else:
        _trip_prompt_field(chat_id, next_field)
    return True


@bot.callback_query_handler(func=lambda call: call.data.startswith("trip_customer_"))
def handle_trip_customer_select(call):
    chat_id = call.message.chat.id
    token = call.data.replace("trip_customer_", "", 1)

    session = get_session(chat_id)
    customer = (session.get("trip_customers_map") or {}).get(token)
    if not customer:
        bot.answer_callback_query(call.id, "Список устарел. Начните заново.")
        return

    data = _trip_request_data(session)
    data["customer_id"] = customer.get("id", "")
    data["customer_name"] = customer.get("name", "")
    session["trip_request_data"] = data
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, "Заказчик выбран")
    _trip_show_carriers(chat_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("trip_carrier_"))
def handle_trip_carrier_select(call):
    chat_id = call.message.chat.id
    token = call.data.replace("trip_carrier_", "", 1)

    session = get_session(chat_id)
    carrier = (session.get("trip_carriers_map") or {}).get(token)
    if not carrier:
        bot.answer_callback_query(call.id, "Список устарел. Начните заново.")
        return

    data = _trip_request_data(session)
    data["carrier_id"] = carrier.get("id", "")
    data["carrier_name"] = carrier.get("name", "")
    data["carrier_inn"] = carrier.get("inn", "")
    session["trip_request_data"] = data
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, "Перевозчик выбран")
    _trip_show_vehicles(chat_id, carrier.get("id", ""))


@bot.callback_query_handler(func=lambda call: call.data.startswith("trip_vehicle_") or call.data in ("trip_vehicle_none", "trip_vehicle_add"))
def handle_trip_vehicle_actions(call):
    chat_id = call.message.chat.id

    if call.data == "trip_vehicle_add":
        bot.answer_callback_query(call.id)
        bot.send_message(
            chat_id,
            "Добавление машины будет отдельным сценарием. Сейчас выберите существующую запись или нажмите 'Без машины'.",
        )
        return

    session = get_session(chat_id)
    data = _trip_request_data(session)

    if call.data == "trip_vehicle_none":
        data["vehicle_id"] = ""
        data["vehicle_number"] = ""
        data["vehicle_model"] = ""
        data["trailer_number"] = ""
        session["trip_request_data"] = data
        save_session(chat_id, session)

        bot.answer_callback_query(call.id, "Продолжаем без машины")
        _trip_show_drivers(chat_id, data.get("carrier_id", ""))
        return

    token = call.data.replace("trip_vehicle_", "", 1)
    vehicle = (session.get("trip_vehicles_map") or {}).get(token)
    if not vehicle:
        bot.answer_callback_query(call.id, "Список устарел. Начните заново.")
        return

    data["vehicle_id"] = vehicle.get("id", "")
    data["vehicle_number"] = vehicle.get("vehicle_number", "")
    data["vehicle_model"] = vehicle.get("vehicle_model", "")
    data["trailer_number"] = vehicle.get("trailer_number", "")
    session["trip_request_data"] = data
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, "Машина выбрана")
    _trip_show_drivers(chat_id, data.get("carrier_id", ""))


@bot.callback_query_handler(func=lambda call: call.data.startswith("trip_driver_") or call.data in ("trip_driver_none", "trip_driver_add"))
def handle_trip_driver_actions(call):
    chat_id = call.message.chat.id

    if call.data == "trip_driver_add":
        bot.answer_callback_query(call.id)
        bot.send_message(
            chat_id,
            "Добавление водителя будет отдельным сценарием. Сейчас выберите существующую запись или нажмите 'Без водителя'.",
        )
        return

    session = get_session(chat_id)
    data = _trip_request_data(session)

    if call.data == "trip_driver_none":
        data["driver_id"] = ""
        data["driver_name"] = "Без водителя"
        data["driver_phone"] = ""
        session["trip_request_data"] = data
        save_session(chat_id, session)

        bot.answer_callback_query(call.id, "Продолжаем без водителя")
        bot.send_message(chat_id, "5/5. Переходим к заполнению данных рейса.")
        _trip_prompt_field(chat_id, TRIP_REQUEST_FIELD_ORDER[0])
        return

    token = call.data.replace("trip_driver_", "", 1)
    driver = (session.get("trip_drivers_map") or {}).get(token)
    if not driver:
        bot.answer_callback_query(call.id, "Список устарел. Начните заново.")
        return

    data["driver_id"] = driver.get("id", "")
    data["driver_name"] = driver.get("driver_name", "")
    data["driver_phone"] = driver.get("driver_phone", "")
    session["trip_request_data"] = data
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, "Водитель выбран")
    bot.send_message(chat_id, "5/5. Переходим к заполнению данных рейса.")
    _trip_prompt_field(chat_id, TRIP_REQUEST_FIELD_ORDER[0])


@bot.callback_query_handler(func=lambda call: call.data in ("trip_vat_with", "trip_vat_without"))
def handle_trip_vat_select(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    data = _trip_request_data(session)

    data["vat_type"] = "с НДС" if call.data == "trip_vat_with" else "без НДС"
    session["trip_request_data"] = data
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, "НДС сохранен")
    _trip_prompt_field(chat_id, _trip_next_field("vat_type"))


@bot.callback_query_handler(func=lambda call: call.data in ("trip_payment_default", "trip_payment_custom"))
def handle_trip_payment_mode(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    data = _trip_request_data(session)

    if call.data == "trip_payment_default":
        data["payment_terms"] = TRIP_REQUEST_DEFAULT_PAYMENT_TERMS
        session["trip_request_data"] = data
        save_session(chat_id, session)
        bot.answer_callback_query(call.id, "Условия оплаты сохранены")
        _trip_prompt_field(chat_id, _trip_next_field("payment_terms"))
        return

    session["state"] = "trip_request_input_payment_terms"
    save_session(chat_id, session)
    bot.answer_callback_query(call.id)
    bot.send_message(chat_id, "Введите условия оплаты текстом:")


@bot.callback_query_handler(func=lambda call: call.data in ("trip_additional_none", "trip_additional_custom"))
def handle_trip_additional_terms(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    data = _trip_request_data(session)

    if call.data == "trip_additional_none":
        data["additional_terms"] = "Нет"
        session["trip_request_data"] = data
        save_session(chat_id, session)
        bot.answer_callback_query(call.id, "Сохранено")
        _trip_show_preview(chat_id)
        return

    session["state"] = "trip_request_input_additional_terms"
    save_session(chat_id, session)
    bot.answer_callback_query(call.id)
    bot.send_message(chat_id, "Введите дополнительные условия:")


@bot.callback_query_handler(func=lambda call: call.data in ("trip_create_confirm", "trip_cancel"))
def handle_trip_finalize(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)

    if call.data == "trip_cancel":
        session = _clean_trip_request_state(session)
        save_session(chat_id, session)
        bot.answer_callback_query(call.id, "Отменено")
        bot.send_message(chat_id, "❌ Создание договор-заявки отменено.")
        return

    bot.answer_callback_query(call.id, "Создаю заявку...")
    data = _trip_request_data(session)
    payload = _trip_build_create_payload(data)

    gs_data, gs_error = call_google_script(payload)
    if gs_error:
        bot.send_message(chat_id, f"❌ Не удалось создать договор-заявку: {gs_error}")
        return

    result = _unwrap_google_result(gs_data)
    if isinstance(result, dict) and result.get("success"):
        request_number = result.get("requestNumber") or result.get("request_number") or result.get("number") or "—"
        request_date = result.get("requestDate") or result.get("date") or "—"
        doc_url = result.get("docUrl") or result.get("url") or ""
        pdf_url = result.get("pdfUrl") or ""

        msg = [
            "✅ Договор-заявка создана!",
            f"Номер: {request_number}",
            f"Дата: {request_date}",
        ]
        if doc_url:
            msg.append(f"Документ: {doc_url}")
        if pdf_url:
            msg.append(f"PDF: {pdf_url}")
        bot.send_message(chat_id, "\n".join(msg))

        session = _clean_trip_request_state(session)
        save_session(chat_id, session)
        return

    err = result.get("error", "Неизвестная ошибка") if isinstance(result, dict) else "Неожиданный формат ответа"
    bot.send_message(chat_id, f"❌ Не удалось создать договор-заявку: {err}")

def handle_voice_command(message, text: str):
    text_lower = (text or "").lower()

    if "новый перевозчик" in text_lower or "добавить перевозчик" in text_lower:
        show_carrier_add_options(message)
        return

    if "новая машина" in text_lower or "добавить машину" in text_lower:
        show_vehicle_add_options(message)
        return

    if "нужна машина" in text_lower or "нужен транспорт" in text_lower:
        pallets = extract_number(text)
        find_suitable_carriers(message, pallets)
        return

    if "рейс" in text_lower:
        parse_route_command(message, text)
        return

    message.text = text
    handle_text(message)


def route_quick_commands(message, text: str) -> bool:
    text_lower = (text or "").lower()

    if "новая машина" in text_lower or "добавить машину" in text_lower:
        show_vehicle_add_options(message)
        return True

    if "нужна машина" in text_lower or "нужен транспорт" in text_lower:
        pallets = extract_number(text)
        find_suitable_carriers(message, pallets)
        return True

    if "рейс" in text_lower:
        parse_route_command(message, text)
        return True

    return False


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


def build_add_customer_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📄 Загрузить карточку DOCX", callback_data="upload_card"))
    markup.add(
        InlineKeyboardButton(
            "📝 Заполнить Google Форму",
            url="https://script.google.com/macros/s/AKfycbwsmt46_3qRA2zBdCgsFW-bkDGXuC3JV-GUMefAMOdWnjC2fErmQdBuF4MhAoWgq88R/execr",
        )
    )
    markup.add(InlineKeyboardButton("⌨️ Ввести ИНН", callback_data="enter_inn_customer"))
    return markup


def build_add_carrier_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton(
            text="📄 Загрузить карточку компании (DOCX/PDF)",
            callback_data="carrier_upload_card",
        )
    )

    carrier_form_url = build_google_form_url("carrier")
    if carrier_form_url:
        markup.add(
            InlineKeyboardButton(
                text="📝 Заполнить Google Форму",
                url=carrier_form_url,
            )
        )

    markup.add(
        InlineKeyboardButton(
            text="⌨️ Ввести ИНН (автозаполнение DaData)",
            callback_data="carrier_enter_inn",
        )
    )
    return markup


def build_dadata_followup_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton(
            "📤 Загрузить карточку",
            callback_data="upload_carrier_card",
        )
    )
    markup.add(
        InlineKeyboardButton(
            "⌨️ Ввести вручную",
            callback_data="carrier_manual_input",
        )
    )
    markup.add(
        InlineKeyboardButton(
            "💾 Сохранить как есть",
            callback_data="skip_carrier_details",
        )
    )
    return markup


def build_existing_carrier_actions(carrier_id: str) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton(
            "🔄 Обновить данные",
            callback_data=f"update_carrier_{carrier_id}",
        )
    )
    markup.add(
        InlineKeyboardButton(
            "👁️ Посмотреть текущие данные",
            callback_data=f"view_carrier_{carrier_id}",
        )
    )
    markup.add(
        InlineKeyboardButton(
            "❌ Отменить",
            callback_data="cancel_carrier",
        )
    )
    return markup


def show_carrier_add_options(message):
    """Показать варианты добавления перевозчика"""
    bot.send_message(
        message.chat.id,
        "📋 Как вы хотите добавить перевозчика?\n\n"
        "• Google Форма — самый удобный способ, все поля в одном месте\n"
        "• ИНН — быстрое добавление с автозаполнением из базы DaData\n"
        "• Карточка — загрузите готовый документ компании\n\n"
        "После заполнения Google Формы вернитесь в бот и нажмите /refresh_carriers "
        "или сразу отправьте команду с ИНН/названием перевозчика.",
        reply_markup=build_add_carrier_markup(),
    )


def show_customer_selection(chat_id: int, force_refresh: bool = False):
    customers = get_customers_list(force_refresh=force_refresh)
    markup = InlineKeyboardMarkup()

    for customer in customers:
        btn = InlineKeyboardButton(
            text=customer.get("name", "Без названия"),
            callback_data=f"select_customer_{customer.get('code', '')}",
        )
        markup.add(btn)

    markup.add(
        InlineKeyboardButton(
            text="➕ Добавить нового заказчика",
            callback_data="add_new_customer",
        )
    )

    bot.send_message(chat_id, "Выберите заказчика:", reply_markup=markup)


def prompt_for_missing_after_customer(chat_id: int, session: Dict[str, Any]):
    still_missing = [f for f in missing_session_fields(session) if f != "customer_name"]
    validation_errors = validate_session_fields(session)

    if still_missing or validation_errors:
        messages = []
        if validation_errors:
            messages.append(format_validation_errors_for_user(validation_errors))
        if still_missing:
            messages.append(format_missing_for_user(still_missing))
        messages.append("\nПришлите корректные/недостающие данные одним сообщением.")
        bot.send_message(chat_id, "\n\n".join(messages))
    else:
        bot.send_message(chat_id, "Заказчик выбран. Все обязательные данные уже собраны, создаю договор...")


def apply_extracted_carrier_data(chat_id: int, extracted: Dict[str, Any], source_hint: str):
    session = get_session(chat_id)
    session["scenario"] = "new_carrier_contract"
    session["awaiting_more_data"] = True
    session["awaiting_carrier_inn"] = False
    session["awaiting_carrier_card_upload"] = False

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
    session["ks"] = clean_digits(extracted.get("corr_account", "") or extracted.get("ks", ""))
    session["bik"] = clean_digits(extracted.get("bik", ""))
    session["director"] = extracted.get("director", "")
    session["customer_name"] = session.get("customer_name", "")
    session["tax_mode"] = session.get("tax_mode", "")

    # Автозаполнение к/с по БИК через DaData если к/с не распознан
    if session.get("bik") and not session.get("ks"):
        logger.info("К/с не распознан, пробую получить по БИК %s через DaData", session["bik"])
        bank_info, bank_err = get_bank_by_bik(session["bik"])
        if not bank_err and bank_info.get("ks"):
            session["ks"] = bank_info["ks"]
            logger.info("К/с получен по БИК: %s", session["ks"])
            # Также обогатим название банка если не распознано
            if not session.get("bank") and bank_info.get("bank_name"):
                session["bank"] = bank_info["bank_name"]

    save_session(chat_id, session)

    lines = [f"Нашёл по карточке ({source_hint}):"]
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

    if "customer_name" in missing and not session.get("customer_name"):
        show_customer_selection(chat_id)


def _build_skip_markup(callback_data: str, text: str = "⏩ Пропустить") -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(text, callback_data=callback_data))
    return markup


def sync_session_with_carrier_data(session: Dict[str, Any]) -> Dict[str, Any]:
    carrier_data = session.get("carrier_data") or {}
    if not isinstance(carrier_data, dict):
        carrier_data = {}

    session["carrier_name"] = carrier_data.get("name", session.get("carrier_name", ""))
    session["carrier_type"] = carrier_data.get("carrier_type", session.get("carrier_type", ""))
    session["inn"] = clean_digits(carrier_data.get("inn", session.get("inn", "")))
    session["kpp"] = clean_digits(carrier_data.get("kpp", session.get("kpp", "")))
    session["ogrn"] = carrier_data.get("ogrn", session.get("ogrn", ""))
    session["registration_address"] = carrier_data.get("address", session.get("registration_address", ""))
    session["director"] = carrier_data.get("director", session.get("director", ""))
    session["phone"] = normalize_phone(carrier_data.get("phone", session.get("phone", "")))
    session["phone2"] = normalize_phone(carrier_data.get("phone2", session.get("phone2", "")))
    session["email"] = carrier_data.get("email", session.get("email", ""))
    session["bank"] = carrier_data.get("bank", session.get("bank", ""))
    session["rs"] = clean_digits(carrier_data.get("account", carrier_data.get("rs", session.get("rs", ""))))
    session["ks"] = clean_digits(carrier_data.get("corr_account", carrier_data.get("ks", session.get("ks", ""))))
    session["bik"] = clean_digits(carrier_data.get("bik", session.get("bik", "")))

    # Автозаполнение к/с по БИК если к/с отсутствует
    if session.get("bik") and not session.get("ks"):
        bank_info, _ = get_bank_by_bik(session["bik"])
        if bank_info.get("ks"):
            session["ks"] = bank_info["ks"]
            carrier_data["corr_account"] = bank_info["ks"]
            if not session.get("bank") and bank_info.get("bank_name"):
                session["bank"] = bank_info["bank_name"]
                carrier_data["bank"] = bank_info["bank_name"]

    tax_mode = normalize_tax_mode(carrier_data.get("tax_mode", session.get("tax_mode", "")))
    if tax_mode:
        session["tax_mode"] = tax_mode
        carrier_data["tax_mode"] = tax_mode

    session["carrier_data"] = carrier_data
    return session


def save_vehicle_to_sheets(chat_id: int) -> bool:
    """Сохранить машину в Google Sheets через Apps Script."""
    session = get_session(chat_id)
    vehicle_data = session.get("vehicle_data", {})
    carrier_id = session.get("vehicle_carrier_id")

    if not vehicle_data or not carrier_id:
        bot.send_message(chat_id, "❌ Ошибка: нет данных машины или перевозчика")
        return False

    url = os.getenv("GOOGLE_SCRIPT_URL")
    if not url:
        bot.send_message(chat_id, "❌ Ошибка: GOOGLE_SCRIPT_URL не задан")
        return False

    payload = {
        "action": "save_vehicle",
        "vehicle_data": {
            "carrier_id": carrier_id,
            "brand": vehicle_data.get("brand", ""),
            "model": vehicle_data.get("model", ""),
            "plate": vehicle_data.get("plate", ""),
            "vin": vehicle_data.get("vin", ""),
            "year": vehicle_data.get("year", ""),
            "capacity_tons": vehicle_data.get("capacity_tons", ""),
            "pallets": vehicle_data.get("pallets", ""),
            "temp_regime": vehicle_data.get("temp_regime", ""),
        }
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data, _ = safe_json_loads(response.text)

        if data.get("success"):
            vehicle_id = data.get("vehicle_id")
            session["vehicle_id"] = vehicle_id
            save_session(chat_id, session)

            bot.send_message(
                chat_id,
                f"✅ Машина добавлена!\n"
                f"Госномер: {vehicle_data.get('plate', '—')}\n"
                f"Грузоподъёмность: {vehicle_data.get('capacity_tons', '—')} т\n"
                f"Вместимость: {vehicle_data.get('pallets', '—')} палет"
            )

            # Спросить про прицеп
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("✅ Да", callback_data="add_trailer_yes"))
            markup.add(InlineKeyboardButton("❌ Нет", callback_data="add_trailer_no"))
            bot.send_message(chat_id, "Есть прицеп?", reply_markup=markup)
            return True
        else:
            error = data.get("error", "Неизвестная ошибка")
            bot.send_message(chat_id, f"❌ Не удалось сохранить машину: {error}")
            return False
    except Exception as e:
        logger.exception("Ошибка сохранения машины: %s", e)
        bot.send_message(chat_id, f"❌ Ошибка: {e}")
        return False


def save_trailer_to_sheets(chat_id: int) -> bool:
    """Сохранить прицеп в Google Sheets через Apps Script."""
    session = get_session(chat_id)
    trailer_data = session.get("trailer_data", {})
    vehicle_id = session.get("vehicle_id")
    carrier_id = session.get("vehicle_carrier_id")

    if not trailer_data or not carrier_id:
        bot.send_message(chat_id, "❌ Ошибка: нет данных прицепа или перевозчика")
        return False

    url = os.getenv("GOOGLE_SCRIPT_URL")
    if not url:
        bot.send_message(chat_id, "❌ Ошибка: GOOGLE_SCRIPT_URL не задан")
        return False

    payload = {
        "action": "save_trailer",
        "trailer_data": {
            "carrier_id": carrier_id,
            "vehicle_id": vehicle_id or "",
            "plate": trailer_data.get("plate", ""),
            "brand": trailer_data.get("brand", ""),
            "model": trailer_data.get("model", ""),
            "capacity_tons": trailer_data.get("capacity_tons", ""),
            "pallets": trailer_data.get("pallets", ""),
            "temp_regime": trailer_data.get("temp_regime", ""),
        }
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data, _ = safe_json_loads(response.text)

        if data.get("success"):
            trailer_id = data.get("trailer_id")
            session["trailer_id"] = trailer_id
            save_session(chat_id, session)

            bot.send_message(
                chat_id,
                f"✅ Прицеп добавлен!\n"
                f"Госномер: {trailer_data.get('plate', '—')}\n"
                f"Грузоподъёмность: {trailer_data.get('capacity_tons', '—')} т\n"
                f"Вместимость: {trailer_data.get('pallets', '—')} палет"
            )

            # Спросить про ещё одну машину
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("➕ Да, добавить ещё", callback_data=f"add_vehicle_to_carrier_{carrier_id}"))
            markup.add(InlineKeyboardButton("✅ Нет, завершить", callback_data="cancel_vehicle_add"))
            bot.send_message(chat_id, "Добавить ещё одну машину?", reply_markup=markup)
            return True
        else:
            error = data.get("error", "Неизвестная ошибка")
            bot.send_message(chat_id, f"❌ Не удалось сохранить прицеп: {error}")
            return False
    except Exception as e:
        logger.exception("Ошибка сохранения прицепа: %s", e)
        bot.send_message(chat_id, f"❌ Ошибка: {e}")
        return False


def save_carrier_to_sheets(chat_id: int) -> bool:
    session = get_session(chat_id)
    carrier_data = session.get("carrier_data") or {}
    if not carrier_data:
        logger.error("save_carrier_to_sheets: пустой carrier_data, chat_id=%s", chat_id)
        return False

    url = os.getenv("GOOGLE_SCRIPT_URL")
    if not url:
        logger.warning("GOOGLE_SCRIPT_URL не задан")
        return False

    payload = {
        "action": "create_carrier",
        "carrier_data": carrier_data,
    }

    logger.info("Отправляю в Apps Script create_carrier, chat_id=%s, payload=%s", chat_id, payload)

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=GOOGLE_SCRIPT_TIMEOUT,
        )
        response.raise_for_status()
        response_text = response.text
        logger.info("Ответ Apps Script create_carrier (raw): %s", response_text)

        data, parse_error = safe_json_loads(response_text)
        if parse_error:
            logger.error("save_carrier_to_sheets: не удалось распарсить JSON: %s", parse_error)
            return False
    except Exception as e:
        logger.exception("Ошибка сохранения перевозчика в Google Sheets: %s", e)
        return False

    # Нормализуем ответ: Apps Script может вернуть {success:...} напрямую
    # или обёрнуто в {ok: true, result: {success:...}}
    result = data.get("result", data) if isinstance(data, dict) else data  # fallback к корню если нет result

    if not isinstance(result, dict) or not result.get("success"):
        error_msg = ""
        if isinstance(result, dict):
            error_msg = result.get("error", "")
        if not error_msg and isinstance(data, dict):
            error_msg = data.get("error", "")
        error_msg = error_msg or "Неизвестная ошибка"
        bot.send_message(chat_id, f"❌ Не удалось сохранить перевозчика: {error_msg}")
        logger.error("Carrier save failed: %s", data)
        return False

    session["carrier_id"] = result.get("carrier_id", "")
    session["carrier_save_action"] = result.get("action", "created")
    session["carrier_save_message"] = result.get("message", "")
    save_session(chat_id, session)
    return True


def finalize_carrier_profile(chat_id: int):
    session = get_session(chat_id)
    session = sync_session_with_carrier_data(session)
    save_session(chat_id, session)

    if not save_carrier_to_sheets(chat_id):
        # Конкретная ошибка уже отправлена пользователю из save_carrier_to_sheets
        return

    session = get_session(chat_id)
    carrier_data = session.get("carrier_data") or {}
    save_action = session.get("carrier_save_action", "created")
    save_message = session.get("carrier_save_message", "")

    session["state"] = ""
    session["awaiting_carrier_inn"] = False
    session["awaiting_carrier_card_upload"] = False
    session["awaiting_carrier_duplicate_decision"] = False
    session.pop("pending_carrier_data", None)
    session.pop("existing_carrier", None)
    save_session(chat_id, session)

    status_emoji = "🔄" if save_action == "updated" else "✅"
    status_text = "Перевозчик обновлён" if save_action == "updated" else "Перевозчик добавлен"

    bot.send_message(
        chat_id,
        f"{status_emoji} {status_text}!\n\n"
        f"Название: {carrier_data.get('name', '—')}\n"
        f"Телефон: {carrier_data.get('phone', '—')}\n"
        f"Email: {carrier_data.get('email', '—')}"
        + (f"\n\n{save_message}" if save_message else ""),
    )

    # Предложить добавить машину и водителя
    carrier_id = session.get('carrier_id', '')
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("🚛 Добавить машину", callback_data=f"add_vehicle_to_carrier_{carrier_id}"),
        InlineKeyboardButton("👤 Добавить водителя", callback_data=f"add_driver_to_carrier_{carrier_id}"),
        InlineKeyboardButton("✅ Готово, завершить", callback_data="finish_carrier_setup")
    )
    bot.send_message(
        chat_id,
        "Хотите добавить транспорт и водителей к этому перевозчику?",
        reply_markup=markup
    )


def merge_extracted_into_carrier_data(session: Dict[str, Any], extracted: Dict[str, Any]) -> Dict[str, Any]:
    carrier_data = session.get("carrier_data") or {}

    mapped = {
        "name": extracted.get("name") or extracted.get("carrier_name", ""),
        "short_name": extracted.get("carrier_short_name", ""),
        "carrier_type": extracted.get("carrier_type", ""),
        "inn": clean_digits(extracted.get("inn", "")),
        "kpp": clean_digits(extracted.get("kpp", "")),
        "ogrn": extracted.get("ogrn", ""),
        "snils": clean_digits(extracted.get("snils", "")),
        "address": extracted.get("address") or extracted.get("registration_address", ""),
        "post_address": extracted.get("post_address", ""),
        "director": extracted.get("director", ""),
        "basis": extracted.get("basis", ""),
        "phone": normalize_phone(extracted.get("phone", "")),
        "phone2": normalize_phone(extracted.get("phone2", "")),
        "email": extracted.get("email", "").strip(),
        "emails": extracted.get("emails", "").strip(),
        "bank": extracted.get("bank", ""),
        "bank_city": extracted.get("bank_city", ""),
        "account": clean_digits(extracted.get("account") or extracted.get("rs", "")),
        "corr_account": clean_digits(extracted.get("corr_account") or extracted.get("ks", "")),
        "bik": clean_digits(extracted.get("bik", "")),
        "tax_mode": normalize_tax_mode(extracted.get("tax_mode", "")),
        "edo": extracted.get("edo", ""),
    }

    for key, value in mapped.items():
        if value:
            carrier_data[key] = value

    session["carrier_data"] = carrier_data
    return sync_session_with_carrier_data(session)


# =========================
# СКАНИРОВАНИЕ КАРТОЧЕК
# =========================

SCAN_ENTITY_FIELDS = {
    "carrier": {
        "label": "Перевозчик",
        "emoji": "🚚",
        "required": [
            "name", "inn", "carrier_type", "director",
            "address", "phone", "email",
            "bank", "rs", "ks", "bik", "tax_mode",
        ],
        "field_labels": {
            "name": "Название компании",
            "inn": "ИНН",
            "kpp": "КПП",
            "ogrn": "ОГРН / ОГРНИП",
            "snils": "СНИЛС",
            "carrier_type": "Тип (ИП / ООО / Самозанятый)",
            "director": "Директор / ФИО ИП",
            "address": "Юридический адрес",
            "phone": "Телефон",
            "email": "Email",
            "bank": "Название банка",
            "rs": "Расчётный счёт (20 цифр)",
            "ks": "Корр. счёт (20 цифр)",
            "bik": "БИК (9 цифр)",
            "tax_mode": "Налогообложение (ОСНО / УСН / Патент / Самозанятый)",
        },
    },
    "customer": {
        "label": "Заказчик",
        "emoji": "🏢",
        "required": [
            "name", "inn", "director",
            "address", "phone", "email",
            "bank", "rs", "ks", "bik",
        ],
        "field_labels": {
            "name": "Название компании",
            "inn": "ИНН",
            "kpp": "КПП",
            "ogrn": "ОГРН",
            "director": "Директор",
            "address": "Юридический адрес",
            "phone": "Телефон",
            "email": "Email",
            "bank": "Название банка",
            "rs": "Расчётный счёт (20 цифр)",
            "ks": "Корр. счёт (20 цифр)",
            "bik": "БИК (9 цифр)",
        },
    },
}


def format_scan_summary(parsed: Dict[str, Any], entity_type: str) -> str:
    """Форматировать результат сканирования в читаемый вид."""
    config = SCAN_ENTITY_FIELDS.get(entity_type, SCAN_ENTITY_FIELDS["carrier"])
    labels = config["field_labels"]
    required = config["required"]

    found_lines = []
    missing_lines = []

    for field in required:
        label = labels.get(field, field)
        value = parsed.get(field, "")
        if value:
            found_lines.append(f"  • {label}: <b>{value}</b>")
        else:
            missing_lines.append(f"  • {label}")

    # Также показываем доп. поля если они найдены
    extra_fields = ["kpp", "ogrn", "snils", "phone2"]
    for field in extra_fields:
        if field not in required:
            value = parsed.get(field, "")
            if value:
                label = labels.get(field, FIELD_LABELS.get(field, field))
                found_lines.append(f"  • {label}: <b>{value}</b>")

    parts = []
    if found_lines:
        parts.append("✅ <b>Распознано:</b>\n" + "\n".join(found_lines))
    if missing_lines:
        parts.append("❓ <b>Не найдено:</b>\n" + "\n".join(missing_lines))

    return "\n\n".join(parts)


def get_next_missing_scan_field(session: Dict[str, Any]) -> Optional[str]:
    """Получить следующее незаполненное обязательное поле для сканирования."""
    entity_type = session.get("scan_entity_type", "carrier")
    config = SCAN_ENTITY_FIELDS.get(entity_type, SCAN_ENTITY_FIELDS["carrier"])
    scan_data = session.get("scan_data", {})

    for field in config["required"]:
        if not scan_data.get(field):
            return field
    return None


def ask_scan_next_field(chat_id: int):
    """Запросить следующее незаполненное поле при сканировании."""
    session = get_session(chat_id)
    entity_type = session.get("scan_entity_type", "carrier")
    config = SCAN_ENTITY_FIELDS.get(entity_type, SCAN_ENTITY_FIELDS["carrier"])
    scan_data = session.get("scan_data", {})

    next_field = get_next_missing_scan_field(session)

    if not next_field:
        # Все поля заполнены — сохраняем
        save_scanned_entity(chat_id)
        return

    label = config["field_labels"].get(next_field, next_field)
    session["scan_waiting_for"] = next_field
    session["state"] = "scan_waiting_field"
    save_session(chat_id, session)

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("⏩ Пропустить", callback_data="scan_skip_field"))
    markup.add(InlineKeyboardButton("💾 Сохранить как есть", callback_data="scan_save_now"))
    markup.add(InlineKeyboardButton("❌ Отменить", callback_data="scan_cancel"))

    bot.send_message(
        chat_id,
        f"📝 Введите <b>{label}</b>:",
        parse_mode="HTML",
        reply_markup=markup,
    )


def save_scanned_entity(chat_id: int):
    """Сохранить отсканированные данные в Google Sheets."""
    session = get_session(chat_id)
    entity_type = session.get("scan_entity_type", "carrier")
    scan_data = session.get("scan_data", {})

    if entity_type == "carrier":
        # Используем существующий flow сохранения перевозчика
        session["carrier_data"] = {
            "name": scan_data.get("name", ""),
            "short_name": scan_data.get("name", ""),
            "carrier_type": scan_data.get("carrier_type", ""),
            "inn": clean_digits(scan_data.get("inn", "")),
            "kpp": clean_digits(scan_data.get("kpp", "")),
            "ogrn": scan_data.get("ogrn", ""),
            "snils": clean_digits(scan_data.get("snils", "")),
            "address": scan_data.get("address", ""),
            "director": scan_data.get("director", ""),
            "phone": normalize_phone(scan_data.get("phone", "")),
            "phone2": normalize_phone(scan_data.get("phone2", "")),
            "email": scan_data.get("email", ""),
            "bank": scan_data.get("bank", ""),
            "account": clean_digits(scan_data.get("rs", "")),
            "corr_account": clean_digits(scan_data.get("ks", "")),
            "bik": clean_digits(scan_data.get("bik", "")),
            "tax_mode": normalize_tax_mode(scan_data.get("tax_mode", "")),
        }
        session = sync_session_with_carrier_data(session)
        session["state"] = ""
        session["scan_mode"] = False
        save_session(chat_id, session)

        if save_carrier_to_sheets(chat_id):
            session = get_session(chat_id)
            save_action = session.get("carrier_save_action", "created")
            status_emoji = "🔄" if save_action == "updated" else "✅"
            status_text = "обновлён" if save_action == "updated" else "добавлен"
            bot.send_message(
                chat_id,
                f"{status_emoji} <b>Перевозчик {status_text}!</b>\n\n"
                f"🚚 {scan_data.get('name', '—')}\n"
                f"ИНН: {scan_data.get('inn', '—')}\n"
                f"📞 {scan_data.get('phone', '—')}\n"
                f"📧 {scan_data.get('email', '—')}",
                parse_mode="HTML",
            )
        else:
            bot.send_message(
                chat_id,
                "❌ Не удалось сохранить перевозчика. Попробуйте ещё раз позже.",
            )
        clear_scan_state(chat_id)

    elif entity_type == "customer":
        # Сохранение заказчика через Apps Script
        payload = {
            "action": "create_customer",
            "customer_data": {
                "name": scan_data.get("name", ""),
                "inn": clean_digits(scan_data.get("inn", "")),
                "kpp": clean_digits(scan_data.get("kpp", "")),
                "ogrn": scan_data.get("ogrn", ""),
                "director": scan_data.get("director", ""),
                "address": scan_data.get("address", ""),
                "phone": normalize_phone(scan_data.get("phone", "")),
                "email": scan_data.get("email", ""),
                "bank": scan_data.get("bank", ""),
                "rs": clean_digits(scan_data.get("rs", "")),
                "ks": clean_digits(scan_data.get("ks", "")),
                "bik": clean_digits(scan_data.get("bik", "")),
            },
        }

        data, error = call_google_script(payload)
        if error:
            bot.send_message(chat_id, f"❌ Ошибка сохранения заказчика: {error}")
        elif data.get("success") or (data.get("ok") and isinstance(data.get("result"), dict) and data["result"].get("success")):
            bot.send_message(
                chat_id,
                f"✅ <b>Заказчик добавлен!</b>\n\n"
                f"🏢 {scan_data.get('name', '—')}\n"
                f"ИНН: {scan_data.get('inn', '—')}\n"
                f"📞 {scan_data.get('phone', '—')}\n"
                f"📧 {scan_data.get('email', '—')}",
                parse_mode="HTML",
            )
            # Обновляем кэш заказчиков
            get_customers_list(force_refresh=True)
        else:
            error_msg = data.get("error", "Неизвестная ошибка")
            bot.send_message(chat_id, f"❌ Ошибка сохранения заказчика: {error_msg}")

        clear_scan_state(chat_id)


def clear_scan_state(chat_id: int):
    """Очистить состояние сканирования."""
    session = get_session(chat_id)
    for key in ["scan_mode", "scan_entity_type", "scan_data", "scan_waiting_for"]:
        session.pop(key, None)
    if session.get("state", "").startswith("scan_"):
        session["state"] = ""
    save_session(chat_id, session)


def process_scan_photo(chat_id: int, file_id: str):
    """Обработать фото в режиме сканирования."""
    bot.send_message(chat_id, "📸 Обрабатываю изображение...")

    image_bytes, download_error = download_telegram_file(file_id)
    if download_error:
        bot.send_message(chat_id, f"❌ Не удалось скачать фото: {download_error}")
        return

    # Распознавание через OpenAI Vision (основной метод)
    extracted, extract_error = extract_card_data_from_image(image_bytes)
    if extract_error:
        bot.send_message(chat_id, f"❌ Ошибка распознавания: {extract_error}")
        return

    # Нормализуем данные из Vision в плоский формат
    scan_data = {
        "name": extracted.get("name") or extracted.get("carrier_name") or "",
        "carrier_type": extracted.get("carrier_type") or "",
        "inn": clean_digits(extracted.get("inn") or ""),
        "kpp": clean_digits(extracted.get("kpp") or ""),
        "ogrn": extracted.get("ogrn") or "",
        "snils": clean_digits(extracted.get("snils") or ""),
        "director": extracted.get("director") or "",
        "address": extracted.get("address") or extracted.get("registration_address") or "",
        "phone": normalize_phone(extracted.get("phone") or ""),
        "phone2": normalize_phone(extracted.get("phone2") or ""),
        "email": (extracted.get("email") or "").strip(),
        "bank": extracted.get("bank") or "",
        "rs": clean_digits(extracted.get("rs") or extracted.get("account") or ""),
        "ks": clean_digits(extracted.get("ks") or extracted.get("corr_account") or ""),
        "bik": clean_digits(extracted.get("bik") or ""),
        "tax_mode": normalize_tax_mode(extracted.get("tax_mode") or ""),
    }

    # Если есть ИНН — обогащаем через DaData
    if scan_data.get("inn") and validate_inn(scan_data["inn"]):
        company, dadata_error = get_company_by_inn(scan_data["inn"])
        if not dadata_error and company:
            if company.get("name") and not scan_data.get("name"):
                scan_data["name"] = company["name"]
            if company.get("address") and not scan_data.get("address"):
                scan_data["address"] = company["address"]
            if company.get("ogrn") and not scan_data.get("ogrn"):
                scan_data["ogrn"] = company["ogrn"]
            if company.get("carrier_type") and not scan_data.get("carrier_type"):
                scan_data["carrier_type"] = company["carrier_type"]
            if company.get("director") and not scan_data.get("director"):
                scan_data["director"] = company["director"]

    session = get_session(chat_id)
    session["scan_data"] = scan_data
    session["state"] = "scan_choose_type"
    save_session(chat_id, session)

    # Формируем превью (пока без entity_type, покажем нейтрально)
    found_lines = []
    for field, label in [
        ("name", "Название"), ("inn", "ИНН"), ("kpp", "КПП"),
        ("ogrn", "ОГРН"), ("carrier_type", "Тип"),
        ("director", "Директор"), ("address", "Адрес"),
        ("phone", "Телефон"), ("email", "Email"),
        ("bank", "Банк"), ("rs", "Р/с"), ("ks", "К/с"), ("bik", "БИК"),
        ("tax_mode", "Налогообложение"),
    ]:
        value = scan_data.get(field, "")
        if value:
            found_lines.append(f"  • {label}: <b>{value}</b>")

    response = "📋 <b>Результат распознавания:</b>\n\n"
    if found_lines:
        response += "\n".join(found_lines)
    else:
        response += "⚠️ Не удалось распознать данные. Попробуйте фото получше."

    response += "\n\n<b>Это перевозчик или заказчик?</b>"

    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton("🚚 Перевозчик", callback_data="scan_type_carrier"),
        InlineKeyboardButton("🏢 Заказчик", callback_data="scan_type_customer"),
    )
    markup.add(InlineKeyboardButton("❌ Отменить", callback_data="scan_cancel"))

    bot.send_message(chat_id, response, parse_mode="HTML", reply_markup=markup)


# =========================
# TELEGRAM
# =========================


@bot.message_handler(commands=["start"])
def handle_start(message):
    bot.send_message(
        message.chat.id,
        "Бот запущен. Выберите действие:",
        reply_markup=get_main_keyboard()
    )


@bot.message_handler(func=lambda m: m.text == "🏠 Главное меню")
def handle_btn_main_menu(message):
    clear_session(message.chat.id)
    bot.send_message(message.chat.id, "🏠 Главное меню", reply_markup=get_main_keyboard())


@bot.message_handler(func=lambda m: m.text == "🚛 Новый перевозчик")
def handle_btn_new_carrier(message):
    show_carrier_add_options(message)


@bot.message_handler(func=lambda m: m.text == "📋 Новый договор")
def handle_btn_new_contract(message):
    cmd_make_contract(message)


@bot.message_handler(func=lambda m: m.text == "📦 Новая заявка")
def handle_btn_new_request(message):
    start_new_trip_request_fsm(message.chat.id)


@bot.message_handler(func=lambda m: m.text == "📄 Мои заявки")
def handle_btn_my_requests(message):
    bot.send_message(message.chat.id, "📄 Функция просмотра заявок в разработке. Скоро будет доступна!")


@bot.message_handler(func=lambda m: m.text == "🚗 Добавить машину")
def handle_btn_add_vehicle(message):
    start_add_vehicle_flow(message.chat.id)


@bot.message_handler(func=lambda m: m.text == "👤 Добавить водителя")
def handle_btn_add_driver(message):
    start_add_driver(message)


@bot.message_handler(func=lambda m: m.text == "👥 Перевозчики")
def handle_btn_carriers(message):
    carriers = get_carriers_list()
    if not carriers:
        bot.send_message(message.chat.id, "❌ Перевозчиков пока нет в базе.")
        return
    lines = ["📋 Перевозчики в базе:\n"]
    for c in carriers:
        lines.append(f"• {c.get('name','—')} | ИНН: {c.get('inn','—')}")
    bot.send_message(message.chat.id, "\n".join(lines))


@bot.message_handler(func=lambda m: m.text == "❓ Помощь")
def handle_btn_help(message):
    bot.send_message(message.chat.id,
        "❓ Помощь:\n\n"
        "🚛 Новый перевозчик — добавить перевозчика в базу\n"
        "📋 Новый договор — создать договор перевозки\n"
        "📦 Новая заявка — создать заявку на рейс\n"
        "🚗 Добавить машину — добавить ТС к перевозчику\n"
        "👤 Добавить водителя — добавить водителя\n"
        "👥 Перевозчики — список всех перевозчиков\n\n"
        "По вопросам: @ваш_контакт"
    )


@bot.callback_query_handler(func=lambda call: call.data == "show_forms")
def callback_show_forms(call):
    """Обработчик callback для кнопки 'Показать формы'."""
    send_forms_inline(call.message.chat.id)
    bot.answer_callback_query(call.id)


@bot.message_handler(commands=["reset", "clear"])
def handle_reset(message):
    clear_session(message.chat.id)
    bot.send_message(message.chat.id, "Сессия очищена.")


# =========================
# КОМАНДЫ СКАНИРОВАНИЯ
# =========================


@bot.message_handler(commands=["scan", "сканировать"])
def cmd_start_scanning(message):
    """Запустить режим сканирования карточки предприятия."""
    chat_id = message.chat.id
    logger.info("/сканировать от chat_id=%s", chat_id)

    session = get_session(chat_id)
    session["scan_mode"] = True
    session["state"] = "scan_waiting_photo"
    save_session(chat_id, session)

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("❌ Отменить", callback_data="scan_cancel"))

    bot.send_message(
        chat_id,
        "📸 <b>Режим сканирования карточки предприятия</b>\n\n"
        "Отправьте фото карточки, визитки или документа с реквизитами.\n\n"
        "Я распознаю данные:\n"
        "• ИНН, КПП, ОГРН\n"
        "• Название, директор, адрес\n"
        "• Телефон, email\n"
        "• Банковские реквизиты (р/с, к/с, БИК)\n\n"
        "Также можно отправить <b>PDF</b> или <b>DOCX</b> файл.\n\n"
        "Для отмены: /отмена",
        parse_mode="HTML",
        reply_markup=markup,
    )


@bot.message_handler(commands=["cancel", "отмена"])
def cmd_cancel(message):
    """Отменить текущую операцию."""
    chat_id = message.chat.id
    session = get_session(chat_id)

    was_scanning = session.get("scan_mode", False)
    clear_scan_state(chat_id)

    if was_scanning:
        bot.send_message(chat_id, "❌ Сканирование отменено.")
    else:
        state = session.get("state", "")
        if state:
            session["state"] = ""
            save_session(chat_id, session)
            bot.send_message(chat_id, "❌ Операция отменена.")
        else:
            bot.send_message(chat_id, "Нечего отменять.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("scan_type_"))
def callback_scan_type_selection(call):
    """Обработка выбора типа сущности при сканировании."""
    chat_id = call.message.chat.id
    entity_type = "carrier" if call.data == "scan_type_carrier" else "customer"

    session = get_session(chat_id)
    session["scan_entity_type"] = entity_type
    session["state"] = "scan_waiting_field"
    save_session(chat_id, session)

    config = SCAN_ENTITY_FIELDS[entity_type]
    bot.answer_callback_query(call.id, f"Выбран: {config['label']}")

    # Показываем сводку с учётом типа
    summary = format_scan_summary(session.get("scan_data", {}), entity_type)

    bot.send_message(
        chat_id,
        f"{config['emoji']} <b>{config['label']}</b>\n\n{summary}",
        parse_mode="HTML",
    )

    # Начинаем спрашивать недостающие поля
    ask_scan_next_field(chat_id)


@bot.callback_query_handler(func=lambda call: call.data == "scan_skip_field")
def callback_scan_skip_field(call):
    """Пропустить текущее поле при сканировании."""
    chat_id = call.message.chat.id
    session = get_session(chat_id)

    current_field = session.get("scan_waiting_for", "")
    if current_field:
        # Отмечаем поле как пропущенное, ставим пустую строку чтобы не спрашивать снова
        scan_data = session.get("scan_data", {})
        scan_data[current_field] = "__skipped__"
        session["scan_data"] = scan_data
        save_session(chat_id, session)

    bot.answer_callback_query(call.id, "Пропущено")
    ask_scan_next_field(chat_id)


@bot.callback_query_handler(func=lambda call: call.data == "scan_save_now")
def callback_scan_save_now(call):
    """Сохранить данные как есть, не дожидаясь заполнения всех полей."""
    chat_id = call.message.chat.id
    bot.answer_callback_query(call.id, "Сохраняю...")

    # Очищаем маркеры пропущенных полей
    session = get_session(chat_id)
    scan_data = session.get("scan_data", {})
    for key, value in list(scan_data.items()):
        if value == "__skipped__":
            scan_data[key] = ""
    session["scan_data"] = scan_data
    save_session(chat_id, session)

    save_scanned_entity(chat_id)


@bot.callback_query_handler(func=lambda call: call.data == "scan_cancel")
def callback_scan_cancel(call):
    """Отменить сканирование."""
    chat_id = call.message.chat.id
    clear_scan_state(chat_id)
    bot.answer_callback_query(call.id, "Отменено")
    bot.send_message(chat_id, "❌ Сканирование отменено.")


# =========================
# GOOGLE FORMS — ССЫЛКИ
# =========================

GOOGLE_FORMS = {
    "перевозчик": {
        "name": "перевозчика",
        "url": "https://docs.google.com/forms/d/e/1FAIpQLScDvy8kPFMMNrXCh6nuXjHv6z6dlzM2yQLMWasAnDR4NO2gNQ/viewform",
        "sheet": "Перевозчики",
    },
    "заказчик": {
        "name": "заказчика",
        "url": "https://docs.google.com/forms/d/e/1FAIpQLSfpbbo9dVF0tR55QuRnLJiJb0Qh0RKdQwvTd7kO6MLl36nhug/viewform",
        "sheet": "Заказчики",
    },
    "водитель": {
        "name": "водителя",
        "url": "https://docs.google.com/forms/d/e/1FAIpQLSeXTVenX8lt4pto2bpZSvNuFqX6OLurMB2OfRTXwrlrz6QUFw/viewform",
        "sheet": "Водители",
    },
    "машина": {
        "name": "машину",
        "url": "https://docs.google.com/forms/d/e/1FAIpQLSd6gGu1VPtj1XumDhk59ZXdBls_nfCwZV--Iz2GQxRpcQhw1A/viewform",
        "sheet": "Машины",
    },
    "прицеп": {
        "name": "прицеп",
        "url": "https://docs.google.com/forms/d/e/1FAIpQLSfdjzsdaj0XnrKDd6r1G8AYnWPBCw02Eby1IfxFKkBgl2dA3w/viewform",
        "sheet": "Прицепы",
    },
}


def _send_form_link(chat_id: int, entity_key: str):
    """Отправляет inline кнопку с ссылкой на Google Form."""
    info = GOOGLE_FORMS[entity_key]
    markup = InlineKeyboardMarkup()
    btn = InlineKeyboardButton(
        f"📋 Открыть форму ({info['name']})",
        url=info["url"],
    )
    markup.add(btn)
    bot.send_message(
        chat_id,
        f"📝 <b>Форма добавления {info['name']}</b>\n\n"
        f"Нажмите кнопку ниже, чтобы открыть форму.\n"
        f"После отправки данные автоматически добавятся в базу (лист «{info['sheet']}»).",
        parse_mode="HTML",
        reply_markup=markup,
    )


@bot.message_handler(commands=["добавить_перевозчика"])
def cmd_form_carrier(message):
    logger.info("/добавить_перевозчика от chat_id=%s", message.chat.id)
    _send_form_link(message.chat.id, "перевозчик")


@bot.message_handler(commands=["добавить_заказчика"])
def cmd_form_customer(message):
    logger.info("/добавить_заказчика от chat_id=%s", message.chat.id)
    _send_form_link(message.chat.id, "заказчик")


@bot.message_handler(commands=["добавить_водителя"])
def cmd_form_driver(message):
    logger.info("/добавить_водителя от chat_id=%s", message.chat.id)
    _send_form_link(message.chat.id, "водитель")


@bot.message_handler(commands=["добавить_машину"])
def cmd_form_vehicle(message):
    logger.info("/добавить_машину от chat_id=%s", message.chat.id)
    _send_form_link(message.chat.id, "машина")


@bot.message_handler(commands=["добавить_прицеп"])
def cmd_form_trailer(message):
    logger.info("/добавить_прицеп от chat_id=%s", message.chat.id)
    _send_form_link(message.chat.id, "прицеп")


@bot.message_handler(commands=["формы"])
def cmd_all_forms(message):
    """Показать список всех форм с inline кнопками."""
    logger.info("/формы от chat_id=%s", message.chat.id)
    send_forms_inline(message.chat.id)


def send_forms_inline(chat_id: int):
    """Отправить inline клавиатуру со всеми формами."""
    markup = InlineKeyboardMarkup(row_width=1)

    btn1 = InlineKeyboardButton(
        "📋 Добавить перевозчика",
        url=GOOGLE_FORMS["перевозчик"]["url"],
    )
    btn2 = InlineKeyboardButton(
        "🏢 Добавить заказчика",
        url=GOOGLE_FORMS["заказчик"]["url"],
    )
    btn3 = InlineKeyboardButton(
        "👤 Добавить водителя",
        url=GOOGLE_FORMS["водитель"]["url"],
    )
    btn4 = InlineKeyboardButton(
        "🚛 Добавить машину",
        url=GOOGLE_FORMS["машина"]["url"],
    )
    btn5 = InlineKeyboardButton(
        "🚚 Добавить прицеп",
        url=GOOGLE_FORMS["прицеп"]["url"],
    )

    markup.add(btn1, btn2, btn3, btn4, btn5)

    bot.send_message(
        chat_id,
        "📝 <b>Формы для добавления данных в CRM:</b>\n\nВыберите нужную форму:",
        parse_mode="HTML",
        reply_markup=markup,
    )


@bot.message_handler(commands=["refresh_carriers"])
def handle_refresh_carriers(message):
    chat_id = message.chat.id
    session = get_session(chat_id)
    session["awaiting_carrier_card_upload"] = False
    session["awaiting_carrier_inn"] = False
    session["state"] = ""
    save_session(chat_id, session)

    bot.send_message(
        chat_id,
        "🔄 Обновил контекст по перевозчикам.\n"
        "Если вы уже заполнили Google Форму, продолжите командой вида:\n"
        "`Сделай договор новый перевозчик ИНН ...`\n"
        "или выберите способ добавления ниже.",
        parse_mode="Markdown",
        reply_markup=build_add_carrier_markup(),
    )


@bot.message_handler(commands=["add_vehicle"])
def cmd_add_vehicle(message):
    """Команда запуска сценария 'Добавить машину'."""
    logger.info("/add_vehicle от chat_id=%s", message.chat.id)
    start_add_vehicle_flow(message.chat.id)


@bot.message_handler(commands=["договор", "make_contract"])
def cmd_make_contract(message):
    """Команда для генерации договора с перевозчиком."""
    chat_id = message.chat.id

    logger.info("Команда /договор от пользователя %s", chat_id)

    carriers = get_carriers_list()

    logger.info("Получено перевозчиков: %s", len(carriers) if carriers else 0)

    if not carriers:
        logger.warning("Список перевозчиков пустой")
        bot.send_message(
            chat_id,
            "❌ Сначала добавьте перевозчика!\n\nИспользуйте команду для добавления перевозчика.",
        )
        return

    markup = InlineKeyboardMarkup()
    for carrier in carriers:
        carrier_id = str(carrier.get("id", "")).strip()
        carrier_name = str(carrier.get("name", "")).strip() or f"ID {carrier_id}"

        if not carrier_id:
            continue

        logger.info("Добавляю кнопку: %s (ID: %s)", carrier_name, carrier_id)
        btn = InlineKeyboardButton(
            text=carrier_name,
            callback_data=f"contract_carrier_{carrier_id}",
        )
        markup.add(btn)

    bot.send_message(
        chat_id,
        "📄 Генерация договора с перевозчиком\n\n"
        "Выберите перевозчика:",
        reply_markup=markup,
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("contract_carrier_"))
def handle_contract_carrier_select(call):
    """Обработка выбора перевозчика для договора - показываем заказчиков."""
    chat_id = call.message.chat.id
    carrier_id = call.data.replace("contract_carrier_", "", 1)

    bot.answer_callback_query(call.id)
    
    # Сохраняем выбранного перевозчика в сессии
    session = get_session(chat_id)
    session["contract_carrier_id"] = carrier_id
    session["contract_carrier_name"] = get_carrier_name_by_id(carrier_id)
    save_session(chat_id, session)
    
    logger.info("Перевозчик выбран: %s (ID: %s)", session.get("contract_carrier_name"), carrier_id)
    
    # Получаем список заказчиков
    customers = get_customers_for_contract()
    
    if not customers:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="❌ Заказчики не найдены.\n\nОбратитесь к администратору для добавления заказчиков.",
        )
        return
    
    # Показываем кнопки с заказчиками
    markup = InlineKeyboardMarkup()
    for customer in customers:
        customer_id = str(customer.get("id", "")).strip()
        customer_name = str(customer.get("name", "")).strip() or f"ID {customer_id}"
        
        if not customer_id:
            continue
        
        logger.info("Добавляю кнопку заказчика: %s (ID: %s)", customer_name, customer_id)
        btn = InlineKeyboardButton(
            text=customer_name,
            callback_data=f"contract_customer_{customer_id}",
        )
        markup.add(btn)
    
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=call.message.message_id,
        text=f"✅ Перевозчик: {session.get('contract_carrier_name')}\n\n"
             f"📋 Выберите заказчика:",
        reply_markup=markup,
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("contract_customer_"))
def handle_contract_customer_select(call):
    """Обработка выбора заказчика - генерируем договор."""
    chat_id = call.message.chat.id
    customer_id = call.data.replace("contract_customer_", "", 1)
    
    bot.answer_callback_query(call.id)
    
    # Получаем сохраненные данные
    session = get_session(chat_id)
    carrier_id = session.get("contract_carrier_id")
    
    if not carrier_id:
        bot.send_message(chat_id, "❌ Ошибка: перевозчик не выбран. Начните заново с команды /договор")
        return
    
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=call.message.message_id,
        text="⏳ Генерирую договор...\n\nЭто может занять несколько секунд.",
    )
    
    # Генерируем договор с указанием customer_id
    result = generate_carrier_contract(carrier_id, customer_id)

    if result.get("success"):
        doc_url = result.get("url")
        contract_number = result.get("contract_number")

        markup = InlineKeyboardMarkup()
        if doc_url:
            markup.add(InlineKeyboardButton("📄 Открыть договор", url=doc_url))

        bot.send_message(
            chat_id,
            f"✅ Договор сгенерирован!\n\n"
            f"📋 Номер: {contract_number}\n"
            f"📅 Дата: {result.get('date')}\n\n"
            f"Нажмите кнопку ниже чтобы открыть документ:",
            reply_markup=markup if doc_url else None,
        )
        
        # Очищаем сессию
        clear_session(chat_id)
    else:
        error_msg = result.get("error", "Неизвестная ошибка")
        bot.send_message(
            chat_id,
            f"❌ Ошибка генерации договора:\n{error_msg}",
        )


@bot.callback_query_handler(func=lambda call: call.data.startswith("select_customer_"))
def handle_customer_selection(call):
    chat_id = call.message.chat.id
    customer_code = call.data.replace("select_customer_", "", 1)

    session = get_session(chat_id)
    customer = get_customer_by_code(customer_code)

    if not customer:
        for item in get_customers_list(force_refresh=True):
            if item.get("code", "").upper() == customer_code.upper():
                customer = item
                break

    if not customer:
        bot.answer_callback_query(call.id, "Не удалось найти заказчика, обновляю список")
        show_customer_selection(chat_id, force_refresh=True)
        return

    session["customer_name"] = customer.get("name", "")
    session["customer_code"] = customer.get("code", "")
    session["customer_data"] = customer
    session["awaiting_customer_inn"] = False
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, f"Выбран: {customer.get('name', 'заказчик')}")
    bot.send_message(chat_id, f"✅ Заказчик выбран: {customer.get('name', '—')}")

    if session.get("scenario") == "new_carrier_contract" and session.get("awaiting_more_data"):
        prompt_for_missing_after_customer(chat_id, session)


@bot.callback_query_handler(func=lambda call: call.data == "add_new_customer")
def handle_add_customer(call):
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "Как добавить заказчика?", reply_markup=build_add_customer_markup())


@bot.callback_query_handler(func=lambda call: call.data == "upload_card")
def handle_upload_card(call):
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        "Отправьте карточку заказчика (фото или файл), я попробую извлечь реквизиты."
    )


@bot.callback_query_handler(func=lambda call: call.data == "enter_inn_customer")
def handle_enter_inn_customer(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["awaiting_customer_inn"] = True
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(chat_id, "Введите ИНН заказчика (10 или 12 цифр).")


@bot.callback_query_handler(func=lambda call: call.data == "carrier_enter_inn")
def handle_carrier_inn_entry(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["scenario"] = "new_carrier_contract"
    session["awaiting_more_data"] = True
    session["awaiting_carrier_inn"] = True
    session["awaiting_carrier_card_upload"] = False
    session["awaiting_carrier_duplicate_decision"] = False
    session.pop("pending_carrier_data", None)
    session.pop("existing_carrier", None)
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "⌨️ Введите ИНН перевозчика для автозаполнения данных из DaData:",
    )


@bot.callback_query_handler(func=lambda call: call.data == "carrier_upload_card")
def handle_carrier_card_upload(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["scenario"] = "new_carrier_contract"
    session["awaiting_more_data"] = True
    session["awaiting_carrier_card_upload"] = True
    session["awaiting_carrier_inn"] = False
    session["awaiting_carrier_duplicate_decision"] = False
    session.pop("pending_carrier_data", None)
    session.pop("existing_carrier", None)
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "📄 Загрузите карточку компании в формате DOCX, PDF или фото.\n\n"
        "Я автоматически извлеку все реквизиты.",
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("view_carrier_"))
def handle_view_existing_carrier(call):
    chat_id = call.message.chat.id
    carrier_id = call.data.replace("view_carrier_", "", 1)
    session = get_session(chat_id)
    existing = session.get("existing_carrier") or {}

    if not existing or str(existing.get("id", "")) != carrier_id:
        bot.answer_callback_query(call.id)
        bot.send_message(chat_id, "Не удалось найти данные перевозчика в сессии. Введите ИНН ещё раз.")
        return

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        f"📋 Текущая карточка перевозчика:\n"
        f"• ID: {existing.get('id', '—')}\n"
        f"• Название: {existing.get('name', '—')}\n"
        f"• ИНН: {existing.get('inn', '—')}\n"
        f"• Телефон: {existing.get('phone', '—') or '—'}\n"
        f"• Email: {existing.get('email', '—') or '—'}\n"
        f"• Банк: {existing.get('bank', '—') or '—'}\n"
        f"• Р/с: {existing.get('rs', '—') or '—'}\n"
        f"• БИК: {existing.get('bik', '—') or '—'}\n"
        f"• К/с: {existing.get('ks', '—') or '—'}\n"
        f"• Налоговый режим: {existing.get('tax_mode', '—') or '—'}",
        reply_markup=build_existing_carrier_actions(carrier_id),
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("update_carrier_"))
def handle_update_existing_carrier(call):
    chat_id = call.message.chat.id
    carrier_id = call.data.replace("update_carrier_", "", 1)
    session = get_session(chat_id)
    pending = session.get("pending_carrier_data") or {}
    existing = session.get("existing_carrier") or {}

    if not existing or str(existing.get("id", "")) != carrier_id:
        bot.answer_callback_query(call.id)
        bot.send_message(chat_id, "Не удалось запустить обновление. Повторите ввод ИНН.")
        return

    merged_carrier_data = {
        "id": existing.get("id", ""),
        "name": pending.get("name") or existing.get("name", ""),
        "carrier_type": pending.get("carrier_type") or existing.get("carrier_type", ""),
        "inn": pending.get("inn") or existing.get("inn", ""),
        "ogrn": pending.get("ogrn") or existing.get("ogrn", ""),
        "address": pending.get("address") or existing.get("registration_address", ""),
        "director": pending.get("director") or existing.get("director", ""),
        "phone": existing.get("phone", ""),
        "email": existing.get("email", ""),
        "bank": existing.get("bank", ""),
        "account": existing.get("rs", ""),
        "bik": existing.get("bik", ""),
        "corr_account": existing.get("ks", ""),
        "tax_mode": pending.get("tax_mode") or existing.get("tax_mode", ""),
    }

    session["carrier_data"] = merged_carrier_data
    session["awaiting_carrier_duplicate_decision"] = False
    session["awaiting_carrier_inn"] = False
    session["awaiting_carrier_card_upload"] = False
    session["awaiting_more_data"] = False
    session["state"] = "waiting_carrier_flexible_input"
    session = sync_session_with_carrier_data(session)
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, "Запускаю обновление")
    bot.send_message(
        chat_id,
        "🔄 Режим обновления существующего перевозчика.\n\n"
        "Пришлите новые данные в свободном виде (телефон/email/банк/реквизиты)\n"
        "или загрузите карточку предприятия.",
        reply_markup=build_dadata_followup_markup(),
    )


@bot.callback_query_handler(func=lambda call: call.data == "cancel_carrier")
def handle_cancel_existing_carrier(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["awaiting_carrier_duplicate_decision"] = False
    session["awaiting_carrier_inn"] = False
    session["awaiting_carrier_card_upload"] = False
    session.pop("pending_carrier_data", None)
    session.pop("existing_carrier", None)
    session["state"] = ""
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, "Отменено")
    bot.send_message(chat_id, "❌ Операция с перевозчиком отменена.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("vehicle_carrier_"))
def handle_vehicle_carrier_select(call):
    chat_id = call.message.chat.id
    carrier_id = call.data.split("_", 2)[-1]
    logger.info("handle_vehicle_carrier_select: chat_id=%s carrier_id=%s", chat_id, carrier_id)

    session = get_session(chat_id)
    session["vehicle_carrier_id"] = carrier_id
    session["state"] = "waiting_sts_upload"
    save_session(chat_id, session)

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📸 Загрузить СТС", callback_data="upload_sts"))
    markup.add(InlineKeyboardButton("✏️ Ввести данные текстом", callback_data="text_vehicle_input"))
    markup.add(InlineKeyboardButton("📝 Заполнить форму вручную", callback_data="manual_vehicle_form"))

    carrier_name = get_carrier_name_by_id(carrier_id) or f"ID {carrier_id}"

    bot.answer_callback_query(call.id, "Перевозчик выбран")
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=call.message.message_id,
        text=(
            "✅ Перевозчик выбран!\n"
            f"Перевозчик: {carrier_name}\n\n"
            "Как добавить машину?"
        ),
        reply_markup=markup,
    )


@bot.callback_query_handler(func=lambda call: call.data == "upload_sts")
def handle_upload_sts_button(call):
    chat_id = call.message.chat.id
    logger.info("handle_upload_sts_button: chat_id=%s", chat_id)

    session = get_session(chat_id)
    session["state"] = "waiting_sts_photo"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "📸 Загрузите фото СТС (свидетельство о регистрации)\n\n"
        "Я распознаю:\n"
        "✅ Госномер\n"
        "✅ Марка и модель\n"
        "✅ VIN\n"
        "✅ Год выпуска",
    )


@bot.callback_query_handler(func=lambda call: call.data == "text_vehicle_input")
def handle_text_vehicle_input(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_sts_photo"
    save_session(chat_id, session)
    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "✏️ Введите данные машины текстом в любом формате.\n\n"
        "Например:\n"
        "Госномер: К 494 НС 138\n"
        "Марка: Мерседес\n"
        "Прицеп: ОА 9708 24"
    )


@bot.callback_query_handler(func=lambda call: call.data == "manual_vehicle_form")
def handle_manual_vehicle_form(call):
    chat_id = call.message.chat.id
    logger.info("handle_manual_vehicle_form: chat_id=%s", chat_id)

    session = get_session(chat_id)
    session["state"] = ""
    session.pop("vehicle_data", None)
    save_session(chat_id, session)

    prefill_url = generate_vehicle_prefill_url(chat_id)

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📝 Открыть форму", url=prefill_url))

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "📝 Откройте форму и заполните данные машины вручную.\n"
        "Перевозчик уже подставлен автоматически.",
        reply_markup=markup,
    )


@bot.callback_query_handler(func=lambda call: call.data == "save_vehicle")
def handle_save_vehicle(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    vehicle_data = session.get("vehicle_data", {})
    carrier_id = session.get("vehicle_carrier_id", "")
    payload = {
        "action": "save_vehicle",
        "carrier_id": carrier_id,
        "vehicle_data": vehicle_data
    }
    data, error = call_google_script(payload)
    if error:
        bot.send_message(chat_id, f"❌ Ошибка: {error}")
        return
    bot.answer_callback_query(call.id)
    bot.send_message(chat_id, "✅ Машина сохранена!", reply_markup=get_main_keyboard())


@bot.callback_query_handler(func=lambda call: call.data == "vehicle_manual_entry")
def handle_vehicle_manual_entry(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["awaiting_vehicle_manual_entry"] = True
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "✏️ Введите данные машины одним сообщением в формате:\n"
        "Перевозчик: <название или ID>\n"
        "Марка: ...\nМодель: ...\nГосномер: ...\n"
        "Паллет: ...\nТонн: ...\nТемп режим: ...\n\n"
        "Или используйте Google Форму для быстрого добавления.",
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("add_vehicle_to_carrier_"))
def handle_add_vehicle_to_carrier(call):
    """Обработчик кнопки 'Добавить машину к перевозчику'."""
    chat_id = call.message.chat.id
    carrier_id = call.data.split("_")[-1]

    session = get_session(chat_id)
    session["vehicle_carrier_id"] = carrier_id
    session["state"] = "waiting_vehicle_method"
    session.pop("vehicle_data", None)
    session.pop("trailer_data", None)
    save_session(chat_id, session)

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📸 Сканировать СТС", callback_data="vehicle_scan_sts"))
    markup.add(InlineKeyboardButton("✏️ Ввести вручную", callback_data="vehicle_manual_input"))

    bot.answer_callback_query(call.id)
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=call.message.message_id,
        text="Как добавить машину?\n\n📸 Сканировать СТС — я распознаю госномер, марку, модель, VIN\n✏️ Ввести вручную — напишите данные текстом",
        reply_markup=markup
    )


@bot.callback_query_handler(func=lambda call: call.data == "vehicle_scan_sts")
def handle_vehicle_scan_sts(call):
    """Обработчик кнопки 'Сканировать СТС' (новый flow)."""
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_vehicle_sts_photo"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "📸 Отправьте фото СТС (свидетельство о регистрации транспортного средства)\n\n"
        "Я распознаю:\n"
        "✅ Госномер\n"
        "✅ Марка и модель\n"
        "✅ VIN\n"
        "✅ Год выпуска"
    )


@bot.callback_query_handler(func=lambda call: call.data == "vehicle_manual_input")
def handle_vehicle_manual_input(call):
    """Обработчик кнопки 'Ввести вручную' (новый flow)."""
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_vehicle_manual_data"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "✏️ Введите данные машины:\n\n"
        "Госномер: ...\n"
        "Марка: ...\n"
        "Модель: ...\n"
        "(опционально VIN и год)"
    )


@bot.callback_query_handler(func=lambda call: call.data == "cancel_vehicle_add")
def handle_cancel_vehicle_add(call):
    """Обработчик кнопки 'Готово' / 'Нет, завершить'."""
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = ""
    session.pop("vehicle_data", None)
    session.pop("trailer_data", None)
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=call.message.message_id,
        text="✅ Готово! Используйте /menu для дальнейших действий."
    )


def ask_missing_driver_fields(chat_id: int):
    """Дозапрос недостающих полей водителя."""
    session = get_session(chat_id)
    driver_data = session.get("driver_data", {})

    # Проверка обязательных полей
    if not driver_data.get("phone"):
        session["state"] = "waiting_driver_phone"
        save_session(chat_id, session)
        bot.send_message(chat_id, "📞 Введите телефон водителя:")
        return

    if not driver_data.get("passport_number"):
        session["state"] = "waiting_driver_passport_number"
        save_session(chat_id, session)
        bot.send_message(chat_id, "🆔 Введите серию и номер паспорта (например: 25 08 123456):")
        return

    if not driver_data.get("license_number"):
        session["state"] = "waiting_driver_license_number"
        save_session(chat_id, session)
        bot.send_message(chat_id, "🚗 Введите номер водительского удостоверения:")
        return

    # Медкнижка
    session["state"] = "waiting_driver_medical_book"
    save_session(chat_id, session)
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Есть", callback_data="driver_medical_yes"))
    markup.add(InlineKeyboardButton("❌ Нет", callback_data="driver_medical_no"))
    bot.send_message(chat_id, "🏥 Есть ли медицинская книжка?", reply_markup=markup)


def select_vehicle_for_driver(chat_id: int):
    """Выбор машины для привязки водителя."""
    session = get_session(chat_id)
    carrier_id = session.get("driver_carrier_id")

    if not carrier_id:
        bot.send_message(chat_id, "❌ Ошибка: не найден ID перевозчика")
        return

    # Запросить список машин через Apps Script
    url = os.getenv("GOOGLE_SCRIPT_URL")
    payload = {
        "action": "get_carrier_vehicles",
        "carrier_id": carrier_id
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data, _ = safe_json_loads(response.text)
        result = data.get("result", data)

        if not result.get("success"):
            bot.send_message(chat_id, "❌ Не удалось получить список машин")
            return

        vehicles = result.get("vehicles", [])

        if not vehicles:
            bot.send_message(chat_id, "У этого перевозчика пока нет машин. Сохраняю водителя без привязки к машине.")
            save_driver_to_sheets(chat_id, vehicle_id=None)
            return

        markup = InlineKeyboardMarkup(row_width=1)
        for v in vehicles:
            plate = v.get("plate", "")
            brand = v.get("brand", "")
            model = v.get("model", "")
            vehicle_id = v.get("vehicle_id", "")
            text = f"{plate} — {brand} {model}"
            markup.add(InlineKeyboardButton(text, callback_data=f"assign_driver_vehicle_{vehicle_id}"))

        markup.add(InlineKeyboardButton("⏭ Пропустить (без машины)", callback_data="assign_driver_vehicle_none"))

        bot.send_message(chat_id, "🚛 Выберите машину для водителя:", reply_markup=markup)

    except Exception as e:
        logger.exception("Ошибка получения списка машин: %s", e)
        bot.send_message(chat_id, f"❌ Ошибка: {e}")


def save_driver_to_sheets(chat_id: int, vehicle_id: str = None) -> bool:
    """Сохранение водителя в Google Sheets через Apps Script."""
    session = get_session(chat_id)
    driver_data = session.get("driver_data", {})
    carrier_id = session.get("driver_carrier_id")

    if not driver_data or not carrier_id:
        bot.send_message(chat_id, "❌ Ошибка: нет данных водителя или перевозчика")
        return False

    url = os.getenv("GOOGLE_SCRIPT_URL")
    payload = {
        "action": "save_driver",
        "driver_data": {
            "carrier_id": carrier_id,
            "vehicle_id": vehicle_id or "",
            "full_name": driver_data.get("full_name", ""),
            "phone": driver_data.get("phone", ""),
            "birth_date": driver_data.get("birth_date", ""),
            "passport_number": driver_data.get("passport_number", ""),
            "passport_issued_by": driver_data.get("passport_issued_by", ""),
            "passport_issue_date": driver_data.get("passport_issue_date", ""),
            "address": driver_data.get("address", ""),
            "license_number": driver_data.get("license_number", ""),
            "license_categories": driver_data.get("categories", ""),
            "license_issue_date": driver_data.get("issue_date", ""),
            "license_expiry_date": driver_data.get("expiry_date", ""),
            "medical_book": driver_data.get("medical_book", "Нет"),
            "medical_book_expiry": driver_data.get("medical_book_expiry", ""),
        }
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data, _ = safe_json_loads(response.text)
        result = data.get("result", data)

        if not result.get("success"):
            error_msg = result.get("error", "Неизвестная ошибка")
            bot.send_message(chat_id, f"❌ Не удалось сохранить водителя: {error_msg}")
            logger.error("Driver save failed: %s", data)
            return False

        driver_id = result.get("driver_id")
        driver_name = result.get("driver_name", driver_data.get("full_name", ""))

        bot.send_message(
            chat_id,
            f"✅ Водитель добавлен!\n"
            f"ФИО: {driver_name}\n"
            f"Телефон: {driver_data.get('phone', '—')}\n"
            f"ВУ: {driver_data.get('license_number', '—')}"
        )

        # Спросить про ещё водителя
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("➕ Добавить ещё водителя", callback_data=f"add_driver_to_carrier_{carrier_id}"))
        markup.add(InlineKeyboardButton("✅ Завершить", callback_data="finish_carrier_setup"))
        bot.send_message(chat_id, "Добавить ещё водителя?", reply_markup=markup)
        return True

    except Exception as e:
        logger.exception("Ошибка сохранения водителя: %s", e)
        bot.send_message(chat_id, f"❌ Ошибка: {e}")
        return False


@bot.callback_query_handler(func=lambda call: call.data.startswith("add_driver_to_carrier_"))
def handle_add_driver_to_carrier(call):
    chat_id = call.message.chat.id
    carrier_id = call.data.split("_")[-1]

    session = get_session(chat_id)
    session["driver_carrier_id"] = carrier_id
    session["driver_data"] = {}
    session["state"] = "waiting_driver_method"
    save_session(chat_id, session)

    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("📸 Сканировать ВУ", callback_data="driver_scan_license"),
        InlineKeyboardButton("📄 Сканировать паспорт", callback_data="driver_scan_passport"),
        InlineKeyboardButton("✏️ Ввести вручную", callback_data="driver_manual_input")
    )

    bot.answer_callback_query(call.id)
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=call.message.message_id,
        text=(
            "Как добавить водителя?\n\n"
            "📸 Сканировать ВУ — распознаю ФИО, дату рождения, номер ВУ, категории, сроки\n"
            "📄 Сканировать паспорт — распознаю паспортные данные, адрес\n"
            "✏️ Ввести вручную — запрошу все поля по очереди"
        ),
        reply_markup=markup
    )


@bot.callback_query_handler(func=lambda call: call.data == "driver_scan_license")
def handle_driver_scan_license(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_driver_license_photo"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "📸 Отправьте фото водительского удостоверения (ВУ)\n\n"
        "Я распознаю: ФИО, дату рождения, номер ВУ, категории, даты выдачи и окончания"
    )


@bot.callback_query_handler(func=lambda call: call.data == "driver_scan_passport")
def handle_driver_scan_passport(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_driver_passport_photo"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "📄 Отправьте фото паспорта (разворот 2-3 или страница с пропиской)\n\n"
        "Я распознаю: ФИО, дату рождения, серию/номер, кем выдан, дату выдачи, адрес"
    )


@bot.callback_query_handler(func=lambda call: call.data == "driver_manual_input")
def handle_driver_manual_input(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_driver_text"
    session["driver_add_mode"] = "quick"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "✏️ Введите данные водителя в свободной форме:\n\n"
        "Например:\n"
        "ФИО: Иванов Иван Иванович\n"
        "Паспорт: 1234 567890\n"
        "ВУ: 12 34 567890\n"
        "Телефон: +7 900 123-45-67"
    )


@bot.callback_query_handler(func=lambda call: call.data == "ask_passport_manual")
def handle_ask_passport_manual(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_driver_passport_number"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(chat_id, "🆔 Введите серию и номер паспорта (например: 25 08 123456):")


@bot.callback_query_handler(func=lambda call: call.data == "driver_medical_yes")
def handle_driver_medical_yes(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["driver_data"]["medical_book"] = "Да"
    session["state"] = "waiting_driver_medical_expiry"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(chat_id, "📅 Введите срок действия медкнижки (ДД.ММ.ГГГГ):")


@bot.callback_query_handler(func=lambda call: call.data == "driver_medical_no")
def handle_driver_medical_no(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["driver_data"]["medical_book"] = "Нет"
    session["driver_data"]["medical_book_expiry"] = ""
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)

    # Привязка к машине
    select_vehicle_for_driver(chat_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("assign_driver_vehicle_"))
def handle_assign_driver_vehicle(call):
    chat_id = call.message.chat.id
    vehicle_id = call.data.replace("assign_driver_vehicle_", "")

    if vehicle_id == "none":
        vehicle_id = None

    bot.answer_callback_query(call.id)
    save_driver_to_sheets(chat_id, vehicle_id=vehicle_id)


@bot.callback_query_handler(func=lambda call: call.data == "finish_carrier_setup")
def handle_finish_carrier_setup(call):
    chat_id = call.message.chat.id
    bot.answer_callback_query(call.id)
    bot.send_message(chat_id, "✅ Настройка перевозчика завершена!")


@bot.callback_query_handler(func=lambda call: call.data == "add_trailer_yes")
def handle_add_trailer_yes(call):
    """Обработчик кнопки 'Да' — добавить прицеп."""
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_trailer_plate"
    session["trailer_data"] = {}
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(chat_id, "🚛 Введите **госномер прицепа**:", parse_mode="Markdown")


@bot.callback_query_handler(func=lambda call: call.data == "add_trailer_no")
def handle_add_trailer_no(call):
    """Обработчик кнопки 'Нет' — без прицепа."""
    chat_id = call.message.chat.id
    session = get_session(chat_id)

    bot.answer_callback_query(call.id)

    # Спросить про ещё одну машину
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("➕ Да, добавить ещё", callback_data=f"add_vehicle_to_carrier_{session.get('vehicle_carrier_id')}"))
    markup.add(InlineKeyboardButton("✅ Нет, завершить", callback_data="cancel_vehicle_add"))
    bot.send_message(chat_id, "Добавить ещё одну машину?", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data in ("carrier_manual_complete", "upload_carrier_card"))
def handle_upload_carrier_card(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_carrier_flexible_input"
    session["awaiting_carrier_card_upload"] = True
    session["awaiting_carrier_inn"] = False
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "📤 Загрузите карточку предприятия (фото или PDF), я извлеку реквизиты автоматически.",
    )


@bot.callback_query_handler(func=lambda call: call.data == "carrier_manual_input")
def handle_carrier_manual_input(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_carrier_flexible_input"
    session["awaiting_carrier_card_upload"] = False
    session["awaiting_carrier_inn"] = False
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "⌨️ Отправьте данные в свободном виде (можно одним сообщением):\n"
        "• телефон\n"
        "• email\n"
        "• банк, р/с, к/с, БИК\n"
        "• режим налогообложения\n\n"
        "Когда данных достаточно, я сохраню перевозчика автоматически.",
        reply_markup=build_dadata_followup_markup(),
    )


@bot.callback_query_handler(func=lambda call: call.data in ("skip_carrier_details", "carrier_skip_details"))
def handle_skip_carrier_details(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = ""
    session["awaiting_carrier_card_upload"] = False
    session["awaiting_carrier_inn"] = False
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    finalize_carrier_profile(chat_id)


@bot.callback_query_handler(func=lambda call: call.data == "skip_email")
def handle_skip_email(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    carrier_data = session.get("carrier_data") or {}
    carrier_data["email"] = ""
    session["carrier_data"] = carrier_data
    session["email"] = ""
    session["state"] = "waiting_carrier_flexible_input"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(
        chat_id,
        "📩 Email пропущен. Можно отправить остальные реквизиты в свободном виде или сохранить как есть.",
        reply_markup=build_dadata_followup_markup(),
    )


@bot.callback_query_handler(func=lambda call: call.data == "upload_bank_card")
def handle_upload_bank_card(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    session["state"] = "waiting_carrier_flexible_input"
    session["awaiting_carrier_card_upload"] = True
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    bot.send_message(chat_id, "📤 Загрузите карточку предприятия (фото/PDF), извлеку реквизиты автоматически.")


@bot.callback_query_handler(func=lambda call: call.data == "skip_bank")
def handle_skip_bank(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    carrier_data = session.get("carrier_data") or {}
    carrier_data.setdefault("bank", "")
    carrier_data.setdefault("account", "")
    carrier_data.setdefault("corr_account", "")
    carrier_data.setdefault("bik", "")
    session["carrier_data"] = carrier_data
    session["state"] = ""
    session["awaiting_carrier_card_upload"] = False
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)
    finalize_carrier_profile(chat_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("select_carrier_auto_"))
def handle_select_auto_carrier(call):
    chat_id = call.message.chat.id
    carrier_id = call.data.replace("select_carrier_auto_", "", 1)

    session = get_session(chat_id)
    carriers_map = session.get("auto_carriers_map", {}) or {}
    carrier = carriers_map.get(str(carrier_id), {})

    carrier_name = carrier.get("name", f"ID {carrier_id}")
    session["selected_carrier_id"] = carrier_id
    session["selected_carrier_name"] = carrier_name
    session["selected_carrier_tax_mode"] = carrier.get("tax_mode", "")
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, f"Выбран перевозчик: {carrier_name}")
    show_carrier_vehicles(call.message, carrier_id, carrier_name)


@bot.callback_query_handler(func=lambda call: call.data.startswith("select_vehicle_"))
def handle_select_vehicle(call):
    chat_id = call.message.chat.id
    vehicle_id = call.data.replace("select_vehicle_", "", 1)

    session = get_session(chat_id)
    carrier_id = session.get("selected_carrier_id")
    if not carrier_id:
        bot.answer_callback_query(call.id, "Сначала выберите перевозчика")
        return

    vehicles_map = session.get("vehicles_map", {}) or {}
    vehicle = vehicles_map.get(str(vehicle_id), {})

    session["selected_vehicle_id"] = vehicle_id
    session["selected_vehicle"] = vehicle
    save_session(chat_id, session)

    bot.answer_callback_query(call.id, "Машина выбрана")
    show_carrier_drivers(call.message, carrier_id=carrier_id, vehicle_id=vehicle_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("select_driver_"))
def handle_select_driver(call):
    chat_id = call.message.chat.id
    parts = call.data.split("_")
    if len(parts) < 4:
        bot.answer_callback_query(call.id, "Некорректные данные водителя")
        return

    driver_id = parts[2]
    vehicle_id = parts[3]

    session = get_session(chat_id)
    carrier_id = session.get("selected_carrier_id", "")
    carrier_name = session.get("selected_carrier_name", "")

    drivers_map = session.get("drivers_map", {}) or {}
    vehicles_map = session.get("vehicles_map", {}) or {}

    driver = drivers_map.get(str(driver_id), {})
    vehicle = vehicles_map.get(str(vehicle_id), session.get("selected_vehicle", {}))

    session["selected_driver_id"] = driver_id
    session["selected_driver"] = driver
    session["selected_vehicle_id"] = vehicle_id
    session["selected_vehicle"] = vehicle
    save_session(chat_id, session)

    form_url = build_google_form_url(
        "trip_request",
        carrier_id=carrier_id,
        carrier_name=carrier_name,
        vehicle_id=vehicle_id,
        driver_id=driver_id,
        route_from=session.get("route_from", ""),
        route_to=session.get("route_to", ""),
        pallets=session.get("pallets", ""),
        price=session.get("price", ""),
    )

    vehicle_title = f"{vehicle.get('brand', '')} {vehicle.get('model', '')} {vehicle.get('number', '')}".strip()
    driver_name = driver.get("full_name", "—")

    text = (
        "✅ Машина и водитель выбраны.\n\n"
        f"Перевозчик: {carrier_name or carrier_id}\n"
        f"Машина: {vehicle_title or vehicle_id}\n"
        f"Водитель: {driver_name}\n\n"
        "Данные частично заполнены. Откройте форму и завершите заявку."
    )

    if form_url:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("📝 Открыть форму заявки", url=form_url))
        bot.send_message(chat_id, text, reply_markup=markup)
    else:
        bot.send_message(chat_id, text)

    bot.answer_callback_query(call.id, "Водитель выбран")


@bot.message_handler(commands=["menu"])
def handle_menu_command(message):
    chat_id = message.chat.id
    clear_session(chat_id)
    show_main_menu(chat_id)


@bot.message_handler(func=lambda msg: msg.text == "🚚 Новый перевозчик")
def menu_new_carrier(message):
    """Кнопка меню: Новый перевозчик."""
    cmd_start_scanning(message)


@bot.message_handler(func=lambda msg: msg.text == "🚛 Добавить машину")
def menu_add_vehicle(message):
    """Кнопка меню: Добавить машину."""
    start_add_vehicle_flow(message.chat.id)


@bot.message_handler(func=lambda msg: msg.text == "📦 Новая заявка")
def menu_new_order(message):
    """Кнопка меню: Новая заявка."""
    start_new_trip_request_fsm(message.chat.id)


@bot.message_handler(func=lambda msg: msg.text == "📋 Мои заявки")
def menu_my_orders(message):
    """Кнопка меню: Мои заявки."""
    bot.send_message(message.chat.id, "📋 Функция просмотра заявок в разработке. Скоро будет доступна!")


@bot.message_handler(func=lambda msg: msg.text == "👤 Перевозчики")
def menu_carriers(message):
    """Кнопка меню: Перевозчики."""
    bot.send_message(message.chat.id, "👤 Список перевозчиков загружается...")
    # Можно вызвать существующую функцию показа перевозчиков
    carriers = get_carriers_list()
    if not carriers:
        bot.send_message(message.chat.id, "Список перевозчиков пуст. Добавьте первого через \"🚚 Новый перевозчик\".")
        return
    text_lines = ["👤 **Перевозчики:**\n"]
    for i, c in enumerate(carriers[:20], 1):
        name = c.get("name", "—")
        cid = c.get("id", "—")
        text_lines.append(f"{i}. {name} (ID: {cid})")
    bot.send_message(message.chat.id, "\n".join(text_lines), parse_mode="Markdown")


@bot.message_handler(func=lambda msg: msg.text == "❓ Помощь")
def menu_help(message):
    """Кнопка меню: Помощь."""
    bot.send_message(
        message.chat.id,
        "❓ **Помощь:**\n\n"
        "🚚 **Новый перевозчик** — сканирование карточки предприятия\n"
        "🚛 **Добавить машину** — добавить ТС к перевозчику\n"
        "📦 **Новая заявка** — создать заявку на перевозку\n"
        "📋 **Мои заявки** — список ваших заявок\n"
        "👤 **Перевозчики** — список перевозчиков\n\n"
        "Также доступны команды:\n"
        "/scan — сканировать карточку\n"
        "/menu — показать меню\n"
        "/cancel — отменить текущее действие",
        parse_mode="Markdown"
    )


def handle_new_carrier_command(message):
    cmd_start_scanning(message)


def handle_create_contract_command(message):
    cmd_make_contract(message)


def handle_new_order_command(message):
    start_new_trip_request_fsm(message.chat.id)


def handle_my_orders_command(message):
    menu_my_orders(message)


def handle_list_carriers_command(message):
    menu_carriers(message)


def handle_help_command(message):
    menu_help(message)


@bot.message_handler(content_types=["photo"])
def handle_photo(message):
    chat_id = message.chat.id

    try:
        largest_photo = message.photo[-1]
        file_id = largest_photo.file_id

        session = get_session(chat_id)
        state = session.get("state", "")

        # ВАЖНО: сначала обрабатываем фото по активному состоянию сессии.
        if state == "waiting_driver_photo":
            bot.send_message(chat_id, "🔍 Распознаю данные водителя...")
            image_bytes, download_error = download_telegram_file(file_id)
            if download_error:
                bot.send_message(chat_id, f"Ошибка загрузки: {download_error}")
                return

            photo_base64 = base64.b64encode(image_bytes).decode("utf-8")

            extracted = {}
            license_data = parse_driver_license(photo_base64)
            passport_data = parse_passport(photo_base64)

            if isinstance(license_data, dict):
                extracted.update({k: v for k, v in license_data.items() if v})
            if isinstance(passport_data, dict):
                if passport_data.get("full_name"):
                    extracted["full_name"] = passport_data.get("full_name")
                if passport_data.get("passport_series"):
                    extracted["passport_series"] = passport_data.get("passport_series")
                if passport_data.get("passport_number"):
                    extracted["passport_number"] = passport_data.get("passport_number")
                if passport_data.get("birth_date") and not extracted.get("birth_date"):
                    extracted["birth_date"] = passport_data.get("birth_date")

            if not extracted:
                bot.send_message(
                    chat_id,
                    "❌ Не удалось распознать данные с фото. Отправьте более четкое фото или нажмите «✏️ Ввести данные текстом».",
                )
                return

            if not extracted.get("full_name") or extracted.get("full_name") == "—":
                bot.send_message(
                    chat_id,
                    "❌ Не удалось распознать ФИО водителя.\n\n"
                    "Пожалуйста, отправьте более четкое фото или введите данные вручную в формате:\n"
                    "ФИО: Иванов Иван Иванович\n"
                    "Паспорт: 1234 567890\n"
                    "ВУ: 12 34 567890"
                )
                return

            session["driver_data"] = extracted
            session["driver_add_mode"] = "quick"

            dd = extracted
            summary = (
                f"✅ Данные распознаны:\n\n"
                f"👤 ФИО: {dd.get('full_name', '—') or '—'}\n"
            )

            if dd.get('passport_series') and dd.get('passport_number'):
                summary += f"📋 Паспорт: {dd['passport_series']} {dd['passport_number']}\n"

            if dd.get('license_number'):
                summary += f"🚗 ВУ: {dd['license_number']}\n"

            if dd.get('phone'):
                summary += f"📞 Телефон: {dd['phone']}\n"
                session["state"] = ""
                save_session(chat_id, session)

                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("💾 Сохранить водителя", callback_data="save_driver"))
                bot.send_message(chat_id, summary, reply_markup=markup)
            else:
                summary += "\n📞 Укажите номер телефона водителя:"
                session["state"] = "waiting_driver_phone"
                save_session(chat_id, session)
                bot.send_message(chat_id, summary)
            return

        # Сценарий добавления машины через СТС
        if state == "waiting_sts_photo":
            bot.send_message(chat_id, "🔍 Распознаю СТС...")
            image_bytes, download_error = download_telegram_file(file_id)
            if download_error:
                bot.send_message(chat_id, f"Не удалось обработать фото СТС: {download_error}")
                return

            photo_base64 = base64.b64encode(image_bytes).decode("utf-8")
            extracted = parse_sts_document(photo_base64)
            if not extracted:
                bot.send_message(
                    chat_id,
                    "❌ Не удалось распознать СТС. Попробуйте загрузить фото получше.",
                )
                return

            session["vehicle_data"] = extracted
            summary = (
                f"✅ Данные распознаны:\n\n"
                f"🚗 Марка: {extracted.get('brand', '—') or '—'}\n"
                f"📋 Модель: {extracted.get('model', '—') or '—'}\n"
                f"🔢 Госномер: {extracted.get('plate', '—') or '—'}\n"
                f"🔑 VIN: {extracted.get('vin', '—') or '—'}\n"
                f"📅 Год: {extracted.get('year', '—') or '—'}\n\n"
                "Открываю форму для заполнения остальных данных..."
            )
            bot.send_message(chat_id, summary)

            prefill_url = generate_vehicle_prefill_url(chat_id)
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("📝 Открыть форму", url=prefill_url))

            bot.send_message(
                chat_id,
                "📝 Форма готова!\n\n"
                "Распознанные данные уже заполнены.\n"
                "Дополните:\n"
                "• Грузоподъёмность (тонн)\n"
                "• Вместимость (европалет)\n"
                "• Температурный режим\n\n"
                "Нажмите кнопку ниже:",
                reply_markup=markup,
            )

            session["state"] = ""
            save_session(chat_id, session)
            return

        # Сценарий добавления машины через СТС (новый flow — после добавления перевозчика)
        if state == "waiting_vehicle_sts_photo":
            bot.send_message(chat_id, "🔍 Распознаю СТС...")
            image_bytes, download_error = download_telegram_file(file_id)
            if download_error:
                bot.send_message(chat_id, f"Ошибка: {download_error}")
                return

            photo_base64 = base64.b64encode(image_bytes).decode("utf-8")
            extracted = parse_sts_document(photo_base64)
            if not extracted:
                bot.send_message(chat_id, "❌ Не удалось распознать СТС. Попробуйте ещё раз.")
                return

            session["vehicle_data"] = extracted
            session["state"] = "waiting_vehicle_capacity"
            save_session(chat_id, session)

            bot.send_message(
                chat_id,
                f"✅ СТС распознан:\n"
                f"Госномер: {extracted.get('plate', '—')}\n"
                f"Марка: {extracted.get('brand', '—')}\n"
                f"Модель: {extracted.get('model', '—')}\n"
                f"VIN: {extracted.get('vin', '—')}\n"
                f"Год: {extracted.get('year', '—')}\n\n"
                f"Теперь укажите **грузоподъёмность (тонн)**:",
                parse_mode="Markdown"
            )
            return

        # --- Сканирование ВУ водителя ---
        if state == "waiting_driver_license_photo":
            bot.send_message(chat_id, "🔍 Распознаю водительское удостоверение...")
            image_bytes, download_error = download_telegram_file(file_id)
            if download_error:
                bot.send_message(chat_id, f"Ошибка загрузки: {download_error}")
                return

            photo_base64 = base64.b64encode(image_bytes).decode("utf-8")
            extracted = parse_driver_license(photo_base64)

            if not extracted:
                bot.send_message(chat_id, "❌ Не удалось распознать ВУ. Попробуйте ещё раз.")
                return

            session["driver_data"].update(extracted)
            save_session(chat_id, session)

            # Показать распознанное
            msg = (
                f"✅ ВУ распознано:\n"
                f"ФИО: {extracted.get('full_name', '—')}\n"
                f"Дата рождения: {extracted.get('birth_date', '—')}\n"
                f"Номер ВУ: {extracted.get('license_number', '—')}\n"
                f"Категории: {extracted.get('categories', '—')}\n"
                f"Выдано: {extracted.get('issue_date', '—')}\n"
                f"Действительно до: {extracted.get('expiry_date', '—')}\n\n"
            )

            # Проверить чего не хватает
            if not extracted.get('full_name') or not extracted.get('birth_date'):
                bot.send_message(chat_id, msg + "❓ Нужны паспортные данные. Отправьте фото паспорта или введите вручную.")
                session["state"] = "waiting_driver_passport_or_manual"
                save_session(chat_id, session)

                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("📄 Сканировать паспорт", callback_data="driver_scan_passport"))
                markup.add(InlineKeyboardButton("✏️ Ввести вручную", callback_data="ask_passport_manual"))
                bot.send_message(chat_id, "Выберите способ:", reply_markup=markup)
            else:
                bot.send_message(chat_id, msg + "Теперь отправьте фото паспорта или введите паспортные данные.")
                session["state"] = "waiting_driver_passport_photo"
                save_session(chat_id, session)
            return

        # --- Сканирование паспорта водителя ---
        if state == "waiting_driver_passport_photo":
            bot.send_message(chat_id, "🔍 Распознаю паспорт...")
            image_bytes, download_error = download_telegram_file(file_id)
            if download_error:
                bot.send_message(chat_id, f"Ошибка загрузки: {download_error}")
                return

            photo_base64 = base64.b64encode(image_bytes).decode("utf-8")
            extracted = parse_passport(photo_base64)

            if not extracted:
                bot.send_message(chat_id, "❌ Не удалось распознать паспорт. Попробуйте ещё раз.")
                return

            # Объединить с данными ВУ
            driver_data = session.get("driver_data", {})

            # Паспортные данные
            passport_full = f"{extracted.get('passport_series', '')} {extracted.get('passport_number', '')}".strip()
            driver_data["passport_number"] = passport_full
            driver_data["passport_issued_by"] = extracted.get("issued_by", "")
            driver_data["passport_issue_date"] = extracted.get("issue_date", "")
            driver_data["address"] = extracted.get("address", "")

            # ФИО и дата рождения из паспорта приоритетнее
            if extracted.get("full_name"):
                driver_data["full_name"] = extracted.get("full_name")
            if extracted.get("birth_date"):
                driver_data["birth_date"] = extracted.get("birth_date")

            session["driver_data"] = driver_data
            save_session(chat_id, session)

            msg = (
                f"✅ Паспорт распознан:\n"
                f"ФИО: {driver_data.get('full_name', '—')}\n"
                f"Дата рождения: {driver_data.get('birth_date', '—')}\n"
                f"Паспорт: {passport_full}\n"
                f"Кем выдан: {driver_data.get('passport_issued_by', '—')}\n"
                f"Дата выдачи: {driver_data.get('passport_issue_date', '—')}\n"
                f"Адрес: {driver_data.get('address', '—')}\n\n"
            )
            bot.send_message(chat_id, msg)

            # Дозапросить недостающие поля
            ask_missing_driver_fields(chat_id)
            return

        # Режим сканирования карточки — только если нет более приоритетного state-сценария.
        if session.get("scan_mode") or state == "scan_waiting_photo":
            process_scan_photo(chat_id, file_id)
            return

        bot.send_message(chat_id, "Получил фото. Считываю реквизиты с карточки...")

        image_bytes, download_error = download_telegram_file(file_id)
        if download_error:
            bot.send_message(chat_id, f"Не удалось обработать фото: {download_error}")
            return

        extracted, extract_error = extract_card_data_from_image(image_bytes)
        if extract_error:
            bot.send_message(chat_id, f"Ошибка распознавания карточки: {extract_error}")
            return

        if state in ("waiting_carrier_phone", "waiting_carrier_email", "waiting_carrier_bank", "waiting_carrier_flexible_input") or session.get("awaiting_carrier_card_upload"):
            bot.send_message(chat_id, "⏳ Обрабатываю карточку предприятия...")
            session = merge_extracted_into_carrier_data(session, extracted)
            session["state"] = ""
            session["awaiting_carrier_card_upload"] = False
            save_session(chat_id, session)
            finalize_carrier_profile(chat_id)
            return

        apply_extracted_carrier_data(chat_id, extracted, source_hint="фото")

    except Exception as e:
        logger.exception("Ошибка в handle_photo: %s", e)
        bot.send_message(
            chat_id,
            "Не удалось обработать фото из-за внутренней ошибки. Попробуйте ещё раз через минуту.",
        )


@bot.message_handler(content_types=["document"])
def handle_document(message):
    chat_id = message.chat.id

    try:
        document = message.document
        file_name = document.file_name or "document"
        mime_type = document.mime_type or ""

        session = get_session(chat_id)
        state = session.get("state", "")

        # В режиме сканирования — обрабатываем документ как карточку
        if session.get("scan_mode") or state == "scan_waiting_photo":
            bot.send_message(chat_id, f"📄 Обрабатываю файл {file_name}...")

            file_bytes, download_error = download_telegram_file(document.file_id)
            if download_error:
                bot.send_message(chat_id, f"❌ Не удалось скачать файл: {download_error}")
                return

            # Если это изображение — обрабатываем как фото
            if mime_type.startswith("image/"):
                extracted, extract_error = extract_card_data_from_image(file_bytes)
            else:
                extracted, extract_error = extract_card_data_from_document(file_bytes, mime_type, file_name)

            if extract_error:
                bot.send_message(chat_id, f"❌ Ошибка распознавания: {extract_error}")
                return

            # Нормализуем и обрабатываем как при сканировании фото
            scan_data = {
                "name": extracted.get("name") or extracted.get("carrier_name") or "",
                "carrier_type": extracted.get("carrier_type") or "",
                "inn": clean_digits(extracted.get("inn") or ""),
                "kpp": clean_digits(extracted.get("kpp") or ""),
                "ogrn": extracted.get("ogrn") or "",
                "snils": clean_digits(extracted.get("snils") or ""),
                "director": extracted.get("director") or "",
                "address": extracted.get("address") or extracted.get("registration_address") or "",
                "phone": normalize_phone(extracted.get("phone") or ""),
                "phone2": normalize_phone(extracted.get("phone2") or ""),
                "email": (extracted.get("email") or "").strip(),
                "bank": extracted.get("bank") or "",
                "rs": clean_digits(extracted.get("rs") or extracted.get("account") or ""),
                "ks": clean_digits(extracted.get("ks") or extracted.get("corr_account") or ""),
                "bik": clean_digits(extracted.get("bik") or ""),
                "tax_mode": normalize_tax_mode(extracted.get("tax_mode") or ""),
            }

            # DaData обогащение
            if scan_data.get("inn") and validate_inn(scan_data["inn"]):
                company, _ = get_company_by_inn(scan_data["inn"])
                if company:
                    for k in ["name", "address", "ogrn", "carrier_type", "director"]:
                        if company.get(k) and not scan_data.get(k):
                            scan_data[k] = company[k]

            session["scan_data"] = scan_data
            session["state"] = "scan_choose_type"
            save_session(chat_id, session)

            # Показываем результат и кнопки выбора типа
            found_lines = []
            for field, label in [
                ("name", "Название"), ("inn", "ИНН"), ("kpp", "КПП"),
                ("ogrn", "ОГРН"), ("carrier_type", "Тип"),
                ("director", "Директор"), ("address", "Адрес"),
                ("phone", "Телефон"), ("email", "Email"),
                ("bank", "Банк"), ("rs", "Р/с"), ("ks", "К/с"), ("bik", "БИК"),
                ("tax_mode", "Налогообложение"),
            ]:
                value = scan_data.get(field, "")
                if value:
                    found_lines.append(f"  • {label}: <b>{value}</b>")

            response = "📋 <b>Результат распознавания:</b>\n\n"
            if found_lines:
                response += "\n".join(found_lines)
            else:
                response += "⚠️ Не удалось распознать данные."

            response += "\n\n<b>Это перевозчик или заказчик?</b>"

            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("🚚 Перевозчик", callback_data="scan_type_carrier"),
                InlineKeyboardButton("🏢 Заказчик", callback_data="scan_type_customer"),
            )
            markup.add(InlineKeyboardButton("❌ Отменить", callback_data="scan_cancel"))

            bot.send_message(chat_id, response, parse_mode="HTML", reply_markup=markup)
            return

        bot.send_message(chat_id, f"Получил файл {file_name}. Извлекаю реквизиты...")

        file_bytes, download_error = download_telegram_file(document.file_id)
        if download_error:
            bot.send_message(chat_id, f"Не удалось обработать файл: {download_error}")
            return

        extracted, extract_error = extract_card_data_from_document(file_bytes, mime_type, file_name)
        if extract_error:
            bot.send_message(chat_id, f"Ошибка распознавания карточки: {extract_error}")
            return

        session = get_session(chat_id)
        state = session.get("state", "")
        if state in ("waiting_carrier_phone", "waiting_carrier_email", "waiting_carrier_bank", "waiting_carrier_flexible_input") or session.get("awaiting_carrier_card_upload"):
            bot.send_message(chat_id, "⏳ Обрабатываю карточку предприятия...")
            session = merge_extracted_into_carrier_data(session, extracted)
            session["state"] = ""
            session["awaiting_carrier_card_upload"] = False
            save_session(chat_id, session)
            finalize_carrier_profile(chat_id)
            return

        apply_extracted_carrier_data(chat_id, extracted, source_hint=file_name)

    except Exception as e:
        logger.exception("Ошибка в handle_document: %s", e)
        bot.send_message(
            chat_id,
            "Не удалось обработать файл из-за внутренней ошибки. Попробуйте ещё раз.",
        )


@bot.message_handler(content_types=["voice"])
def handle_voice(message):
    chat_id = message.chat.id

    with tempfile.TemporaryDirectory(prefix="voice_") as tmp_dir:
        try:
            bot.send_message(chat_id, "🎤 Получил голосовое. Распознаю...")
            file_info = bot.get_file(message.voice.file_id)
            file_bytes = bot.download_file(file_info.file_path)

            voice_path = os.path.join(tmp_dir, f"{chat_id}_voice.ogg")
            with open(voice_path, "wb") as f:
                f.write(file_bytes)

            mp3_path, convert_error = convert_ogg_to_mp3(voice_path)
            if convert_error:
                bot.send_message(chat_id, f"❌ {convert_error}")
                return

            text, transcribe_error = transcribe_audio_with_whisper(mp3_path)
            if transcribe_error:
                bot.send_message(chat_id, f"❌ {transcribe_error}")
                return

            bot.send_message(chat_id, f"🎤 Вы сказали: {text}")
            handle_voice_command(message, text)
        except Exception as e:
            logger.exception("Ошибка обработки voice: %s", e)
            bot.send_message(chat_id, "Не удалось обработать голосовое сообщение. Попробуйте ещё раз.")


@bot.message_handler(func=lambda m: m.text == "📋 Новый договор")
def handle_btn_new_contract(message):
    cmd_make_contract(message)


@bot.message_handler(func=lambda m: m.text == "👤 Добавить водителя")
def handle_btn_add_driver(message):
    start_add_driver(message)


@bot.message_handler(func=lambda m: m.text == "🏠 Главное меню")
def handle_btn_main_menu(message):
    clear_session(message.chat.id)
    bot.send_message(
        message.chat.id,
        "🏠 Главное меню",
        reply_markup=get_main_keyboard()
    )


def start_add_driver(message):
    chat_id = message.chat.id
    session = get_session(chat_id)
    session["state"] = "selecting_carrier_for_driver"
    session["driver_add_mode"] = "quick"
    save_session(chat_id, session)

    payload = {"action": "list_carriers"}
    data, error = call_google_script(payload)
    if error:
        bot.send_message(chat_id, f"❌ Ошибка: {error}")
        return

    if isinstance(data, dict) and data.get("ok") and isinstance(data.get("result"), dict):
        data = data.get("result", {})

    carriers = data.get("carriers", [])
    if not carriers:
        bot.send_message(chat_id, "❌ Нет перевозчиков. Сначала добавьте перевозчика.")
        return

    markup = InlineKeyboardMarkup()
    for c in carriers:
        carrier_id = c.get("id", "")
        name = c.get("name", "Без названия")
        markup.add(InlineKeyboardButton(name, callback_data=f"add_driver_carrier_{carrier_id}"))

    bot.send_message(chat_id, "👤 Выберите перевозчика:", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("add_driver_carrier_"))
def handle_add_driver_carrier(call):
    chat_id = call.message.chat.id
    carrier_id = call.data.replace("add_driver_carrier_", "")

    session = get_session(chat_id)
    session["driver_carrier_id"] = carrier_id
    session["state"] = "waiting_driver_photo"
    session["driver_data"] = {}
    session["driver_add_mode"] = "quick"
    save_session(chat_id, session)

    bot.answer_callback_query(call.id)

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✏️ Ввести данные текстом", callback_data="driver_manual_input"))

    bot.send_message(
        chat_id,
        "📸 Отправьте фото водительского удостоверения или паспорта для распознавания.\n\n"
        "Или нажмите кнопку ниже для ввода данных вручную.",
        reply_markup=markup,
    )


@bot.callback_query_handler(func=lambda call: call.data == "save_driver")
def handle_save_driver(call):
    chat_id = call.message.chat.id
    session = get_session(chat_id)
    driver_data = session.get("driver_data", {})
    carrier_id = session.get("driver_carrier_id", "")

    payload = {
        "action": "save_driver",
        "carrier_id": carrier_id,
        "driver_data": driver_data,
    }

    data, error = call_google_script(payload)
    if error:
        bot.send_message(chat_id, f"❌ Ошибка: {error}")
        return

    bot.answer_callback_query(call.id)
    session["state"] = ""
    session["driver_add_mode"] = ""
    save_session(chat_id, session)
    bot.send_message(chat_id, "✅ Водитель сохранен!", reply_markup=get_main_keyboard())


@bot.message_handler(func=lambda message: True, content_types=['text'])
def handle_text(message):
    chat_id = message.chat.id
    text = (message.text or "").strip()

    # Обработка постоянной кнопки Главное меню
    if text == "🏠 Главное меню":
        # Очистить сессию
        clear_session(chat_id)
        show_main_menu(chat_id)
        return

    # Обработка кнопок главного меню
    if text == "🚛 Новый перевозчик":
        handle_new_carrier_command(message)
        return

    if text == "📋 Новый договор":
        handle_create_contract_command(message)
        return

    if text == "📦 Новая заявка":
        handle_new_order_command(message)
        return

    if text == "📄 Мои заявки":
        handle_my_orders_command(message)
        return

    if text == "🚗 Добавить машину":
        # TODO: реализовать добавление машины отдельно
        bot.send_message(chat_id, "Эта функция доступна после добавления перевозчика", reply_markup=get_main_menu_keyboard())
        return

    if text == "👤 Добавить водителя":
        start_add_driver(message)
        return

    if text == "👥 Перевозчики":
        handle_list_carriers_command(message)
        return

    if text == "❓ Помощь":
        handle_help_command(message)
        return

    user_text = text

    if not user_text:
        bot.send_message(chat_id, "Пустое сообщение.")
        return

    try:
        session = get_session(chat_id)
        state = session.get("state", "")

        if state.startswith("trip_request_") and process_trip_request_text_input(chat_id, user_text):
            return

        if state == "waiting_driver_photo":
            extracted = extract_driver_from_text(user_text)

            if not extracted.get("full_name") or extracted.get("full_name") == "—":
                bot.send_message(
                    chat_id,
                    "❌ Не удалось распознать ФИО водителя.\n\n"
                    "Попробуйте ввести данные в формате:\n"
                    "ФИО: Иванов Иван Иванович\n"
                    "Паспорт: 1234 567890\n"
                    "ВУ: 12 34 567890",
                    reply_markup=get_main_keyboard()
                )
                return

            session["driver_data"] = extracted
            session["state"] = "waiting_driver_phone"
            session["driver_add_mode"] = "quick"
            save_session(chat_id, session)

            dd = extracted
            summary = (
                f"✅ Данные распознаны:\n\n"
                f"👤 ФИО: {dd.get('full_name', '—') or '—'}\n"
                f"📋 Паспорт: {dd.get('passport_series', '')} {dd.get('passport_number', '')}\n"
                f"🚗 ВУ: {dd.get('license_number', '—') or '—'}\n\n"
                f"📞 Укажите номер телефона водителя:"
            )
            bot.send_message(chat_id, summary)
            return

        if state == "waiting_driver_text":
            extracted = extract_driver_from_text(user_text)

            if not extracted.get("full_name") or extracted.get("full_name") == "—":
                bot.send_message(
                    chat_id,
                    "❌ Не удалось распознать ФИО водителя.\n\n"
                    "Попробуйте ввести данные в формате:\n"
                    "ФИО: Иванов Иван Иванович\n"
                    "Паспорт: 1234 567890\n"
                    "ВУ: 12 34 567890",
                    reply_markup=get_main_keyboard()
                )
                return

            session["driver_data"] = extracted
            session["state"] = "waiting_driver_phone"
            session["driver_add_mode"] = "quick"
            save_session(chat_id, session)

            dd = extracted
            summary = (
                f"✅ Данные получены:\n\n"
                f"👤 ФИО: {dd.get('full_name', '—') or '—'}\n"
                f"📋 Паспорт: {dd.get('passport_series', '')} {dd.get('passport_number', '')}\n"
                f"🚗 ВУ: {dd.get('license_number', '—') or '—'}\n\n"
                f"📞 Укажите номер телефона водителя:"
            )
            bot.send_message(chat_id, summary)
            return

        if state == "waiting_driver_phone" and session.get("driver_add_mode") == "quick":
            driver_data = session.get("driver_data", {})
            
            # Разделяем телефоны если их несколько через запятую
            phone_text = user_text.strip()
            phones = [p.strip() for p in re.split(r'[,;/]', phone_text) if p.strip()]
            
            if len(phones) > 0:
                driver_data["phone"] = phones[0]
            if len(phones) > 1:
                driver_data["phone2"] = phones[1]
            elif len(phones) == 1:
                # Если только один телефон, убедимся что phone2 не установлен
                driver_data.pop("phone2", None)
            
            session["driver_data"] = driver_data
            session["state"] = ""
            save_session(chat_id, session)

            dd = driver_data

            # ВАЛИДАЦИЯ: проверяем обязательные поля
            if not dd.get('full_name') or dd.get('full_name') == '—':
                bot.send_message(
                    chat_id,
                    "❌ Не удалось распознать ФИО водителя.\n\n"
                    "Попробуйте ввести данные в формате:\n"
                    "ФИО: Иванов Иван Иванович\n"
                    "Паспорт: 1234 567890\n"
                    "ВУ: 12 34 567890",
                    reply_markup=get_main_keyboard()
                )
                return

            summary = (
                f"✅ Водитель готов к сохранению:\n\n"
                f"👤 ФИО: {dd.get('full_name', '—')}\n"
            )

            if dd.get('passport_series') and dd.get('passport_number'):
                summary += f"📋 Паспорт: {dd.get('passport_series')} {dd.get('passport_number')}\n"

            if dd.get('license_number'):
                summary += f"🚗 ВУ: {dd.get('license_number')}\n"

            summary += f"📞 Телефон: {dd.get('phone', '—')}\n"

            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("💾 Сохранить водителя", callback_data="save_driver"))
            bot.send_message(chat_id, summary, reply_markup=markup)
            return

        if state == "waiting_sts_photo" and user_text:
            bot.send_message(chat_id, "🔍 Распознаю данные машины из текста...")
            
            payload = {
                "model": OPENAI_CARD_MODEL,
                "input": [
                    {
                        "role": "user",
                        "content": (
                            "Извлеки данные транспортного средства из текста. "
                            "Верни строго JSON без markdown:\n"
                            '{"plate":"","brand":"","model":"","vin":"","year":"",'
                            '"trailer_plate":"","trailer_brand":""}\n\n'
                            f"Текст: {user_text}"
                        )
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
                timeout=OPENAI_ROUTER_TIMEOUT,
                source="OpenAI Vehicle Parser",
            )
            if not error:
                output_text = extract_output_text(data)
                extracted, parse_error = safe_json_loads(output_text)
                if not parse_error and extracted:
                    session["vehicle_data"] = extracted
                    session["state"] = "waiting_vehicle_temp"
                    save_session(chat_id, session)

                    vd = extracted
                    summary = (
                        f"✅ Данные распознаны:\n\n"
                        f"🚗 Марка: {vd.get('brand','—') or '—'}\n"
                        f"📋 Модель: {vd.get('model','—') or '—'}\n"
                        f"🔢 Госномер: {vd.get('plate','—') or '—'}\n"
                    )
                    if vd.get('trailer_plate'):
                        summary += f"🚛 Прицеп: {vd.get('trailer_plate')}\n"

                    summary += "\n🌡 Укажите температурный режим:\nПример: -5/+12 или без температуры"
                    bot.send_message(chat_id, summary)
                    return
    
            bot.send_message(chat_id, "❌ Не удалось распознать. Попробуйте фото СТС или заполните форму вручную.")
            return

        # Обработка ввода поля в режиме сканирования
        if state == "scan_waiting_field" and session.get("scan_waiting_for"):
            field = session["scan_waiting_for"]
            scan_data = session.get("scan_data", {})

            # Валидация введённого значения
            value = user_text.strip()

            if field == "inn":
                value = clean_digits(value)
                if not validate_inn(value):
                    bot.send_message(chat_id, "⚠️ ИНН должен содержать 10 или 12 цифр. Попробуйте ещё раз:")
                    return
            elif field == "bik":
                value = clean_digits(value)
                if not validate_bik(value):
                    bot.send_message(chat_id, "⚠️ БИК должен содержать 9 цифр. Попробуйте ещё раз:")
                    return
            elif field in ("rs", "ks"):
                value = clean_digits(value)
                if not validate_account_20(value):
                    bot.send_message(chat_id, "⚠️ Счёт должен содержать 20 цифр. Попробуйте ещё раз:")
                    return
            elif field == "phone":
                value = normalize_phone(value)
                if not value:
                    bot.send_message(chat_id, "⚠️ Некорректный номер телефона. Формат: +7XXXXXXXXXX или 8XXXXXXXXXX")
                    return
            elif field == "email":
                if not validate_email(value):
                    bot.send_message(chat_id, "⚠️ Некорректный email. Попробуйте ещё раз:")
                    return
            elif field == "tax_mode":
                normalized = normalize_tax_mode(value)
                if normalized:
                    value = normalized
                else:
                    bot.send_message(
                        chat_id,
                        "⚠️ Укажите один из вариантов:\n• ОСНО (с НДС)\n• УСН (без НДС)\n• Патент\n• Самозанятый (НПД)"
                    )
                    return
            elif field == "carrier_type":
                ct = value.upper().strip()
                if "САМОЗАН" in ct or "НПД" in ct:
                    value = "САМОЗАНЯТЫЙ"
                elif ct in ("ИП", "ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ"):
                    value = "ИП"
                elif ct in ("ООО", "ОАО", "ЗАО", "ПАО", "АО"):
                    value = "ООО"
                else:
                    bot.send_message(chat_id, "⚠️ Укажите: ИП, ООО или Самозанятый")
                    return

            scan_data[field] = value
            session["scan_data"] = scan_data
            del session["scan_waiting_for"]
            save_session(chat_id, session)

            bot.send_message(chat_id, f"✅ {FIELD_LABELS.get(field, field)}: {value}")

            # Спрашиваем следующее поле
            ask_scan_next_field(chat_id)
            return

        # === Обработка ручного ввода данных машины ===
        if state == "waiting_vehicle_manual_data":
            # Парсим введённые данные
            lines = user_text.split("\n")
            vehicle_data = {}
            for line in lines:
                line = line.strip()
                if ":" in line:
                    key, val = line.split(":", 1)
                    key = key.strip().lower()
                    val = val.strip()
                    if "госномер" in key or "номер" in key:
                        vehicle_data["plate"] = val
                    elif "марка" in key:
                        vehicle_data["brand"] = val
                    elif "модель" in key:
                        vehicle_data["model"] = val
                    elif "vin" in key:
                        vehicle_data["vin"] = val
                    elif "год" in key:
                        vehicle_data["year"] = val

            if not vehicle_data.get("plate") and not vehicle_data.get("brand"):
                bot.send_message(
                    chat_id,
                    "❌ Не удалось распознать данные. Введите в формате:\n\n"
                    "Госномер: А123БВ777\n"
                    "Марка: Volvo\n"
                    "Модель: FH\n"
                    "VIN: XYZ123456789\n"
                    "Год: 2020"
                )
                return

            session["vehicle_data"] = vehicle_data
            session["state"] = "waiting_vehicle_capacity"
            save_session(chat_id, session)

            summary = (
                f"✅ Данные приняты:\n"
                f"Госномер: {vehicle_data.get('plate', '—')}\n"
                f"Марка: {vehicle_data.get('brand', '—')}\n"
                f"Модель: {vehicle_data.get('model', '—')}\n"
                f"VIN: {vehicle_data.get('vin', '—')}\n"
                f"Год: {vehicle_data.get('year', '—')}\n\n"
                f"Теперь укажите **грузоподъёмность (тонн)**:"
            )
            bot.send_message(chat_id, summary, parse_mode="Markdown")
            return

        # === Дозапрос параметров машины: тонны ===
        if state == "waiting_vehicle_capacity":
            try:
                capacity_tons = float(user_text.strip().replace(",", "."))
                session["vehicle_data"]["capacity_tons"] = capacity_tons
                session["state"] = "waiting_vehicle_pallets"
                save_session(chat_id, session)
                bot.send_message(chat_id, "✅ Грузоподъёмность сохранена.\n\nТеперь укажите **вместимость (европалет)**:", parse_mode="Markdown")
            except (ValueError, TypeError):
                bot.send_message(chat_id, "❌ Введите число (например: 20 или 20.5)")
            return

        # === Дозапрос параметров машины: палеты ===
        if state == "waiting_vehicle_pallets" and session.get("vehicle_data", {}).get("capacity_tons") is not None:
            try:
                pallets = int(user_text.strip())
                session["vehicle_data"]["pallets"] = pallets
                session["state"] = "waiting_vehicle_temp"
                save_session(chat_id, session)
                bot.send_message(chat_id, "✅ Вместимость сохранена.\n\nУкажите **температурный режим** (например: -20/+20, Тент, Изотерм):", parse_mode="Markdown")
            except (ValueError, TypeError):
                bot.send_message(chat_id, "❌ Введите целое число")
            return

        # === Дозапрос параметров машины: температурный режим ===
        if state == "waiting_vehicle_temp" and session.get("vehicle_data", {}).get("capacity_tons") is not None:
            temp_regime = user_text.strip()
            session["vehicle_data"]["temp_regime"] = temp_regime
            session["state"] = ""
            save_session(chat_id, session)

            # Сохраняем машину в Google Sheets
            save_vehicle_to_sheets(chat_id)
            return

        if state == "waiting_vehicle_temp":
            vehicle_data = session.get("vehicle_data", {})
            vehicle_data["temp"] = user_text.strip()
            session["vehicle_data"] = vehicle_data
            session["state"] = "waiting_vehicle_pallets"
            save_session(chat_id, session)
            bot.send_message(chat_id, "📦 Вместимость (количество палет)?")
            return

        if state == "waiting_vehicle_pallets":
            vehicle_data = session.get("vehicle_data", {})
            vehicle_data["pallets"] = user_text.strip()
            session["vehicle_data"] = vehicle_data
            session["state"] = ""
            save_session(chat_id, session)

            vd = vehicle_data
            summary = (
                f"✅ Машина готова к сохранению:\n\n"
                f"🚗 Марка: {vd.get('brand','—') or '—'}\n"
                f"📋 Модель: {vd.get('model','—') or '—'}\n"
                f"🔢 Госномер: {vd.get('plate','—') or '—'}\n"
                f"🌡 Темп режим: {vd.get('temp','—')}\n"
                f"📦 Палет: {vd.get('pallets','—')}\n"
            )
            if vd.get('trailer_plate'):
                summary += f"🚛 Прицеп: {vd.get('trailer_plate')}\n"

            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("💾 Сохранить машину", callback_data="save_vehicle"))
            bot.send_message(chat_id, summary, reply_markup=markup)
            return

        # === Обработка ввода данных прицепа ===
        if state == "waiting_trailer_plate":
            session["trailer_data"]["plate"] = user_text.strip()
            session["state"] = "waiting_trailer_brand"
            save_session(chat_id, session)
            bot.send_message(chat_id, "Введите **марку прицепа**:", parse_mode="Markdown")
            return

        if state == "waiting_trailer_brand":
            session["trailer_data"]["brand"] = user_text.strip()
            session["state"] = "waiting_trailer_model"
            save_session(chat_id, session)
            bot.send_message(chat_id, "Введите **модель прицепа**:", parse_mode="Markdown")
            return

        if state == "waiting_trailer_model":
            session["trailer_data"]["model"] = user_text.strip()
            session["state"] = "waiting_trailer_capacity"
            save_session(chat_id, session)
            bot.send_message(chat_id, "Укажите **грузоподъёмность прицепа (тонн)**:", parse_mode="Markdown")
            return

        if state == "waiting_trailer_capacity":
            try:
                capacity_tons = float(user_text.strip().replace(",", "."))
                session["trailer_data"]["capacity_tons"] = capacity_tons
                session["state"] = "waiting_trailer_pallets"
                save_session(chat_id, session)
                bot.send_message(chat_id, "Укажите **вместимость прицепа (европалет)**:", parse_mode="Markdown")
            except (ValueError, TypeError):
                bot.send_message(chat_id, "❌ Введите число (например: 20 или 20.5)")
            return

        if state == "waiting_trailer_pallets":
            try:
                pallets = int(user_text.strip())
                session["trailer_data"]["pallets"] = pallets
                session["state"] = "waiting_trailer_temp"
                save_session(chat_id, session)
                bot.send_message(chat_id, "Укажите **температурный режим прицепа** (например: -20/+20, Тент, Изотерм):", parse_mode="Markdown")
            except (ValueError, TypeError):
                bot.send_message(chat_id, "❌ Введите целое число")
            return

        if state == "waiting_trailer_temp":
            temp_regime = user_text.strip()
            session["trailer_data"]["temp_regime"] = temp_regime
            session["state"] = ""
            save_session(chat_id, session)

            # Сохраняем прицеп в Google Sheets
            save_trailer_to_sheets(chat_id)
            return

        # --- Ручной ввод данных водителя ---
        if state == "waiting_driver_full_name":
            session["driver_data"]["full_name"] = user_text.strip()
            session["state"] = "waiting_driver_birth_date"
            save_session(chat_id, session)
            bot.send_message(chat_id, "📅 Введите дату рождения (ДД.ММ.ГГГГ):")
            return

        if state == "waiting_driver_birth_date":
            session["driver_data"]["birth_date"] = user_text.strip()
            save_session(chat_id, session)
            ask_missing_driver_fields(chat_id)
            return

        if state == "waiting_driver_phone":
            session["driver_data"]["phone"] = user_text.strip()
            save_session(chat_id, session)
            ask_missing_driver_fields(chat_id)
            return

        if state == "waiting_driver_passport_number":
            session["driver_data"]["passport_number"] = user_text.strip()
            session["state"] = "waiting_driver_passport_issued_by"
            save_session(chat_id, session)
            bot.send_message(chat_id, "🏛 Введите кем выдан паспорт:")
            return

        if state == "waiting_driver_passport_issued_by":
            session["driver_data"]["passport_issued_by"] = user_text.strip()
            session["state"] = "waiting_driver_passport_issue_date"
            save_session(chat_id, session)
            bot.send_message(chat_id, "📅 Введите дату выдачи паспорта (ДД.ММ.ГГГГ):")
            return

        if state == "waiting_driver_passport_issue_date":
            session["driver_data"]["passport_issue_date"] = user_text.strip()
            session["state"] = "waiting_driver_address"
            save_session(chat_id, session)
            bot.send_message(chat_id, "🏠 Введите адрес регистрации (прописка):")
            return

        if state == "waiting_driver_address":
            session["driver_data"]["address"] = user_text.strip()
            save_session(chat_id, session)
            ask_missing_driver_fields(chat_id)
            return

        if state == "waiting_driver_license_number":
            session["driver_data"]["license_number"] = user_text.strip()
            session["state"] = "waiting_driver_license_categories"
            save_session(chat_id, session)
            bot.send_message(chat_id, "🚗 Введите категории прав (например: B,C,CE):")
            return

        if state == "waiting_driver_license_categories":
            session["driver_data"]["categories"] = user_text.strip()
            session["state"] = "waiting_driver_license_issue_date"
            save_session(chat_id, session)
            bot.send_message(chat_id, "📅 Введите дату выдачи прав (ДД.ММ.ГГГГ):")
            return

        if state == "waiting_driver_license_issue_date":
            session["driver_data"]["issue_date"] = user_text.strip()
            session["state"] = "waiting_driver_license_expiry"
            save_session(chat_id, session)
            bot.send_message(chat_id, "📅 Введите срок действия прав (ДД.ММ.ГГГГ):")
            return

        if state == "waiting_driver_license_expiry":
            session["driver_data"]["expiry_date"] = user_text.strip()
            save_session(chat_id, session)
            ask_missing_driver_fields(chat_id)
            return

        if state == "waiting_driver_medical_expiry":
            session["driver_data"]["medical_book_expiry"] = user_text.strip()
            save_session(chat_id, session)
            select_vehicle_for_driver(chat_id)
            return

        if state in ("waiting_carrier_phone", "waiting_carrier_email", "waiting_carrier_bank", "waiting_carrier_flexible_input"):
            carrier_data = session.get("carrier_data") or {}
            parsed = parse_bulk_reply(user_text, {})
            updated_fields: List[str] = []

            phone = normalize_phone(parsed.get("phone", "")) if parsed.get("phone") else ""
            if not phone:
                direct_phone = normalize_phone(user_text)
                if direct_phone:
                    phone = direct_phone

            if phone:
                carrier_data["phone"] = phone
                updated_fields.append("телефон")

            email = (parsed.get("email") or "").strip()
            if not email:
                direct_email = (user_text or "").strip()
                if validate_email(direct_email):
                    email = direct_email
            if email and validate_email(email):
                carrier_data["email"] = email
                updated_fields.append("email")

            if parsed.get("bank"):
                carrier_data["bank"] = parsed.get("bank")
                updated_fields.append("банк")
            if parsed.get("rs"):
                carrier_data["account"] = clean_digits(parsed.get("rs"))
                updated_fields.append("р/с")
            if parsed.get("ks"):
                carrier_data["corr_account"] = clean_digits(parsed.get("ks"))
                updated_fields.append("к/с")
            if parsed.get("bik"):
                carrier_data["bik"] = clean_digits(parsed.get("bik"))
                updated_fields.append("БИК")
            if parsed.get("tax_mode"):
                carrier_data["tax_mode"] = normalize_tax_mode(parsed.get("tax_mode"))
                updated_fields.append("налогообложение")
            if parsed.get("carrier_name") and not carrier_data.get("name"):
                carrier_data["name"] = parsed.get("carrier_name")
                updated_fields.append("название")
            if parsed.get("inn"):
                carrier_data["inn"] = clean_digits(parsed.get("inn"))
                updated_fields.append("ИНН")
            if parsed.get("ogrn"):
                carrier_data["ogrn"] = parsed.get("ogrn")
                updated_fields.append("ОГРН")
            if parsed.get("registration_address"):
                carrier_data["address"] = parsed.get("registration_address")
                updated_fields.append("адрес")

            if not updated_fields:
                bot.send_message(
                    chat_id,
                    "Не распознал новые реквизиты в сообщении. "
                    "Отправьте телефон/email или загрузите карточку предприятия.",
                    reply_markup=build_dadata_followup_markup(),
                )
                return

            session["carrier_data"] = carrier_data
            session["state"] = "waiting_carrier_flexible_input"
            session = sync_session_with_carrier_data(session)
            save_session(chat_id, session)

            missing_required = []
            if not carrier_data.get("phone"):
                missing_required.append("телефон")
            if not carrier_data.get("email"):
                missing_required.append("email")

            if missing_required:
                bot.send_message(
                    chat_id,
                    "✅ Обновил: " + ", ".join(sorted(set(updated_fields))) + "\n\n"
                    "Ещё нужно: " + ", ".join(missing_required) + ".\n"
                    "Можно отправить одним сообщением или загрузить карточку.",
                    reply_markup=build_dadata_followup_markup(),
                )
                return

            bot.send_message(chat_id, "✅ Данных достаточно. Сохраняю перевозчика в Google Sheets...")
            session["state"] = ""
            save_session(chat_id, session)
            finalize_carrier_profile(chat_id)
            return

        if session.get("awaiting_customer_inn"):
            inn = clean_digits(user_text)
            if not validate_inn(inn):
                bot.send_message(chat_id, "ИНН заказчика должен содержать 10 или 12 цифр. Попробуйте снова.")
                return

            customer = get_customer_by_inn(inn)
            if not customer:
                session["awaiting_customer_inn"] = False
                save_session(chat_id, session)
                bot.send_message(
                    chat_id,
                    "Заказчик с таким ИНН не найден в Google Sheets. Можно добавить нового заказчика через кнопку ниже.",
                    reply_markup=build_add_customer_markup(),
                )
                return

            session["customer_name"] = customer.get("name", "")
            session["customer_code"] = customer.get("code", "")
            session["customer_data"] = customer
            session["awaiting_customer_inn"] = False
            save_session(chat_id, session)

            bot.send_message(chat_id, f"✅ Заказчик выбран: {customer.get('name', '—')}")
            if session.get("scenario") == "new_carrier_contract" and session.get("awaiting_more_data"):
                prompt_for_missing_after_customer(chat_id, session)
            return

        if session.get("awaiting_vehicle_manual_entry"):
            vehicle_form_url = build_google_form_url("vehicle")
            markup = InlineKeyboardMarkup()
            if vehicle_form_url:
                markup.add(InlineKeyboardButton("📝 Открыть форму машины", url=vehicle_form_url))

            bot.send_message(
                chat_id,
                "Понял. Для корректной привязки машины к перевозчику используйте Google Форму.",
                reply_markup=markup if vehicle_form_url else None,
            )
            session["awaiting_vehicle_manual_entry"] = False
            save_session(chat_id, session)
            return

        if session.get("awaiting_carrier_card_upload"):
            bot.send_message(
                chat_id,
                "Ожидаю карточку перевозчика файлом (DOCX/PDF) или фото. "
                "После загрузки я автоматически извлеку реквизиты.",
            )
            return

        if session.get("awaiting_carrier_inn"):
            inn = clean_digits(user_text)
            if not validate_inn(inn):
                bot.send_message(chat_id, "ИНН перевозчика должен содержать 10 или 12 цифр. Попробуйте снова.")
                return

            company, company_error = get_company_by_inn(inn)
            if company_error:
                bot.send_message(
                    chat_id,
                    f"Не удалось получить данные из DaData: {company_error}\n"
                    "Можно попробовать снова, загрузить карточку или заполнить Google Форму.",
                    reply_markup=build_add_carrier_markup(),
                )
                return

            if not company:
                bot.send_message(
                    chat_id,
                    "По этому ИНН перевозчик не найден. Проверьте ИНН или используйте загрузку карточки/Google Форму.",
                    reply_markup=build_add_carrier_markup(),
                )
                return

            carrier_data = {
                "name": company.get("name", ""),
                "carrier_type": company.get("carrier_type", ""),
                "inn": inn,
                "ogrn": company.get("ogrn", ""),
                "address": company.get("address", ""),
                "director": company.get("director", ""),
                "tax_mode": normalize_tax_mode(session.get("tax_mode", "")),
            }

            exists_result, exists_error = check_carrier_exists_in_sheets(inn)
            if exists_error:
                logger.warning("Проверка дубликатов по ИНН недоступна: %s", exists_error)
            elif exists_result.get("exists"):
                existing = exists_result.get("carrier") or {}
                existing_id = str(existing.get("id", ""))

                session["pending_carrier_data"] = carrier_data
                session["existing_carrier"] = existing
                session["awaiting_carrier_duplicate_decision"] = True
                session["awaiting_carrier_inn"] = False
                session["awaiting_carrier_card_upload"] = False
                session["awaiting_more_data"] = False
                save_session(chat_id, session)

                bot.send_message(
                    chat_id,
                    f"⚠️ Перевозчик с ИНН {inn} уже есть в базе!\n\n"
                    f"📋 Текущие данные:\n"
                    f"• Название: {existing.get('name', '—')}\n"
                    f"• Телефон: {existing.get('phone', '—') or '—'}\n"
                    f"• Email: {existing.get('email', '—') or '—'}\n"
                    f"• Банк: {existing.get('bank', '—') or '—'}\n\n"
                    "Что делать?",
                    reply_markup=build_existing_carrier_actions(existing_id),
                )
                return

            session["carrier_data"] = carrier_data
            session["state"] = "waiting_carrier_flexible_input"
            session["awaiting_carrier_inn"] = False
            session["awaiting_carrier_card_upload"] = False
            session["awaiting_more_data"] = False
            session = sync_session_with_carrier_data(session)
            save_session(chat_id, session)

            bot.send_message(
                chat_id,
                "✅ Основные данные получены из DaData!\n\n"
                "📤 Загрузите карточку предприятия с реквизитами (фото или PDF)\n\n"
                "Или отправьте телефон и email текстом.",
                reply_markup=build_dadata_followup_markup(),
            )
            return

        if route_quick_commands(message, user_text):
            return

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
            if session.get("tax_mode"):
                session["tax_mode"] = normalize_tax_mode(session.get("tax_mode", ""))

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
                if "customer_name" in still_missing and not session.get("customer_name"):
                    show_customer_selection(chat_id)
                return

            # Получаем полные данные заказчика
            cust = session.get("customer_data") or get_customer_by_alias(session.get("customer_name", ""))

            payload = {
                "action": "create_carrier_and_contract",
                "customer_name": cust.get("name", session.get("customer_name", "")),
                "customer_code": cust.get("code", session.get("customer_code") or detect_customer_code(session.get("customer_name", ""))),
                "customer_inn": cust.get("inn", ""),
                "customer_kpp": cust.get("kpp", ""),
                "customer_director": cust.get("director", ""),
                "customer_basis": cust.get("basis", ""),
                "customer_address": cust.get("address", ""),
                "customer_phone": cust.get("phone", ""),
                "customer_email": cust.get("email", ""),
                "customer_bank": cust.get("bank", ""),
                "customer_rs": cust.get("rs", ""),
                "customer_ks": cust.get("ks", ""),
                "customer_bik": cust.get("bik", ""),
                "customer_tax_mode": cust.get("tax_mode", ""),
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
                "tax_mode": normalize_tax_mode(session.get("tax_mode", "")),
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

        if known.get("tax_mode"):
            known["tax_mode"] = normalize_tax_mode(known.get("tax_mode", ""))

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
                "awaiting_carrier_inn": False,
                "awaiting_carrier_card_upload": False,
                "customer_name": known.get("customer_name", ""),
                "customer_code": known.get("customer_code", ""),
                "customer_data": known.get("customer_data", {}),
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
                "tax_mode": normalize_tax_mode(known.get("tax_mode", "")),
                "director": known.get("director", ""),
                "selected_vehicle_id": "",
                "selected_driver_id": "",
            }

            validation_errors = validate_session_fields(session_data)
            if validation_errors:
                bot.send_message(chat_id, format_validation_errors_for_user(validation_errors))

            save_session(chat_id, session_data)

            if session_data.get("inn"):
                carrier_data = {
                    "name": session_data.get("carrier_name", ""),
                    "carrier_type": session_data.get("carrier_type", ""),
                    "inn": session_data.get("inn", ""),
                    "ogrn": session_data.get("ogrn", ""),
                    "address": session_data.get("registration_address", ""),
                    "director": session_data.get("director", ""),
                    "tax_mode": session_data.get("tax_mode", ""),
                    "phone": session_data.get("phone", ""),
                    "email": session_data.get("email", ""),
                    "bank": session_data.get("bank", ""),
                    "account": session_data.get("rs", ""),
                    "corr_account": session_data.get("ks", ""),
                    "bik": session_data.get("bik", ""),
                }
                session_data["carrier_data"] = carrier_data
                session_data["state"] = "waiting_carrier_flexible_input"
                session_data["awaiting_more_data"] = False
                save_session(chat_id, session_data)
                bot.send_message(
                    chat_id,
                    "✅ Основные данные получены из DaData!\n\n"
                    "📤 Загрузите карточку предприятия с реквизитами (фото или PDF)\n\n"
                    "Или отправьте телефон и email текстом.",
                    reply_markup=build_dadata_followup_markup(),
                )
                return

            if not session_data.get("inn") and not session_data.get("carrier_name"):
                show_carrier_add_options(message)
                return

            if not session_data.get("customer_name"):
                show_customer_selection(chat_id)

        elif result.get("scenario") == "existing_carrier_trip_request":
            known = result.get("known", {}) or {}
            session_data = get_session(chat_id)
            session_data["scenario"] = "existing_carrier_trip_request"
            session_data["route_from"] = known.get("route_from", session_data.get("route_from", ""))
            session_data["route_to"] = known.get("route_to", session_data.get("route_to", ""))
            session_data["route_name"] = known.get("route_name", session_data.get("route_name", ""))
            session_data["carrier_name"] = known.get("carrier_name", session_data.get("carrier_name", ""))
            session_data["price"] = known.get("price", session_data.get("price", ""))
            session_data["pallets"] = known.get("pallets") or session_data.get("pallets") or extract_number(user_text)
            session_data["selected_vehicle_id"] = session_data.get("selected_vehicle_id", "")
            session_data["selected_driver_id"] = session_data.get("selected_driver_id", "")
            save_session(chat_id, session_data)

            pallets = int(session_data.get("pallets") or 0)
            if pallets > 0:
                find_suitable_carriers(message, pallets)
            else:
                bot.send_message(chat_id, "Для подбора перевозчиков укажите количество паллет (например: 10 паллет).")

    except Exception as e:
        logger.exception("Ошибка в handle_text: %s", e)
        bot.send_message(
            chat_id,
            "Произошла внутренняя ошибка при обработке сообщения. Попробуйте снова через минуту.",
        )


# =========================
# GRACEFUL SHUTDOWN
# =========================

_is_shutting_down = False


def graceful_shutdown(signum, frame):
    """Обработчик SIGTERM для graceful shutdown при деплое Railway."""
    global _is_shutting_down
    _is_shutting_down = True
    logger.info("Получен сигнал %s, останавливаем бот...", signum)
    try:
        bot.stop_polling()
    except Exception:
        pass
    logger.info("Бот остановлен gracefully")
    sys.exit(0)


def start_polling_with_retry(max_retries: int = 5, retry_delay: int = 5):
    """Запуск polling с повторными попытками при 409 Conflict."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Попытка запуска polling #%d/%d", attempt, max_retries)
            bot.delete_webhook(drop_pending_updates=True)
            time.sleep(2)
            bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=30)
            break  # Нормальный выход
        except telebot.apihelper.ApiTelegramException as e:
            if "409" in str(e) and attempt < max_retries:
                wait_time = retry_delay * attempt
                logger.warning(
                    "409 Conflict на попытке #%d. Ждём %d сек перед повтором...",
                    attempt, wait_time,
                )
                time.sleep(wait_time)
                continue
            else:
                logger.exception("Критическая ошибка polling: %s", e)
                raise
        except Exception as e:
            if _is_shutting_down:
                logger.info("Polling остановлен (shutdown)")
                break
            logger.exception("Критическая ошибка polling: %s", e)
            raise


if __name__ == "__main__":
    # Регистрация signal handlers для graceful shutdown
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    logger.info("Бот запущен (polling mode)")
    start_polling_with_retry()
