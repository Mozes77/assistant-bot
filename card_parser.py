"""
Модуль парсинга данных с карточки предприятия.
Используется как дополнительный слой извлечения после OCR (OpenAI Vision).
Regex-паттерны для извлечения ИНН, КПП, ОГРН, БИК, телефонов, email, счетов.
"""

import re
from typing import Dict, List, Optional


class CardParser:
    """Парсинг данных с карточки предприятия."""

    # ============ Извлечение отдельных полей ============

    @staticmethod
    def extract_inn(text: str) -> Optional[str]:
        """Извлечь ИНН (10 или 12 цифр)."""
        patterns = [
            r'ИНН[:\s/]*(\d{10}|\d{12})',
            r'инн[:\s/]*(\d{10}|\d{12})',
            r'I[НH][НH][:\s/]*(\d{10}|\d{12})',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def extract_kpp(text: str) -> Optional[str]:
        """Извлечь КПП (9 цифр)."""
        patterns = [
            r'КПП[:\s/]*(\d{9})',
            r'кпп[:\s/]*(\d{9})',
            r'K[ПП]П[:\s/]*(\d{9})',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def extract_ogrn(text: str) -> Optional[str]:
        """Извлечь ОГРН (13 цифр) или ОГРНИП (15 цифр)."""
        patterns = [
            r'ОГРНИП[:\s/]*(\d{15})',
            r'ОГРН[:\s/]*(\d{13}|\d{15})',
            r'огрн(?:ип)?[:\s/]*(\d{13}|\d{15})',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def extract_snils(text: str) -> Optional[str]:
        """Извлечь СНИЛС (11 цифр, возможно с разделителями)."""
        pattern = r'СНИЛС[:\s]*(\d{3}[\s-]?\d{3}[\s-]?\d{3}[\s-]?\d{2})'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return re.sub(r'[\s-]', '', match.group(1))
        return None

    @staticmethod
    def extract_bik(text: str) -> Optional[str]:
        """Извлечь БИК (9 цифр)."""
        patterns = [
            r'БИК[:\s/]*(\d{9})',
            r'бик[:\s/]*(\d{9})',
            r'Б[ИI]К[:\s/]*(\d{9})',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def extract_phones(text: str) -> List[str]:
        """Извлечь все телефоны из текста."""
        patterns = [
            r'\+7[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
            r'8[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
        ]
        phones = []
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                phone = match.group(0).strip()
                if phone not in phones:
                    phones.append(phone)
        return phones

    @staticmethod
    def extract_phone(text: str) -> Optional[str]:
        """Извлечь первый телефон."""
        phones = CardParser.extract_phones(text)
        return phones[0] if phones else None

    @staticmethod
    def extract_emails(text: str) -> List[str]:
        """Извлечь все email-адреса."""
        pattern = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
        return list(set(re.findall(pattern, text)))

    @staticmethod
    def extract_email(text: str) -> Optional[str]:
        """Извлечь первый email."""
        emails = CardParser.extract_emails(text)
        return emails[0] if emails else None

    @staticmethod
    def extract_rs(text: str) -> Optional[str]:
        """Извлечь расчётный счёт (20 цифр)."""
        patterns = [
            r'[Рр][/\\][Сс][чч]?[ёе]?т?[:\s]*(\d{20})',
            r'[Рр]\s*/\s*[Сс][:\s]*(\d{20})',
            r'расч[её]тный\s+сч[её]т[:\s]*(\d{20})',
            r'Расчётный счёт[:\s]*(\d{20})',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def extract_ks(text: str) -> Optional[str]:
        """Извлечь корреспондентский счёт (20 цифр)."""
        patterns = [
            r'[Кк][/\\][Сс][чч]?[ёе]?т?[:\s]*(\d{20})',
            r'[Кк]\s*/\s*[Сс][:\s]*(\d{20})',
            r'корр?[её]сп?[оа]нд[её]нтский\s+сч[её]т[:\s]*(\d{20})',
            r'Корр?\.\s*сч[её]т[:\s]*(\d{20})',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def extract_bank(text: str) -> Optional[str]:
        """Извлечь название банка."""
        known_banks = [
            "Сбербанк", "СБЕРБАНК", "ПАО Сбербанк",
            "Альфа-Банк", "АЛЬФА-БАНК",
            "Т-Банк", "Тинькофф",
            "ВТБ",
            "Россельхозбанк", "РСХБ",
            "Газпромбанк",
            "Совкомбанк",
            "Открытие",
            "Райффайзенбанк",
            "Промсвязьбанк",
        ]
        text_upper = text.upper()
        for bank in known_banks:
            if bank.upper() in text_upper:
                return bank

        patterns = [
            r'[Бб]анк[:\s]+([^\n,;]{3,50})',
            r'(?:ПАО|АО|ООО)\s+[«"]?([^\n,;]*[Бб]анк[^\n,;]*)[»"]?',
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip().rstrip('.')
        return None

    @staticmethod
    def extract_name(text: str) -> Optional[str]:
        """Извлечь название организации."""
        patterns = [
            r'((?:ООО|ОАО|ЗАО|ПАО|АО)\s*[«"]([^»"]+)[»"])',
            r'((?:ИП|Индивидуальный предприниматель)\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0).strip()
        return None

    @staticmethod
    def extract_director(text: str) -> Optional[str]:
        """Извлечь ФИО директора."""
        patterns = [
            r'(?:Директор|Генеральный директор|Руководитель|ИП)[:\s]+([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?)',
            r'(?:в лице)[:\s]+([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None

    @staticmethod
    def extract_address(text: str) -> Optional[str]:
        """Извлечь юридический адрес."""
        patterns = [
            r'(?:Юр(?:идический)?\.?\s*адрес|Адрес)[:\s]+(.+?)(?:\n|$)',
            r'(\d{6},\s*[^\n]+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                addr = match.group(1).strip().rstrip(',.')
                if len(addr) > 10:
                    return addr
        return None

    # ============ Полный парсинг карточки ============

    @staticmethod
    def parse_card(text: str) -> Dict[str, Optional[str]]:
        """Парсинг всех данных с карточки."""
        phones = CardParser.extract_phones(text)
        emails = CardParser.extract_emails(text)

        return {
            'name': CardParser.extract_name(text),
            'inn': CardParser.extract_inn(text),
            'kpp': CardParser.extract_kpp(text),
            'ogrn': CardParser.extract_ogrn(text),
            'snils': CardParser.extract_snils(text),
            'bik': CardParser.extract_bik(text),
            'phone': phones[0] if phones else None,
            'phone2': phones[1] if len(phones) > 1 else None,
            'email': emails[0] if emails else None,
            'rs': CardParser.extract_rs(text),
            'ks': CardParser.extract_ks(text),
            'bank': CardParser.extract_bank(text),
            'director': CardParser.extract_director(text),
            'address': CardParser.extract_address(text),
        }

    @staticmethod
    def get_missing_fields(parsed_data: Dict, required_fields: List[str]) -> List[str]:
        """Получить список недостающих полей."""
        return [field for field in required_fields if not parsed_data.get(field)]

    @staticmethod
    def merge_with_vision(regex_data: Dict, vision_data: Dict) -> Dict:
        """Объединить результаты regex-парсинга и OpenAI Vision.
        
        Vision-данные имеют приоритет, regex заполняет пропуски.
        """
        merged = dict(vision_data)
        for key, value in regex_data.items():
            if value and not merged.get(key):
                merged[key] = value
        return merged


# Список обязательных полей для перевозчика
CARRIER_REQUIRED_FIELDS = [
    'name', 'inn', 'phone', 'email',
    'bank', 'rs', 'ks', 'bik',
]

# Список обязательных полей для заказчика
CUSTOMER_REQUIRED_FIELDS = [
    'name', 'inn', 'phone', 'email',
    'bank', 'rs', 'ks', 'bik',
]

# Маппинг полей к русским названиям
FIELD_LABELS = {
    'name': 'Название',
    'inn': 'ИНН',
    'kpp': 'КПП',
    'ogrn': 'ОГРН',
    'snils': 'СНИЛС',
    'bik': 'БИК',
    'phone': 'Телефон',
    'phone2': 'Телефон 2',
    'email': 'Email',
    'rs': 'Р/с',
    'ks': 'К/с',
    'bank': 'Банк',
    'director': 'Директор',
    'address': 'Юр. адрес',
    'tax_mode': 'Налогообложение',
    'carrier_type': 'Тип (ИП/ООО)',
}
