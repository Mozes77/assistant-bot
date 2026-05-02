/**
 * Функция для добавления заказчика из Telegram бота.
 * Добавить в существующий Apps Script проект (ID: 1fZE3aKYVvLLXtO4c5sFZiycaxGnNX0GLoi5htY1xLoaeoQASc4cMX_8U)
 * 
 * Вызывается из бота через action: "create_customer"
 * 
 * Столбцы листа Форма_Заказчики:
 * A: Отметка времени
 * B: Краткое название
 * C: Полное название
 * D: Должность подписанта
 * E: ФИО подписанта
 * F: Основание
 * G: ИНН/КПП
 * H: ОГРН
 * I: Юр адрес
 * J: Почтовый адрес
 * K: Телефон
 * L: Email
 * M: Банк
 * N: Город банка
 * O: Р/с
 * P: К/с
 * Q: БИК
 * R: ЭДО
 */

function handleCreateCustomer(payload) {
  var data = payload.customer_data || {};
  
  var ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  var sheet = ss.getSheetByName('Форма_Заказчики');
  
  if (!sheet) {
    return { success: false, error: 'Лист Форма_Заказчики не найден' };
  }
  
  // Проверка дубликата по ИНН
  var inn = (data.inn || '').toString().trim();
  if (inn) {
    var existingData = sheet.getDataRange().getValues();
    for (var i = 1; i < existingData.length; i++) {
      var cellInn = (existingData[i][6] || '').toString().trim(); // Столбец G (ИНН/КПП)
      if (cellInn.indexOf(inn) !== -1) {
        return {
          success: true,
          action: 'exists',
          message: 'Заказчик с таким ИНН уже есть в базе (строка ' + (i + 1) + ')'
        };
      }
    }
  }
  
  // Формируем ИНН/КПП
  var innKpp = inn;
  if (data.kpp) {
    innKpp += '/' + data.kpp;
  }
  
  // Добавляем строку
  var row = [
    new Date(),                                    // A: Отметка времени
    data.name || '',                               // B: Краткое название
    data.name || '',                               // C: Полное название
    'Генеральный директор',                        // D: Должность подписанта
    data.director || '',                           // E: ФИО подписанта
    'Устава',                                      // F: Основание
    innKpp,                                        // G: ИНН/КПП
    data.ogrn || '',                               // H: ОГРН
    data.address || '',                            // I: Юр адрес
    data.address || '',                            // J: Почтовый адрес
    data.phone || '',                              // K: Телефон
    data.email || '',                              // L: Email
    data.bank || '',                               // M: Банк
    '',                                            // N: Город банка
    data.rs || '',                                 // O: Р/с
    data.ks || '',                                 // P: К/с
    data.bik || '',                                // Q: БИК
    ''                                             // R: ЭДО
  ];
  
  sheet.appendRow(row);
  
  return {
    success: true,
    action: 'created',
    message: 'Заказчик "' + (data.name || 'Без названия') + '" добавлен в базу'
  };
}

// В основной обработчик doPost добавить:
// case 'create_customer':
//   result = handleCreateCustomer(payload);
//   break;
