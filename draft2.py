from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, CallbackContext, MessageHandler, filters
import random
import datetime
import asyncpg
import nanoid
from nanoid import generate
from datetime import datetime, timedelta
import logging
import asyncio
import aiohttp
from telegram.ext import JobQueue
from telegram import Chat ,User


# Функция для инициализации подключения к базе данных PostgreSQL
async def init_db():
    return await asyncpg.connect(
        user='postgres.zywjihrcgdozorytmbhy', 
        password='Karypb@ev05',
        database='postgres', 
        host='aws-0-eu-central-1.pooler.supabase.com',
        port = '6543',
        statement_cache_size=0,
    )


async def save_user(connection, telegram_username, phone_number):
    query = """
    INSERT INTO users (telegram_username, phone_number, is_paid) 
    VALUES ($1, $2, FALSE) 
    ON CONFLICT (telegram_username) DO UPDATE 
    SET phone_number = EXCLUDED.phone_number 
    RETURNING user_id;
    """
    user_id = await connection.fetchval(query, telegram_username, phone_number)
    if user_id is None:
        raise ValueError("Ошибка: user_id не был сохранен в базу данных.")
    return user_id




async def save_request(connection, request_id, user_id,  # Это id из твоей базы
                       warehouses, delivery_type, request_date, 
                       coefficient, photo, warehouse_ids, date_period, telegram_user_id):  # Это id из Telegram
    query = """
    INSERT INTO requests (request_id, user_id, warehouses, delivery_type, request_date, coefficient, photo, warehouse_ids, date_period, telegram_user_id)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
    """
    await connection.execute(query, request_id, user_id, warehouses, delivery_type, request_date, coefficient, photo, warehouse_ids, date_period, telegram_user_id)


def format_date(date_obj):
    return date_obj.strftime('%d.%m.%Y')

def get_period_range(date_period):
    today = datetime.now().date()
    
    if date_period == 'week':
        start_date = today
        end_date = start_date + timedelta(weeks=1)
    elif date_period == 'month':
        start_date = today
        end_date = start_date + timedelta(days=30)
    elif date_period == 'tomorrow':
        start_date = today + timedelta(days=1)
        end_date = start_date  
    elif date_period == 'today':
        start_date = today
        end_date = start_date  
    else:
        return "Период не выбран"
    
    return f"{format_date(start_date)} - {format_date(end_date)}"

warehouses_data = [
    (1, "Подольск 3", 29.8, 59.6, 59.6, 218623, [2, 5, 6]), 
    (2, "Коледино", 29.27, 59.4, 59.4, 507, [2, 5, 6]),
    (3, "Подольск", 29.27, 66, "н/д", 117501, [2, 5, 6]),
    (4, "Электросталь", 11.2, 47.85, 47.85, 120762, [2, 5, 6]),
    (5, "Тула", 7.13, 47.85, 66, 206348, [2, 5, 6]),
    (6, "Обухово", 28, "н/д", 56.1, 218210, [2, 5, 6]),
    (7, "Астана", 1.77, 64.35, 41.25, 204939, [2, 5, 6]),
    (8, "Белые Столбы", 7.01, 92.4, "н/д", 206236, [2, 5, 6]),
    (9, "Казань", 12.91, 64.35, 82.5, 117986, [2, 5, 6]),
    (10, "СЦ Вёшки", 1.72, "н/д", 66, 210515, [2, 5, 6]),
    (11, "Рязань (Тюшевское)", 12.34, 46.2, 41.25, 301760, [2, 5, 6]),
    (12, "Котовск", 2.95, "н/д", 57.75, 301809, [2, 5, 6]),
    (13, "Краснодар", 4.92, 54.45, 51.15, 130744, [2, 5, 6]),
    (14, "Чехов 2", 2.39, 56.1, 64.35, 210001, [2, 5, 6]),
    (15, "Уткина-Заводь", 6.72, 70.95, "н/д", 2737, [2, 5, 6]),
    (16, "Невинномысск", 13.84, 49.5, 57.75, 208277, [2, 5, 6]),
    (17, "СЦ Кузнецк", 1.12, "н/д", 47.85, 302335, [2, 5, 6]),
    (18, "Новосибирск", 10.53, 141.9, "н/д", 686, [2, 5, 6]),
    (19, "Испытателей", 6.58, 66, "н/д", 1733, [2, 5, 6]),
    (20, "Хабаровск", 0.8, 72.6, 33, 1193, [2, 5, 6]),
    (21, "Минск", 1.45, "н/д", 69.3, 211622, [2, 5, 6]),
    (22, "Алматы Атакент", 1.85, 41.25, 18.15, 218987, [2, 5, 6])
]

regions_data = {
    "Центральный": [1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14],
    "Северо-Западный": [15, 2, 3, 4, 5, 6, 7, 12, 8, 10, 13, 14, 9, 11, 13],
    "Приволжский": [9, 4, 5, 2, 7, 13, 3, 8, 18, 10, 15, 17, 11, 12, 9],
    "Южный": [13, 19, 5, 2, 4, 15, 3, 10, 9, 8, 18, 20, 14],
    "Уральский": [17, 9, 4, 2, 3, 7, 8, 15, 5],
    "Сибирский": [16, 17, 9, 4, 2, 5, 8, 13, 7, 15, 18, 14],
    "Дальневосточный": [18, 9, 5, 2, 16, 19, 17, 8, 14, 13, 7, 12, 15],
    "Беларусь": [16, 13, 2, 3, 5, 4, 8, 7, 11],
    "Казахстан": [20, 17, 9, 4, 2, 5, 18, 8, 13, 7]
}

def translate_to_russian(key, value):
    if key == 'delivery_type':
        delivery_type_translation = {
            'super_safe': 'Суперсейф',
            'box': 'Короба',
            'mono': 'Монопаллеты',
            'qr_box': 'QR поставка коробами'
        }
        return delivery_type_translation.get(value, 'Не выбрано')
    
    if key == 'date_period':
        date_translation = {
            'today': 'Сегодня',
            'tomorrow': 'Завтра',
            'week': 'Неделя',
            'month': 'Месяц'
        }
        return date_translation.get(value, value) 

    return value 

requests = {}  

def generate_request_id():
    return str (generate('123456789', 4))

def get_warehouses_by_region(region):
    warehouse_ids = regions_data.get(region, [])
    return [wh for wh in warehouses_data if wh[0] in warehouse_ids]


async def start(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("Подбор складов", callback_data='select_warehouses_main')],
        [InlineKeyboardButton("Топ складов по регионам", callback_data='top_warehouses_main')],
        [InlineKeyboardButton("Поиск лимитов", callback_data='search_limits')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.message: 
        await update.message.reply_text(
            "Привет! Я бот для работы со складами Wildberries. Вот что я умею:\n\n"
            "1. Подбор складов\n"
            "2. Топ складов по округам\n" 
            "3. Поиск лимитов на складе\n\n"
            "Выберите действие:",
            reply_markup=reply_markup
        )
    elif update.callback_query:  
        await update.callback_query.message.reply_text(
            "Выберите действие:\n\n"
            "1. Подбор складов\n"
            "2. Топ складов по округам\n"
            "3. Поиск лимитов на складе\n\n",
            reply_markup=reply_markup
        )


async def select_warehouse_main(update: Update, context: CallbackContext):
    query = update.callback_query
    if query is None:
        return

    context.user_data['selected_count'] = 6  

    await update_warehouse_message(update, context)



async def update_warehouse_message(update: Update, context: CallbackContext):
    selected_count = context.user_data.get('selected_count', 6)
    selected_warehouses = random.sample(warehouses_data, selected_count)
    
    context.user_data['selected_warehouses'] = selected_warehouses

    message = "При загрузке складов:\n"
    for warehouse in selected_warehouses:
        message += f"{warehouse[1]} - {warehouse[2]}% / {warehouse[3]}р / {warehouse[4]}р\n"
    
    message += "\nВы получите 37.2 из 40 возможных процента в ранжировании или 93.0 из 100 процентов лучшей скорости доставки.\n"
    message += "Ср. цена логистики на ед. товара этой комбинации складов:\n📦 Короба - 76.93р\n🚚 Монопаллета - 79.86р\n"

    keyboard = [
        [InlineKeyboardButton("Изменить количество складов", callback_data='change_count')],
        [InlineKeyboardButton("Заменить склад", callback_data='replace_warehouse')],
        [InlineKeyboardButton("Главное меню", callback_data='main_menu')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query.message:
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
    else:
        await update.callback_query.answer("Ошибка: сообщение не найдено.")


async def change_count(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton(str(i), callback_data=f'count_{i}') for i in range(1, 6)],
        [InlineKeyboardButton(str(i), callback_data=f'count_{i}') for i in range(6, 10)],
        [InlineKeyboardButton("Главное меню", callback_data='main_menu')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query.message:
        await update.callback_query.edit_message_text("Выберите количество складов:", reply_markup=reply_markup)
    else:
        await update.callback_query.answer("Ошибка: сообщение не найдено.")


async def count_selected(update: Update, context: CallbackContext):
    query = update.callback_query
    if query is None:
        return

    selected_count = int(query.data.split('_')[-1])
    context.user_data['selected_count'] = selected_count

    await update_warehouse_message(update, context)


async def replace_warehouse(update: Update, context: CallbackContext):
    query = update.callback_query
    if query is None:
        return
    await query.answer()  

    selected_warehouses = context.user_data.get('selected_warehouses', [])

    keyboard = [
        [InlineKeyboardButton(warehouse[1], callback_data=f'replace_{warehouse[0]}')]
        for warehouse in selected_warehouses
    ]
    keyboard.append([InlineKeyboardButton("Главное меню", callback_data='main_menu')])
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query.message:
        await query.edit_message_text("Выберите склад для замены:", reply_markup=reply_markup)
    else:
        await query.answer("Ошибка: сообщение не найдено.")


async def handle_replace_warehouse(update: Update, context: CallbackContext):
    query = update.callback_query
    if query is None:
        return
    await query.answer() 

    warehouse_id_to_replace = int(query.data.split('_')[-1])

    selected_warehouses = context.user_data.get('selected_warehouses', [])
    
    warehouse_to_replace = next((w for w in selected_warehouses if w[0] == warehouse_id_to_replace), None)
    if warehouse_to_replace is None:
        await query.answer("Ошибка: склад не найден.")
        return

    available_warehouses = [w for w in warehouses_data if w[0] != warehouse_id_to_replace]
    if not available_warehouses:
        await query.answer("Нет доступных складов для замены.")
        return

    new_warehouse = random.choice(available_warehouses)

    for i, warehouse in enumerate(selected_warehouses):
        if warehouse[0] == warehouse_id_to_replace:
            selected_warehouses[i] = new_warehouse
            break

    context.user_data['selected_warehouses'] = selected_warehouses

    await update_warehouse_message(update, context)



async def top_warehouses_main(update: Update, context: CallbackContext):
    query = update.callback_query
    if query is None:
        return

    context.user_data['selected_region'] = "Центральный"
    await update_region_message(update, context)


async def update_region_message(update: Update, context: CallbackContext):
    selected_region = context.user_data.get('selected_region', "Центральный")  
    warehouses = get_warehouses_by_region(selected_region) 

    message = f"Склады округа {selected_region}:\n"
    for warehouse in warehouses:
        message += f"{warehouse[1]} - {warehouse[2]}% / {warehouse[3]}р / {warehouse[4]}р\n"

    keyboard = []
    regions = list(regions_data.keys())
    for i in range(0, len(regions), 2):
        row = [
            InlineKeyboardButton(f"{regions[i]} {'✅' if regions[i] == selected_region else ''}", callback_data=f'region_{regions[i]}')
        ]
        if i + 1 < len(regions):
            row.append(InlineKeyboardButton(f"{regions[i+1]} {'✅' if regions[i+1] == selected_region else ''}", callback_data=f'region_{regions[i+1]}'))
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("Главное меню", callback_data='main_menu')]) 
    reply_markup = InlineKeyboardMarkup(keyboard)

    current_message = update.callback_query.message.text
    if current_message != message:
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
    else:
        await update.callback_query.answer("Сообщение не изменилось.")



async def region_selected(update: Update, context: CallbackContext):
    query = update.callback_query
    if query is None:
        return

    selected_region = query.data.split('_')[-1]
    context.user_data['selected_region'] = selected_region

    await update_region_message(update, context)


async def search_limits(update: Update, context: CallbackContext):
    query = update.callback_query
    if query is None:
        return

    keyboard = [[InlineKeyboardButton("Добавить запрос", callback_data='add_request')]]

    if requests:
        for request_id in requests:
            # Генерируем уникальный callback_data
            unique_data = f'edit_request_{request_id}_{generate(size=6)}'  # добавляем случайную строку
            keyboard.append([InlineKeyboardButton(f"Изменить запрос {request_id}", callback_data=unique_data)])

    keyboard.append([InlineKeyboardButton("Главное меню", callback_data='main_menu')])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text("Выберите запрос или создайте новый:", reply_markup=reply_markup)


async def check_payment(connection, user_id):
    """
    Проверяет, оплатил ли пользователь хотя бы один запрос, 
    обращаясь к полю is_paid в таблице users.
    """
    query = """
    SELECT EXISTS (
        SELECT 1 FROM users 
        WHERE user_id = $1 AND is_paid = TRUE
    );
    """
    result = await connection.fetchval(query, user_id)
    return result

async def add_request(update: Update, context: CallbackContext):
    """
    Обработчик кнопки "Добавить запрос". 
    Начинает процесс создания нового запроса без проверки оплаты.
    """
    try:
        connection = await init_db()

        telegram_username = update.effective_user.username
        phone_number = None  # Или получите номер телефона, если он доступен

        user_id = await save_user(connection, telegram_username, phone_number)
        context.user_data['user_id'] = user_id  # Сохраняем user_id в контексте

        context.user_data['request'] = {} 
        await create_new_request(update, context) 

    except Exception as e:
        logging.error(f"Ошибка при добавлении запроса: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка. Пожалуйста, попробуйте позже.")
    finally:
        if connection:
            await connection.close()

async def get_request_from_db(connection, request_id):
    """
    Получает данные запроса из базы данных по его ID.
    """
    query = """
    SELECT * FROM requests 
    WHERE request_id = $1;
    """
    result = await connection.fetchrow(query, int(request_id))
    if result is None:
        raise ValueError(f"Запрос с ID {request_id} не найден в базе данных.")

    warehouses = {}
    for wh_data in result['warehouses']:
        if ' - ' in wh_data:
            try:
                wh_id, wh_name = wh_data.split(' - ', 1)  # Разделяем строку на ID и название
                warehouses[int(wh_id)] = wh_name
            except ValueError:
                logging.error(f"Ошибка при распаковке данных склада: {wh_data}")
                continue  # Пропускаем некорректные данные
        else:
            logging.warning(f"Неверный формат данных склада: {wh_data}")
            continue  # Пропускаем данные с неверным форматом

    # Преобразуем данные из базы данных в формат, совместимый с context.user_data['request']
    request_data = {
        'warehouses': warehouses,
        'delivery_type': result['delivery_type'],
        'date_period': result['request_date'],  # или другое поле, если дата хранится иначе
        'acceptance_coefficient': result['coefficient'],
        'warehouse_ids': result['warehouse_ids'],  # Добавляем warehouse_ids
        'date_period': result['date_period'],
        'telegram_user_id': result['telegram_user_id']
    }

    return request_data



async def save_request_changes(connection, request_id, updated_data):
    """
    Сохраняет изменения в существующем запросе в базе данных.
    """
    try:
        # Формируем SQL-запрос UPDATE
        query = """
        UPDATE requests
        SET warehouses = $1, delivery_type = $2, request_date = $3, coefficient = $4
        WHERE request_id = $5;
        """

        # Выполняем запрос
        await connection.execute(query, 
                                updated_data['warehouses'], 
                                updated_data['delivery_type'], 
                                updated_data['date_period'], 
                                updated_data['acceptance_coefficient'], 
                                int(request_id))

    except Exception as e:
        logging.error(f"Ошибка при сохранении изменений в запросе: {e}")
        raise  # Передаем исключение дальше для обработки в вызывающей функции


async def create_new_request(update: Update, context: CallbackContext):
    """
    Функция для создания нового запроса. Вызывает функции для выбора складов, типа доставки и т.д.
    """
    await select_warehouse_for_limits(update, context)  # Начинаем с выбора складов

async def edit_existing_request(update: Update, context: CallbackContext):
    """
    Функция для редактирования существующего запроса. 
    Получает данные запроса из базы данных и вызывает функции для их изменения.
    """
    query = update.callback_query
    request_id = query.data.split('_')[-1]

    try:
        connection = await init_db() 
        request_data = await get_request_from_db(connection, request_id)

        context.user_data['request'] = request_data
        context.user_data['request_id'] = request_id 
        await select_warehouse_for_limits(update, context) 

    except ValueError as e: 
        await query.answer(str(e)) 
    finally:
        if connection:
            await connection.close()
    # TODO: Получить данные запроса из базы данных по request_id
    request_data = await get_request_from_db(connection, request_id)
    await select_warehouse_for_limits(update, context)  # Начинаем с выбора складов (можно изменить на другую функцию)

async def select_warehouse_for_limits(update: Update, context: CallbackContext):
    selected_warehouses = context.user_data.get('request', {}).get('warehouses', {})

    keyboard = []
    for i in range(0, len(warehouses_data), 2):
        row = [
            InlineKeyboardButton(
                f"✅ {warehouses_data[i][1]}" if warehouses_data[i][1] in selected_warehouses else warehouses_data[i][1],
                callback_data=f"warehouse_{warehouses_data[i][1]}"
            )
        ]
        if i + 1 < len(warehouses_data):
            row.append(
                InlineKeyboardButton(
                    f"✅ {warehouses_data[i + 1][1]}" if warehouses_data[i + 1][1] in selected_warehouses else warehouses_data[i + 1][1],
                    callback_data=f"warehouse_{warehouses_data[i + 1][1]}"
                )
            )
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("Далее", callback_data="next_step")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text("Выберите склады для поиска лимитов:", reply_markup=reply_markup)


async def warehouse_selected(update: Update, context: CallbackContext):
    query = update.callback_query
    warehouse_name = query.data.split('_')[-1]

    if 'request' not in context.user_data:
        await query.answer("Ошибка: данные заявки отсутствуют.")
        return

    selected_warehouses = context.user_data['request'].get('warehouses', {})

    if warehouse_name in selected_warehouses:
        del selected_warehouses[warehouse_name]
    else:
        selected_warehouses[warehouse_name] = warehouse_name 

    context.user_data['request']['warehouses'] = selected_warehouses

    await select_warehouse_for_limits(update, context)

async def next_step(update: Update, context: CallbackContext):
    await select_delivery_type(update, context)


async def select_delivery_type(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("Суперсейф", callback_data='delivery_super_safe_6')], # добавим ID типа поставки
        [InlineKeyboardButton("Короба", callback_data='delivery_box_2')],
        [InlineKeyboardButton("Монопаллеты", callback_data='delivery_mono_5')],
        [InlineKeyboardButton("QR поставка коробами", callback_data='delivery_qr_box')], #  ID для QR поставки  не указан
        [InlineKeyboardButton("Назад", callback_data='search_limits')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text("Выберите тип приемки:", reply_markup=reply_markup)


async def delivery_type_selected(update: Update, context: CallbackContext):
    query = update.callback_query
    delivery_type, delivery_type_id = query.data.split('_')[-2:]  # разделим данные для получения типа и ID
    
    context.user_data['request']['delivery_type'] = delivery_type
    context.user_data['request']['delivery_type_id'] = int(delivery_type_id)  # сохраним ID типа поставки

    await select_date(update, context)


async def select_date(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("Сегодня", callback_data='date_today'), InlineKeyboardButton("Завтра", callback_data='date_tomorrow')],
        [InlineKeyboardButton("Неделя", callback_data='date_week'), InlineKeyboardButton("Месяц", callback_data='date_month')],
        [InlineKeyboardButton("Выберите диапазон дат", callback_data='date_range')],
        [InlineKeyboardButton("Назад", callback_data='select_delivery_type')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text("Выберите дату:", reply_markup=reply_markup)

async def date_selected(update: Update, context: CallbackContext):
    query = update.callback_query
    selected_date = query.data.split('_')[-1]

    if selected_date == 'range':
        await query.edit_message_text("Введите начальную дату (ДД-ММ-ГГГГ):")
        context.user_data['awaiting_start_date'] = True  
        return

    context.user_data['request']['date_period'] = selected_date
    await select_acceptance_coefficient(update, context)

async def handle_date_input(update: Update, context: CallbackContext):
    user_data = context.user_data

    if user_data.get('awaiting_start_date'):
        try:
            start_date = datetime.datetime.strptime(update.message.text, "%d-%m-%Y").date()
            context.user_data['start_date'] = start_date
            await update.message.reply_text("Введите конечную дату (ДД-ММ-ГГГГ):")
            user_data['awaiting_start_date'] = False
            user_data['awaiting_end_date'] = True  
        except ValueError:
            await update.message.reply_text("Неверный формат даты. Попробуйте еще раз: ДД-ММ-ГГГГ")
    
    elif user_data.get('awaiting_end_date'):
        try:
            end_date = datetime.datetime.strptime(update.message.text, "%d-%m-%Y").date()
            start_date = user_data.get('start_date')

            if end_date < start_date:
                await update.message.reply_text("Конечная дата не может быть раньше начальной. Попробуйте снова.")
            else:
                user_data['request']['date_period'] = f"{start_date} - {end_date}"

                await update.message.reply_text(f"Вы выбрали диапазон дат: {start_date} - {end_date}")
                
                await select_acceptance_coefficient(update, context)

                user_data['awaiting_end_date'] = False

        except ValueError:
            await update.message.reply_text("Неверный формат даты. Попробуйте еще раз: ДД-ММ-ГГГГ")


async def select_acceptance_coefficient(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("Только бесплатная = 0", callback_data='coef_0')],
        [InlineKeyboardButton(str(i), callback_data=f'coef_{i}') for i in range(1, 6)],
        [InlineKeyboardButton(str(i), callback_data=f'coef_{i}') for i in range(6, 11)],
        [InlineKeyboardButton(str(i), callback_data=f'coef_{i}') for i in range(11, 16)],
        [InlineKeyboardButton(str(i), callback_data=f'coef_{i}') for i in range(16, 21)],
        [InlineKeyboardButton("Назад", callback_data='select_date')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text("Выберите коэффициент приемки:", reply_markup=reply_markup)
    elif update.message:
        await update.message.reply_text("Выберите коэффициент приемки:", reply_markup=reply_markup)

async def coefficient_selected(update: Update, context: CallbackContext):
    query = update.callback_query
    coefficient = query.data.split('_')[-1]
    
    if 'request' not in context.user_data:
        context.user_data['request'] = {}
    
    context.user_data['request']['acceptance_coefficient'] = coefficient
    await confirm_request(update, context)


async def confirm_request(update: Update, context: CallbackContext):
    user_data = context.user_data.get('request', {})
    user_id = context.user_data.get('user_id') 

    if not user_data:
        await update.callback_query.edit_message_text("Ошибка: данные заявки отсутствуют.")
        return

    request_id = generate_request_id()
    requests[request_id] = user_data
    telegram_user_id = update.effective_user.id
    delivery_type = translate_to_russian('delivery_type', user_data.get('delivery_type', 'Не выбрано'))
    date_period = user_data.get('date_period', 'Не выбран')
    period_range = get_period_range(date_period)

    delivery_type = user_data.get('delivery_type', 'Не выбрано')
    delivery_type_id = delivery_type.split('_')[-1]  # Получаем последний элемент после '_'
    if delivery_type_id.isdigit():
        delivery_type_id = int(delivery_type_id)
    else:
        delivery_type_id = None  # Если это не число, установим в None


    try:
        connection = await init_db()
        
        
        # Формируем корректный список warehouse_ids
        warehouse_ids = [str(wh[5]) for wh in warehouses_data if wh[1] in user_data.get('warehouses', {}).values() and wh[5]]
        
        # Логируем warehouse_ids для проверки
        print(f"Сформированные warehouse_ids: {warehouse_ids}")  # Ожидаем список полных ID складов
        
        warehouse_ids_str = ','.join(warehouse_ids)

        has_paid = await check_payment(connection, user_id)
        
        # Если пользователь ранее уже оплатил
        if has_paid:

            warehouses = ', '.join(list(user_data.get('warehouses', {}).values()))
            message = (
                "Заявка успешно создана:\n"
                f"🏦 Склады: {warehouses}\n"
                f"📦 Тип приемки: {delivery_type}\n"
                f"📅 Период: {period_range}\n"
                f"💸 Коэффициент: {user_data.get('acceptance_coefficient', 'Не выбран')}\n\n"
                f"ID заявки: {request_id}\n\n"
                "Поиск лимитов активирован ✅."
            )
            keyboardline = [
                [InlineKeyboardButton("Главное меню", callback_data='main_menu')],
            ]
            reply_markup = InlineKeyboardMarkup(keyboardline)

            await update.callback_query.edit_message_text(message, reply_markup=reply_markup)


            await save_request(
                connection,
                int(request_id),
                user_id,
                warehouses,  # Строка с названиями складов
                user_data.get('delivery_type', 'Не выбрано'),
                datetime.now().date(),
                user_data.get('acceptance_coefficient', 0),
                None,
                warehouse_ids_str,  # Передаем список ID складов
                user_data.get('date_period'),
                telegram_user_id
            )


            context.user_data['request']['warehouse_ids'] = warehouse_ids
            await connection.close()
            return        
        
        warehouse_nums = list(user_data.get('warehouses', {}).keys()) 
        warehouse_names = [wh[1] for wh in warehouses_data if wh[1] in warehouse_nums]
        # Проверяем, что список складов не пустой 
        if not warehouse_nums:
            await update.callback_query.edit_message_text("Ошибка: список складов пуст.")
            return

        # Дополнительная проверка на наличие складов в warehouses_data
        if not warehouse_names:
            await update.callback_query.edit_message_text("Ошибка: выбранные склады не найдены в базе данных.")
            return

        warehouses = ', '.join(warehouse_names)

        # Сохранение заявки с is_paid=False
        await save_request(
            connection,
            int(request_id),
            user_id,
            warehouses,  # Строка с названиями складов
            user_data.get('delivery_type', 'Не выбрано'),
            datetime.now().date(),
            user_data.get('acceptance_coefficient', 0),
            None,
            warehouse_ids_str,  # Передаем список ID складов
            user_data.get('date_period'),
            telegram_user_id
        )

    except Exception as e:
        logging.error(f"Ошибка при сохранении заявки: {e}")
        if update.callback_query:
            await update.callback_query.edit_message_text("Произошла ошибка при сохранении заявки.")
        else:
            await update.message.reply_text("Произошла ошибка при сохранении заявки.")
        return  # Завершаем выполнение функции после обработки ошибки

    # Отправляем сообщение с ссылкой на оплату, если она еще не была произведена
    message = (
        "Запрос успешно создан:\n"
        f"🏦 Склады: {warehouses}\n"
        f"📦 Тип приемки: {delivery_type}\n"
        f"📅 Период: {period_range}\n"
        f"💸 Коэффициент: {user_data.get('acceptance_coefficient', 'Не выбран')}\n\n"
        f"ID заявки: {request_id}\n\n"
        "Для подтверждения заявки, пожалуйста, оплатите по ссылке ниже и отправьте чек или фотографию чека после оплаты.\n\n"
        "После оплаты, отправьте чек в виде фотографии."
    )

    keyboard = [
        [InlineKeyboardButton("Главное меню", callback_data='main_menu')],
        [InlineKeyboardButton("Оплатить", url='https://www.sberbank.com/sms/pbpn?requisiteNumber=9774160969&utm_campaign=sberitem_banner')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(message, reply_markup=reply_markup)

    context.user_data['request_id'] = request_id
    context.user_data['awaiting_receipt'] = True
    context.user_data['request']['warehouse_ids'] = warehouse_ids

    job_data = {
        'warehouse_ids': warehouse_ids_str,
        'delivery_type_id': delivery_type_id,
        'acceptance_coefficient': user_data.get('acceptance_coefficient', 0),
        'delivery_type': user_data.get('delivery_type', 'Не выбрано'),
        'user_id': user_id,
        'selected_warehouses': user_data.get('warehouses', {}),
        'date_period': user_data.get('date_period'),
        'warehouses_data': warehouses_data,
        'telegram_user_id': telegram_user_id
    }
    
    # Создаем задачу с помощью create_search_limits_job
    job = search_limits_job(job_data) 

    # Запускаем задачу
    asyncio.create_task(job(context))

    await connection.close()


async def search_limits_job(context: CallbackContext, job_data: dict):
    """
    Фоновая задача для поиска лимитов.
    """
    try:
        warehouse_ids = job_data['warehouse_ids']
        delivery_type_id = job_data['delivery_type_id']
        acceptance_coefficient = job_data['acceptance_coefficient']
        delivery_type = job_data['delivery_type']
        user_id = job_data['user_id']
        selected_warehouses = job_data['selected_warehouses']
        date_period = job_data['date_period']
        warehouses_data = job_data['warehouses_data']
        telegram_user_id = job_data['telegram_user_id']

        # Логирование начала процесса
        logging.info(f"Запущен поиск лимитов для пользователя {user_id}, склады: {warehouse_ids}, коэффициент приемки: {acceptance_coefficient}")

        # Определяем конец периода поиска лимитов
        end_date = determine_end_date(date_period)
        
        # Основной цикл поиска лимитов
        while datetime.now() < end_date:
            warehouse_ids_list = parse_warehouse_ids(warehouse_ids)
            limits_data = await get_limits(warehouse_ids_list)

            if not limits_data:
                logging.warning(f"Лимиты не найдены для складов {warehouse_ids}. Повтор через 60 секунд.")
                await asyncio.sleep(60)
                continue

            filtered_limits = filter_limits(limits_data, selected_warehouses, acceptance_coefficient, delivery_type, delivery_type_id, warehouses_data)

            if filtered_limits:
                await send_limit_messages(context, telegram_user_id, filtered_limits)
                break  # Выходим, если лимиты найдены

            logging.info("Лимиты не найдены. Повтор через 90 секунд.")
            await asyncio.sleep(90)

    except Exception as e:
        logging.error(f"Ошибка в фоновом процессе поиска лимитов: {e}")


def determine_end_date(date_period):
    """
    Возвращает дату завершения поиска в зависимости от указанного периода.
    """
    if date_period == 'today':
        return datetime.now() + timedelta(days=1)
    elif date_period == 'tomorrow':
        return datetime.now() + timedelta(days=2)
    elif date_period == 'week':
        return datetime.now() + timedelta(weeks=1)
    else:
        return datetime.now() + timedelta(days=30)  # По умолчанию месяц


def parse_warehouse_ids(warehouse_ids_str):
    """
    Преобразует строку с ID складов в список чисел.
    """
    return [int(x) for x in warehouse_ids_str.split(',') if x]


async def get_limits(warehouse_ids):
    """
    Асинхронная функция для получения лимитов с API Wildberries.
    """
    url = "https://supplies-api.wildberries.ru/api/v1/acceptance/coefficients"
    headers = {"Authorization": "Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjQwOTA0djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc0MjI2NjM5NSwiaWQiOiIwMTkxZmI1Mi1kNGUzLTc3MTAtOWM0MC00ZjVmYmM4MGIzYzYiLCJpaWQiOjQ1MTYxMTc0LCJvaWQiOjMxNjA1NSwicyI6MTA1Niwic2lkIjoiNmU3MTI1NDUtMjRlOC00MWJmLWI0MTktN2ZjOTI1Y2NmYTE0IiwidCI6ZmFsc2UsInVpZCI6NDUxNjExNzR9.c4nF0zK4egZTrp7MZPILKGHBxgWY-KNZ0jDmW4HLCymM68HcjlaRlUFBid4bxwyfSt9eMqiGTTIkB7L6TFqsjA"}
    
    params = {"warehouseIDs": ','.join(map(str, warehouse_ids))}
    logging.info(f"Отправляем запрос с параметрами: {params}")

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                limits_data = await response.json()
                logging.info(f"Получены лимиты: {limits_data}")
                return limits_data
            else:
                error_text = await response.text()
                logging.error(f"Ошибка при запросе к API: {response.status}, {error_text}")
                return None


def filter_limits(limits_data, selected_warehouses, acceptance_coefficient, delivery_type, delivery_type_id, warehouses_data):
    """
    Фильтрация полученных лимитов на основе введенных параметров.
    """
    filtered_limits = []
    warehouses_by_id = {wh[5]: wh for wh in warehouses_data}

    for limit in limits_data:
        try:
            warehouse_id = limit['warehouseID']
            coefficient = limit['coefficient']
            box_type_id = limit.get('boxTypeID')

            if warehouse_id not in warehouses_by_id:
                continue
            if coefficient == -1:
                continue  # Пропускаем склады с коэффициентом -1

            warehouse_name = warehouses_by_id[warehouse_id][1]
            if warehouse_name in selected_warehouses and coefficient <= acceptance_coefficient:
                if delivery_type == 'delivery_qr_box' and limit['boxTypeName'] == 'QR поставка коробами':
                    filtered_limits.append(limit)
                elif box_type_id is not None and delivery_type_id == box_type_id:
                    filtered_limits.append(limit)

        except KeyError as e:
            logging.error(f"Отсутствующий ключ в данных лимита: {e}")

    # Сортировка лимитов по коэффициенту
    return sorted(filtered_limits, key=lambda x: x['coefficient'], reverse=True)


async def send_limit_messages(context: CallbackContext, telegram_user_id, filtered_limits):
    """
    Отправка сообщений с найденными лимитами пользователю.
    """
    for limit in filtered_limits:
        message = (
            f"Лимит найден! ✅\n"
            f"🏦 Склад: {limit['warehouseName']}\n"
            f"📦 Тип приемки: {limit['boxTypeName']}\n"
            f"💸 Коэффициент: {limit['coefficient']}\n"
            f"📅 Дата: {limit['date']}"
        )
        logging.info(f"Отправляем сообщение пользователю {telegram_user_id}")
        await context.bot.send_message(chat_id=telegram_user_id, text=message)


async def handle_receipt_photo(update: Update, context: CallbackContext):
    if context.user_data.get('awaiting_receipt'):
        request_id = context.user_data.get('request_id')
        user_id = context.user_data.get('user_id')  # Предполагаем, что user_id сохраняется где-то в контексте

        if update.message.photo:
            photo_file = await update.message.photo[-1].get_file()
            photo_data = await photo_file.download_as_bytearray()

            connection = await init_db()

            # Обновляем photo в таблице requests
            query = """
            UPDATE requests
            SET photo = $1
            WHERE request_id = $2
            """
            await connection.execute(query, photo_data, int(request_id))

            # Обновляем is_paid в таблице users
            query = """
            UPDATE users
            SET is_paid = TRUE
            WHERE user_id = $1
            """
            await connection.execute(query, user_id)

            await connection.close()

            keyboard = [
                [InlineKeyboardButton("Главное меню", callback_data='main_menu')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"Чек получен. Ваша заявка {request_id} подтверждена.\nПоиск лимитов активирован ✅",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text("Пожалуйста, отправьте фотографию чека.")
    else:
        await update.message.reply_text("Вы пока не создали заявку для проверки чека.")


  

async def main_menu(update: Update, context: CallbackContext):
    await start(update, context)


def main():
    job_queue = JobQueue()

    application = (Application.builder().token("7588760839:AAFQNSlWVM2TA1rXLWCQ3ZsNqAX4dwxvUKM")
                   .job_queue(job_queue)
                   .build()
                   )
    

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(add_request, pattern='^add_request$'))
    application.add_handler(CallbackQueryHandler(select_warehouse_for_limits, pattern='^select_warehouses$'))
    application.add_handler(CallbackQueryHandler(warehouse_selected, pattern='^warehouse_'))
    application.add_handler(CallbackQueryHandler(next_step, pattern='^next_step$'))
    application.add_handler(CallbackQueryHandler(delivery_type_selected, pattern='^delivery_'))
    application.add_handler(CallbackQueryHandler(date_selected, pattern='^date_'))
    application.add_handler(CallbackQueryHandler(coefficient_selected, pattern='^coef_'))
    application.add_handler(CallbackQueryHandler(search_limits, pattern='^search_limits$'))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_date_input))
    application.add_handler(CallbackQueryHandler(main_menu, pattern='^main_menu$'))
    application.add_handler(CallbackQueryHandler(select_warehouse_main, pattern='^select_warehouses_main$'))
    application.add_handler(CallbackQueryHandler(top_warehouses_main, pattern='^top_warehouses_main$'))
    application.add_handler(MessageHandler(filters.PHOTO, handle_receipt_photo))
    application.add_handler(CallbackQueryHandler(change_count, pattern='change_count'))
    application.add_handler(CallbackQueryHandler(replace_warehouse, pattern='replace_warehouse'))
    application.add_handler(CallbackQueryHandler(count_selected, pattern=r'count_\d+'))
    application.add_handler(CallbackQueryHandler(handle_replace_warehouse, pattern=r'replace_\d+'))
    application.add_handler(CallbackQueryHandler(region_selected, pattern=r'region_.*'))

    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    

    # Запускаем задачи для поиска лимитов при запуске бота
    loop.create_task(search_limits_job(application))

    application.run_polling()


if __name__ == "__main__":
    main()