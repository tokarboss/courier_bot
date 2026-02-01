import asyncio
import logging
import gspread
import os
import time
from datetime import datetime, timedelta
# Заменяем старый oauth2client на новый google.auth для работы с временем
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, 
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from geopy.geocoders import Nominatim

# --- БЛОК ДИАГНОСТИКИ (ВЫПОЛНЯЕТСЯ ПРИ СТАРТЕ) ---
print(f"--- ДИАГНОСТИКА ---")
print(f"Системное время: {time.ctime()}")
if os.path.exists("credentials.json"):
    size = os.path.getsize("credentials.json")
    print(f"Файл credentials.json найден. Размер: {size} байт")
    try:
        with open("credentials.json", "r") as f:
            content = f.read()
            if "-----BEGIN PRIVATE KEY-----" in content:
                print("✅ Заголовок ключа найден")
            else:
                print("❌ ЗАГОЛОВОК КЛЮЧА НЕ НАЙДЕН! Файл поврежден или имеет неверный формат.")
    except Exception as e:
        print(f"❌ Ошибка чтения файла: {e}")
else:
    print("❌ ФАЙЛ credentials.json НЕ НАЙДЕН В КОРНЕВОЙ ПАПКЕ")
print(f"-------------------")

# --- НАСТРОЙКИ ---
TOKEN = "8578056545:AAEWWP_JyQ2SDCFmQ-IwZhk-cfF0AozFYqo"
GROUP_ID = -1003891823517  
ADMIN_ID = 5859374128  # Твой ID
SHEET_NAME = "Заявки Курьеры Яндекс Еда"
TRAINING_LINK = "https://t.me/your_training_bot_or_channel" 
PARTNER_LINK = "https://clck.ru/3RZuNV" 

CONFIG = {"auto_distribute": False, "active_managers": []}
scheduler = AsyncIOScheduler()
geolocator = Nominatim(user_agent="yandex_courier_bot_v1")

class CourierForm(StatesGroup):
    city = State()
    citizenship = State()
    transport = State()
    phone = State()

class AdminStates(StatesGroup):
    mailing_text = State()

# --- ЛОГИКА ТАБЛИЦ (С ПОДМЕНОЙ ВРЕМЕНИ) ---
def get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # ЛОГИКА ПОДМЕНЫ/СИНХРОНИЗАЦИИ ВРЕМЕНИ:
    # Используем service_account.Credentials вместо oauth2client
    creds = service_account.Credentials.from_service_account_file(
        "credentials.json", scopes=scope
    )
    
    # Принудительно запрашиваем обновление, чтобы синхронизироваться с Google, 
    # даже если системные часы сервера спешат или отстают
    auth_request = Request()
    creds.refresh(auth_request)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open(SHEET_NAME)
    
    main_sheet = spreadsheet.sheet1
    
    try:
        bl_sheet = spreadsheet.worksheet("Blacklist")
    except:
        bl_sheet = spreadsheet.add_worksheet(title="Blacklist", rows="1000", cols="3")
        bl_sheet.append_row(["User ID", "Username", "Date"])
    
    try:
        log_s = spreadsheet.worksheet("Logs")
    except:
        log_s = spreadsheet.add_worksheet(title="Logs", rows="5000", cols="4")
        log_s.append_row(["Время", "Событие", "Детали", "Кто выполнил"])

    try:
        u_sheet = spreadsheet.worksheet("Users")
    except:
        u_sheet = spreadsheet.add_worksheet(title="Users", rows="10000", cols="2")
        u_sheet.append_row(["User ID", "Username"])
        
    return main_sheet, bl_sheet, log_s, u_sheet

# Инициализация листов
sheet, blacklist_sheet, log_sheet, users_sheet = get_sheets()

# --- СЕРВИСНЫЕ ФУНКЦИИ ---

def add_user_to_base(user_id, username):
    try:
        ids = users_sheet.col_values(1)
        if str(user_id) not in ids:
            users_sheet.append_row([str(user_id), f"@{username}" if username else "NoName"])
    except: pass

def add_log(event, details, initiator="Система"):
    try:
        now = datetime.now().strftime("%d.%m %H:%M:%S")
        log_sheet.append_row([now, event, str(details), str(initiator)])
    except Exception as e:
        logging.error(f"Ошибка логирования: {e}")

async def clear_logs_job():
    try:
        num_rows = len(log_sheet.get_all_values())
        if num_rows > 1:
            log_sheet.delete_rows(2, num_rows)
            add_log("Очистка", "Таблица логов очищена по расписанию")
    except Exception as e:
        logging.error(f"Ошибка при очистке логов: {e}")

def check_duplicate(user_id):
    try:
        ids = sheet.col_values(9)
        return str(user_id) in ids
    except: return False

def get_status_col():
    try:
        headers = sheet.row_values(1)
        for i, h in enumerate(headers):
            if h.lower().strip() == "статус": return i + 1
        return 8
    except: return 8

def get_manager_stats(username):
    try:
        if not username: return 0, 0, 0, 0
        records = sheet.get_all_records()
        t_all, l_all, t_day, l_day = 0, 0, 0, 0
        today = datetime.now().strftime("%Y-%m-%d")
        m_user = f"@{username.lower()}"
        for row in records:
            row_clean = {str(k).lower().strip(): str(v).lower() for k, v in row.items()}
            stat = row_clean.get('статус', '')
            if m_user in stat:
                t_all += 1
                if "лид" in stat: l_all += 1
                first_val = str(list(row.values())[0])
                if today in first_val:
                    t_day += 1
                    if "лид" in stat: l_day += 1
        return t_all, l_all, t_day, l_day
    except: return 0, 0, 0, 0

def get_global_rating():
    try:
        records = sheet.get_all_records()
        stats = {}
        for row in records:
            row_clean = {str(k).lower().strip(): str(v) for k, v in row.items()}
            stat = row_clean.get('статус', '')
            if "✅ лид" in stat.lower() and "@" in stat:
                try:
                    manager = "@" + stat.split("@")[-1].split(")")[0].strip()
                    stats[manager] = stats.get(manager, 0) + 1
                except: continue
        return sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
    except: return []

# --- РАСПРЕДЕЛЕНИЕ ---

async def distribute_lead(idx: int):
    if not CONFIG["auto_distribute"] or not CONFIG["active_managers"]:
        return False
    manager_id = CONFIG["active_managers"].pop(0)
    CONFIG["active_managers"].append(manager_id)
    
    row = sheet.row_values(idx + 1)
    name, city, phone, u_id = row[1], row[3], row[6], row[8]
    
    try:
        m_info = await bot.get_chat(manager_id)
        m_user = f"@{m_info.username}" if m_info.username else m_info.full_name
        sheet.update_cell(idx + 1, get_status_col(), f"В работе ({m_user})")
        add_log("Авто-распределение", f"Заявка №{idx}", m_user)
        
        clean_phone = phone.replace('+', '').strip()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать", url=f"https://t.me/+{clean_phone}")],
            [InlineKeyboardButton(text="✅ ЛИД", callback_data=f"fin_{idx}_LID_{u_id}"), 
             InlineKeyboardButton(text="❌ НЕ ЛИД", callback_data=f"fin_{idx}_NOT_{u_id}")]
        ])
        
        text = (f"📥 **АВТО-НАЗНАЧЕНИЕ №{idx}**\n━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 **Имя:** {name}\n"
                f"🏙 **Город:** {city}\n"
                f"📞 **Телефон:** `{phone}`")
        
        await bot.send_message(manager_id, text, reply_markup=kb, parse_mode="Markdown")
        await bot.send_message(GROUP_ID, f"🤖 Заявка №{idx} распределена на {m_user}")
        return True
    except: return False

# --- БОТ И ДИСПЕТЧЕР ---
bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- КЛАВИАТУРЫ ---
kb_geo = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="📍 Отправить местоположение", request_location=True)],
    [KeyboardButton(text="⌨️ Ввести город вручную")]
], resize_keyboard=True)

kb_manager = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="📊 Моя статистика"), KeyboardButton(text="🏆 Рейтинг")],
    [KeyboardButton(text="✅ На смене / ❌ Уйти")],
    [KeyboardButton(text="🔗 Моя ссылка")]
], resize_keyboard=True)

kb_transport = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="Пеший 🏃"), KeyboardButton(text="Вело 🚲")],
    [KeyboardButton(text="Авто 🚗"), KeyboardButton(text="Самокат 🛴")]
], resize_keyboard=True)

kb_citizenship = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="РФ 🇷🇺"), KeyboardButton(text="СНГ 🇰🇬")],
    [KeyboardButton(text="Другое 🌍")]
], resize_keyboard=True)

# --- АДМИНКА ---

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    status = "🟢 ВКЛ" if CONFIG["auto_distribute"] else "🔴 ВЫКЛ"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Автораспределение: {status}", callback_data="toggle_dist")],
        [InlineKeyboardButton(text="👥 Кто на смене?", callback_data="check_managers")],
        [InlineKeyboardButton(text="📜 Последние 50 логов", callback_data="view_logs_50")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="start_mailing")]
    ])
    await message.answer("⚙️ Панель управления:", reply_markup=kb)

@dp.callback_query(F.data == "toggle_dist")
async def cb_toggle(callback: CallbackQuery):
    CONFIG["auto_distribute"] = not CONFIG["auto_distribute"]
    await callback.message.delete()
    await admin_panel(callback.message)
    add_log("Система", f"Автораспределение: {CONFIG['auto_distribute']}", callback.from_user.full_name)

@dp.callback_query(F.data == "start_mailing")
async def cb_start_mailing(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("📝 Введите текст для рассылки всем пользователям:")
    await state.set_state(AdminStates.mailing_text)
    await callback.answer()

@dp.message(AdminStates.mailing_text)
async def process_mailing(message: types.Message, state: FSMContext):
    u_ids = users_sheet.col_values(1)[1:] 
    await message.answer(f"🚀 Начинаю рассылку на {len(u_ids)} пользователей...")
    count = 0
    for uid in u_ids:
        try:
            await bot.send_message(uid, message.text)
            count += 1
            await asyncio.sleep(0.05) 
        except: continue
    await message.answer(f"✅ Рассылка завершена!\nУспешно отправлено: {count}")
    add_log("Рассылка", f"Отправлено: {count} чел.", message.from_user.full_name)
    await state.clear()

@dp.callback_query(F.data == "view_logs_50")
async def cb_view_logs(callback: CallbackQuery):
    try:
        all_logs = log_sheet.get_all_values()
        logs = all_logs[-50:] if len(all_logs) > 50 else all_logs[1:]
        res = "📜 **Последние 50 логов:**\n"
        for l in logs:
            res += f"▫️ `{l[0]}` | {l[1]} | {l[3]}\n"
        await callback.message.answer(res[:4000], parse_mode="Markdown")
    except: await callback.answer("Ошибка чтения логов")
    await callback.answer()

@dp.callback_query(F.data == "check_managers")
async def cb_check(callback: CallbackQuery):
    if not CONFIG["active_managers"]:
        return await callback.answer("На смене никого нет", show_alert=True)
    names = []
    for m_id in CONFIG["active_managers"]:
        try:
            c = await bot.get_chat(m_id)
            names.append(f"@{c.username}" if c.username else c.full_name)
        except: names.append(f"ID: {m_id}")
    await callback.message.answer("👥 На смене:\n" + "\n".join(names))
    await callback.answer()

# --- ОБРАБОТЧИКИ АНКЕТЫ ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    add_user_to_base(message.from_user.id, message.from_user.username)
    await state.clear()
    try:
        member = await bot.get_chat_member(GROUP_ID, message.from_user.id)
        if member.status in ['member', 'creator', 'administrator']:
            return await message.answer("👋 Меню менеджера:", reply_markup=kb_manager)
    except: pass
    await message.answer("Привет! В каком городе хочешь работать?", reply_markup=kb_geo)
    await state.set_state(CourierForm.city)

@dp.message(CourierForm.city, F.location)
async def p1_location(message: types.Message, state: FSMContext):
    lat, lon = message.location.latitude, message.location.longitude
    try:
        location = geolocator.reverse(f"{lat}, {lon}", language="ru")
        address = location.raw.get('address', {})
        city = address.get('city') or address.get('town') or address.get('village') or "Не определен"
        road = address.get('road', '')
        display_name = f"{city}, {road}".strip(", ")
    except Exception:
        display_name = f"Координаты: {lat}, {lon}"

    await state.update_data(city=display_name)
    await message.answer(f"📍 Твой город: {display_name}\n\nТвое гражданство?", reply_markup=kb_citizenship)
    await state.set_state(CourierForm.citizenship)

@dp.message(CourierForm.city, F.text == "⌨️ Ввести город вручную")
async def p1_manual(message: types.Message):
    await message.answer("Напиши название своего города:", reply_markup=ReplyKeyboardRemove())

@dp.message(CourierForm.city)
async def p1_text(message: types.Message, state: FSMContext):
    await state.update_data(city=message.text)
    await message.answer("Твое гражданство?", reply_markup=kb_citizenship)
    await state.set_state(CourierForm.citizenship)

@dp.message(CourierForm.citizenship)
async def p2(message: types.Message, state: FSMContext):
    await state.update_data(citizenship=message.text)
    await message.answer("На чем планируешь работать?", reply_markup=kb_transport)
    await state.set_state(CourierForm.transport)

@dp.message(CourierForm.transport)
async def p3(message: types.Message, state: FSMContext):
    await state.update_data(transport=message.text)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📱 Отправить контакт", request_contact=True)]], resize_keyboard=True)
    await message.answer("Отправь контакт кнопкой ниже:", reply_markup=kb)
    await state.set_state(CourierForm.phone)

@dp.message(CourierForm.phone, F.contact)
async def p4(message: types.Message, state: FSMContext):
    u_id = message.from_user.id
    if check_duplicate(u_id):
        return await message.answer("⚠️ Ты уже оставлял заявку!")

    data = await state.get_data()
    row = [datetime.now().strftime("%Y-%m-%d %H:%M"), message.from_user.full_name, f"@{message.from_user.username}", data.get('city'), data.get('citizenship'), data.get('transport'), message.contact.phone_number, "Новая", str(u_id)]
    sheet.append_row(row, value_input_option='USER_ENTERED')
    idx = len(sheet.get_all_values()) - 1
    
    add_log("Новая заявка", f"№{idx}", message.from_user.full_name)
    await message.answer("✅ Заявка принята! Ожидай звонка.", reply_markup=ReplyKeyboardRemove())
    await state.clear()

    if not await distribute_lead(idx):
        text = (f"🚀 **НОВАЯ ЗАЯВКА №{idx}**\n━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 **Имя:** {message.from_user.full_name}\n🏙 **Город:** {data.get('city')}\n"
                f"🚲 **Транспорт:** {data.get('transport')}\n📞 **Телефон:** 🔒 Скрыт")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🤝 Взять в работу", callback_data=f"take_{idx}")]])
        await bot.send_message(GROUP_ID, text, reply_markup=kb, parse_mode="Markdown")

# --- CALLBACKS ПРИНЯТИЯ В РАБОТУ ---

@dp.callback_query(F.data.startswith("take_"))
async def cb_take(callback: CallbackQuery):
    idx = int(callback.data.split("_")[1])
    row = sheet.row_values(idx + 1)
    name, city, phone, u_id = row[1], row[3], row[6], row[8]
    
    m_user = f"@{callback.from_user.username}" if callback.from_user.username else callback.from_user.full_name
    sheet.update_cell(idx + 1, get_status_col(), f"В работе ({m_user})")
    add_log("Захват", f"Заявка №{idx}", m_user)
    
    await callback.message.edit_text(f"🔴 ЗАЯВКА №{idx} В РАБОТЕ ({m_user})")
    
    clean_phone = phone.replace('+', '').strip()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Написать", url=f"https://t.me/+{clean_phone}")],
        [InlineKeyboardButton(text="✅ ЛИД", callback_data=f"fin_{idx}_LID_{u_id}"), 
         InlineKeyboardButton(text="❌ НЕ ЛИД", callback_data=f"fin_{idx}_NOT_{u_id}")]
    ])
    
    text = (f"📋 **ЗАЯВКА №{idx}**\n━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 **Имя:** {name}\n"
            f"🏙 **Город:** {city}\n"
            f"📞 **Телефон:** `{phone}`")
            
    await bot.send_message(callback.from_user.id, text, reply_markup=kb, parse_mode="Markdown")

@dp.callback_query(F.data.startswith("fin_"))
async def cb_fin(callback: CallbackQuery):
    parts = callback.data.split("_")
    idx, res, u_id = parts[1], parts[2], parts[3]
    st = "✅ ЛИД" if res == "LID" else "❌ НЕ ЛИД"
    m_user = f"@{callback.from_user.username}" if callback.from_user.username else callback.from_user.full_name
    
    sheet.update_cell(int(idx) + 1, get_status_col(), f"{st} ({m_user})")
    add_log("Финиш", f"№{idx} {st}", m_user)
    
    emoji = "🎉" if res == "LID" else "📁"
    group_msg = f"{emoji} **Заявка №{idx} закрыта!**\nРезультат: {st}\nМенеджер: {m_user}"
    await bot.send_message(GROUP_ID, group_msg, parse_mode="Markdown")
    
    if res == "LID":
        try: await bot.send_message(u_id, f"🎉 Одобрено! Обучение: {TRAINING_LINK}")
        except: pass
    
    await callback.message.edit_text(f"🏁 Заявка №{idx} закрыта: {st}")

# --- МЕНЕДЖЕРЫ ---

@dp.message(F.text == "✅ На смене / ❌ Уйти")
async def toggle_work(message: types.Message):
    uid = message.from_user.id
    if uid in CONFIG["active_managers"]:
        CONFIG["active_managers"].remove(uid)
        await message.answer("❌ Ты ушел со смены.")
    else:
        CONFIG["active_managers"].append(uid)
        await message.answer("✅ Ты на смене!")

@dp.message(F.text == "📊 Моя статистика")
async def stats_h(message: types.Message):
    t_a, l_a, t_d, l_d = get_manager_stats(message.from_user.username)
    await message.answer(f"📈 Твои итоги:\n📅 Сегодня: {t_d} взял, {l_d} лидов\n🌍 Всего: {t_a} взял, {l_a} лидов")

@dp.message(F.text == "🏆 Рейтинг")
async def rating_h(message: types.Message):
    rating = get_global_rating()
    res = "🏆 ТОП МЕНЕДЖЕРОВ:\n" + "\n".join([f"{u} — {c} лидов" for u, c in rating])
    await message.answer(res)

@dp.message(F.text == "🔗 Моя ссылка")
async def link_h(message: types.Message):
    await message.answer(f"🔗 Ссылка: `{PARTNER_LINK}`", parse_mode="Markdown")

# --- ЗАПУСК ---
async def main():
    scheduler.add_job(clear_logs_job, 'interval', days=3)
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())