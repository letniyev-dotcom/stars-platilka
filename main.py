import asyncio
import random
import string
import re
import aiosqlite
import html
import logging
import uuid
import os
from contextlib import suppress

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, LabeledPrice, PreCheckoutQuery, InlineQueryResultArticle, InputTextMessageContent
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ------------------- КОНФИГУРАЦИЯ -------------------
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
BOT_USERNAME = os.getenv("BOT_USERNAME")
XTR_TO_RUB_RATE = 1.8
DB_NAME = "bot_database.db"
CONFETTI_EFFECT_ID = "5046509860389126442"
CODE_LENGTH = 4
MIN_WITHDRAWAL_RUB = 100  # Минимальная сумма вывода в рублях

# Эмодзи
EMOJI_BANK_REQ = "5192678313415434135"  # 🏦 для требования реквизитов
EMOJI_PHONE = "5409357944619802453"     # 📱 телефон
EMOJI_T_BANK = "5192689390136089826"    # 🏦 иконка банка (пример)

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# ------------------- БАЗА ДАННЫХ -------------------
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Создаем базовую таблицу, если её нет
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER DEFAULT 0
            )
        """)
        
        # Миграция: пытаемся добавить новые колонки, если их нет (для старой БД)
        columns_to_add = [
            ("payment_method", "TEXT"),
            ("payment_number", "TEXT"),
            ("payment_bank", "TEXT")
        ]
        
        for col_name, col_type in columns_to_add:
            try:
                await db.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
            except Exception:
                # Колонка уже существует, игнорируем ошибку
                pass

        # Таблица выводов
        await db.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                rub_amount INTEGER,
                details TEXT,
                user_message_id INTEGER,
                status TEXT DEFAULT 'wait'
            )
        """)
        # Таблица использованных ссылок
        await db.execute("""
            CREATE TABLE IF NOT EXISTS used_links (
                link_uuid TEXT PRIMARY KEY
            )
        """)
        await db.commit()

async def get_user_data(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        # Выбираем все поля. Если запись есть, но поля NULL - это ок.
        async with db.execute("SELECT balance, payment_method, payment_number, payment_bank FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def add_balance(user_id: int, amount: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)", (user_id,))
        await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()

async def save_payment_details(user_id: int, method: str, number: str, bank: str = None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)", (user_id,))
        await db.execute("""
            UPDATE users 
            SET payment_method = ?, payment_number = ?, payment_bank = ? 
            WHERE user_id = ?
        """, (method, number, bank, user_id))
        await db.commit()

async def reset_balance_safe(user_id: int) -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN")
        async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or row[0] <= 0:
                await db.rollback()
                return 0
            amount = row[0]
        
        await db.execute("UPDATE users SET balance = 0 WHERE user_id = ?", (user_id,))
        await db.commit()
        return amount

async def create_withdrawal(user_id: int, amount: int, rub_amount: int, details: str, message_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "INSERT INTO withdrawals (user_id, amount, rub_amount, details, user_message_id, status) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, amount, rub_amount, details, message_id, 'wait')
        )
        await db.commit()
        return cursor.lastrowid

async def get_withdrawal(wd_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, amount, user_message_id, status FROM withdrawals WHERE id = ?", (wd_id,)) as cursor:
            return await cursor.fetchone()

async def update_withdrawal_status(wd_id: int, new_status: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE withdrawals SET status = ? WHERE id = ?", (new_status, wd_id))
        await db.commit()

async def is_link_used(uuid_str: str) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT 1 FROM used_links WHERE link_uuid = ?", (uuid_str,)) as cursor:
            return bool(await cursor.fetchone())

async def mark_link_used(uuid_str: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO used_links (link_uuid) VALUES (?)", (uuid_str,))
        await db.commit()

# ------------------- ХРАНИЛИЩЕ (RAM) -------------------
active_sessions = {} 
merchant_transactions = {}

class PaymentState(StatesGroup):
    waiting_for_input = State()

class ProfileState(StatesGroup):
    waiting_for_sbp_phone = State()
    waiting_for_sbp_bank = State()
    waiting_for_card = State()
    confirm_sbp = State()

# ------------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ -------------------
def generate_code():
    while True:
        code = ''.join(random.choices(string.digits, k=CODE_LENGTH))
        if code not in active_sessions:
            return code

def get_user_link(user_id, first_name):
    safe_name = html.escape(first_name or "пользователь")
    return f"<a href='tg://user?id={user_id}'>{safe_name}</a>"

def normalize_phone(phone_raw):
    digits = re.sub(r'\D', '', phone_raw)
    if len(digits) == 11:
        if digits.startswith('8'):
            digits = '7' + digits[1:]
        elif digits.startswith('7'):
            pass 
    elif len(digits) == 10 and digits.startswith('9'):
        digits = '7' + digits
    return f"+{digits}"

async def main_menu_kb(user_id):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📥 принять оплату", callback_data="receive_payment"))
    builder.row(InlineKeyboardButton(text="💸 оплатить", callback_data="make_payment"))
    builder.row(InlineKeyboardButton(text="👤 профиль", callback_data="open_profile"))
    return builder.as_markup()

def code_generation_kb():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔄 сгенерировать новый", callback_data="regenerate_code"))
    builder.row(InlineKeyboardButton(text="🔙 назад", callback_data="back_to_menu"))
    return builder.as_markup()

def confirm_invoice_kb(code, amount):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ подтвердить", callback_data=f"confirm_{code}_{amount}"),
        InlineKeyboardButton(text="❌ отменить", callback_data="cancel_invoice")
    )
    return builder.as_markup()

def admin_withdrawal_kb(current_status, wd_id):
    builder = InlineKeyboardBuilder()
    if current_status == 'wait':
        builder.row(InlineKeyboardButton(text="👀 на рассмотрении", callback_data=f"setstat_review_{wd_id}"))
    elif current_status == 'review':
        builder.row(InlineKeyboardButton(text="🚀 скоро отправим", callback_data=f"setstat_soon_{wd_id}"))
    elif current_status == 'soon':
        builder.row(InlineKeyboardButton(text="✅ отправили", callback_data=f"setstat_done_{wd_id}"))
    return builder.as_markup() if current_status != 'done' else None

# ------------------- ИНЛАЙН РЕЖИМ -------------------
@router.inline_query()
async def inline_query_handler(query: types.InlineQuery):
    amount_str = query.query.strip()
    if not amount_str.isdigit():
        return
    
    amount = int(amount_str)
    if amount <= 0 or amount > 10000:
        return
    
    merchant_id = query.from_user.id
    unique_link_id = uuid.uuid4().hex[:12]
    
    text = f"оплатите счёт на {amount} stars <tg-emoji emoji-id=\"4983746717313664194\">⭐</tg-emoji>"
    start_param = f"inline_pay_{amount}_{merchant_id}_{unique_link_id}"
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="оплатить", url=f"https://t.me/{BOT_USERNAME}?start={start_param}"))
    
    results = [
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=f"отправить счёт на {amount} ⭐",
            description="нажмите чтобы создать и отправить счёт пользователю",
            input_message_content=InputTextMessageContent(message_text=text, parse_mode="HTML"),
            reply_markup=kb.as_markup(),
            thumb_url="https://files.catbox.moe/5uy724.png",
            thumb_width=512,
            thumb_height=512
        )
    ]
    await query.answer(results, cache_time=1)

# Обработка /start
@router.message(Command("start"))
async def cmd_start(message: types.Message, command: CommandObject, state: FSMContext):
    await state.clear()
    await add_balance(message.from_user.id, 0)
    
    name = html.escape(message.from_user.first_name).lower()
    args = command.args

    if args and args.startswith("inline_pay_"):
        try:
            parts = args.split("_")
            if len(parts) >= 4: 
                amount = int(parts[2])
                merchant_id = int(parts[3])
                link_uuid = parts[4]
                
                if await is_link_used(link_uuid):
                    await message.answer("❌ этот счёт уже оплачен или недействителен")
                    return

                payer_id = message.from_user.id
                if payer_id == merchant_id:
                    await message.answer("нельзя оплатить свой счёт")
                    return
                
                payload = f"inline_inv_{merchant_id}_{link_uuid}"
                prices = [LabeledPrice(label="услуга", amount=amount)]
                
                try:
                    invoice_msg = await bot.send_invoice(
                        chat_id=payer_id, title="оплата", description=f"перевод {amount} stars",
                        payload=payload, provider_token="", currency="XTR", prices=prices, start_parameter="pay"
                    )
                    
                    merchant_text = "<tg-emoji emoji-id=\"6113789201717660877\">⏳</tg-emoji> счёт отправлен, ждем оплату.."
                    try:
                        merchant_msg = await bot.send_message(merchant_id, merchant_text, parse_mode="HTML")
                        merchant_msg_id = merchant_msg.message_id
                    except Exception:
                        merchant_msg_id = None
                    
                    merchant_transactions[payload] = {
                        "merchant_id": merchant_id, 
                        "merchant_msg_id": merchant_msg_id,
                        "payer_id": payer_id,
                        "invoice_msg_id": invoice_msg.message_id,
                        "link_uuid": link_uuid,
                        "original_chat_id": None,
                        "original_msg_id": None
                    }
                except Exception as e:
                    logger.error(f"Ошибка отправки инвойса: {e}")
                    await message.answer("ошибка отправки счёта")
                return
        except Exception as e:
            logger.error(f"Ошибка парсинга start: {e}")
    
    await message.answer(
        f"привет, {name} <tg-emoji emoji-id=\"5472055112702629499\">👋</tg-emoji>\nчто хочешь сделать?",
        reply_markup=await main_menu_kb(message.from_user.id),
        parse_mode="HTML"
    )

# ------------------- ПРОФИЛЬ И РЕКВИЗИТЫ -------------------
@router.callback_query(F.data == "open_profile")
async def open_profile(callback: types.CallbackQuery):
    await show_profile(callback.message, callback.from_user.id, is_edit=True)

async def show_profile(message: types.Message, user_id: int, is_edit=True):
    user_data = await get_user_data(user_id)
    # Если user_data пустой или неполный (база была пустой), обрабатываем
    if not user_data:
        balance = 0
        p_method, p_number, p_bank = None, None, None
    else:
        balance = user_data[0]
        # Используем get с дефолтным значением, если кортеж короче ожидаемого (защита от старых данных в памяти)
        try:
            p_method = user_data[1]
            p_number = user_data[2]
            p_bank = user_data[3]
        except IndexError:
            p_method, p_number, p_bank = None, None, None

    rub_balance = int(balance * XTR_TO_RUB_RATE)
    
    text = f"ваш баланс: {balance} <tg-emoji emoji-id=\"4983746717313664194\">⭐</tg-emoji> • {rub_balance} ₽\n"
    
    kb = InlineKeyboardBuilder()
    
    if not p_number:
        text += f"для вывода средств добавьте <tg-emoji emoji-id=\"{EMOJI_BANK_REQ}\">🏦</tg-emoji> введите сбп или 💳 карту"
        kb.row(InlineKeyboardButton(text="добавить реквизиты", callback_data="add_payment_details"))
    else:
        text += "ваши реквизиты:\n<blockquote>"
        if p_method == 'sbp':
            text += f"сбп • {p_number} • {p_bank}"
        else:
            text += f"карта • {p_number}"
        text += "</blockquote>"
        
        kb.row(InlineKeyboardButton(text="✏️ изменить реквизиты", callback_data="add_payment_details"))
        kb.row(InlineKeyboardButton(text="💎 вывести", callback_data="withdraw_funds"))

    kb.row(InlineKeyboardButton(text="назад", callback_data="back_to_menu"))
    
    if is_edit:
        await message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())
    else:
        await message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())

# --- Добавление реквизитов ---
@router.callback_query(F.data == "add_payment_details")
async def start_add_details(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(interface_msg_id=callback.message.message_id)
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="сбп", callback_data="set_method_sbp"), 
           InlineKeyboardButton(text="карту", callback_data="set_method_card"))
    kb.row(InlineKeyboardButton(text="назад", callback_data="open_profile"))
    await callback.message.edit_text("что хотите добавить?)", reply_markup=kb.as_markup())

# --- СБП FLOW ---
@router.callback_query(F.data == "set_method_sbp")
async def ask_sbp_phone(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(ProfileState.waiting_for_sbp_phone)
    text = f"<tg-emoji emoji-id=\"{EMOJI_PHONE}\">📱</tg-emoji> введите номер телефона, который привязан к вашему банку"
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="назад", callback_data="open_profile"))
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())

@router.message(ProfileState.waiting_for_sbp_phone)
async def process_sbp_phone(message: types.Message, state: FSMContext):
    with suppress(Exception):
        await message.delete()
    phone = normalize_phone(message.text)
    if len(phone) < 10: 
        return 
    await state.update_data(sbp_phone=phone)
    await state.set_state(ProfileState.waiting_for_sbp_bank)
    
    data = await state.get_data()
    msg_id = data.get("interface_msg_id")
    
    text = f"теперь отправьте название вашего банка, например <tg-emoji emoji-id=\"{EMOJI_T_BANK}\">🏦</tg-emoji> т-банк"
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="назад", callback_data="open_profile"))
    
    if msg_id:
        await bot.edit_message_text(text=text, chat_id=message.chat.id, message_id=msg_id, parse_mode="HTML", reply_markup=kb.as_markup())

@router.message(ProfileState.waiting_for_sbp_bank)
async def process_sbp_bank(message: types.Message, state: FSMContext):
    with suppress(Exception):
        await message.delete()
    bank_name = html.escape(message.text)
    await state.update_data(sbp_bank=bank_name)
    
    data = await state.get_data()
    msg_id = data.get("interface_msg_id")
    phone = data.get("sbp_phone")
    
    await state.set_state(ProfileState.confirm_sbp)
    text = (f"ваш номер: {phone}\nваш банк: {bank_name}\nвсё верно?)")
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="✅ подтвердить", callback_data="save_sbp"),
           InlineKeyboardButton(text="✏️ изменить", callback_data="set_method_sbp"))
    
    if msg_id:
        await bot.edit_message_text(text=text, chat_id=message.chat.id, message_id=msg_id, parse_mode="HTML", reply_markup=kb.as_markup())

@router.callback_query(F.data == "save_sbp")
async def save_sbp_data(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    phone = data.get("sbp_phone")
    bank = data.get("sbp_bank")
    await save_payment_details(callback.from_user.id, "sbp", phone, bank)
    await state.clear()
    await show_profile(callback.message, callback.from_user.id, is_edit=True)

# --- CARD FLOW ---
@router.callback_query(F.data == "set_method_card")
async def ask_card_number(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(ProfileState.waiting_for_card)
    text = "введите номер карты"
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="назад", callback_data="open_profile"))
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())

@router.message(ProfileState.waiting_for_card)
async def process_card(message: types.Message, state: FSMContext):
    with suppress(Exception):
        await message.delete()
    card_num = re.sub(r'\D', '', message.text) 
    if len(card_num) < 13: 
        return
    await save_payment_details(message.from_user.id, "card", card_num, None)
    
    data = await state.get_data()
    msg_id = data.get("interface_msg_id")
    await state.clear()
    
    if msg_id:
        user_data = await get_user_data(message.from_user.id)
        balance = user_data[0]
        rub_balance = int(balance * XTR_TO_RUB_RATE)
        text = (
            f"ваш баланс: {balance} <tg-emoji emoji-id=\"4983746717313664194\">⭐</tg-emoji> • {rub_balance} ₽\n"
            f"ваши реквизиты:\n<blockquote>карта • {card_num}</blockquote>"
        )
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="✏️ изменить реквизиты", callback_data="add_payment_details"))
        kb.row(InlineKeyboardButton(text="💎 вывести", callback_data="withdraw_funds"))
        kb.row(InlineKeyboardButton(text="назад", callback_data="back_to_menu"))
        await bot.edit_message_text(text=text, chat_id=message.chat.id, message_id=msg_id, parse_mode="HTML", reply_markup=kb.as_markup())

# ------------------- ВЫВОД СРЕДСТВ -------------------
@router.callback_query(F.data == "withdraw_funds")
async def withdraw_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_data = await get_user_data(user_id)
    if not user_data:
        await callback.answer("Ошибка данных", show_alert=True)
        return

    balance_stars = user_data[0]
    p_method, p_number, p_bank = user_data[1], user_data[2], user_data[3]
    balance_rub = int(balance_stars * XTR_TO_RUB_RATE)
    
    if balance_rub < MIN_WITHDRAWAL_RUB:
        await callback.answer(f"Минимальная сумма вывода {MIN_WITHDRAWAL_RUB}₽", show_alert=True)
        return

    if p_method == 'sbp':
        details_str = f"СБП: {p_number} ({p_bank})"
    elif p_method == 'card':
        details_str = f"Карта: {p_number}"
    else:
        await callback.answer("Заполните реквизиты!", show_alert=True)
        return

    await callback.message.edit_text("⏳ создаем заявку...", reply_markup=None)
    amount_withdrawn = await reset_balance_safe(user_id)
    if amount_withdrawn <= 0:
        await callback.message.edit_text("ошибка баланса", reply_markup=await main_menu_kb(user_id))
        return
        
    final_rub = int(amount_withdrawn * XTR_TO_RUB_RATE)
    initial_status = "на рассмотрении.. <tg-emoji emoji-id=\"5373153968769735192\">🧐</tg-emoji>"
    text = (
        "<b>заявка принята</b>\n\n"
        f"сумма: {amount_withdrawn} <tg-emoji emoji-id=\"4983746717313664194\">⭐</tg-emoji>\n"
        f"к получению: {final_rub} ₽\n"
        f"реквизиты: {details_str}\n"
        f"статус:\n<blockquote>{initial_status}</blockquote>"
    )
    kb = InlineKeyboardBuilder().add(InlineKeyboardButton(text="🔙 назад", callback_data="back_to_menu")).as_markup()
    msg = await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

    wd_id = await create_withdrawal(user_id, amount_withdrawn, final_rub, details_str, msg.message_id)

    try:
        user_link = get_user_link(user_id, callback.from_user.first_name)
        admin_text = (
            f"<tg-emoji emoji-id=\"5206222720416643915\">🔔</tg-emoji> <b>новая заявка</b> #{wd_id}\n\n"
            f"от: {user_link}\n"
            f"id: <code>{user_id}</code>\n"
            f"сумма: <b>{amount_withdrawn} ⭐</b> (~{final_rub} ₽)\n"
            f"реквизиты: <code>{details_str}</code>"
        )
        await bot.send_message(
            chat_id=ADMIN_ID, 
            text=admin_text, 
            parse_mode="HTML",
            reply_markup=admin_withdrawal_kb('wait', wd_id)
        )
    except Exception as e:
        logger.error(f"Ошибка отправки админу: {e}")

# ------------------- ЛОГИКА АДМИНА -------------------
@router.callback_query(F.data.startswith("setstat_"))
async def change_status_handler(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    action = parts[1]
    wd_id = int(parts[2])
    
    wd_data = await get_withdrawal(wd_id)
    if not wd_data:
        await callback.answer("заявка не найдена", show_alert=True)
        return

    user_id, amount, user_msg_id, current_status = wd_data
    
    new_status = action
    status_text_user = ""
    status_emoji_admin = ""
    
    if action == "review":
        status_text_user = "на рассмотрении.. <tg-emoji emoji-id=\"5373153968769735192\">🧐</tg-emoji>"
        status_emoji_admin = "статус: <tg-emoji emoji-id=\"5424885441100782420\">👀</tg-emoji> на рассмотрении"
    elif action == "soon":
        status_text_user = "скоро отправим.. <tg-emoji emoji-id=\"5445284980978621387\">🚀</tg-emoji>"
        status_emoji_admin = "статус: <tg-emoji emoji-id=\"5445284980978621387\">🚀</tg-emoji> скоро будет"
    elif action == "done":
        status_text_user = "отправили <tg-emoji emoji-id=\"5472164874886846699\">✨</tg-emoji>"
        status_emoji_admin = "статус: <tg-emoji emoji-id=\"5206607081334906820\">✅</tg-emoji> выполнено"
    
    await update_withdrawal_status(wd_id, new_status)
    
    user_text = (
        "<b>заявка принята</b>\n\n"
        f"сумма: {amount} <tg-emoji emoji-id=\"4983746717313664194\">⭐</tg-emoji>\n"
        f"статус:\n<blockquote>{status_text_user}</blockquote>"
    )
    try:
        back_btn = InlineKeyboardBuilder().add(InlineKeyboardButton(text="🔙 назад", callback_data="back_to_menu")).as_markup()
        await bot.edit_message_text(
            text=user_text, chat_id=user_id, message_id=user_msg_id,
            parse_mode="HTML", reply_markup=back_btn
        )
    except Exception as e:
        logger.error(f"Ошибка редактирования сообщения пользователя: {e}")
        
    try:
        updated_admin_text = (f"{callback.message.text}\n\n<b>{status_emoji_admin}</b>")
        new_kb = admin_withdrawal_kb(new_status, wd_id) if new_status != 'done' else None
        await callback.message.edit_text(updated_admin_text, parse_mode="HTML", reply_markup=new_kb)
    except Exception as e:
        logger.error(f"Ошибка редактирования сообщения админа: {e}")

# ------------------- ОБЫЧНЫЕ ХЭНДЛЕРЫ -------------------
@router.callback_query(F.data == "back_to_menu")
async def back_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    code = data.get("current_code")
    if code and code in active_sessions:
        del active_sessions[code]
    await state.clear()
    await callback.message.edit_text("главное меню:", reply_markup=await main_menu_kb(callback.from_user.id))

@router.callback_query(F.data == "make_payment")
async def start_payment_mode(callback: types.CallbackQuery, state: FSMContext):
    await generate_and_show_code(callback.message, callback.from_user.id, state)

@router.callback_query(F.data == "regenerate_code")
async def regenerate_code_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    old_code = data.get("current_code")
    if old_code and old_code in active_sessions:
        del active_sessions[old_code]
    await generate_and_show_code(callback.message, callback.from_user.id, state, is_edit=True)

async def generate_and_show_code(message: types.Message, user_id: int, state: FSMContext, is_edit=True):
    new_code = generate_code()
    await state.update_data(current_code=new_code)
    text = (f"твой код: <code>{new_code}</code>\n\nскажи этот код продавцу\nесли код не работает, нажми кнопку ниже")
    active_sessions[new_code] = {"user_id": user_id, "active": True, "message_id": None}
    kb = code_generation_kb()
    if is_edit:
        msg = await message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        msg = await message.answer(text, parse_mode="HTML", reply_markup=kb)
    active_sessions[new_code]["message_id"] = msg.message_id

@router.callback_query(F.data == "receive_payment")
async def receive_payment_start(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(interface_msg_id=callback.message.message_id)
    await state.set_state(PaymentState.waiting_for_input)
    await callback.message.edit_text(
        "введи <b>код клиента</b> и <b>сумму</b> через пробел\nнапример: <code>1234 50</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text="🔙 назад", callback_data="cancel_invoice")).as_markup()
    )

@router.message(PaymentState.waiting_for_input)
async def process_merchant_input(message: types.Message, state: FSMContext):
    with suppress(Exception):
        await message.delete()

    data = await state.get_data()
    interface_msg_id = data.get("interface_msg_id")
    if not interface_msg_id: return

    try:
        parts = message.text.split()
        if len(parts) != 2: raise ValueError
        code, amount = parts[0], int(parts[1])
        if amount <= 0 or amount > 10000:
            raise ValueError("invalid_amount")
        
        session = active_sessions.get(code)
        if not session or not session["active"]:
            await bot.edit_message_text(
                chat_id=message.chat.id, message_id=interface_msg_id,
                text="<tg-emoji emoji-id=\"5210952531676504517\">❌</tg-emoji> код не найден или устарел\nпопробуй снова:",
                reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text="🔙 назад", callback_data="cancel_invoice")).as_markup(),
                parse_mode="HTML"
            )
            return

        payer_id = session["user_id"]
        try:
            payer_info = await bot.get_chat(payer_id)
            p_name = payer_info.first_name if payer_info.first_name else "клиент"
            payer_link = get_user_link(payer_id, p_name)
        except Exception:
            payer_link = "клиенту"

        confirm_text = f"выставить счёт {payer_link} на <b>{amount} <tg-emoji emoji-id=\"4983746717313664194\">⭐</tg-emoji></b>?"
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=interface_msg_id,
            text=confirm_text, parse_mode="HTML",
            reply_markup=confirm_invoice_kb(code, amount)
        )
    except ValueError as e:
        error_text = "<tg-emoji emoji-id=\"5276240711795107620\">⚠️</tg-emoji> сумма должна быть от 1 до 10000" if str(e) == "invalid_amount" else "<tg-emoji emoji-id=\"5276240711795107620\">⚠️</tg-emoji> неверный формат. нужно: <b>КОД СУММА</b>\nнапример: 1234 100"
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=interface_msg_id,
            text=error_text, parse_mode="HTML",
            reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text="🔙 назад", callback_data="cancel_invoice")).as_markup()
        )

@router.callback_query(F.data.startswith("confirm_"))
async def send_invoice_to_user(callback: types.CallbackQuery):
    _, code, amount_str = callback.data.split("_")
    amount = int(amount_str)
    
    await callback.message.edit_text("<tg-emoji emoji-id=\"6113789201717660877\">⏳</tg-emoji> счёт отправлен, ждем оплату..", reply_markup=None, parse_mode="HTML")
    
    session = active_sessions.get(code)
    if session:
        active_sessions[code]["active"] = False
        try:
            await bot.edit_message_text(
                chat_id=session["user_id"], message_id=session["message_id"],
                text="тебе выставили счёт, жми кнопку для оплаты <tg-emoji emoji-id=\"5470177992950946662\">👇</tg-emoji>", reply_markup=None,
                parse_mode="HTML"
            )
        except Exception:
            pass

        payload = f"inv_{callback.from_user.id}_{uuid.uuid4().hex}"
        prices = [LabeledPrice(label="услуга", amount=amount)]
        
        invoice_msg = await bot.send_invoice(
            chat_id=session["user_id"], title="оплата", description=f"перевод {amount} stars",
            payload=payload, provider_token="", currency="XTR", prices=prices, start_parameter="pay"
        )
        merchant_transactions[payload] = {
            "merchant_id": callback.from_user.id, 
            "merchant_msg_id": callback.message.message_id,
            "payer_id": session["user_id"],
            "payer_prompt_msg_id": session["message_id"],
            "invoice_msg_id": invoice_msg.message_id
        }
    else:
        await callback.message.edit_text("<tg-emoji emoji-id=\"5210952531676504517\">❌</tg-emoji> ошибка: клиент ушел или код истек", reply_markup=await main_menu_kb(callback.from_user.id), parse_mode="HTML")

@router.callback_query(F.data == "cancel_invoice")
async def cancel_invoice(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("отменено", reply_markup=await main_menu_kb(callback.from_user.id))

# ------------------- ФИНАЛ ОПЛАТЫ -------------------
@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    payload = query.invoice_payload
    if "inline_inv_" in payload:
        parts = payload.split("_")
        if len(parts) >= 4:
            link_uuid = parts[3]
            if await is_link_used(link_uuid):
                 await query.answer(ok=False, error_message="Ссылка уже использована")
                 return
    if payload not in merchant_transactions:
        await query.answer(ok=False, error_message="Транзакция не найдена")
        return
    await query.answer(ok=True)

@router.message(F.successful_payment)
async def successful_payment(message: types.Message):
    info = message.successful_payment
    payload = info.invoice_payload
    amount = info.total_amount

    if payload not in merchant_transactions:
        await message.answer("ошибка: транзакция не найдена")
        return

    data = merchant_transactions[payload]
    if "link_uuid" in data:
        await mark_link_used(data["link_uuid"])

    await message.answer(
        "<tg-emoji emoji-id=\"5206607081334906820\">✅</tg-emoji> оплата прошла успешно! спасибо",
        message_effect_id=CONFETTI_EFFECT_ID,
        reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text="в меню", callback_data="back_to_menu")).as_markup(),
        parse_mode="HTML"
    )

    m_id = data["merchant_id"]
    await add_balance(m_id, amount)
    
    if "merchant_msg_id" in data and data["merchant_msg_id"]:
        # Убрана строка с балансом
        success_text = (
            f"<tg-emoji emoji-id=\"5206607081334906820\">✅</tg-emoji> <b>счёт оплачен!</b>\n"
            f"получено: {amount} <tg-emoji emoji-id=\"4983746717313664194\">⭐</tg-emoji>"
        )
        try:
            await bot.edit_message_text(chat_id=m_id, message_id=data["merchant_msg_id"], text=success_text, parse_mode="HTML", reply_markup=await main_menu_kb(m_id))
        except Exception:
            await bot.send_message(m_id, success_text, parse_mode="HTML", reply_markup=await main_menu_kb(m_id))

    payer_id = data.get("payer_id")
    payer_prompt_msg_id = data.get("payer_prompt_msg_id")
    invoice_msg_id = data.get("invoice_msg_id")
    if payer_id and payer_prompt_msg_id:
        with suppress(Exception):
            await bot.delete_message(chat_id=payer_id, message_id=payer_prompt_msg_id)
    if payer_id and invoice_msg_id:
        with suppress(Exception):
            await bot.delete_message(chat_id=payer_id, message_id=invoice_msg_id)

    if "original_chat_id" in data and "original_msg_id" in data and data["original_chat_id"]:
        try:
            await bot.edit_message_text(
                chat_id=data["original_chat_id"],
                message_id=data["original_msg_id"],
                text="счёт оплачен <tg-emoji emoji-id=\"5206607081334906820\">✅</tg-emoji>",
                parse_mode="HTML", reply_markup=None
            )
        except Exception:
            pass
    del merchant_transactions[payload]

async def main():
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("бот работает..")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
