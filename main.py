import asyncio
import random
import string
import re
import html
import logging
import uuid
import os
from contextlib import suppress

# Если ошибка возникает здесь, значит нужно установить библиотеку: pip install asyncpg
import asyncpg

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
DB_URL = os.getenv("DB_URL") 
XTR_TO_RUB_RATE = 1.8
CONFETTI_EFFECT_ID = "5046509860389126442"
CODE_LENGTH = 4
MIN_WITHDRAWAL_RUB = 10 

# Эмодзи
EMOJI_DONE = "✅"

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# ------------------- БАЗА ДАННЫХ -------------------
async def init_db():
    # Проверка подключения
    if not DB_URL:
        logger.error("ОШИБКА: Не указан DB_URL в переменных окружения!")
        return

    try:
        conn = await asyncpg.connect(DB_URL)
        try:
            # Таблица пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    balance BIGINT DEFAULT 0
                )
            """)

            # Таблица реквизитов
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS requisites (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    method TEXT,
                    details TEXT,
                    bank_name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Таблица выводов
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    amount BIGINT,
                    rub_amount BIGINT,
                    details TEXT,
                    user_message_id BIGINT,
                    status TEXT DEFAULT 'wait'
                )
            """)

            # Таблица использованных ссылок
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS used_links (
                    link_uuid TEXT PRIMARY KEY
                )
            """)
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")

# --- Функции БД ---

async def get_user_balance(user_id: int) -> int:
    conn = await asyncpg.connect(DB_URL)
    try:
        await conn.execute("INSERT INTO users (user_id, balance) VALUES ($1, 0) ON CONFLICT (user_id) DO NOTHING", user_id)
        row = await conn.fetchrow("SELECT balance FROM users WHERE user_id = $1", user_id)
        return row['balance'] if row else 0
    finally:
        await conn.close()

async def add_balance(user_id: int, amount: int):
    conn = await asyncpg.connect(DB_URL)
    try:
        await conn.execute("INSERT INTO users (user_id, balance) VALUES ($1, 0) ON CONFLICT (user_id) DO NOTHING", user_id)
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, user_id)
    finally:
        await conn.close()

async def add_requisite(user_id: int, method: str, details: str, bank_name: str = None):
    conn = await asyncpg.connect(DB_URL)
    try:
        count = await conn.fetchval("SELECT COUNT(*) FROM requisites WHERE user_id = $1", user_id)
        if count >= 5:
            return False
        await conn.execute(
            "INSERT INTO requisites (user_id, method, details, bank_name) VALUES ($1, $2, $3, $4)",
            user_id, method, details, bank_name
        )
        return True
    finally:
        await conn.close()

async def get_user_requisites(user_id: int):
    conn = await asyncpg.connect(DB_URL)
    try:
        rows = await conn.fetch("SELECT id, method, details, bank_name FROM requisites WHERE user_id = $1 ORDER BY id ASC", user_id)
        return rows
    finally:
        await conn.close()

async def delete_requisite(req_id: int, user_id: int):
    conn = await asyncpg.connect(DB_URL)
    try:
        await conn.execute("DELETE FROM requisites WHERE id = $1 AND user_id = $2", req_id, user_id)
    finally:
        await conn.close()

async def reset_balance_safe(user_id: int) -> int:
    conn = await asyncpg.connect(DB_URL)
    try:
        async with conn.transaction():
            row = await conn.fetchrow("SELECT balance FROM users WHERE user_id = $1 FOR UPDATE", user_id)
            if not row or row['balance'] <= 0:
                return 0
            amount = row['balance']
            await conn.execute("UPDATE users SET balance = 0 WHERE user_id = $1", user_id)
            return amount
    finally:
        await conn.close()

async def create_withdrawal(user_id: int, amount: int, rub_amount: int, details: str, message_id: int):
    conn = await asyncpg.connect(DB_URL)
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO withdrawals (user_id, amount, rub_amount, details, user_message_id, status)
            VALUES ($1, $2, $3, $4, $5, 'wait')
            RETURNING id
            """,
            user_id, amount, rub_amount, details, message_id
        )
        return row['id']
    finally:
        await conn.close()

async def get_withdrawal(wd_id: int):
    conn = await asyncpg.connect(DB_URL)
    try:
        return await conn.fetchrow("SELECT user_id, amount, user_message_id, status FROM withdrawals WHERE id = $1", wd_id)
    finally:
        await conn.close()

async def update_withdrawal_status(wd_id: int, new_status: str):
    conn = await asyncpg.connect(DB_URL)
    try:
        await conn.execute("UPDATE withdrawals SET status = $1 WHERE id = $2", new_status, wd_id)
    finally:
        await conn.close()

async def is_link_used(uuid_str: str) -> bool:
    conn = await asyncpg.connect(DB_URL)
    try:
        row = await conn.fetchrow("SELECT 1 FROM used_links WHERE link_uuid = $1", uuid_str)
        return bool(row)
    finally:
        await conn.close()

async def mark_link_used(uuid_str: str):
    conn = await asyncpg.connect(DB_URL)
    try:
        await conn.execute("INSERT INTO used_links (link_uuid) VALUES ($1) ON CONFLICT DO NOTHING", uuid_str)
    finally:
        await conn.close()

# ------------------- STATE & RAM -------------------
active_sessions = {} 
merchant_transactions = {}

class PaymentState(StatesGroup):
    waiting_for_input = State()

class ProfileState(StatesGroup):
    waiting_for_sbp_phone = State()
    waiting_for_sbp_bank = State()
    waiting_for_card = State()

# ------------------- UTIL -------------------
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
        if digits.startswith('8'): digits = '7' + digits[1:]
    elif len(digits) == 10 and digits.startswith('9'):
        digits = '7' + digits
    return f"+{digits}"

# ------------------- KEYBOARDS -------------------
def main_menu_kb():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="принять оплату", callback_data="receive_payment"))
    builder.row(InlineKeyboardButton(text="оплатить", callback_data="make_payment"))
    builder.row(InlineKeyboardButton(text="профиль", callback_data="open_profile"))
    return builder.as_markup()

def back_kb():
    return InlineKeyboardBuilder().add(InlineKeyboardButton(text="назад", callback_data="back_to_menu")).as_markup()

def code_generation_kb():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="новый код", callback_data="regenerate_code"))
    builder.row(InlineKeyboardButton(text="назад", callback_data="back_to_menu"))
    return builder.as_markup()

def confirm_invoice_kb(code, amount):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="подтвердить", callback_data=f"confirm_{code}_{amount}"),
        InlineKeyboardButton(text="отменить", callback_data="cancel_invoice")
    )
    return builder.as_markup()

def admin_withdrawal_kb(current_status, wd_id):
    builder = InlineKeyboardBuilder()
    if current_status == 'wait':
        builder.row(InlineKeyboardButton(text="на рассмотрение", callback_data=f"setstat_review_{wd_id}"))
    elif current_status == 'review':
        builder.row(InlineKeyboardButton(text="скоро отправим", callback_data=f"setstat_soon_{wd_id}"))
    elif current_status == 'soon':
        builder.row(InlineKeyboardButton(text="выполнено", callback_data=f"setstat_done_{wd_id}"))
    return builder.as_markup() if current_status != 'done' else None

# ------------------- INLINE QUERY -------------------
@router.inline_query()
async def inline_query_handler(query: types.InlineQuery):
    amount_str = query.query.strip()
    if not amount_str.isdigit(): return
    
    amount = int(amount_str)
    if amount <= 0 or amount > 10000: return
    
    merchant_id = query.from_user.id
    unique_link_id = uuid.uuid4().hex[:12]
    start_param = f"inline_pay_{amount}_{merchant_id}_{unique_link_id}"
    
    text = f"счёт на оплату <b>{amount} звёзд</b>"
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text=f"оплатить {amount} ⭐️", url=f"https://t.me/{BOT_USERNAME}?start={start_param}"))
    
    results = [
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=f"Счёт на {amount} ⭐️",
            description="нажмите, чтобы отправить счёт",
            input_message_content=InputTextMessageContent(message_text=text, parse_mode="HTML"),
            reply_markup=kb.as_markup(),
        )
    ]
    await query.answer(results, cache_time=1)

# ------------------- START & MENU -------------------
@router.message(Command("start"))
async def cmd_start(message: types.Message, command: CommandObject, state: FSMContext):
    await state.clear()
    await add_balance(message.from_user.id, 0) 
    
    args = command.args
    if args and args.startswith("inline_pay_"):
        parts = args.split("_")
        if len(parts) >= 4:
            amount = int(parts[2])
            merchant_id = int(parts[3])
            link_uuid = parts[4]
            
            if await is_link_used(link_uuid):
                await message.answer("этот счёт уже оплачен")
                return

            payer_id = message.from_user.id
            if payer_id == merchant_id:
                await message.answer("нельзя платить самому себе")
                return
            
            payload = f"inline_inv_{merchant_id}_{link_uuid}"
            prices = [LabeledPrice(label="услуга", amount=amount)]
            
            try:
                msg = await bot.send_invoice(
                    chat_id=payer_id, title="оплата", description=f"перевод {amount} stars",
                    payload=payload, provider_token="", currency="XTR", prices=prices, start_parameter="pay"
                )
                
                merchant_msg_id = None
                with suppress(Exception):
                    m_msg = await bot.send_message(merchant_id, "⏳ счёт отправлен, ждем оплату..")
                    merchant_msg_id = m_msg.message_id
                
                merchant_transactions[payload] = {
                    "merchant_id": merchant_id, "merchant_msg_id": merchant_msg_id,
                    "payer_id": payer_id, "invoice_msg_id": msg.message_id, "link_uuid": link_uuid
                }
            except Exception:
                await message.answer("ошибка создания счёта")
            return

    name = html.escape(message.from_user.first_name).lower()
    await message.answer(
        f"привет, {name}\nчто хочешь сделать?",
        reply_markup=main_menu_kb(),
        parse_mode="HTML"
    )

# ------------------- ПРОФИЛЬ -------------------
@router.callback_query(F.data == "open_profile")
async def open_profile(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    balance = await get_user_balance(user_id)
    rub_balance = int(balance * XTR_TO_RUB_RATE)
    
    requisites = await get_user_requisites(user_id)
    
    text = (
        f"<b>твой профиль</b>\n\n"
        f"баланс: <b>{balance} ⭐️</b> ≈ {rub_balance} ₽\n\n"
        f"твои реквизиты:"
    )
    
    kb = InlineKeyboardBuilder()
    
    if not requisites:
        text += "\n<i>список пуст</i>"
    else:
        for req in requisites:
            r_id = req['id']
            r_method = req['method']
            r_details = req['details']
            r_bank = req['bank_name']
            
            if r_method == 'sbp':
                label = f"СБП: {r_details} ({r_bank})"
            else:
                label = f"Карта: {r_details}"
            
            kb.row(
                InlineKeyboardButton(text=f"🗑 {label}", callback_data=f"del_req_{r_id}")
            )

    text += "\n\nможно добавить несколько карт или номеров"
    kb.row(InlineKeyboardButton(text="➕ добавить реквизиты", callback_data="add_payment_details"))
    
    if balance > 0 and requisites:
        kb.row(InlineKeyboardButton(text="💎 вывести средства", callback_data="withdraw_funds"))
        
    kb.row(InlineKeyboardButton(text="назад", callback_data="back_to_menu"))
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("del_req_"))
async def delete_req_handler(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])
    await delete_requisite(req_id, callback.from_user.id)
    await callback.answer("удалено")
    await open_profile(callback, FSMContext(storage=dp.storage, key=callback.message.chat.id))

# --- Добавление реквизитов ---
@router.callback_query(F.data == "add_payment_details")
async def start_add_details(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(interface_msg_id=callback.message.message_id)
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="сбп (номер)", callback_data="set_method_sbp"), 
           InlineKeyboardButton(text="карта", callback_data="set_method_card"))
    kb.row(InlineKeyboardButton(text="назад", callback_data="open_profile"))
    await callback.message.edit_text("выбери способ получения денег", reply_markup=kb.as_markup())

# СБП
@router.callback_query(F.data == "set_method_sbp")
async def ask_sbp_phone(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(ProfileState.waiting_for_sbp_phone)
    kb = InlineKeyboardBuilder().add(InlineKeyboardButton(text="назад", callback_data="open_profile")).as_markup()
    await callback.message.edit_text("введи номер телефона для сбп", reply_markup=kb)

@router.message(ProfileState.waiting_for_sbp_phone)
async def process_sbp_phone(message: types.Message, state: FSMContext):
    with suppress(Exception): await message.delete()
    phone = normalize_phone(message.text)
    if len(phone) < 10: 
        return 
    
    await state.update_data(sbp_phone=phone)
    await state.set_state(ProfileState.waiting_for_sbp_bank)
    
    data = await state.get_data()
    msg_id = data.get("interface_msg_id")
    
    kb = InlineKeyboardBuilder().add(InlineKeyboardButton(text="назад", callback_data="open_profile")).as_markup()
    text = "напиши название банка (например: т-банк, сбер)"
    
    if msg_id:
        await bot.edit_message_text(text=text, chat_id=message.chat.id, message_id=msg_id, reply_markup=kb)

@router.message(ProfileState.waiting_for_sbp_bank)
async def process_sbp_bank(message: types.Message, state: FSMContext):
    with suppress(Exception): await message.delete()
    bank_name = html.escape(message.text)
    
    data = await state.get_data()
    phone = data.get("sbp_phone")
    
    success = await add_requisite(message.from_user.id, "sbp", phone, bank_name)
    
    msg_id = data.get("interface_msg_id")
    if msg_id:
        if success:
            await bot.edit_message_text("реквизиты добавлены!", chat_id=message.chat.id, message_id=msg_id)
        else:
            await bot.edit_message_text("слишком много реквизитов, удали старые", chat_id=message.chat.id, message_id=msg_id)
        await asyncio.sleep(1.5)
        fake_cb = types.CallbackQuery(id='0', from_user=message.from_user, message=message, chat_instance='0', data='open_profile')
        await open_profile(fake_cb, state)

# КАРТА
@router.callback_query(F.data == "set_method_card")
async def ask_card_number(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(ProfileState.waiting_for_card)
    kb = InlineKeyboardBuilder().add(InlineKeyboardButton(text="назад", callback_data="open_profile")).as_markup()
    await callback.message.edit_text("введи номер карты", reply_markup=kb)

@router.message(ProfileState.waiting_for_card)
async def process_card(message: types.Message, state: FSMContext):
    with suppress(Exception): await message.delete()
    card_num = re.sub(r'\D', '', message.text) 
    if len(card_num) < 13: return
    
    data = await state.get_data()
    msg_id = data.get("interface_msg_id")
    
    success = await add_requisite(message.from_user.id, "card", card_num, None)
    
    if msg_id:
        text = "карта добавлена!" if success else "лимит реквизитов исчерпан"
        await bot.edit_message_text(text, chat_id=message.chat.id, message_id=msg_id)
        await asyncio.sleep(1.5)
        fake_cb = types.CallbackQuery(id='0', from_user=message.from_user, message=message, chat_instance='0', data='open_profile')
        await open_profile(fake_cb, state)

# ------------------- ВЫВОД СРЕДСТВ -------------------
@router.callback_query(F.data == "withdraw_funds")
async def withdraw_start(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    balance = await get_user_balance(user_id)
    rub_balance = int(balance * XTR_TO_RUB_RATE)
    
    if rub_balance < MIN_WITHDRAWAL_RUB:
        await callback.answer(f"минимум для вывода: {MIN_WITHDRAWAL_RUB} ₽", show_alert=True)
        return

    requisites = await get_user_requisites(user_id)
    if not requisites:
        await callback.answer("сначала добавь реквизиты", show_alert=True)
        return

    if len(requisites) == 1:
        req = requisites[0]
        await process_withdrawal(callback.message, user_id, req)
    else:
        kb = InlineKeyboardBuilder()
        for req in requisites:
            label = f"{req['bank_name']} {req['details']}" if req['method'] == 'sbp' else f"Карта {req['details']}"
            kb.row(InlineKeyboardButton(text=label, callback_data=f"wd_sel_{req['id']}"))
        kb.row(InlineKeyboardButton(text="назад", callback_data="open_profile"))
        await callback.message.edit_text("куда вывести деньги?", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("wd_sel_"))
async def withdraw_select_req(callback: types.CallbackQuery):
    req_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    
    conn = await asyncpg.connect(DB_URL)
    req = await conn.fetchrow("SELECT method, details, bank_name FROM requisites WHERE id = $1 AND user_id = $2", req_id, user_id)
    await conn.close()
    
    if not req:
        await callback.answer("ошибка выбора реквизитов", show_alert=True)
        return
        
    await process_withdrawal(callback.message, user_id, req)

async def process_withdrawal(message: types.Message, user_id: int, req_data):
    amount_stars = await reset_balance_safe(user_id)
    if amount_stars <= 0:
        await message.edit_text("баланс пуст или изменился", reply_markup=back_kb())
        return
        
    rub_amount = int(amount_stars * XTR_TO_RUB_RATE)
    
    details_str = f"СБП: {req_data['details']} ({req_data['bank_name']})" if req_data['method'] == 'sbp' else f"Карта: {req_data['details']}"
    
    text = (
        "<b>заявка создана</b>\n\n"
        f"сумма: {amount_stars} ⭐️\n"
        f"к получению: {rub_amount} ₽\n"
        f"куда: {details_str}\n\n"
        f"<blockquote>статус: на проверке..</blockquote>"
    )
    
    msg = await message.edit_text(text, parse_mode="HTML", reply_markup=back_kb())
    wd_id = await create_withdrawal(user_id, amount_stars, rub_amount, details_str, msg.message_id)

    try:
        user_link = get_user_link(user_id, "пользователь")
        admin_text = (
            f"🔔 <b>заявка #{wd_id}</b>\n"
            f"от: {user_link} (id: {user_id})\n"
            f"сумма: {amount_stars} ⭐️ (~{rub_amount} ₽)\n"
            f"реквизиты: <code>{details_str}</code>"
        )
        await bot.send_message(ADMIN_ID, admin_text, parse_mode="HTML", reply_markup=admin_withdrawal_kb('wait', wd_id))
    except Exception as e:
        logger.error(f"Err admin send: {e}")

# ------------------- АДМИНКА -------------------
@router.callback_query(F.data.startswith("setstat_"))
async def admin_change_status(callback: types.CallbackQuery):
    _, action, wd_id_str = callback.data.split("_")
    wd_id = int(wd_id_str)
    
    wd = await get_withdrawal(wd_id)
    if not wd: return await callback.answer("не найдено")
    
    status_map = {
        "review": ("на рассмотрении", "на проверке.."),
        "soon": ("скоро отправим", "средства отправляются.."),
        "done": ("выполнено", "средства отправлены ✅")
    }
    
    adm_st, user_st = status_map.get(action, ("?", "?"))
    
    await update_withdrawal_status(wd_id, action)
    
    try:
        user_text = (
            "<b>заявка обновлена</b>\n\n"
            f"сумма: {wd['amount']} ⭐️\n"
            f"<blockquote>статус: {user_st}</blockquote>"
        )
        await bot.edit_message_text(
            text=user_text, chat_id=wd['user_id'], message_id=wd['user_message_id'],
            parse_mode="HTML", reply_markup=back_kb()
        )
    except Exception: pass
    
    new_kb = admin_withdrawal_kb(action, wd_id)
    lines = callback.message.html_text.split("\n")
    base_text = "\n".join(lines[:4])
    final_text = f"{base_text}\n\nстатус: <b>{adm_st}</b>"
    
    await callback.message.edit_text(final_text, parse_mode="HTML", reply_markup=new_kb)

# ------------------- ПРИЕМ ОПЛАТЫ -------------------
@router.callback_query(F.data == "back_to_menu")
async def back_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    code = data.get("current_code")
    if code and code in active_sessions: del active_sessions[code]
    await state.clear()
    await callback.message.edit_text("главное меню", reply_markup=main_menu_kb())

@router.callback_query(F.data == "make_payment")
async def start_pay_mode(callback: types.CallbackQuery, state: FSMContext):
    new_code = generate_code()
    await state.update_data(current_code=new_code)
    
    text = (
        f"код для оплаты: <code>{new_code}</code>\n\n"
        f"продиктуй этот код продавцу"
    )
    
    active_sessions[new_code] = {"user_id": callback.from_user.id, "active": True, "message_id": None}
    msg = await callback.message.edit_text(text, parse_mode="HTML", reply_markup=code_generation_kb())
    active_sessions[new_code]["message_id"] = msg.message_id

@router.callback_query(F.data == "regenerate_code")
async def regen_code(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if data.get("current_code"): del active_sessions[data["current_code"]]
    await start_pay_mode(callback, state)

@router.callback_query(F.data == "receive_payment")
async def receive_start(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(interface_msg_id=callback.message.message_id)
    await state.set_state(PaymentState.waiting_for_input)
    await callback.message.edit_text(
        "введи <b>код</b> и <b>сумму</b> через пробел\nпример: <code>1234 100</code>",
        parse_mode="HTML", reply_markup=back_kb()
    )

@router.message(PaymentState.waiting_for_input)
async def process_merch_input(message: types.Message, state: FSMContext):
    with suppress(Exception): await message.delete()
    data = await state.get_data()
    if_msg_id = data.get("interface_msg_id")
    if not if_msg_id: return

    try:
        parts = message.text.split()
        if len(parts) != 2: raise ValueError
        code, amount = parts[0], int(parts[1])
        if amount <= 0 or amount > 10000: raise ValueError
        
        session = active_sessions.get(code)
        if not session or not session["active"]:
            await bot.edit_message_text("код не найден или устарел, попробуй еще раз", chat_id=message.chat.id, message_id=if_msg_id, reply_markup=back_kb())
            return

        await bot.edit_message_text(
            f"выставить счёт на <b>{amount} ⭐️</b>?", 
            chat_id=message.chat.id, message_id=if_msg_id, parse_mode="HTML",
            reply_markup=confirm_invoice_kb(code, amount)
        )
    except ValueError:
        await bot.edit_message_text("ошибка формата. нужно: код сумма\nпример: 1234 50", chat_id=message.chat.id, message_id=if_msg_id, reply_markup=back_kb())

@router.callback_query(F.data.startswith("confirm_"))
async def send_inv(callback: types.CallbackQuery):
    _, code, amount = callback.data.split("_")
    amount = int(amount)
    
    session = active_sessions.get(code)
    if not session:
        return await callback.message.edit_text("клиент отключился", reply_markup=main_menu_kb())
    
    active_sessions[code]["active"] = False 
    
    with suppress(Exception):
        await bot.edit_message_text(
            "тебе выставлен счёт, кнопка внизу 👇", 
            chat_id=session["user_id"], message_id=session["message_id"]
        )
    
    payload = f"inv_{callback.from_user.id}_{uuid.uuid4().hex}"
    msg = await bot.send_invoice(
        chat_id=session["user_id"], title="оплата", description=f"сумма: {amount} stars",
        payload=payload, provider_token="", currency="XTR", prices=[LabeledPrice(label="услуга", amount=amount)], start_parameter="pay"
    )
    
    merchant_transactions[payload] = {
        "merchant_id": callback.from_user.id,
        "merchant_msg_id": callback.message.message_id,
        "payer_id": session["user_id"],
        "prompt_msg_id": session["message_id"],
        "inv_msg_id": msg.message_id
    }
    
    await callback.message.edit_text("счёт отправлен, ждем...", reply_markup=None)

@router.callback_query(F.data == "cancel_invoice")
async def cancel_inv(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("отменено", reply_markup=main_menu_kb())

@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)

@router.message(F.successful_payment)
async def success_pay(message: types.Message):
    info = message.successful_payment
    payload = info.invoice_payload
    amount = info.total_amount

    if payload not in merchant_transactions:
        return await message.answer("транзакция не найдена")

    data = merchant_transactions[payload]
    
    if "link_uuid" in data:
        await mark_link_used(data["link_uuid"])

    m_id = data["merchant_id"]
    await add_balance(m_id, amount)

    await message.answer(
        f"<b>оплата прошла успешно</b> {EMOJI_DONE}\nспасибо!",
        message_effect_id=CONFETTI_EFFECT_ID,
        parse_mode="HTML",
        reply_markup=back_kb()
    )

    try:
        success_txt = f"оплата получена!\n<b>+{amount} ⭐️</b>"
        if data.get("merchant_msg_id"):
            await bot.edit_message_text(success_txt, chat_id=m_id, message_id=data["merchant_msg_id"], parse_mode="HTML", reply_markup=main_menu_kb())
        else:
            await bot.send_message(m_id, success_txt, parse_mode="HTML", reply_markup=main_menu_kb())
    except Exception: pass

    if data.get("payer_id"):
        with suppress(Exception): await bot.delete_message(chat_id=data["payer_id"], message_id=data.get("prompt_msg_id"))
        with suppress(Exception): await bot.delete_message(chat_id=data["payer_id"], message_id=data.get("inv_msg_id"))

    del merchant_transactions[payload]

async def main():
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())