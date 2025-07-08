import sqlite3
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton,
    ReplyKeyboardRemove,
    InputMediaPhoto
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from config import ADMIN_ID, BOT_TOKEN

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Bot initialization
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Database setup
conn = sqlite3.connect('earn_bot.db', check_same_thread=False)
cursor = conn.cursor()

# Create tables
def create_tables():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        balance REAL DEFAULT 0,
        earned REAL DEFAULT 0,
        referral_id INTEGER DEFAULT NULL,
        referrals_count INTEGER DEFAULT 0,
        earned_from_refs REAL DEFAULT 0,
        is_banned BOOLEAN DEFAULT FALSE,
        registration_date TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tasks (
        task_id INTEGER PRIMARY KEY AUTOINCREMENT,
        description TEXT,
        link TEXT,
        reward REAL,
        creation_date TEXT DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT TRUE,
        max_completions INTEGER DEFAULT NULL,
        completions_count INTEGER DEFAULT 0
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS completed_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        task_id INTEGER,
        completion_date TEXT DEFAULT CURRENT_TIMESTAMP,
        screenshot_id TEXT DEFAULT NULL,
        is_verified BOOLEAN DEFAULT FALSE,
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(task_id) REFERENCES tasks(task_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS task_screenshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_completion_id INTEGER,
        screenshot_id TEXT,
        position INTEGER,
        FOREIGN KEY(task_completion_id) REFERENCES completed_tasks(id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS payments (
        payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        payment_date TEXT DEFAULT CURRENT_TIMESTAMP,
        status TEXT DEFAULT 'pending',
        payment_method TEXT,
        wallet_data TEXT,
        FOREIGN KEY(user_id) REFERENCES users(user_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS referral_earnings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER,
        referral_id INTEGER,
        amount REAL,
        earning_date TEXT DEFAULT CURRENT_TIMESTAMP,
        description TEXT,
        FOREIGN KEY(referrer_id) REFERENCES users(user_id),
        FOREIGN KEY(referral_id) REFERENCES users(user_id)
    )
    ''')
    conn.commit()

create_tables()

# States
class TaskStates(StatesGroup):
    description = State()
    link = State()
    reward = State()
    max_completions = State()

class WithdrawStates(StatesGroup):
    amount = State()
    wallet_data = State()

class VerifyTaskStates(StatesGroup):
    screenshot = State()
    more_screenshots = State()

class BroadcastStates(StatesGroup):
    message_content = State()
    confirmation = State()

# Helper functions
def is_user_banned(user_id: int) -> bool:
    cursor.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    return result[0] if result else False

async def check_ban_and_respond(message: types.Message):
    if is_user_banned(message.from_user.id):
        await message.answer(
            "⛔ Вы забанены администратором и больше не можете использовать бота.\n"
            "Если вы считаете ограничения ошибкой обратитесь в поддержку: @MargetSeller",
            reply_markup=ReplyKeyboardRemove()
        )
        return True
    return False

async def add_referral_earning(referral_id: int, amount: float, description: str):
    try:
        cursor.execute("SELECT referral_id FROM users WHERE user_id = ?", (referral_id,))
        row = cursor.fetchone()
        
        if row and row[0]:
            referrer_id = row[0]
            referral_bonus = amount * 0.10
            
            cursor.execute(
                "UPDATE users SET balance = balance + ?, earned_from_refs = earned_from_refs + ? WHERE user_id = ?", 
                (referral_bonus, referral_bonus, referrer_id)
            )
            
            cursor.execute(
                "INSERT INTO referral_earnings (referrer_id, referral_id, amount, description) VALUES (?, ?, ?, ?)",
                (referrer_id, referral_id, referral_bonus, description)
            )
            conn.commit()
            
            try:
                await bot.send_message(
                    referrer_id,
                    f"🎉 Ваш реферал заработал {amount:.2f} RUB\n"
                    f"💰 Вам начислено 10% от его заработка: {referral_bonus:.2f} RUB\n"
                    f"📝 Операция: {description}"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить реферера {referrer_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка в add_referral_earning: {e}")

# Keyboards
def main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    
    if is_user_banned(user_id):
        return ReplyKeyboardRemove()
    
    builder.row(
        KeyboardButton(text="📌 Доступные задания"),
        KeyboardButton(text="💰 Мой баланс")
    )
    builder.row(
        KeyboardButton(text="👥 Реферальная программа"),
        KeyboardButton(text="💸 Вывод средств")
    )
    if user_id == ADMIN_ID:
        builder.row(KeyboardButton(text="🔐 Админ-панель"))
    return builder.as_markup(resize_keyboard=True)

def admin_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="📊 Статистика"),
        KeyboardButton(text="📝 Добавить задание")
    )
    builder.row(
        KeyboardButton(text="📋 Список заданий"),
        KeyboardButton(text="👥 Список пользователей")
    )
    builder.row(
        KeyboardButton(text="❌ Удалить задание"),
        KeyboardButton(text="📢 Рассылка")
    )
    builder.row(
        KeyboardButton(text="📤 Заявки на вывод"),
        KeyboardButton(text="📤 Заявки на выполнение"),
        KeyboardButton(text="🏠 Главное меню")
    )
    return builder.as_markup(resize_keyboard=True)

# ========== COMMAND HANDLERS ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    
    if is_user_banned(user_id):
        await message.answer(
            "⛔ Вы забанены администратором и больше не можете использовать бота.\n"
            "Если вы считаете ограничения ошибкой обратитесь в поддержку: @MargetSeller",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    
    username = message.from_user.username
    
    # Handle referral link
    ref_id = None
    if len(message.text.split()) > 1:
        try:
            ref_id = int(message.text.split()[1])
            if ref_id == user_id:
                await message.answer("❌ Нельзя использовать свою реферальную ссылку!")
                ref_id = None
        except ValueError:
            pass

    # Register user
    cursor.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (user_id, username))
    
    # If new user and has referral
    if cursor.rowcount == 1 and ref_id:
        cursor.execute("UPDATE users SET referral_id = ? WHERE user_id = ?", (ref_id, user_id))
        cursor.execute("UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id = ?", (ref_id,))
        await bot.send_message(ref_id, f"🎉 У вас новый реферал! @{username} присоединился по вашей ссылке.")
    
    conn.commit()
    
    bot_info = await bot.get_me()
    await message.answer(
    f"👋 Добро пожаловать в ONI!\n\n"
    f"🔗 Ваша реферальная ссылка: https://t.me/{bot_info.username}?start={user_id}\n"
    f"💎 За каждого приглашенного друга вы получаете 10% от его заработка!\n\n"
    f"🛡️Продолжая пользоваться ботом вы автоматически\n"
    f"      принимаете [Условия использования](https://telegra.ph/Usloviya-polzovaniya-botom-ONI-06-30)\n\n"
    f"📌Чтобы начать, нажмите кнопку 'Доступные задания'",
    parse_mode="Markdown",  # Поддержка Markdown-ссылок
    disable_web_page_preview=True,  # Отключает превью ссылки
    reply_markup=main_keyboard(user_id)
    )

# ========== TASK SYSTEM ==========
@dp.message(F.text == "📌 Доступные задания")
async def show_tasks(message: types.Message, state: FSMContext):
    if await check_ban_and_respond(message):
        return
    
    user_id = message.from_user.id
    
    cursor.execute("""
        SELECT * FROM tasks 
        WHERE is_active = TRUE 
        AND (max_completions IS NULL OR completions_count < max_completions)
        ORDER BY task_id
    """)
    tasks = cursor.fetchall()

    if not tasks:
        await message.answer("📭 На данный момент нет доступных заданий. Загляните позже!")
        return

    await state.update_data(tasks=tasks, current_task_index=0)
    await show_task(message, state)

async def show_task(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data['tasks']
    current_index = data['current_task_index']
    
    if current_index >= len(tasks):
        await message.answer("🎉 Вы просмотрели все доступные задания!")
        await state.clear()
        return
    
    task = tasks[current_index]
    task_id, description, link, reward, creation_date, is_active, max_completions, completions_count = task
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(
            text="✅ Взять задание",
            callback_data=f"take_task_{task_id}"
        )
    )
    
    if len(tasks) > 1:
        row_buttons = []
        if current_index > 0:
            row_buttons.append(InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data="prev_task"
            ))
        if current_index < len(tasks) - 1:
            row_buttons.append(InlineKeyboardButton(
                text="➡️ Вперед",
                callback_data="next_task"
            ))
        if row_buttons:
            keyboard.row(*row_buttons)
    
    await message.answer(
        f"📌 Задание #{task_id}\n\n"
        f"📝 {description}\n"
        f"🔗 Ссылка: {link}\n\n"
        f"💰 Вознаграждение: {reward:.2f} RUB\n"
        f"🔄 Доступно выполнений: {f'{completions_count}/{max_completions}' if max_completions else 'Без ограничений'}",
        reply_markup=keyboard.as_markup(),
        disable_web_page_preview=True
    )

@dp.callback_query(F.data == "prev_task")
async def prev_task(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    current_index = data['current_task_index']
    await state.update_data(current_task_index=current_index - 1)
    await callback.message.delete()
    await show_task(callback.message, state)
    await callback.answer()

@dp.callback_query(F.data == "next_task")
async def next_task(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    current_index = data['current_task_index']
    await state.update_data(current_task_index=current_index + 1)
    await callback.message.delete()
    await show_task(callback.message, state)
    await callback.answer()

@dp.callback_query(F.data.startswith("take_task_"))
async def take_task(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if is_user_banned(user_id):
        await callback.answer("⛔ Вы забанены и не можете выполнять задания", show_alert=True)
        return

    task_id = int(callback.data.split('_')[-1])

    # Check if already completed
    cursor.execute("SELECT * FROM completed_tasks WHERE user_id = ? AND task_id = ?", (user_id, task_id))
    if cursor.fetchone():
        await callback.answer("❌ Вы уже выполняли это задание!", show_alert=True)
        return

    # Check task availability
    cursor.execute("SELECT reward FROM tasks WHERE task_id = ? AND is_active = TRUE", (task_id,))
    task = cursor.fetchone()
    
    if not task:
        await callback.answer("❌ Задание не найдено или неактивно!", show_alert=True)
        return

    reward = task[0]

    await state.update_data(task_id=task_id, reward=reward, screenshots=[])
    await state.set_state(VerifyTaskStates.screenshot)
    
    await callback.message.edit_text(
        "📌 Вы взяли задание!\n\n"
        "📸 Отправьте скриншот выполнения задания (можно несколько):\n"
        "Когда закончите, нажмите кнопку 'Готово'",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Готово", callback_data="screenshots_done")]
        ])
    )
    await callback.answer()

@dp.message(VerifyTaskStates.screenshot, F.photo)
async def process_task_screenshot(message: types.Message, state: FSMContext):
    screenshot_id = message.photo[-1].file_id
    data = await state.get_data()
    
    screenshots = data.get('screenshots', [])
    screenshots.append(screenshot_id)
    await state.update_data(screenshots=screenshots)
    
    await state.set_state(VerifyTaskStates.more_screenshots)
    await message.answer(
        "📸 Скриншот получен! Хотите добавить ещё?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить ещё", callback_data="add_more_screenshots"),
             InlineKeyboardButton(text="✅ Готово", callback_data="screenshots_done")]
        ])
    )

@dp.callback_query(F.data == "add_more_screenshots", VerifyTaskStates.more_screenshots)
async def add_more_screenshots(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(VerifyTaskStates.screenshot)
    await callback.message.edit_text(
        "📸 Отправьте следующий скриншот:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Готово", callback_data="screenshots_done")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "screenshots_done")
async def finish_screenshots(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    task_id = data['task_id']
    reward = data['reward']
    screenshots = data.get('screenshots', [])
    
    if not screenshots:
        await callback.answer("❌ Нужно отправить хотя бы один скриншот!", show_alert=True)
        return
    
    try:
        # Save first screenshot
        cursor.execute(
            "INSERT INTO completed_tasks (user_id, task_id, screenshot_id) VALUES (?, ?, ?)",
            (user_id, task_id, screenshots[0])
        )
        task_completion_id = cursor.lastrowid
        
        # Save additional screenshots
        for i, screenshot_id in enumerate(screenshots[1:], start=2):
            cursor.execute(
                "INSERT INTO task_screenshots (task_completion_id, screenshot_id, position) VALUES (?, ?, ?)",
                (task_completion_id, screenshot_id, i)
            )
        
        # Update completions count
        cursor.execute("UPDATE tasks SET completions_count = completions_count + 1 WHERE task_id = ?", (task_id,))
        conn.commit()
        
        # Notify admin
        media_group = []
        for i, screenshot_id in enumerate(screenshots, start=1):
            caption = (
                f"⚠️ Новое выполненное задание!\n\n"
                f"👤 Пользователь: @{callback.from_user.username} (ID: {user_id})\n"
                f"📌 Задание ID: {task_id}\n"
                f"💰 Вознаграждение: {reward:.2f} RUB\n"
                f"📸 Скриншот {i}/{len(screenshots)}"
                if i == 1 else ""
            )
            media_group.append(InputMediaPhoto(media=screenshot_id, caption=caption))
        
        await bot.send_media_group(ADMIN_ID, media=media_group)
        
        # Send approve/reject buttons
        await bot.send_message(
            ADMIN_ID,
            "Подтвердить выполнение задания?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"verify_task_{user_id}_{task_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_task_{user_id}_{task_id}")
                ]
            ])
        )
        
        await state.clear()
        await callback.message.answer(
            "✅ Скриншоты отправлены на проверку!\n"
            "Обычно проверка занимает до 24 часов.",
            reply_markup=main_keyboard(user_id)
        )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка при обработке скриншотов: {e}")
        await callback.message.answer(
            "❌ Произошла ошибка при обработке задания. Попробуйте позже.",
            reply_markup=main_keyboard(user_id)
        )
        await state.clear()

@dp.callback_query(F.data.startswith("verify_task_"))
async def verify_task_completion(callback: types.CallbackQuery):
    try:
        parts = callback.data.split('_')
        user_id = int(parts[-2])
        task_id = int(parts[-1])
        
        cursor.execute("SELECT reward FROM tasks WHERE task_id = ?", (task_id,))
        task = cursor.fetchone()
        
        if not task:
            await callback.answer("❌ Задание не найдено", show_alert=True)
            return
            
        reward = task[0]
        
        cursor.execute(
            "UPDATE completed_tasks SET is_verified = TRUE WHERE user_id = ? AND task_id = ?",
            (user_id, task_id)
        )
        
        cursor.execute(
            "UPDATE users SET balance = balance + ?, earned = earned + ? WHERE user_id = ?",
            (reward, reward, user_id)
        )
        conn.commit()
        
        try:
            await bot.send_message(
                user_id,
                f"✅ Ваше задание #{task_id} проверено!\n"
                f"💰 Вам начислено {reward:.2f} RUB."
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
        
        await add_referral_earning(user_id, reward, f"Задание #{task_id}")
        
        if callback.message.photo:
            await callback.message.edit_caption(
                caption=f"✅ Задание #{task_id} подтверждено!\n"
                       f"👤 Пользователь: ID {user_id}\n"
                       f"💰 Сумма: {reward:.2f} RUB"
            )
        else:
            await callback.message.edit_text(
                f"✅ Задание #{task_id} подтверждено!\n"
                f"👤 Пользователь: ID {user_id}\n"
                f"💰 Сумма: {reward:.2f} RUB"
            )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка при подтверждении задания: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("reject_task_"))
async def reject_task_completion(callback: types.CallbackQuery):
    try:
        parts = callback.data.split('_')
        user_id = int(parts[-2])
        task_id = int(parts[-1])
        
        cursor.execute("SELECT id FROM completed_tasks WHERE user_id = ? AND task_id = ?", (user_id, task_id))
        completion_id = cursor.fetchone()[0]
        
        cursor.execute("DELETE FROM completed_tasks WHERE id = ?", (completion_id,))
        cursor.execute("DELETE FROM task_screenshots WHERE task_completion_id = ?", (completion_id,))
        cursor.execute("UPDATE tasks SET completions_count = completions_count - 1 WHERE task_id = ?", (task_id,))
        conn.commit()
        
        try:
            await bot.send_message(
                user_id,
                f"❌ Ваше выполнение задания #{task_id} отклонено!\n"
                f"Причина: скриншоты не соответствуют требованиям"
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
        
        if callback.message.photo:
            await callback.message.edit_caption(
                caption=f"❌ Задание #{task_id} отклонено!\n"
                       f"👤 Пользователь: ID {user_id}"
            )
        else:
            await callback.message.edit_text(
                f"❌ Задание #{task_id} отклонено!\n"
                f"👤 Пользователь: ID {user_id}"
            )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка при отклонении задания: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

# ========== REFERRAL SYSTEM ==========
@dp.message(F.text == "👥 Реферальная программа")
async def show_referral_info(message: types.Message):
    if await check_ban_and_respond(message):
        return
        
    user_id = message.from_user.id
    cursor.execute("SELECT referrals_count, earned_from_refs FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    
    if row:
        refs_count, earned_from_refs = row
        
        bot_info = await bot.get_me()
        await message.answer(
            f"👥 Реферальная программа\n\n"
            f"🔗 Ваша ссылка: https://t.me/{bot_info.username}?start={user_id}\n\n"
            f"💎 Вы получаете 10% от заработка каждого приглашенного друга!\n"
            f"👥 Приглашено друзей: {refs_count}\n"
            f"💰 Заработано с рефералов: {earned_from_refs:.2f} RUB\n\n"
            f"Чем больше друзей вы пригласите, тем больше будет ваш пассивный доход!"
        )

# ========== PAYMENT SYSTEM ==========
@dp.message(F.text == "💰 Мой баланс")
async def show_balance(message: types.Message):
    if await check_ban_and_respond(message):
        return
        
    user_id = message.from_user.id
    cursor.execute("SELECT balance, earned, referrals_count, earned_from_refs FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    
    if row:
        balance, earned, refs_count, earned_from_refs = row
        await message.answer(
            f"💰 Ваш баланс: {balance:.2f} RUB\n"
            f"💵 Всего заработано: {earned:.2f} RUB\n"
            f"👥 Рефералов: {refs_count}\n"
            f"🎁 Заработано с рефералов: {earned_from_refs:.2f} RUB"
        )

@dp.message(F.text == "💸 Вывод средств")
async def withdraw_start(message: types.Message, state: FSMContext):
    if await check_ban_and_respond(message):
        return
        
    user_id = message.from_user.id
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    
    if row:
        balance = row[0]
        if balance < 50:
            await message.answer(f"❌ Минимальная сумма вывода - 50 RUB. Ваш баланс: {balance:.2f} RUB")
            return
        
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="💳 Вывод на карту")],
                [KeyboardButton(text="🤖 Вывод через CryptoBot")],
                [KeyboardButton(text="🔙 Назад")]
            ],
            resize_keyboard=True
        )
        
        await message.answer(
            f"💰 Ваш баланс: {balance:.2f} RUB\n"
            f"💳 Минимальная сумма вывода: 50 RUB\n\n"
            f"Выберите способ вывода:",
            reply_markup=keyboard
        )

@dp.message(F.text == "💳 Вывод на карту")
async def card_withdraw_start(message: types.Message, state: FSMContext):
    if await check_ban_and_respond(message):
        return
        
    user_id = message.from_user.id
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    balance = cursor.fetchone()[0]
    
    await state.set_state(WithdrawStates.amount)
    await state.update_data(withdrawal_method="card")
    await message.answer(
        f"💳 Вывод средств на банковскую карту\n\n"
        f"💰 Ваш баланс: {balance:.2f} RUB\n"
        f"💳 Минимальная сумма вывода: 50 RUB\n\n"
        f"Введите сумму для вывода:",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(F.text == "🤖 Вывод через CryptoBot")
async def cryptobot_withdraw_start(message: types.Message, state: FSMContext):
    if await check_ban_and_respond(message):
        return
        
    user_id = message.from_user.id
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    balance = cursor.fetchone()[0]
    
    await state.set_state(WithdrawStates.amount)
    await state.update_data(withdrawal_method="cryptobot")
    await message.answer(
        f"🤖 Вывод средств через CryptoBot\n\n"
        f"💰 Ваш баланс: {balance:.2f} RUB\n"
        f"💳 Минимальная сумма вывода: 50 RUB\n\n"
        f"Введите сумму для вывода:",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(WithdrawStates.amount)
async def process_withdraw_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        if amount < 50:
            await message.answer("❌ Минимальная сумма вывода - 50 RUB")
            return
            
        user_id = message.from_user.id
        cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        balance = cursor.fetchone()[0]
        
        if amount > balance:
            await message.answer(f"❌ Недостаточно средств. Ваш баланс: {balance:.2f} RUB")
            return
            
        await state.update_data(amount=amount)
        data = await state.get_data()
        
        if data.get('withdrawal_method') == "card":
            await state.set_state(WithdrawStates.wallet_data)
            await message.answer(
                "💳 Введите данные для вывода на карту:\n"
                "<b>Название банка, номер карты</b>\n\n"
                "Пример: <i>Тинькофф, 1234 5678 9012 3456</i>\n"
                "Можно ввести в любом формате",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode="HTML"
            )
        else:
            await state.set_state(WithdrawStates.wallet_data)
            await message.answer(
                "🤖 Введите ваш юзернейм в CryptoBot (например, @CryptoBot):\n\n"
                "Как найти юзернейм в CryptoBot:\n"
                "1. Откройте @CryptoBot\n"
                "2. Нажмите 'Начать'\n"
                "3. Ваш юзернейм будет указан в профиле",
                reply_markup=ReplyKeyboardRemove()
            )
        
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.message(WithdrawStates.wallet_data)
async def process_wallet_data(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    wallet_data = message.text.strip()
    data = await state.get_data()
    amount = data['amount']
    withdrawal_method = data.get('withdrawal_method')
    
    try:
        if withdrawal_method == "card":
            if "," in wallet_data:
                bank_name, card_number = wallet_data.split(",", 1)
                wallet_data = f"{bank_name.strip()}, {card_number.strip()}"
            else:
                wallet_data = f"Не указан, {wallet_data}"
            
            payment_method = "Bank Card"
            
        elif withdrawal_method == "cryptobot":
            if not wallet_data.startswith('@'):
                wallet_data = f"@{wallet_data}"
            payment_method = "CryptoBot"
        
        # Deduct balance
        cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))
        
        # Create withdrawal request
        cursor.execute(
            "INSERT INTO payments (user_id, amount, payment_method, wallet_data) VALUES (?, ?, ?, ?)",
            (user_id, amount, payment_method, wallet_data)
        )
        conn.commit()
        
        # Notify admin
        await bot.send_message(
            ADMIN_ID,
            f"⚠️ Новая заявка на вывод через {payment_method}!\n\n"
            f"👤 Пользователь: @{message.from_user.username} (ID: {user_id})\n"
            f"💰 Сумма: {amount:.2f} RUB\n"
            f"📌 Реквизиты: {wallet_data}"
        )
        
        await message.answer(
            f"✅ Заявка на вывод {amount:.2f} RUB создана!\n"
            f"Способ: {payment_method}\n"
            f"Реквизиты: {wallet_data}\n\n"
            f"Ожидайте обработки в течение 24 часов.",
            reply_markup=main_keyboard(user_id)
        )
        await state.clear()
        
    except Exception as e:
        logger.error(f"Ошибка при создании заявки на вывод: {e}")
        await message.answer(
            "❌ Произошла ошибка при создании заявки. Попробуйте позже.",
            reply_markup=main_keyboard(user_id)
        )
        await state.clear()

@dp.message(F.text == "🔙 Назад")
async def back_to_main_menu(message: types.Message):
    await message.answer("Главное меню", reply_markup=main_keyboard(message.from_user.id))

# ========== ADMIN PANEL ==========
@dp.message(F.text == "🔐 Админ-панель", F.from_user.id == ADMIN_ID)
async def admin_panel(message: types.Message):
    await message.answer("🔐 Админ-панель", reply_markup=admin_keyboard())

@dp.message(F.text == "🏠 Главное меню", F.from_user.id == ADMIN_ID)
async def back_to_main(message: types.Message):
    await message.answer("Главное меню", reply_markup=main_keyboard(message.from_user.id))

# Task management
@dp.message(F.text == "📝 Добавить задание", F.from_user.id == ADMIN_ID)
async def add_task_command(message: types.Message, state: FSMContext):
    await state.set_state(TaskStates.description)
    await message.answer("📝 Введите описание задания:", reply_markup=ReplyKeyboardRemove())

@dp.message(TaskStates.description, F.from_user.id == ADMIN_ID)
async def process_description(message: types.Message, state: FSMContext):
    await state.update_data(description=message.text)
    await state.set_state(TaskStates.link)
    await message.answer("🔗 Теперь введите ссылку для задания:")

@dp.message(TaskStates.link, F.from_user.id == ADMIN_ID)
async def process_link(message: types.Message, state: FSMContext):
    await state.update_data(link=message.text)
    await state.set_state(TaskStates.reward)
    await message.answer("💰 Введите вознаграждение (в RUB):")

@dp.message(TaskStates.reward, F.from_user.id == ADMIN_ID)
async def process_reward(message: types.Message, state: FSMContext):
    try:
        reward = float(message.text)
        if reward <= 0:
            await message.answer("❌ Вознаграждение должно быть больше 0!")
            return
    except ValueError:
        await message.answer("❌ Ошибка! Введите число.")
        return

    await state.update_data(reward=reward)
    await state.set_state(TaskStates.max_completions)
    await message.answer("🔢 Введите максимальное количество выполнений (или 0 для безлимита):")

@dp.message(TaskStates.max_completions, F.from_user.id == ADMIN_ID)
async def process_max_completions(message: types.Message, state: FSMContext):
    try:
        max_completions = int(message.text)
        if max_completions < 0:
            await message.answer("❌ Число должно быть положительным!")
            return
    except ValueError:
        await message.answer("❌ Ошибка! Введите целое число.")
        return

    data = await state.get_data()
    cursor.execute(
        "INSERT INTO tasks (description, link, reward, max_completions) VALUES (?, ?, ?, ?)",
        (data['description'], data['link'], data['reward'], max_completions if max_completions > 0 else None)
    )
    conn.commit()

    await state.clear()
    await message.answer("✅ Задание успешно добавлено!", reply_markup=admin_keyboard())


@dp.message(F.text == "📋 Список заданий", F.from_user.id == ADMIN_ID)
async def list_tasks(message: types.Message):
    cursor.execute("SELECT * FROM tasks ORDER BY task_id DESC")
    tasks = cursor.fetchall()

    if not tasks:
        await message.answer("📭 Нет активных заданий.")
        return

    response = "📋 Список заданий:\n\n"
    for task in tasks:
        task_id, description, link, reward, creation_date, is_active, max_completions, completions_count = task
        
        status = "🟢 Активно" if is_active else "🔴 Неактивно"
        limit = f"{completions_count}/{max_completions}" if max_completions else "∞"
        
        response += (
            f"🔹 ID: {task_id}\n"
            f"📝 Описание: {description}\n"
            f"🔗 Ссылка: {link}\n"
            f"💰 Награда: {reward:.2f} RUB\n"
            f"🔄 Выполнено: {limit}\n"
            f"📅 Дата: {creation_date}\n"
            f"Статус: {status}\n\n"
        )

    await message.answer(response[:4000], disable_web_page_preview=True)

# ========== TASK DELETION SYSTEM ==========
@dp.message(F.text == "❌ Удалить задание", F.from_user.id == ADMIN_ID)
async def delete_task_command(message: types.Message):
    try:
        cursor.execute("""
            SELECT task_id, description 
            FROM tasks 
            WHERE description IS NOT NULL
            ORDER BY task_id DESC
        """)
        tasks = cursor.fetchall()

        if not tasks:
            await message.answer("📭 Нет заданий для удаления.")
            return

        builder = InlineKeyboardBuilder()
        for task in tasks:
            task_id, description = task
            display_text = f"❌ Удалить #{task_id} - {description[:20]}" if description else f"❌ Удалить #{task_id}"
            builder.add(InlineKeyboardButton(
                text=display_text,
                callback_data=f"confirm_delete_task_{task_id}"
            ))
        builder.adjust(1)

        await message.answer(
            "📋 Выберите задание для удаления:\n"
            "⚠️ Это полностью удалит задание из базы данных!",
            reply_markup=builder.as_markup()
        )

    except Exception as e:
        logger.error(f"Ошибка при получении списка заданий: {e}")
        await message.answer(
            "❌ Произошла ошибка при загрузке списка заданий",
            reply_markup=admin_keyboard()
        )

@dp.callback_query(F.data.startswith("confirm_delete_task_"), F.from_user.id == ADMIN_ID)
async def confirm_delete_task(callback: types.CallbackQuery):
    try:
        task_id = int(callback.data.split('_')[-1])
        
        cursor.execute("SELECT description FROM tasks WHERE task_id = ?", (task_id,))
        task_data = cursor.fetchone()
        
        if not task_data:
            await callback.answer("❌ Задание не найдено", show_alert=True)
            return
            
        task_description = task_data[0] or "Без описания"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"final_delete_task_{task_id}"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_delete")
            ]
        ])
        
        await callback.message.edit_text(
            f"⚠️ Вы уверены, что хотите полностью удалить задание?\n\n"
            f"ID: {task_id}\n"
            f"Описание: {task_description[:100]}{'...' if len(task_description) > 100 else ''}\n\n"
            f"Это действие нельзя отменить!",
            reply_markup=keyboard
        )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка при подтверждении удаления: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("final_delete_task_"), F.from_user.id == ADMIN_ID)
async def final_delete_task(callback: types.CallbackQuery):
    try:
        task_id = int(callback.data.split('_')[-1])
        
        # Start transaction
        cursor.execute("BEGIN TRANSACTION")
        
        # Delete related records first
        cursor.execute("DELETE FROM completed_tasks WHERE task_id = ?", (task_id,))
        cursor.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
        
        conn.commit()
        
        await callback.message.edit_text(
            f"✅ Задание #{task_id} полностью удалено из базы данных!"
        )
        await callback.answer()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при удалении задания: {e}")
        await callback.answer("❌ Произошла ошибка при удалении", show_alert=True)
        await callback.message.edit_text(
            "❌ Не удалось удалить задание из-за ошибки базы данных"
        )

@dp.callback_query(F.data == "cancel_delete", F.from_user.id == ADMIN_ID)
async def cancel_delete(callback: types.CallbackQuery):
    try:
        await callback.message.edit_text("❌ Удаление отменено")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка при отмене удаления: {e}")
        await callback.answer("❌ Ошибка при отмене", show_alert=True)

# Управление выплатами
@dp.message(F.text == "📤 Заявки на вывод", F.from_user.id == ADMIN_ID)
async def show_withdraw_requests(message: types.Message):
    cursor.execute("""
        SELECT payment_id, user_id, amount, payment_method, wallet_data 
        FROM payments 
        WHERE status = 'pending' 
        ORDER BY payment_date
    """)
    requests = cursor.fetchall()

    if not requests:
        await message.answer("📭 Нет заявок на вывод.")
        return

    for req in requests:
        payment_id, user_id, amount, method, wallet = req
        
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_payment_{payment_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_payment_{payment_id}")
        )
        
        cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
        username = cursor.fetchone()[0] or "N/A"
        
        if method == "CryptoBot":
            instructions = (
                f"Для выплаты через CryptoBot:\n"
                f"1. Откройте @CryptoBot\n"
                f"2. Выберите 'Отправить'\n"
                f"3. Введите сумму: {amount:.2f} RUB\n"
                f"4. Укажите получателя: {wallet}"
            )
        else:
            bank_name, card_number = wallet.split(',', 1) if ',' in wallet else ("Не указан", wallet)
            instructions = (
                f"Для выплаты на карту:\n"
                f"1. Откройте приложение вашего банка\n"
                f"2. Переведите {amount:.2f} RUB\n"
                f"3. На карту: {card_number.strip()}\n"
                f"4. Банк: {bank_name.strip()}"
            )
        
        await message.answer(
            f"🆔 ID заявки: {payment_id}\n"
            f"👤 Пользователь: @{username} (ID: {user_id})\n"
            f"💰 Сумма: {amount:.2f} RUB\n"
            f"💳 Способ: {method}\n"
            f"📌 Реквизиты: {wallet}\n\n"
            f"{instructions}",
            reply_markup=builder.as_markup()
        )

@dp.callback_query(F.data.startswith("approve_payment_"), F.from_user.id == ADMIN_ID)
async def approve_payment(callback: types.CallbackQuery):
    try:
        payment_id = int(callback.data.split('_')[-1])
        
        cursor.execute("SELECT user_id, amount FROM payments WHERE payment_id = ?", (payment_id,))
        row = cursor.fetchone()
        
        if not row:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
            
        user_id, amount = row
        cursor.execute("UPDATE payments SET status = 'approved' WHERE payment_id = ?", (payment_id,))
        conn.commit()
        
        await callback.message.edit_text(
            f"✅ Заявка #{payment_id} одобрена!\n"
            f"👤 Пользователь: ID {user_id}\n"
            f"💰 Сумма: {amount:.2f} RUB"
        )
        
        try:
            await bot.send_message(
                user_id,
                f"✅ Ваша заявка на вывод {amount:.2f} RUB одобрена!\n"
                f"Деньги должны поступить в течение 24 часов."
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
            
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка при одобрении выплаты: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("reject_payment_"), F.from_user.id == ADMIN_ID)
async def reject_payment(callback: types.CallbackQuery):
    try:
        payment_id = int(callback.data.split('_')[-1])
        
        cursor.execute("SELECT user_id, amount FROM payments WHERE payment_id = ?", (payment_id,))
        row = cursor.fetchone()
        
        if not row:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
            
        user_id, amount = row
        cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        cursor.execute("UPDATE payments SET status = 'rejected' WHERE payment_id = ?", (payment_id,))
        conn.commit()
        
        await callback.message.edit_text(
            f"❌ Заявка #{payment_id} отклонена!\n"
            f"👤 Пользователь: ID {user_id}\n"
            f"💰 Сумма: {amount:.2f} RUB возвращена на баланс"
        )
        
        try:
            await bot.send_message(
                user_id,
                f"❌ Ваша заявка на вывод {amount:.2f} RUB отклонена.\n"
                f"Средства возвращены на ваш баланс."
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
            
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка при отклонении выплаты: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

# Заявки на выполнение заданий
@dp.message(F.text == "📤 Заявки на выполнение", F.from_user.id == ADMIN_ID)
async def show_task_completion_requests(message: types.Message):
    cursor.execute("""
        SELECT ct.id, ct.user_id, ct.task_id, t.description, u.username, ct.completion_date 
        FROM completed_tasks ct
        JOIN tasks t ON ct.task_id = t.task_id
        JOIN users u ON ct.user_id = u.user_id
        WHERE ct.is_verified = FALSE
        ORDER BY ct.completion_date
    """)
    requests = cursor.fetchall()

    if not requests:
        await message.answer("📭 Нет заявок на выполнение заданий.")
        return

    for req in requests:
        request_id, user_id, task_id, task_desc, username, completion_date = req
        
        cursor.execute("""
            SELECT screenshot_id FROM (
                SELECT screenshot_id FROM completed_tasks WHERE id = ?
                UNION ALL
                SELECT screenshot_id FROM task_screenshots WHERE task_completion_id = ?
            ) ORDER BY CASE WHEN rowid = 1 THEN 0 ELSE 1 END
        """, (request_id, request_id))
        screenshots = [row[0] for row in cursor.fetchall()]
        
        if not screenshots:
            await message.answer(
                f"🆔 ID заявки: {request_id}\n"
                f"📌 Задание #{task_id}: {task_desc[:50]}...\n"
                f"👤 Пользователь: @{username} (ID: {user_id})\n"
                f"📅 Дата выполнения: {completion_date}\n"
                f"⚠️ Скриншоты отсутствуют!",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"verify_task_{user_id}_{task_id}"),
                        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_task_{user_id}_{task_id}")
                    ]
                ])
            )
            continue
        
        media_group = []
        for i, screenshot_id in enumerate(screenshots, start=1):
            media_group.append(
                InputMediaPhoto(
                    media=screenshot_id,
                    caption=(
                        f"🆔 ID заявки: {request_id}\n"
                        f"📌 Задание #{task_id}: {task_desc[:50]}...\n"
                        f"👤 Пользователь: @{username} (ID: {user_id})\n"
                        f"📅 Дата выполнения: {completion_date}\n"
                        f"📸 Скриншот {i}/{len(screenshots)}"
                    ) if i == 1 else ""
                )
            )
        
        await bot.send_media_group(
            message.chat.id,
            media=media_group
        )
        
        await message.answer(
            "Выберите действие:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"verify_task_{user_id}_{task_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_task_{user_id}_{task_id}")
                ]
            ])
        )

# Управление пользователями
@dp.message(F.text == "👥 Список пользователей", F.from_user.id == ADMIN_ID)
async def list_users(message: types.Message):
    cursor.execute("""
        SELECT user_id, username, balance, referrals_count, earned_from_refs, is_banned 
        FROM users 
        ORDER BY is_banned, balance DESC 
        LIMIT 50
    """)
    users = cursor.fetchall()

    if not users:
        await message.answer("📭 Нет пользователей в базе.")
        return

    for user in users:
        user_id, username, balance, refs_count, earned_from_refs, is_banned = user
        
        status = "🔴 Забанен" if is_banned else "🟢 Активен"
        username = username if username else "N/A"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⛔ Забанить" if not is_banned else "✅ Разбанить",
                    callback_data=f"toggle_ban_{user_id}"
                ),
                InlineKeyboardButton(
                    text="👀 Подробнее",
                    callback_data=f"user_details_{user_id}"
                )
            ]
        ])
        
        await message.answer(
            f"🆔 ID: {user_id}\n"
            f"👤 Ник: @{username}\n"
            f"💰 Баланс: {balance:.2f} RUB\n"
            f"👥 Рефералов: {refs_count}\n"
            f"💎 Заработано с рефералов: {earned_from_refs:.2f} RUB\n"
            f"Статус: {status}",
            reply_markup=keyboard
        )

@dp.callback_query(F.data.startswith("toggle_ban_"), F.from_user.id == ADMIN_ID)
async def toggle_user_ban(callback: types.CallbackQuery):
    user_id = int(callback.data.split('_')[-1])
    
    cursor.execute("SELECT is_banned, username FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    
    if not result:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
        
    is_banned, username = result
    new_status = not is_banned
    username = username if username else "N/A"
    
    cursor.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (new_status, user_id))
    conn.commit()
    
    action = "забанен" if new_status else "разбанен"
    emoji = "⛔" if new_status else "✅"
    
    await callback.message.edit_text(
        f"{emoji} Пользователь @{username} (ID: {user_id}) {action}!\n\n"
        f"Новый статус: {'🔴 Забанен' if new_status else '🟢 Активен'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад к списку", callback_data="back_to_users_list")]
        ])
    )
    
    if new_status:
        try:
            await bot.send_message(
                user_id,
                "⛔ Ваш аккаунт был заблокирован администратором.\n\n"
                "Вы больше не можете выполнять задания или выводить средства."
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
    else:
        try:
            await bot.send_message(
                user_id,
                "✅ Вы разбанены администратором!\n"
                "Для использования бота нажмите /start",
                reply_markup=ReplyKeyboardRemove()
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
    
    await callback.answer(f"Пользователь {action}")

@dp.callback_query(F.data.startswith("user_details_"), F.from_user.id == ADMIN_ID)
async def show_user_details(callback: types.CallbackQuery):
    user_id = int(callback.data.split('_')[-1])
    
    cursor.execute("""
        SELECT u.username, u.balance, u.earned, u.referrals_count, u.earned_from_refs, 
               u.is_banned, u.registration_date,
               COUNT(ct.id) as completed_tasks,
               COUNT(p.payment_id) as payments
        FROM users u
        LEFT JOIN completed_tasks ct ON u.user_id = ct.user_id
        LEFT JOIN payments p ON u.user_id = p.user_id
        WHERE u.user_id = ?
    """, (user_id,))
    
    user_data = cursor.fetchone()
    
    if not user_data:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
        
    (username, balance, earned, refs_count, earned_from_refs, 
     is_banned, reg_date, completed_tasks, payments) = user_data
    
    status = "🔴 Забанен" if is_banned else "🟢 Активен"
    username = username if username else "N/A"
    
    cursor.execute("""
        SELECT u.username, re.amount, re.earning_date 
        FROM referral_earnings re
        JOIN users u ON re.referral_id = u.user_id
        WHERE re.referrer_id = ?
        ORDER BY re.earning_date DESC
        LIMIT 5
    """, (user_id,))
    referrals = cursor.fetchall()
    
    ref_info = "\n".join(
        [f"• @{ref[0]} ({ref[1]:.2f} RUB, {ref[2]})" for ref in referrals]
    ) if referrals else "Нет рефералов"
    
    await callback.message.edit_text(
        f"📊 Подробная информация о пользователе:\n\n"
        f"🆔 ID: {user_id}\n"
        f"👤 Ник: @{username}\n"
        f"💰 Баланс: {balance:.2f} RUB\n"
        f"💵 Всего заработано: {earned:.2f} RUB\n"
        f"👥 Рефералов: {refs_count}\n"
        f"💎 Заработано с рефералов: {earned_from_refs:.2f} RUB\n"
        f"✅ Выполнено заданий: {completed_tasks}\n"
        f"💸 Выводов средств: {payments}\n"
        f"📅 Дата регистрации: {reg_date}\n"
        f"Статус: {status}\n\n"
        f"Последние рефералы:\n{ref_info}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⛔ Забанить" if not is_banned else "✅ Разбанить",
                    callback_data=f"toggle_ban_{user_id}"
                ),
                InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_users_list")
            ]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_users_list", F.from_user.id == ADMIN_ID)
async def back_to_users_list(callback: types.CallbackQuery):
    await callback.message.delete()
    await list_users(callback.message)
    await callback.answer()

# ========== BROADCAST SYSTEM ==========
class BroadcastStates(StatesGroup):
    message_content = State()
    confirmation = State()

@dp.message(F.text == "📢 Рассылка", F.from_user.id == ADMIN_ID)
async def start_broadcast(message: types.Message, state: FSMContext):
    await state.set_state(BroadcastStates.message_content)
    await message.answer(
        "📢 Отправьте сообщение для рассылки (текст, фото, фото с подписью):",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(BroadcastStates.message_content, F.content_type.in_({'text', 'photo'}))
async def process_broadcast_content(message: types.Message, state: FSMContext):
    content = {}
    
    if message.photo:
        content.update({
            'type': 'photo',
            'file_id': message.photo[-1].file_id,
            'caption': message.caption or ""
        })
    else:
        content.update({
            'type': 'text',
            'text': message.text
        })
    
    await state.update_data(content=content)
    
    # Store the sent message for later editing
    sent_message = None
    preview_text = "📢 Предпросмотр рассылки:\n\n"
    
    if content['type'] == 'photo':
        preview_text += f"🖼 Фото + текст: {content['caption']}" if content['caption'] else "🖼 Фото без текста"
        sent_message = await message.answer_photo(
            content['file_id'],
            caption=preview_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_broadcast")],
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_broadcast")]
            ])
        )
    else:
        preview_text += content['text']
        sent_message = await message.answer(
            preview_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_broadcast")],
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_broadcast")]
            ])
        )
    
    await state.update_data(preview_message_id=sent_message.message_id)
    await state.set_state(BroadcastStates.confirmation)

@dp.callback_query(F.data == "confirm_broadcast", BroadcastStates.confirmation, F.from_user.id == ADMIN_ID)
async def confirm_broadcast(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    content = data['content']
    
    try:
        # Edit the original preview message first
        try:
            if content['type'] == 'photo':
                await callback.message.edit_caption(
                    caption="📢 Рассылка начата...",
                    reply_markup=None
                )
            else:
                await callback.message.edit_text(
                    "📢 Рассылка начата...",
                    reply_markup=None
                )
        except Exception as e:
            logger.error(f"Couldn't edit preview message: {e}")

        cursor.execute("SELECT user_id FROM users WHERE is_banned = FALSE")
        users = cursor.fetchall()
        
        success = 0
        failed = 0
        
        for user in users:
            try:
                if content['type'] == 'photo':
                    await bot.send_photo(
                        user[0],
                        content['file_id'],
                        caption=content['caption']
                    )
                else:
                    await bot.send_message(
                        user[0],
                        content['text']
                    )
                success += 1
            except Exception as e:
                logger.error(f"Failed to send to user {user[0]}: {e}")
                failed += 1
        
        # Send results as new message instead of editing
        await callback.message.answer(
            f"📢 Рассылка завершена!\n\n"
            f"✅ Успешно: {success}\n"
            f"❌ Не удалось: {failed}",
            reply_markup=admin_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Broadcast error: {e}")
        await callback.message.answer(
            "❌ Ошибка при рассылке",
            reply_markup=admin_keyboard()
        )
    finally:
        await state.clear()

@dp.callback_query(F.data == "cancel_broadcast", BroadcastStates.confirmation, F.from_user.id == ADMIN_ID)
async def cancel_broadcast(callback: types.CallbackQuery, state: FSMContext):
    try:
        if callback.message.photo:
            await callback.message.edit_caption(
                caption="❌ Рассылка отменена",
                reply_markup=None
            )
        else:
            await callback.message.edit_text(
                "❌ Рассылка отменена",
                reply_markup=None
            )
    except Exception as e:
        logger.error(f"Couldn't edit cancel message: {e}")
        await callback.message.answer(
            "❌ Рассылка отменена",
            reply_markup=admin_keyboard()
        )
    
    await state.clear()
    await bot.send_message(
        callback.from_user.id,
        "Главное меню",
        reply_markup=admin_keyboard()
    )

@dp.message(BroadcastStates.message_content)
async def wrong_broadcast_content(message: types.Message):
    await message.answer(
        "❌ Пожалуйста, отправьте только текст или фото (с подписью или без)",
        reply_markup=ReplyKeyboardRemove()
    )

# Статистика
@dp.message(F.text == "📊 Статистика", F.from_user.id == ADMIN_ID)
async def show_stats(message: types.Message):
    cursor.execute("SELECT COUNT(*) FROM users")
    users_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM users WHERE is_banned = TRUE")
    banned_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM tasks WHERE is_active = TRUE")
    active_tasks = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM completed_tasks")
    completed_tasks = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(balance) FROM users")
    total_balance = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT SUM(earned_from_refs) FROM users")
    total_ref_earnings = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT SUM(amount) FROM payments WHERE status = 'approved'")
    total_payouts = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT SUM(amount) FROM payments WHERE status = 'pending'")
    pending_payouts = cursor.fetchone()[0] or 0

    await message.answer(
        f"📊 Статистика бота:\n\n"
        f"👥 Пользователей: {users_count} (🔴 {banned_count} забанено)\n"
        f"📌 Активных заданий: {active_tasks}\n"
        f"✅ Выполнено заданий: {completed_tasks}\n"
        f"💰 Общий баланс: {total_balance:.2f} RUB\n"
        f"💎 Заработано с рефералов: {total_ref_earnings:.2f} RUB\n"
        f"💸 Выплачено: {total_payouts:.2f} RUB\n"
        f"⏳ На выплате: {pending_payouts:.2f} RUB"
    )

# ========== ЗАПУСК БОТА ==========
async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())