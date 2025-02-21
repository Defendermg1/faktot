#!/usr/bin/env python3
"""
Factory Empires Bot – версия с профилем клана и лимитами по типам фирм.
Дата: 21 февраля 2025.
"""

import logging
import sqlite3
from datetime import datetime, timedelta
import pytz

from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler

# ---------------------------- Конфигурация ----------------------------
BOT_TOKEN = '8025017306:AAHsps3yLqL1aw-QvDrbRXJ6jim4xUye-ls'
BOT_USERNAME = 'FactoryEmpiresBot'  # для формирования реферальной ссылки
ADMIN_ID = 6901597812

INITIAL_BALANCE = 500
REFERRAL_BONUS = 1000
REFERRAL_TOKEN_BONUS = 3
DAILY_REWARD = 100
MAX_FIRMS_PER_TYPE = 5

FIRM_TYPES = {
    1: {"name": "Мини цех",     "price": 200,   "income": 20},
    2: {"name": "Мастерская",   "price": 5000,  "income": 50},
    3: {"name": "Ателье",       "price": 10000, "income": 100},
    4: {"name": "Фабрика",      "price": 25000, "income": 250},
    5: {"name": "Комбинат",     "price": 50000, "income": 500},
}

WORKER_INCOME = 5
WORKER_COST = 50
MAX_WORKERS_PER_FIRM = 100

# ---------------------------- Логирование ----------------------------
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------- Клавиатуры ----------------------------
def get_main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton("Профиль"), KeyboardButton("Купить фирму")],
        [KeyboardButton("Мои фабрики"), KeyboardButton("Купить рабочих")],
        [KeyboardButton("Дневная награда"), KeyboardButton("Клан")],
        [KeyboardButton("Аукцион"), KeyboardButton("Рефералы")],
        [KeyboardButton("Топ")]
    ]
    if user_id == ADMIN_ID:
        kb.append([KeyboardButton("Админ-панель")])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def get_clan_keyboard(clan_id: int = None) -> ReplyKeyboardMarkup:
    if clan_id:
        return ReplyKeyboardMarkup([
            [KeyboardButton("Клан"), KeyboardButton("Состав")],
            [KeyboardButton("Внести деньги"), KeyboardButton("Аукцион клана")],
            [KeyboardButton("Фирмы клана"), KeyboardButton("Покинуть клан")],
            [KeyboardButton("Назад")]
        ], resize_keyboard=True)
    return ReplyKeyboardMarkup([
        [KeyboardButton("Создать клан"), KeyboardButton("Вступить в клан")],
        [KeyboardButton("Назад")]
    ], resize_keyboard=True)

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([
        [KeyboardButton("/ban"), KeyboardButton("/unban")],
        [KeyboardButton("/reward"), KeyboardButton("/withdraw")],
        [KeyboardButton("Назад")]
    ], resize_keyboard=True)

def get_buy_firm_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[KeyboardButton(info["name"])] for info in FIRM_TYPES.values()] + [[KeyboardButton("Отмена")]], resize_keyboard=True)

# ---------------------------- База данных ----------------------------
conn = sqlite3.connect('factory_empires.db', check_same_thread=False)
cursor = conn.cursor()

def init_db():
    try:
        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                balance INTEGER DEFAULT 0,
                donation_tokens INTEGER DEFAULT 0,
                referral_id INTEGER,
                chat_id INTEGER,
                banned INTEGER DEFAULT 0,
                last_daily TEXT
            );
            CREATE TABLE IF NOT EXISTS firms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                firm_type INTEGER,
                purchase_time TEXT,
                workers INTEGER DEFAULT 0,
                custom_name TEXT,
                custom_income INTEGER,
                clan_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS auctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_type INTEGER,
                min_price INTEGER,
                duration INTEGER,
                end_time TEXT,
                highest_bid INTEGER DEFAULT 0,
                highest_bidder INTEGER,
                active INTEGER DEFAULT 1,
                custom_name TEXT,
                custom_income INTEGER,
                is_clan INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS clans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                emblem TEXT,
                leader_id INTEGER,
                money INTEGER DEFAULT 0,
                donation_tokens INTEGER DEFAULT 0,
                exp INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS clan_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                clan_id INTEGER,
                user_id INTEGER,
                role TEXT
            );
            CREATE TABLE IF NOT EXISTS clan_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                clan_id INTEGER,
                user_id INTEGER
            );
        ''')
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Database init error: {e}")

# ---------------------------- Функции данных ----------------------------
def add_user(user_id: int, username: str, chat_id: int, referral_id: int = None) -> bool:
    try:
        if not get_user(user_id):
            cursor.execute('INSERT INTO users (user_id, username, balance, referral_id, chat_id) VALUES (?, ?, ?, ?, ?)',
                           (user_id, username, INITIAL_BALANCE, referral_id, chat_id))
            conn.commit()
            if referral_id and referral_id != user_id:
                update_balance(referral_id, REFERRAL_BONUS)
                update_donation_tokens(referral_id, REFERRAL_TOKEN_BONUS)
            logger.info(f"User {user_id} registered")
            return True
    except sqlite3.Error as e:
        logger.error(f"Add user error: {e}")
    return False

def get_user(user_id: int):
    try:
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        return cursor.fetchone()
    except sqlite3.Error as e:
        logger.error(f"Get user error: {e}")
        return None

def update_balance(user_id: int, amount: int):
    try:
        cursor.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Update balance error: {e}")

def update_donation_tokens(user_id: int, amount: int):
    try:
        cursor.execute('UPDATE users SET donation_tokens = donation_tokens + ? WHERE user_id = ?', (amount, user_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Update tokens error: {e}")

def update_last_daily(user_id: int, timestamp: str):
    try:
        cursor.execute('UPDATE users SET last_daily = ? WHERE user_id = ?', (timestamp, user_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Update last daily error: {e}")

def add_firm(user_id: int = None, firm_type: int = None, custom_name: str = None, custom_income: int = None, clan_id: int = None):
    try:
        purchase_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('INSERT INTO firms (user_id, firm_type, purchase_time, custom_name, custom_income, clan_id) VALUES (?, ?, ?, ?, ?, ?)',
                       (user_id, firm_type, purchase_time, custom_name, custom_income, clan_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Add firm error: {e}")

def get_firms(user_id: int = None, clan_id: int = None):
    try:
        if user_id:
            cursor.execute('SELECT * FROM firms WHERE user_id = ? AND clan_id IS NULL', (user_id,))
        elif clan_id:
            cursor.execute('SELECT * FROM firms WHERE clan_id = ?', (clan_id,))
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"Get firms error: {e}")
        return []

def count_firms_by_type(user_id: int, firm_type: int) -> int:
    try:
        cursor.execute('SELECT COUNT(*) FROM firms WHERE user_id = ? AND firm_type = ? AND clan_id IS NULL', (user_id, firm_type))
        return cursor.fetchone()[0]
    except sqlite3.Error as e:
        logger.error(f"Count firms by type error: {e}")
        return 0

def get_total_income(user_id: int) -> int:
    try:
        firms = get_firms(user_id)
        return sum((f[6] if f[2] == 0 else FIRM_TYPES.get(f[2], {"income": 0})["income"]) + f[4] * WORKER_INCOME for f in firms)
    except Exception as e:
        logger.error(f"Get total income error: {e}")
        return 0

def update_firm_workers(firm_id: int, workers: int):
    try:
        cursor.execute('UPDATE firms SET workers = workers + ? WHERE id = ?', (workers, firm_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Update firm workers error: {e}")

def get_clan(user_id: int):
    try:
        cursor.execute('SELECT clan_id FROM clan_members WHERE user_id = ?', (user_id,))
        res = cursor.fetchone()
        if res:
            cursor.execute('SELECT * FROM clans WHERE id = ?', (res[0],))
            return cursor.fetchone()
        return None
    except sqlite3.Error as e:
        logger.error(f"Get clan error: {e}")
        return None

def create_clan(name: str, emblem: str, leader_id: int) -> int:
    try:
        cursor.execute('INSERT INTO clans (name, emblem, leader_id) VALUES (?, ?, ?)', (name, emblem, leader_id))
        clan_id = cursor.lastrowid
        cursor.execute('INSERT INTO clan_members (clan_id, user_id, role) VALUES (?, ?, ?)', (clan_id, leader_id, "Лидер"))
        conn.commit()
        return clan_id
    except sqlite3.IntegrityError:
        return -1
    except sqlite3.Error as e:
        logger.error(f"Create clan error: {e}")
        return -1

def add_clan_member(clan_id: int, user_id: int, role: str = "Участник"):
    try:
        cursor.execute('INSERT INTO clan_members (clan_id, user_id, role) VALUES (?, ?, ?)', (clan_id, user_id, role))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Add clan member error: {e}")

def remove_clan_member(user_id: int):
    try:
        cursor.execute('DELETE FROM clan_members WHERE user_id = ?', (user_id,))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Remove clan member error: {e}")

def get_clan_members(clan_id: int):
    try:
        cursor.execute('SELECT user_id, role FROM clan_members WHERE clan_id = ?', (clan_id,))
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"Get clan members error: {e}")
        return []

def get_clan_requests(clan_id: int):
    try:
        cursor.execute('SELECT user_id FROM clan_requests WHERE clan_id = ?', (clan_id,))
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"Get clan requests error: {e}")
        return []

def create_auction(firm_type: int, min_price: int, duration: int, custom_name: str = None, custom_income: int = None, is_clan: int = 0):
    try:
        end_time = (datetime.now() + timedelta(minutes=duration)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('INSERT INTO auctions (firm_type, min_price, duration, end_time, custom_name, custom_income, is_clan) VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (firm_type, min_price, duration, end_time, custom_name, custom_income, is_clan))
        conn.commit()
        return cursor.lastrowid
    except sqlite3.Error as e:
        logger.error(f"Create auction error: {e}")
        return -1

def get_active_auctions(is_clan: bool = False):
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('SELECT * FROM auctions WHERE active = 1 AND end_time > ? AND is_clan = ?', (now, 1 if is_clan else 0))
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"Get active auctions error: {e}")
        return []

def update_auction_bid(auction_id: int, bid: int, bidder: int):
    try:
        cursor.execute('UPDATE auctions SET highest_bid = ?, highest_bidder = ? WHERE id = ?', (bid, bidder, auction_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Update auction bid error: {e}")

def end_auction(auction_id: int):
    try:
        cursor.execute('SELECT * FROM auctions WHERE id = ?', (auction_id,))
        auction = cursor.fetchone()
        if auction and auction[7]:
            cursor.execute('UPDATE auctions SET active = 0 WHERE id = ?', (auction_id,))
            if auction[6]:
                if auction[10]:
                    clan = get_clan(auction[6])
                    if clan:
                        add_firm(clan_id=clan[0], firm_type=0, custom_name=auction[8], custom_income=auction[9])
                else:
                    add_firm(auction[6], 0, auction[8], auction[9])
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"End auction error: {e}")

# ---------------------------- Команды ----------------------------
def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    referral_id = int(context.args[0]) if context.args else None
    if add_user(user.id, user.username, update.effective_chat.id, referral_id):
        update.message.reply_text(f"Добро пожаловать! Баланс: {INITIAL_BALANCE}", reply_markup=get_main_keyboard(user.id))
    else:
        update.message.reply_text(f"С возвращением! Баланс: {get_user(user.id)[2]}", reply_markup=get_main_keyboard(user.id))

def profile_command(update: Update, context: CallbackContext):
    user = update.effective_user
    user_data = get_user(user.id)
    if not user_data or user_data[6]:
        update.message.reply_text("Вы не зарегистрированы или забанены.", reply_markup=get_main_keyboard(user.id))
        return
    income = get_total_income(user.id)
    firms = get_firms(user.id)
    firm_counts = {ft: sum(1 for f in firms if f[2] == ft) for ft in FIRM_TYPES}
    firm_msg = "\n".join(f"{FIRM_TYPES[ft]['name']}: {count}/{MAX_FIRMS_PER_TYPE}" for ft, count in firm_counts.items() if count > 0)
    msg = (f"Профиль @{user_data[1]}:\nID: {user.id}\nБаланс: {user_data[2]}\nТокены: {user_data[3]}\n"
           f"Фирмы:\n{firm_msg or 'Нет фирм'}\nДоход: {income} в час")
    update.message.reply_text(msg, reply_markup=get_main_keyboard(user.id))

def buy_firm_command(update: Update, context: CallbackContext):
    user = update.effective_user
    context.user_data["buy_firm"] = True
    update.message.reply_text("Выберите фирму:", reply_markup=get_buy_firm_keyboard())

def my_firms_command(update: Update, context: CallbackContext):
    user = update.effective_user
    firms = get_firms(user.id)
    if not firms:
        update.message.reply_text("У вас нет фирм.", reply_markup=get_main_keyboard(user.id))
        return
    firm_counts = {ft: sum(1 for f in firms if f[2] == ft) for ft in FIRM_TYPES}
    msg = "Ваши фирмы:\n" + "\n".join(
        f"{FIRM_TYPES[ft]['name']}: {count}/{MAX_FIRMS_PER_TYPE}" for ft, count in firm_counts.items() if count > 0)
    update.message.reply_text(msg, reply_markup=get_main_keyboard(user.id))

def buy_workers_command(update: Update, context: CallbackContext):
    user = update.effective_user
    try:
        firm_id, qty = map(int, context.args)
        firm = cursor.execute('SELECT * FROM firms WHERE id = ? AND user_id = ?', (firm_id, user.id)).fetchone()
        if not firm or firm[7]:
            update.message.reply_text("Фирма не найдена или принадлежит клану.", reply_markup=get_main_keyboard(user.id))
            return
        if firm[4] + qty > MAX_WORKERS_PER_FIRM:
            update.message.reply_text(f"Максимум рабочих: {MAX_WORKERS_PER_FIRM}", reply_markup=get_main_keyboard(user.id))
            return
        cost = qty * WORKER_COST
        user_data = get_user(user.id)
        if user_data[2] < cost:
            update.message.reply_text("Недостаточно монет.", reply_markup=get_main_keyboard(user.id))
            return
        update_balance(user.id, -cost)
        update_firm_workers(firm_id, qty)
        update.message.reply_text(f"Куплено {qty} рабочих за {cost}.", reply_markup=get_main_keyboard(user.id))
    except (ValueError, sqlite3.Error) as e:
        update.message.reply_text("Используйте: /buy_workers <id> <кол-во>", reply_markup=get_main_keyboard(user.id))
        logger.error(f"Buy workers error: {e}")

def daily_reward_command(update: Update, context: CallbackContext):
    user = update.effective_user
    user_data = get_user(user.id)
    if not user_data:
        update.message.reply_text("Вы не зарегистрированы.", reply_markup=get_main_keyboard(user.id))
        return
    now = datetime.now(pytz.utc)
    last = user_data[7] and datetime.strptime(user_data[7], '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
    if last and (now - last) < timedelta(hours=24):
        update.message.reply_text("Награда доступна раз в 24 часа.", reply_markup=get_main_keyboard(user.id))
        return
    update_balance(user.id, DAILY_REWARD)
    update_last_daily(user.id, now.strftime('%Y-%m-%d %H:%M:%S'))
    update.message.reply_text(f"Получено {DAILY_REWARD} монет!", reply_markup=get_main_keyboard(user.id))

def clan_command(update: Update, context: CallbackContext):
    user = update.effective_user
    clan = get_clan(user.id)
    if not clan:
        update.message.reply_text("Вы не состоите в клане.", reply_markup=get_clan_keyboard())
        return
    members = get_clan_members(clan[0])
    msg = (f"Профиль клана '{clan[1]}' {clan[2]}:\n"
           f"Лидер: ID {clan[3]}\n"
           f"Казна: {clan[4]} монет\n"
           f"Участников: {len(members)}\n"
           f"Состав:\n" + "\n".join(f"ID: {m[0]} | {m[1]}" for m in members[:5]) + (f"\nи ещё {len(members)-5}..." if len(members) > 5 else ""))
    update.message.reply_text(msg, reply_markup=get_clan_keyboard(clan[0]))

def create_clan_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if get_clan(user.id):
        update.message.reply_text("Вы уже в клане.", reply_markup=get_main_keyboard(user.id))
        return
    if not context.args or len(context.args) < 2:
        update.message.reply_text("Используйте: /create_clan <название> <эмблема>", reply_markup=get_main_keyboard(user.id))
        return
    name, emblem = context.args[0], context.args[1]
    clan_id = create_clan(name, emblem, user.id)
    if clan_id == -1:
        update.message.reply_text("Клан с таким названием уже существует.", reply_markup=get_main_keyboard(user.id))
    else:
        update.message.reply_text(f"Клан '{name}' создан! ID: {clan_id}", reply_markup=get_clan_keyboard(clan_id))

def join_clan_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if get_clan(user.id):
        update.message.reply_text("Вы уже в клане.", reply_markup=get_main_keyboard(user.id))
        return
    clans = cursor.execute('SELECT id, name, emblem FROM clans LIMIT 10').fetchall()
    if not clans:
        update.message.reply_text("Нет доступных кланов.", reply_markup=get_main_keyboard(user.id))
        return
    keyboard = [[InlineKeyboardButton(f"{n} {e}", callback_data=f"join:{i}")] for i, n, e in clans]
    update.message.reply_text("Выберите клан:", reply_markup=InlineKeyboardMarkup(keyboard))

def clan_members_command(update: Update, context: CallbackContext):
    user = update.effective_user
    clan = get_clan(user.id)
    if not clan:
        update.message.reply_text("Вы не в клане.", reply_markup=get_main_keyboard(user.id))
        return
    members = get_clan_members(clan[0])
    requests = get_clan_requests(clan[0]) if clan[3] == user.id else []
    msg = "Состав клана:\n" + "\n".join(f"ID: {m[0]} | {m[1]}" for m in members)
    if requests and clan[3] == user.id:
        msg += "\n\nЗаявки на вступление:\n" + "\n".join(f"ID: {r[0]}" for r in requests)
        keyboard = [[InlineKeyboardButton(f"Принять {r[0]}", callback_data=f"accept:{r[0]}")] for r in requests] + \
                  [[InlineKeyboardButton(f"Исключить {m[0]}", callback_data=f"exclude:{m[0]}")] for m in members if m[1] != "Лидер"]
        update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        update.message.reply_text(msg, reply_markup=get_clan_keyboard(clan[0]))

def contribute_clan_command(update: Update, context: CallbackContext):
    user = update.effective_user
    clan = get_clan(user.id)
    if not clan:
        update.message.reply_text("Вы не в клане.", reply_markup=get_main_keyboard(user.id))
        return
    try:
        amount = int(context.args[0])
        user_data = get_user(user.id)
        if user_data[2] < amount:
            update.message.reply_text("Недостаточно монет.", reply_markup=get_clan_keyboard(clan[0]))
            return
        update_balance(user.id, -amount)
        cursor.execute('UPDATE clans SET money = money + ? WHERE id = ?', (amount, clan[0]))
        conn.commit()
        update.message.reply_text(f"Внесено {amount} в казну.", reply_markup=get_clan_keyboard(clan[0]))
    except:
        update.message.reply_text("Используйте: /contribute <сумма>", reply_markup=get_clan_keyboard(clan[0]))

def auction_command(update: Update, context: CallbackContext, is_clan: bool = False):
    user = update.effective_user
    auctions = get_active_auctions(is_clan)
    if not auctions:
        update.message.reply_text("Нет активных аукционов.", reply_markup=get_main_keyboard(user.id) if not is_clan else get_clan_keyboard(get_clan(user.id)[0]))
        return
    msg = "Аукционы:\n" + "\n".join(f"ID: {a[0]} | {a[8]} | Ставка: {a[5] or a[2]} | До: {a[4]}" for a in auctions)
    keyboard = [[InlineKeyboardButton(f"Ставка {a[0]}", callback_data=f"bid:{a[0]}")] for a in auctions]
    update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))

def clan_firms_command(update: Update, context: CallbackContext):
    user = update.effective_user
    clan = get_clan(user.id)
    if not clan:
        update.message.reply_text("Вы не в клане.", reply_markup=get_main_keyboard(user.id))
        return
    firms = get_firms(clan_id=clan[0])
    if not firms:
        update.message.reply_text("У клана нет фирм.", reply_markup=get_clan_keyboard(clan[0]))
        return
    msg = "Фирмы клана:\n" + "\n".join(f"{f[5]} | Доход: {f[6]}" for f in firms)
    update.message.reply_text(msg, reply_markup=get_clan_keyboard(clan[0]))

def leave_clan_command(update: Update, context: CallbackContext):
    user = update.effective_user
    clan = get_clan(user.id)
    if not clan:
        update.message.reply_text("Вы не в клане.", reply_markup=get_main_keyboard(user.id))
        return
    if clan[3] == user.id:
        update.message.reply_text("Лидер не может покинуть клан. Используйте /disband.", reply_markup=get_clan_keyboard(clan[0]))
        return
    remove_clan_member(user.id)
    update.message.reply_text("Вы покинули клан.", reply_markup=get_main_keyboard(user.id))

def disband_clan_command(update: Update, context: CallbackContext):
    user = update.effective_user
    clan = get_clan(user.id)
    if not clan or clan[3] != user.id:
        update.message.reply_text("Только лидер может расформировать клан.", reply_markup=get_main_keyboard(user.id))
        return
    try:
        cursor.executescript('DELETE FROM clan_members WHERE clan_id = ?; DELETE FROM clans WHERE id = ?; DELETE FROM firms WHERE clan_id = ?',
                             (clan[0], clan[0], clan[0]))
        conn.commit()
        update.message.reply_text("Клан расформирован.", reply_markup=get_main_keyboard(user.id))
    except sqlite3.Error as e:
        logger.error(f"Disband clan error: {e}")
        update.message.reply_text("Ошибка при расформировании.", reply_markup=get_main_keyboard(user.id))

def admin_panel_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if user.id != ADMIN_ID:
        update.message.reply_text("Доступно только админу.", reply_markup=get_main_keyboard(user.id))
        return
    update.message.reply_text("Админ-панель:\nИспользуйте команды:\n/ban <id>\n/unban <id>\n/reward <id> <сумма>\n/withdraw <id> <сумма>", reply_markup=get_admin_keyboard())

def ban_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if user.id != ADMIN_ID:
        update.message.reply_text("Доступно только админу.", reply_markup=get_main_keyboard(user.id))
        return
    try:
        target_id = int(context.args[0])
        cursor.execute('UPDATE users SET banned = 1 WHERE user_id = ?', (target_id,))
        conn.commit()
        update.message.reply_text(f"Пользователь {target_id} заблокирован.", reply_markup=get_admin_keyboard())
    except:
        update.message.reply_text("Используйте: /ban <id>", reply_markup=get_admin_keyboard())

def unban_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if user.id != ADMIN_ID:
        update.message.reply_text("Доступно только админу.", reply_markup=get_main_keyboard(user.id))
        return
    try:
        target_id = int(context.args[0])
        cursor.execute('UPDATE users SET banned = 0 WHERE user_id = ?', (target_id,))
        conn.commit()
        update.message.reply_text(f"Пользователь {target_id} разблокирован.", reply_markup=get_admin_keyboard())
    except:
        update.message.reply_text("Используйте: /unban <id>", reply_markup=get_admin_keyboard())

def reward_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if user.id != ADMIN_ID:
        update.message.reply_text("Доступно только админу.", reply_markup=get_main_keyboard(user.id))
        return
    try:
        target_id, amount = int(context.args[0]), int(context.args[1])
        update_balance(target_id, amount)
        update.message.reply_text(f"Игроку {target_id} начислено {amount}.", reply_markup=get_admin_keyboard())
    except:
        update.message.reply_text("Используйте: /reward <id> <сумма>", reply_markup=get_admin_keyboard())

def withdraw_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if user.id != ADMIN_ID:
        update.message.reply_text("Доступно только админу.", reply_markup=get_main_keyboard(user.id))
        return
    try:
        target_id, amount = int(context.args[0]), int(context.args[1])
        user_data = get_user(target_id)
        if user_data[2] < amount:
            update.message.reply_text("У игрока недостаточно средств.", reply_markup=get_admin_keyboard())
            return
        update_balance(target_id, -amount)
        update.message.reply_text(f"С игрока {target_id} изъято {amount}.", reply_markup=get_admin_keyboard())
    except:
        update.message.reply_text("Используйте: /withdraw <id> <сумма>", reply_markup=get_admin_keyboard())

def sosdaf_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if user.id != ADMIN_ID:
        update.message.reply_text("Доступно только админу.", reply_markup=get_main_keyboard(user.id))
        return
    try:
        name, income, min_price, duration = context.args[0], int(context.args[1]), int(context.args[2]), int(context.args[3])
        auction_id = create_auction(0, min_price, duration, name, income)
        update.message.reply_text(f"Фирма '{name}' выставлена на аукцион (ID: {auction_id}).", reply_markup=get_main_keyboard(user.id))
    except:
        update.message.reply_text("Используйте: /sosdaf <название> <доход> <мин. ставка> <время>", reply_markup=get_main_keyboard(user.id))

def sosdaf_clan_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if user.id != ADMIN_ID:
        update.message.reply_text("Доступно только админу.", reply_markup=get_main_keyboard(user.id))
        return
    try:
        name, income, min_price, duration = context.args[0], int(context.args[1]), int(context.args[2]), int(context.args[3])
        auction_id = create_auction(0, min_price, duration, name, income, is_clan=1)
        update.message.reply_text(f"Клановая фирма '{name}' выставлена на аукцион (ID: {auction_id}).", reply_markup=get_main_keyboard(user.id))
    except:
        update.message.reply_text("Используйте: /sosdaf_clan <название> <доход> <мин. ставка> <время>", reply_markup=get_main_keyboard(user.id))

# ---------------------------- Обработчики ----------------------------
def text_handler(update: Update, context: CallbackContext):
    user = update.effective_user
    text = update.message.text.strip()
    user_data = get_user(user.id)
    if not user_data:
        update.message.reply_text("Зарегистрируйтесь с помощью /start.", reply_markup=get_main_keyboard(user.id))
        return
    if user_data[6]:
        update.message.reply_text("Вы забанены.", reply_markup=get_main_keyboard(user.id))
        return

    if context.user_data.get("buy_firm"):
        if text == "Отмена":
            context.user_data["buy_firm"] = False
            update.message.reply_text("Отменено.", reply_markup=get_main_keyboard(user.id))
            return
        firm_type = next((ft for ft, info in FIRM_TYPES.items() if info["name"] == text), None)
        if firm_type:
            if count_firms_by_type(user.id, firm_type) >= MAX_FIRMS_PER_TYPE:
                update.message.reply_text(f"Максимум фирм '{text}': {MAX_FIRMS_PER_TYPE}.", reply_markup=get_main_keyboard(user.id))
                context.user_data["buy_firm"] = False
                return
            price = FIRM_TYPES[firm_type]["price"]
            if user_data[2] >= price:
                update_balance(user.id, -price)
                add_firm(user.id, firm_type)
                context.user_data["buy_firm"] = False
                update.message.reply_text(f"Куплена '{text}' за {price}.", reply_markup=get_main_keyboard(user.id))
            else:
                update.message.reply_text("Недостаточно монет.", reply_markup=get_main_keyboard(user.id))
        return

    clan = get_clan(user.id)
    commands = {
        "Профиль": profile_command,
        "Купить фирму": buy_firm_command,
        "Мои фабрики": my_firms_command,
        "Купить рабочих": lambda u, c: u.message.reply_text("Используйте: /buy_workers <id> <кол-во>", reply_markup=get_main_keyboard(user.id)),
        "Дневная награда": daily_reward_command,
        "Клан": clan_command,
        "Аукцион": lambda u, c: auction_command(u, c, False),
        "Рефералы": lambda u, c: u.message.reply_text(f"Ссылка: https://t.me/{BOT_USERNAME}?start={u.effective_user.id}", reply_markup=get_main_keyboard(user.id)),
        "Топ": lambda u, c: u.message.reply_text("Топ:\n" + "\n".join(f"{i}. @{u[1]}: {u[2]}" for i, u in enumerate(cursor.execute('SELECT * FROM users ORDER BY balance DESC LIMIT 5').fetchall(), 1)), reply_markup=get_main_keyboard(user.id)),
        "Админ-панель": admin_panel_command,
        "Назад": lambda u, c: u.message.reply_text("Главное меню:", reply_markup=get_main_keyboard(user.id)),
        "Создать клан": lambda u, c: u.message.reply_text("Используйте: /create_clan <название> <эмблема>", reply_markup=get_main_keyboard(user.id)),
        "Вступить в клан": join_clan_command,
        "Состав": clan_members_command,
        "Внести деньги": contribute_clan_command,
        "Аукцион клана": lambda u, c: auction_command(u, c, True),
        "Фирмы клана": clan_firms_command,
        "Покинуть клан": leave_clan_command,
    }
    if text in commands:
        try:
            commands[text](update, context)
        except Exception as e:
            logger.error(f"Command '{text}' error: {e}")
            update.message.reply_text("Произошла ошибка. Попробуйте снова.", reply_markup=get_main_keyboard(user.id))
    else:
        update.message.reply_text("Неизвестная команда.", reply_markup=get_main_keyboard(user.id))

def callback_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    data = query.data
    user = query.from_user
    query.answer()
    try:
        if data.startswith("join:"):
            clan_id = int(data.split(":")[1])
            if not get_clan(user.id):
                cursor.execute('INSERT INTO clan_requests (clan_id, user_id) VALUES (?, ?)', (clan_id, user.id))
                conn.commit()
                clan = cursor.execute('SELECT leader_id FROM clans WHERE id = ?', (clan_id,)).fetchone()
                if clan:
                    context.bot.send_message(clan[0], f"Игрок ID: {user.id} подал заявку на вступление в ваш клан.")
                query.edit_message_text("Заявка отправлена.")
            else:
                query.edit_message_text("Вы уже в клане.")
        elif data.startswith("bid:"):
            auction_id = int(data.split(":")[1])
            query.edit_message_text(f"Введите: /bid {auction_id} <ставка>")
        elif data.startswith("exclude:"):
            target_id = int(data.split(":")[1])
            clan = get_clan(user.id)
            if clan and clan[3] == user.id:
                remove_clan_member(target_id)
                query.edit_message_text(f"Игрок {target_id} исключён.")
        elif data.startswith("accept:"):
            target_id = int(data.split(":")[1])
            clan = get_clan(user.id)
            if clan and clan[3] == user.id:
                cursor.execute('DELETE FROM clan_requests WHERE clan_id = ? AND user_id = ?', (clan[0], target_id))
                add_clan_member(clan[0], target_id)
                conn.commit()
                context.bot.send_message(target_id, f"Вас приняли в клан '{clan[1]}'!")
                query.edit_message_text(f"Игрок {target_id} принят.")
    except Exception as e:
        logger.error(f"Callback error: {e}")
        query.edit_message_text("Произошла ошибка.")

def bid_command(update: Update, context: CallbackContext):
    user = update.effective_user
    try:
        auction_id, bid = map(int, context.args)
        auction = cursor.execute('SELECT * FROM auctions WHERE id = ?', (auction_id,)).fetchone()
        if not auction or not auction[7]:
            update.message.reply_text("Аукцион завершён.", reply_markup=get_main_keyboard(user.id))
            return
        if bid <= (auction[5] or auction[2]):
            update.message.reply_text("Ставка должна быть выше текущей.", reply_markup=get_main_keyboard(user.id))
            return
        user_data = get_user(user.id)
        if user_data[2] < bid:
            update.message.reply_text("Недостаточно монет.", reply_markup=get_main_keyboard(user.id))
            return
        update_balance(user.id, -bid)
        update_auction_bid(auction_id, bid, user.id)
        update.message.reply_text(f"Ставка {bid} принята.", reply_markup=get_main_keyboard(user.id))
    except:
        update.message.reply_text("Используйте: /bid <id> <ставка>", reply_markup=get_main_keyboard(user.id))

# ---------------------------- Автоматика ----------------------------
def income_job(bot):
    try:
        for user in cursor.execute('SELECT user_id, chat_id FROM users WHERE banned = 0').fetchall():
            income = get_total_income(user[0])
            logger.info(f"Calculating income for user {user[0]}: {income}")
            if income > 0:
                update_balance(user[0], income)
                try:
                    bot.send_message(user[1], f"Доход: +{income} монет.")
                except Exception as e:
                    logger.error(f"Income notification error for {user[0]}: {e}")
        for clan in cursor.execute('SELECT id FROM clans').fetchall():
            firms = get_firms(clan_id=clan[0])
            income = sum(f[6] for f in firms if f[6])
            if income > 0:
                cursor.execute('UPDATE clans SET money = money + ? WHERE id = ?', (income, clan[0]))
                conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Income job error: {e}")

def auction_check_job(bot):
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for auction in cursor.execute('SELECT id FROM auctions WHERE active = 1 AND end_time <= ?', (now,)).fetchall():
            end_auction(auction[0])
    except sqlite3.Error as e:
        logger.error(f"Auction check job error: {e}")

# ---------------------------- Основной цикл ----------------------------
def main():
    init_db()
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_command, pass_args=True))
    dp.add_handler(CommandHandler("buy_workers", buy_workers_command, pass_args=True))
    dp.add_handler(CommandHandler("create_clan", create_clan_command, pass_args=True))
    dp.add_handler(CommandHandler("disband", disband_clan_command))
    dp.add_handler(CommandHandler("bid", bid_command, pass_args=True))
    dp.add_handler(CommandHandler("sosdaf", sosdaf_command, pass_args=True))
    dp.add_handler(CommandHandler("sosdaf_clan", sosdaf_clan_command, pass_args=True))
    dp.add_handler(CommandHandler("contribute", contribute_clan_command, pass_args=True))
    dp.add_handler(CommandHandler("ban", ban_command, pass_args=True))
    dp.add_handler(CommandHandler("unban", unban_command, pass_args=True))
    dp.add_handler(CommandHandler("reward", reward_command, pass_args=True))
    dp.add_handler(CommandHandler("withdraw", withdraw_command, pass_args=True))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, text_handler))
    dp.add_handler(CallbackQueryHandler(callback_handler))

    scheduler = BackgroundScheduler(timezone=pytz.utc)
    scheduler.add_job(income_job, 'interval', minutes=5, args=[updater.bot])
    scheduler.add_job(auction_check_job, 'interval', minutes=1, args=[updater.bot])
    scheduler.start()

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()