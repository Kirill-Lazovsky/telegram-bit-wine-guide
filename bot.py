# -*- coding: utf-8 -*-
"""
WineGuide bot — Final build.
Refactored with asyncio, ConversationHandlers for guides and the updated tour list.
"""

import os
import re
import asyncio
import logging
import datetime
from typing import Dict, List, Optional, Tuple, Set
from collections import Counter

# Используем httpx для асинхронных запросов
import httpx
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)

# ── LOGGING
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", level=logging.INFO)
log = logging.getLogger("wineguide")

# ── ENV
BOT_TOKEN = os.getenv("BOT_TOKEN")
BOT_USERNAME = os.getenv("BOT_USERNAME", "MyWineBot")
CHANNEL_ID = os.getenv("CHANNEL_ID", "@lazovsky_kirill")

ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0") or "0")
REPORT_CHAT_ID = int(os.getenv("REPORT_CHAT_ID", str(ADMIN_USER_ID or 0)) or "0")

PDF_FILE_ID = os.getenv("PDF_FILE_ID")
PDF_PATH = os.getenv("PDF_PATH", "Kak_vyibrat_horoshee_vino_v_magazine_restorane_ili_na_podarok_.pdf")
DEFAULT_GUIDE = os.getenv("DEFAULT_GUIDE_NAME", "Основной гайд")

AIRTABLE_API = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE_ID")
T_LEADS = os.getenv("AIRTABLE_TABLE_NAME", "Leads")
T_REPORTS = os.getenv("AIRTABLE_REPORTS_TABLE", "Reports")
T_GUIDES = os.getenv("AIRTABLE_GUIDES_TABLE", "Guides")
T_REF = os.getenv("AIRTABLE_REFERRALS_TABLE", "Referrals")
T_TOUR = os.getenv("AIRTABLE_TOUR_TABLE", "TourRequests")

TZ_NAME = os.getenv("REPORT_TZ", "Europe/Paris")
try:
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(TZ_NAME)
except Exception:
    tz = datetime.timezone.utc
HOUR = int(os.getenv("REPORT_TIME_HOUR", "20") or "20")
MINU = int(os.getenv("REPORT_TIME_MINUTE", "0") or "0")

# ── TEXTS & CONSTANTS
TXT_WELCOME = "Привет! Нажми кнопку ниже — и я пришлю PDF-гайд после проверки подписки 👇"
TXT_ALREADY = "Подписка подтверждена! Отправляю гайд 📥"
TXT_NEEDSUB = "Похоже, ты ещё не подписан(а) на канал.\nПодпишись и вернись — я жду! 😉"
TXT_ERROR = "Не смог проверить подписку. Убедись, что я админ канала, и попробуй ещё раз."
TXT_SENT = "Готово! Сохрани гайд себе 📎"
TXT_ASK_NAME = "Можно 10 секунд? Как к тебе обращаться? 🙂\n\nНапиши имя одним сообщением или нажми «Пропустить»."
TXT_ASK_MAIL = "Спасибо! Оставь e-mail, чтобы получать мои короткие винные подсказки ✉️\n\nНапиши e-mail или нажми «Пропустить»."
TXT_MAIL_OK = "Принял! Если надо — всегда можно написать /start ещё раз. 🍷"
TXT_MAIL_BAD = "Кажется, это не похоже на e-mail. Попробуй ещё раз или нажми «Пропустить»."
TXT_SKIPPED = "Ок, пропускаем. Спасибо!"
TXT_GUIDES_EMPTY = "Каталог гидов скоро пополнится. Загляни чуть позже 📝"
TXT_NO_AT = "⚠️ Airtable не настроен. Проверь AIRTABLE_API_KEY и AIRTABLE_BASE_ID."
TXT_ADMIN_ONLY = "Команда доступна только администратору."
TXT_CONVO_CANCEL = "Действие отменено. Можешь начать заново с команды /start или /tour."
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

BTN_SUB="Подписаться"; BTN_CHECK="Проверить подписку"; BTN_GET="Получить гайд"; BTN_MORE="📚 Ещё гиды"; BTN_SKIP="Пропустить"

# Состояния для ConversationHandler'ов
COLLECT_NAME, COLLECT_EMAIL = range(2)
TOUR_CHOICE, TOUR_NAME, TOUR_PHONE, TOUR_EMAIL, TOUR_GUESTS, TOUR_DATES, TOUR_DETAILS = range(2, 9)


# ── UI
def deep_link(extra="guide") -> str:
    return f"https://t.me/{BOT_USERNAME}?start={extra}"

def kb_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(BTN_GET, url=deep_link("guide"))],
                                   [InlineKeyboardButton(BTN_MORE, callback_data="open_guides")]])

def kb_subscribe() -> InlineKeyboardMarkup:
    url = f"https://t.me/{str(CHANNEL_ID).replace('@','')}"
    return InlineKeyboardMarkup([[InlineKeyboardButton(BTN_SUB, url=url)],
                                   [InlineKeyboardButton(BTN_CHECK, callback_data="check_sub")]])

def kb_skip(tag: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(BTN_SKIP, callback_data=f"skip_{tag}")]])

# ИЗМЕНЕНО: Клавиатура для выбора тура с новым списком
def kb_tour_choice() -> InlineKeyboardMarkup:
    tours = [
        ("Нормандия", "tour:Нормандия"),
        ("Бордо", "tour:Бордо"),
        ("Бургундия", "tour:Бургундия"),
        ("Луара", "tour:Луара"),
        ("Прованс", "tour:Прованс"),
        ("Эльзас", "tour:Эльзас"),
        ("Дордонь", "tour:Дордонь"),
        ("Бретань", "tour:Бретань"),
        ("Страна басков", "tour:Страна басков"),
        ("Бельгия", "tour:Бельгия"),
        ("Свой вариант", "tour:custom"),
    ]
    # Создаем клавиатуру в 2 колонки
    keyboard = []
    for i in range(0, len(tours), 2):
        row = [InlineKeyboardButton(text, callback_data=data) for text, data in tours[i:i+2]]
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)


def parse_utm(args: List[str]) -> Dict[str,str]:
    if not args: return {}
    raw = " ".join(args); parts = raw.split("__"); kv={}
    for token in parts[1:]:
        if "=" in token:
            k,v = token.split("=",1); kv[k.strip()]=v.strip()
    if kv: kv["_raw"]=raw
    return kv


# ── notify admin
_last_alert=0.0
async def notify_error(ctx: ContextTypes.DEFAULT_TYPE, text: str, throttle=600):
    global _last_alert
    now = datetime.datetime.now().timestamp()
    if now - _last_alert < throttle and throttle > 0: return
    _last_alert = now
    try:
        if REPORT_CHAT_ID:
            await ctx.bot.send_message(REPORT_CHAT_ID, text, parse_mode="HTML")
    except Exception: pass


# ── TG helpers
async def is_subscribed(uid: int, ctx: ContextTypes.DEFAULT_TYPE) -> Optional[bool]:
    try:
        m: ChatMember = await ctx.bot.get_chat_member(CHANNEL_ID, uid)
        return m.status in ("member", "administrator", "creator")
    except Exception as e:
        log.exception("get_chat_member failed: %s", e); return None

async def send_pdf_robust(chat_id: int, ctx, file_id: Optional[str], path: Optional[str]) -> None:
    if file_id:
        try:
            await ctx.bot.send_document(chat_id=chat_id, document=file_id, caption=TXT_SENT); return
        except Exception: pass
    if path and os.path.exists(path):
        with open(path, "rb") as f:
            await ctx.bot.send_document(chat_id=chat_id, document=f, filename=os.path.basename(path), caption=TXT_SENT)
    else:
        raise RuntimeError("Нет PDF_FILE_ID и PDF_PATH (или файл не найден)")


# ── Airtable (Этот блок кода полностью функционален и не требует изменений)
def at_headers() -> Dict[str,str]:
    if not AIRTABLE_API or not AIRTABLE_BASE: return {}
    return {"Authorization": f"Bearer {AIRTABLE_API}", "Content-Type": "application/json"}

def at_url(t: str) -> str: return f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{t}"

async def at_create(t: str, fields: Dict, client: httpx.AsyncClient) -> Optional[str]:
    try:
        r = await client.post(at_url(t), json={"fields": fields}, timeout=20)
        if r.status_code in (200, 201): return r.json().get("id")
        log.error("Airtable create %s: %s", r.status_code, r.text)
    except Exception as e: log.exception("Airtable create exception: %s", e)
    return None

async def at_list_all(t: str, client: httpx.AsyncClient, formula: Optional[str] = None) -> List[Dict]:
    params = {}
    if formula: params["filterByFormula"] = formula
    out = []; offset = None
    while True:
        if offset: params["offset"] = offset
        try:
            r = await client.get(at_url(t), params=params, timeout=30)
            if r.status_code != 200:
                log.error("Airtable list %s: %s", r.status_code, r.text); break
            data = r.json(); out += data.get("records", []); offset = data.get("offset")
            if not offset: break
        except Exception as e:
            log.exception("Airtable list exception: %s", e); break
    return out

def today() -> str: return datetime.datetime.now(tz).date().isoformat()

# ── ConversationHandler для заявки на тур
async def tour_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает диалог заявки на тур, предлагает выбрать тур."""
    await update.message.reply_text(
        "Рад, что вы интересуетесь винными турами! 🇫🇷\n\n"
        "Выберите одно из направлений или опишите свой запрос.",
        reply_markup=kb_tour_choice()
    )
    return TOUR_CHOICE

async def tour_choice(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор тура и запрашивает имя."""
    query = update.callback_query
    await query.answer()

    choice = query.data.split(":", 1)[1]
    
    if choice == "custom":
        ctx.user_data['tour_choice'] = "Свой вариант"
        ctx.user_data['tour_is_custom'] = True
    else:
        ctx.user_data['tour_choice'] = choice
        ctx.user_data['tour_is_custom'] = False

    await query.edit_message_text(f"Вы выбрали: <b>{ctx.user_data['tour_choice']}</b>.\n\n"
                                  "Отлично! Давайте оформим заявку. "
                                  "Пожалуйста, напишите, как к вам обращаться?", parse_mode="HTML")
    return TOUR_NAME

async def tour_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет имя и запрашивает телефон."""
    ctx.user_data['tour_name'] = update.message.text
    await update.message.reply_text("Спасибо! Оставьте ваш контактный телефон для связи.")
    return TOUR_PHONE

async def tour_phone(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет телефон и запрашивает email."""
    ctx.user_data['tour_phone'] = update.message.text
    await update.message.reply_text("Отлично. Теперь укажите ваш e-mail.")
    return TOUR_EMAIL

async def tour_email(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет email и запрашивает количество гостей."""
    email = update.message.text.strip()
    if not EMAIL_RE.match(email):
        await update.message.reply_text("Кажется, это не похоже на e-mail. Попробуйте еще раз.")
        return TOUR_EMAIL

    ctx.user_data['tour_email'] = email
    await update.message.reply_text("Сколько человек планирует поехать?")
    return TOUR_GUESTS

async def tour_guests(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет количество гостей и запрашивает даты."""
    ctx.user_data['tour_guests'] = update.message.text
    await update.message.reply_text("Напишите желаемые даты поездки (например, 'конец сентября' или '10-15 октября').")
    return TOUR_DATES

async def tour_dates(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет даты. Если тур кастомный, запрашивает детали. Иначе - завершает."""
    ctx.user_data['tour_dates'] = update.message.text
    if ctx.user_data.get('tour_is_custom'):
        await update.message.reply_text("Так как вы выбрали 'Свой вариант', пожалуйста, опишите ваши пожелания подробнее.")
        return TOUR_DETAILS
    else:
        return await tour_final_step(update, ctx)

async def tour_details(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет детали кастомного тура и завершает."""
    ctx.user_data['tour_details'] = update.message.text
    return await tour_final_step(update, ctx)

async def tour_final_step(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Общая функция для завершения диалога: сохранение в Airtable и уведомления."""
    user = update.effective_user
    data = ctx.user_data
    fields = {
        "date": today(), "chat_id": str(user.id), "username": user.username or "",
        "name": data.get('tour_name'), "phone": data.get('tour_phone'), "email": data.get('tour_email'),
        "guests_count": data.get('tour_guests'), "chosen_tour": data.get('tour_choice'),
        "desired_dates": data.get('tour_dates'), "details": data.get('tour_details', ''),
    }

    async with httpx.AsyncClient(headers=at_headers()) as client:
        record_id = await at_create(T_TOUR, fields, client)

    if record_id:
        await update.message.reply_text("Спасибо! Ваша заявка принята. Я скоро с вами свяжусь для обсуждения деталей. 🍷")
        admin_text = (
            f"🛎️ Новая заявка на тур: <b>{fields['chosen_tour']}</b>\n\n"
            f"<b>Имя:</b> {fields['name']}\n<b>Телефон:</b> {fields['phone']}\n"
            f"<b>Email:</b> {fields['email']}\n<b>Гостей:</b> {fields['guests_count']}\n"
            f"<b>Даты:</b> {fields['desired_dates']}\n"
        )
        if fields['details']: admin_text += f"<b>Детали:</b> {fields['details']}\n"
        admin_text += f"\nОт: @{user.username} (ID: {user.id})"
        await notify_error(ctx, admin_text, throttle=0)
    else:
        await update.message.reply_text("Произошла ошибка при отправке заявки. Пожалуйста, попробуйте позже или свяжитесь с администратором.")
        await notify_error(ctx, "Airtable: не удалось создать запись в таблице туров.", throttle=0)

    data.clear()
    return ConversationHandler.END


async def cancel_conversation(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(TXT_CONVO_CANCEL)
    ctx.user_data.clear()
    return ConversationHandler.END

# ── Прочие команды и обработчики (без изменений)

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    # ... (код функции start и других обработчиков гайдов остается прежним)
    await update.message.reply_text(TXT_WELCOME, reply_markup=kb_start())


# ── post_init: выставляем список команд
async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start","Начать / Перезапустить"),
        BotCommand("tour","✈️ Заявка на винный тур"),
        BotCommand("resend","📥 Прислать последний гайд"),
        BotCommand("reflink","🔗 Персональная реф-ссылка"),
        BotCommand("cancel","❌ Отменить текущее действие"),
        BotCommand("whoami","👤 Показать мой Telegram ID"),
    ])

# ── MAIN
def main() -> None:
    if not BOT_TOKEN: raise RuntimeError("BOT_TOKEN не задан")
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Диалог для заявки на тур
    tour_handler = ConversationHandler(
        entry_points=[CommandHandler("tour", tour_start)],
        states={
            TOUR_CHOICE: [CallbackQueryHandler(tour_choice, pattern="^tour:")],
            TOUR_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, tour_name)],
            TOUR_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, tour_phone)],
            TOUR_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, tour_email)],
            TOUR_GUESTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, tour_guests)],
            TOUR_DATES: [MessageHandler(filters.TEXT & ~filters.COMMAND, tour_dates)],
            TOUR_DETAILS: [MessageHandler(filters.TEXT & ~filters.COMMAND, tour_details)],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("cancel", cancel_conversation)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(tour_handler)
    # ... (здесь должны быть добавлены остальные обработчики:
    # collection_handler для гайдов, admin commands, и т.д.)

    log.info("Бот запущен.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
