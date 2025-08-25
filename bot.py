import os, re, json, logging, datetime
from typing import Optional, Dict, Tuple

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ChatMember
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# ——— ЛОГИ ————————————————————————————————————————————————————————————————
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("awg-guide-bot")

# ——— КОНФИГ ИЗ ENV ——————————————————————————————————————————————————————
BOT_TOKEN = os.getenv("BOT_TOKEN")
BOT_USERNAME = os.getenv("BOT_USERNAME", "ArtWineGuideBot")     # без @
CHANNEL_ID = os.getenv("CHANNEL_ID", "@lazovsky_kirill")        # публичный канал
PDF_PATH = os.getenv("PDF_PATH", "Kak_vyibrat_horoshee_vino_v_magazine_restorane_ili_na_podarok_.pdf")
PDF_FILE_ID = os.getenv("PDF_FILE_ID")                          # опц.: document file_id
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))

# Google Sheets (опционально)
GOOGLE_SA_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")       # текст JSON сервис-аккаунта (строкой)
SHEET_ID = os.getenv("SHEET_ID")                                # ID таблицы (из URL)
SHEET_NAME = os.getenv("SHEET_NAME", "Leads")

# ——— gspread подключаем только если всё для Sheets задано ————————————————
SHEETS_ENABLED = False
try:
    if GOOGLE_SA_JSON and SHEET_ID:
        import gspread
        from google.oauth2.service_account import Credentials
        SHEETS_ENABLED = True
except Exception as e:
    logger.warning("Sheets libs not available: %s", e)
    SHEETS_ENABLED = False

# ——— ТЕКСТЫ ————————————————————————————————————————————————————————————————
TEXT_WELCOME = "Привет! Нажми кнопку ниже — и я пришлю PDF-гайд после проверки подписки 👇"
TEXT_ALREADY_SUB = "Подписка подтверждена! Отправляю гайд 📥"
TEXT_NEED_SUB = "Похоже, ты ещё не подписан(а) на канал.\nПодпишись и вернись — я жду! 😉"
TEXT_ERROR = "Упс, не смог проверить подписку. Убедись, что я админ канала, и попробуй ещё раз."
TEXT_SENT = "Готово! Сохрани гайд себе 📎"
TEXT_ASK_NAME = "Можно 10 секунд? Как к тебе обращаться? 🙂\n\nНапиши имя одним сообщением или нажми «Пропустить»."
TEXT_ASK_EMAIL = ("Спасибо! Оставь e-mail, чтобы получать мои короткие винные подсказки и новые гайды ✉️\n\n"
                  "Напиши e-mail или нажми «Пропустить».")
TEXT_EMAIL_OK = "Принял! Если надо — всегда можно написать /start ещё раз. 🍷"
TEXT_EMAIL_BAD = "Кажется, это не похоже на e-mail. Попробуй ещё раз или нажми «Пропустить»."
TEXT_SKIPPED = "Ок, пропускаем. Спасибо!"
TEXT_NO_SHEETS = "⚠️ Запись в таблицу отключена (Google Sheets не настроены)."
TEXT_STATS_DENIED = "Команда недоступна."
TEXT_STATS = "Всего скачиваний (строк в листе): <b>{count}</b>"

BUTTON_SUBSCRIBE = "Подписаться"
BUTTON_CHECK = "Проверить подписку"
BUTTON_GET_GUIDE = "Получить гайд"
BUTTON_SKIP = "Пропустить"

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# ——— УТИЛИТЫ ————————————————————————————————————————————————————————————————
def deep_link() -> str:
    return f"https://t.me/{BOT_USERNAME}?start=guide"

def subscribe_keyboard() -> InlineKeyboardMarkup:
    url = f"https://t.me/{str(CHANNEL_ID).replace('@','')}"  # публичный канал
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(BUTTON_SUBSCRIBE, url=url)],
        [InlineKeyboardButton(BUTTON_CHECK, callback_data="check_sub")]
    ])

def get_guide_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(BUTTON_GET_GUIDE, url=deep_link())]])

def skip_keyboard(tag: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(BUTTON_SKIP, callback_data=f"skip_{tag}")]])

def parse_utm(args: list[str]) -> Dict[str, str]:
    """
    Дип-ссылка с UTM:
    https://t.me/ArtWineGuideBot?start=guide__utm_source=instagram__utm_medium=social__utm_campaign=aug25__utm_content=reel1
    """
    if not args:
        return {}
    raw = " ".join(args)
    parts = raw.split("__")
    kvs = {}
    for token in parts[1:]:
        if "=" in token:
            k, v = token.split("=", 1)
            kvs[k.strip()] = v.strip()
    if kvs:
        kvs["_raw"] = raw
    return kvs

async def is_subscribed(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Optional[bool]:
    try:
        member: ChatMember = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as e:
        logger.exception("Ошибка проверки подписки: %s", e)
        return None

# ——— GOOGLE SHEETS ————————————————————————————————————————————————————————
def gs_client_and_sheet() -> Tuple[Optional["gspread.Client"], Optional["gspread.Worksheet"]]:
    if not SHEETS_ENABLED:
        return None, None
    try:
        info = json.loads(GOOGLE_SA_JSON)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        gc = gspread.Client(auth=creds); gc.session = gc.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet(SHEET_NAME)
        except Exception:
            ws = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)
        headers = [
            "timestamp","chat_id","username","first_name","last_name",
            "display_name","email","utm_source","utm_medium","utm_campaign",
            "utm_content","utm_term","start_param_raw","trigger","row_key"
        ]
        if not ws.row_values(1):
            ws.update("A1", [headers])
        return gc, ws
    except Exception as e:
        logger.exception("Не удалось инициализировать Google Sheets: %s", e)
        return None, None

def gs_append_download(ws, user, display_name, email, utm: Dict[str,str], trigger: str, start_param_raw: Optional[str]) -> Optional[int]:
    try:
        ts = datetime.datetime.utcnow().isoformat()
        row_key = f"{user.id}-{int(datetime.datetime.utcnow().timestamp())}"
        data = [
            ts, str(user.id), user.username or "", user.first_name or "", user.last_name or "",
            display_name or "", email or "",
            utm.get("utm_source",""), utm.get("utm_medium",""), utm.get("utm_campaign",""),
            utm.get("utm_content",""), utm.get("utm_term",""),
            start_param_raw or "", trigger, row_key
        ]
        ws.append_row(data, value_input_option="USER_ENTERED")
        values = ws.get_all_values()
        return len(values)  # индекс новой строки
    except Exception as e:
        logger.exception("Ошибка записи строки в Sheets: %s", e)
        return None

def gs_update_last_row_email(ws, row_index: int, display_name: Optional[str], email: Optional[str]) -> None:
    try:
        if display_name is not None:
            ws.update(f"F{row_index}", [[display_name]])
        if email is not None:
            ws.update(f"G{row_index}", [[email]])
    except Exception as e:
        logger.exception("Ошибка обновления строки в Sheets: %s", e)

def gs_count_downloads(ws) -> int:
    try:
        values = ws.get_all_values()
        return max(0, len(values) - 1)
    except Exception:
        return 0

# ——— ОТПРАВКА PDF ————————————————————————————————————————————————————————
async def send_pdf(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    if PDF_FILE_ID:
        await context.bot.send_document(chat_id=chat_id, document=PDF_FILE_ID, caption=TEXT_SENT)
    else:
        with open(PDF_PATH, "rb") as f:
            await context.bot.send_document(
                chat_id=chat_id, document=f,
                filename=os.path.basename(PDF_PATH), caption=TEXT_SENT
            )

# ——— ЛОГИКА ————————————————————————————————————————————————————————————————
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    context.user_data["utm"] = parse_utm(args)
    context.user_data["start_param_raw"] = " ".join(args) if args else None

    first = args[0].lower() if args else ""
    if first in ("guide", "гайд") or first.startswith("guide"):
        await guide_flow(update, context, trigger="start")
        return

    await update.message.reply_text(TEXT_WELCOME, reply_markup=get_guide_keyboard())

async def guide_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, trigger: str) -> None:
    user = update.effective_user
    chat_id = update.effective_chat.id

    sub = await is_subscribed(user.id, context)
    if sub is True:
        await update.effective_message.reply_text(TEXT_ALREADY_SUB)
        await send_pdf(chat_id, context)

        # Запись лида
        _, ws = gs_client_and_sheet()
        lead_row_index = None
        if ws:
            lead_row_index = gs_append_download(
                ws=ws, user=user, display_name=None, email=None,
                utm=context.user_data.get("utm", {}), trigger=trigger,
                start_param_raw=context.user_data.get("start_param_raw")
            )
        context.user_data["lead_row_index"] = lead_row_index

        # Сбор имени → e-mail
        context.user_data["awaiting_name"] = True
        context.user_data["awaiting_email"] = False
        await update.effective_message.reply_text(TEXT_ASK_NAME, reply_markup=skip_keyboard("name"))

    elif sub is False:
        await update.effective_message.reply_text(TEXT_NEED_SUB, reply_markup=subscribe_keyboard())
    else:
        await update.effective_message.reply_text(TEXT_ERROR)

async def on_check_sub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = query.from_user
    chat_id = query.message.chat_id

    sub = await is_subscribed(user.id, context)
    if sub is True:
        await query.edit_message_text(TEXT_ALREADY_SUB)
        await send_pdf(chat_id, context)

        _, ws = gs_client_and_sheet()
        lead_row_index = None
        if ws:
            lead_row_index = gs_append_download(
                ws=ws, user=user, display_name=None, email=None,
                utm=context.user_data.get("utm", {}), trigger="button_check",
                start_param_raw=context.user_data.get("start_param_raw")
            )
        context.user_data["lead_row_index"] = lead_row_index

        context.user_data["awaiting_name"] = True
        context.user_data["awaiting_email"] = False
        await query.message.reply_text(TEXT_ASK_NAME, reply_markup=skip_keyboard("name"))
    elif sub is False:
        await query.edit_message_text(TEXT_NEED_SUB, reply_markup=subscribe_keyboard())
    else:
        await query.edit_message_text(TEXT_ERROR)

async def on_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if query.data == "skip_name":
        context.user_data["awaiting_name"] = False
        context.user_data["awaiting_email"] = True
        await query.message.reply_text(TEXT_ASK_EMAIL, reply_markup=skip_keyboard("email"))
    elif query.data == "skip_email":
        context.user_data["awaiting_email"] = False
        await query.message.reply_text(TEXT_SKIPPED)

async def collector(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()

    # Имя
    if context.user_data.get("awaiting_name"):
        context.user_data["display_name"] = text
        context.user_data["awaiting_name"] = False
        context.user_data["awaiting_email"] = True
        await update.message.reply_text(TEXT_ASK_EMAIL, reply_markup=skip_keyboard("email"))
        return

    # E-mail
    if context.user_data.get("awaiting_email"):
        if not EMAIL_RE.match(text):
            await update.message.reply_text(TEXT_EMAIL_BAD, reply_markup=skip_keyboard("email"))
            return
        context.user_data["awaiting_email"] = False
        _, ws = gs_client_and_sheet()
        lead_row = context.user_data.get("lead_row_index")
        if ws and lead_row:
            gs_update_last_row_email(ws, lead_row, context.user_data.get("display_name"), text)
        await update.message.reply_text(TEXT_EMAIL_OK)
        return

async def keyword_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    low = (update.message.text or "").lower()
    if "гайд" in low:
        context.user_data.setdefault("utm", {})
        context.user_data.setdefault("start_param_raw", None)
        await guide_flow(update, context, trigger="keyword")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if ADMIN_USER_ID and update.effective_user.id == ADMIN_USER_ID:
        _, ws = gs_client_and_sheet()
        if ws:
            await update.message.reply_html(TEXT_STATS.format(count=gs_count_downloads(ws)))
        else:
            await update.message.reply_text(TEXT_NO_SHEETS)
    else:
        await update.message.reply_text(TEXT_STATS_DENIED)

def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN (ENV). Задай переменную окружения и перезапусти.")
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CallbackQueryHandler(on_check_sub, pattern="^check_sub$"))
    app.add_handler(CallbackQueryHandler(on_skip, pattern="^skip_(name|email)$"))

    # Приоритет: сначала сбор ответов, потом ключевое слово
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, collector), group=0)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, keyword_handler), group=1)

    logger.info("Бот запущен. Ожидаю события…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()