"""
MyWineBot — гиды, рефералка, Airtable-логирование, тур-заявки и дневной отчёт.

ENV (Railway → Variables):
  # Telegram
  BOT_TOKEN=...
  BOT_USERNAME=MyWineBot
  CHANNEL_ID=@lazovsky_kirill
  ADMIN_USER_ID=123456789                # твой Telegram ID
  REPORT_CHAT_ID=-1001234567890          # канал/чат для отчётов и алертов (бот — админ)

  # Основной гайд
  PDF_FILE_ID=AAQCBA...                  # рекомендуется
  # или
  PDF_PATH=Kak_vyibrat_horoshee_vino_v_magazine_restorane_ili_na_podarok_.pdf
  DEFAULT_GUIDE_NAME=Основной гайд

  # Airtable
  AIRTABLE_API_KEY=patXXXXXXXX
  AIRTABLE_BASE_ID=appXXXXXXXXXXXXXX
  AIRTABLE_TABLE_NAME=Leads
  AIRTABLE_REPORTS_TABLE=Reports
  AIRTABLE_GUIDES_TABLE=Guides
  AIRTABLE_REFERRALS_TABLE=Referrals
  AIRTABLE_TOUR_TABLE=TourRequests

  # Дневной отчёт (по умолчанию 20:00 Europe/Paris)
  REPORT_TIME_HOUR=20
  REPORT_TIME_MINUTE=0
  REPORT_TZ=Europe/Paris
"""

import os, re, json, asyncio, logging, datetime
from typing import Optional, Dict, Tuple, List, Set
from zoneinfo import ZoneInfo
from collections import Counter

import requests
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ChatMember
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# ───────────────────────── ЛОГИ ─────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("mywinebot")

# ───────────────────────── КОНФИГ ───────────────────────
BOT_TOKEN         = os.getenv("BOT_TOKEN")
BOT_USERNAME      = os.getenv("BOT_USERNAME", "MyWineBot")
CHANNEL_ID        = os.getenv("CHANNEL_ID", "@lazovsky_kirill")
ADMIN_USER_ID     = int(os.getenv("ADMIN_USER_ID", "0"))
REPORT_CHAT_ID    = int(os.getenv("REPORT_CHAT_ID", str(ADMIN_USER_ID or 0)))

PDF_PATH          = os.getenv("PDF_PATH", "Kak_vyibrat_horoshee_vino_v_magazine_restorane_ili_na_podarok_.pdf")
PDF_FILE_ID       = os.getenv("PDF_FILE_ID")
DEFAULT_GUIDE_NAME= os.getenv("DEFAULT_GUIDE_NAME", "Основной гайд")

AIRTABLE_API_KEY  = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID  = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE    = os.getenv("AIRTABLE_TABLE_NAME", "Leads")
AIRTABLE_REPORTS  = os.getenv("AIRTABLE_REPORTS_TABLE", "Reports")
AIRTABLE_GUIDES   = os.getenv("AIRTABLE_GUIDES_TABLE", "Guides")
AIRTABLE_REFERRALS= os.getenv("AIRTABLE_REFERRALS_TABLE", "Referrals")
AIRTABLE_TOUR     = os.getenv("AIRTABLE_TOUR_TABLE", "TourRequests")

REPORT_TZ         = os.getenv("REPORT_TZ", "Europe/Paris")
REPORT_TIME_HOUR  = int(os.getenv("REPORT_TIME_HOUR", "20"))
REPORT_TIME_MIN   = int(os.getenv("REPORT_TIME_MINUTE", "0"))
tz = ZoneInfo(REPORT_TZ)

# ───────────────────────── ТЕКСТЫ ───────────────────────
TEXT_WELCOME      = "Привет! Нажми кнопку — и пришлю PDF после проверки подписки 👇"
TEXT_ALREADY_SUB  = "Подписка подтверждена! Отправляю гайд 📥"
TEXT_NEED_SUB     = "Похоже, ты ещё не подписан(а) на канал.\nПодпишись и вернись — я жду! 😉"
TEXT_ERROR        = "Упс, не смог проверить подписку. Убедись, что я админ канала, и попробуй ещё раз."
TEXT_SENT         = "Готово! Сохрани гайд себе 📎"
TEXT_ASK_NAME     = "Можно 10 секунд? Как к тебе обращаться? 🙂\n\nНапиши имя одним сообщением или нажми «Пропустить»."
TEXT_ASK_EMAIL    = ("Спасибо! Оставь e-mail, чтобы получать мои короткие винные подсказки ✉️\n\n"
                     "Напиши e-mail или нажми «Пропустить».")
TEXT_EMAIL_OK     = "Принял! Если надо — всегда можно написать /start ещё раз. 🍷"
TEXT_EMAIL_BAD    = "Кажется, это не похоже на e-mail. Попробуй ещё раз или нажми «Пропустить»."
TEXT_SKIPPED      = "Ок, пропускаем. Спасибо!"
TEXT_STATS_DENIED = "Команда недоступна."
TEXT_NO_AT        = "⚠️ Airtable не настроен. Задай AIRTABLE_API_KEY, AIRTABLE_BASE_ID и таблицы."
TEXT_GUIDES_EMPTY = "Каталог гидов скоро пополнится. Загляни чуть позже 📝"
TEXT_TOUR_START   = "Давай подберём тур. Сначала — какой регион интересен? (например: Бордо, Шампань, Прованс)"
TEXT_TOUR_DATES   = "Отлично! Укажи даты поездки (например: 12–18 мая)."
TEXT_TOUR_BUDGET  = "Принял. Примерный бюджет на человека? (до 200€, 200–400€, 400€+)"
TEXT_TOUR_PAX     = "Сколько человек поедет?"
TEXT_TOUR_CONTACT = "Оставь контакт: e-mail или телефон, чтобы я вернулся с предложением."
TEXT_TOUR_DONE    = "Спасибо! Я получил заявку и свяжусь с тобой. 🥂"
EMAIL_RE          = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

BUTTON_SUBSCRIBE  = "Подписаться"
BUTTON_CHECK      = "Проверить подписку"
BUTTON_GET_GUIDE  = "Получить гайд"
BUTTON_MORE       = "📚 Ещё гиды"
BUTTON_SKIP       = "Пропустить"

# ──────────────────── UTM и клавиатуры ──────────────────
def deep_link(extra: str = "guide") -> str:
    return f"https://t.me/{BOT_USERNAME}?start={extra}"

def subscribe_keyboard() -> InlineKeyboardMarkup:
    url = f"https://t.me/{str(CHANNEL_ID).replace('@','')}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(BUTTON_SUBSCRIBE, url=url)],
        [InlineKeyboardButton(BUTTON_CHECK, callback_data="check_sub")]
    ])

def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(BUTTON_GET_GUIDE, url=deep_link("guide"))],
        [InlineKeyboardButton(BUTTON_MORE, callback_data="open_guides")]
    ])

def skip_keyboard(tag: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(BUTTON_SKIP, callback_data=f"skip_{tag}")]])

def parse_utm(args: List[str]) -> Dict[str, str]:
    # поддержка ref: ?start=guide__ref=<chat_id>__utm_source=...
    if not args: return {}
    raw = " ".join(args)
    parts = raw.split("__")
    kvs = {}
    for token in parts[1:]:
        if "=" in token:
            k, v = token.split("=", 1)
            kvs[k.strip()] = v.strip()
    if kvs: kvs["_raw"] = raw
    return kvs

# ──────────────── Telegram helpers ──────────────────────
async def is_subscribed(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Optional[bool]:
    try:
        member: ChatMember = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as e:
        logger.exception("Ошибка проверки подписки: %s", e)
        return None

async def send_pdf(chat_id: int, context: ContextTypes.DEFAULT_TYPE, file_id: Optional[str] = None) -> None:
    fid = file_id or PDF_FILE_ID
    if fid:
        await context.bot.send_document(chat_id=chat_id, document=fid, caption=TEXT_SENT)
    else:
        with open(PDF_PATH, "rb") as f:
            await context.bot.send_document(chat_id=chat_id, document=f,
                                            filename=os.path.basename(PDF_PATH), caption=TEXT_SENT)

# ──────────────── Алерты в админ-канал ──────────────────
_last_alert_ts: float = 0.0
async def notify_error(context: ContextTypes.DEFAULT_TYPE, text: str, throttle_sec: int = 600):
    global _last_alert_ts
    now = datetime.datetime.now().timestamp()
    if now - _last_alert_ts < throttle_sec:
        return
    _last_alert_ts = now
    try:
        if REPORT_CHAT_ID:
            await context.bot.send_message(chat_id=REPORT_CHAT_ID, text=f"⚠️ Ошибка: {text[:3500]}")
    except Exception as e:
        logger.exception("notify_error failed: %s", e)

# ──────────────── Airtable API ──────────────────────────
def at_headers() -> Dict[str, str]:
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID: return {}
    return {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}

def at_url(table: str) -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}"

def at_create(table: str, fields: Dict) -> Optional[str]:
    try:
        r = requests.post(at_url(table), headers=at_headers(), json={"fields": fields}, timeout=20)
        if r.status_code in (200, 201): return r.json().get("id")
        logger.error("Airtable create error %s: %s", r.status_code, r.text)
    except Exception as e:
        logger.exception("Airtable create exception: %s", e)
    return None

def at_patch(table: str, record_id: str, fields: Dict) -> bool:
    try:
        r = requests.patch(f"{at_url(table)}/{record_id}", headers=at_headers(), json={"fields": fields}, timeout=20)
        if r.status_code in (200, 201): return True
        logger.error("Airtable patch error %s: %s", r.status_code, r.text)
    except Exception as e:
        logger.exception("Airtable patch exception: %s", e)
    return False

def at_get(table: str, record_id: str) -> Optional[Dict]:
    try:
        r = requests.get(f"{at_url(table)}/{record_id}", headers=at_headers(), timeout=15)
        if r.status_code == 200: return r.json()
        logger.error("Airtable get error %s: %s", r.status_code, r.text)
    except Exception as e:
        logger.exception("Airtable get exception: %s", e)
    return None

def at_list_all(table: str, filter_formula: Optional[str] = None) -> List[Dict]:
    params = {}
    if filter_formula: params["filterByFormula"] = filter_formula
    out, offset = [], None
    headers = at_headers()
    if not headers: return out
    while True:
        if offset: params["offset"] = offset
        r = requests.get(at_url(table), headers=headers, params=params, timeout=30)
        if r.status_code != 200:
            logger.error("Airtable list error %s: %s", r.status_code, r.text); break
        data = r.json(); out.extend(data.get("records", [])); offset = data.get("offset")
        if not offset: break
    return out

# ──────────────── Лиды/рефералка/гиды ───────────────────
def today_iso() -> str:
    return datetime.datetime.now(tz).date().isoformat()

def lead_fields(user, display_name, email, utm, trigger, start_raw, subscribed,
                guide_name: str, guide_file_id: Optional[str], ref: Optional[str]) -> Dict:
    now = datetime.datetime.now(tz)
    return {
        "timestamp": now.isoformat(),
        "date": now.date().isoformat(),
        "chat_id": str(user.id),
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "display_name": display_name or "",
        "email": email or "",
        "utm_source": utm.get("utm_source",""),
        "utm_medium": utm.get("utm_medium",""),
        "utm_campaign": utm.get("utm_campaign",""),
        "utm_content": utm.get("utm_content",""),
        "utm_term": utm.get("utm_term",""),
        "start_param_raw": start_raw or "",
        "trigger": trigger,
        "subscribed": subscribed,
        "guide_name": guide_name,
        "guide_file_id": guide_file_id or "",
        "ref": ref or "",
    }

def at_find_today_lead(chat_id: int) -> Optional[Dict]:
    formula = f"AND({{chat_id}} = '{chat_id}', {{date}} = '{today_iso()}')"
    recs = at_list_all(AIRTABLE_TABLE, filter_formula=formula)
    return recs[0] if recs else None

def at_upsert_today_lead(user, display_name, email, utm, trigger, start_raw, subscribed,
                         guide_name: str, guide_file_id: Optional[str], ref: Optional[str]) -> str:
    existing = at_find_today_lead(user.id)
    fields = lead_fields(user, display_name, email, utm, trigger, start_raw, subscribed, guide_name, guide_file_id, ref)
    if existing:
        rec_id = existing["id"]
        safe_update = {"timestamp": fields["timestamp"], "guide_name": guide_name,
                       "guide_file_id": guide_file_id or "", "trigger": trigger}
        for k in ("display_name","email","utm_source","utm_medium","utm_campaign","utm_content","utm_term","start_param_raw","ref"):
            v = fields.get(k); old = existing.get("fields", {}).get(k)
            if v and v != old: safe_update[k] = v
        at_patch(AIRTABLE_TABLE, rec_id, safe_update); return rec_id
    else:
        rec_id = at_create(AIRTABLE_TABLE, fields); return rec_id or ""

def at_record_referral(inviter_chat_id: str, invited_chat_id: str) -> None:
    if not inviter_chat_id or inviter_chat_id == invited_chat_id: return
    formula = f"AND({{inviter_chat_id}}='{inviter_chat_id}', {{invited_chat_id}}='{invited_chat_id}')"
    if at_list_all(AIRTABLE_REFERRALS, filter_formula=formula): return
    at_create(AIRTABLE_REFERRALS, {"date": today_iso(),
                                   "inviter_chat_id": str(inviter_chat_id),
                                   "invited_chat_id": str(invited_chat_id)})

def at_last_lead_for_user(chat_id: int) -> Optional[Dict]:
    recs = at_list_all(AIRTABLE_TABLE, filter_formula=f"{{chat_id}} = '{chat_id}'")
    if not recs: return None
    recs.sort(key=lambda r: r.get("fields", {}).get("timestamp", ""), reverse=True)
    return recs[0]

def at_list_guides_active() -> List[Dict]:
    return at_list_all(AIRTABLE_GUIDES, filter_formula="{is_active}")

# ──────────────── Отчёты ─────────────────────────────────
def at_count_today_metrics() -> Tuple[int, int, Counter, Counter, Counter]:
    formula = f"{{date}} = '{today_iso()}'"
    recs = at_list_all(AIRTABLE_TABLE, filter_formula=formula)
    total = len(recs)
    uniq: Set[str] = set()
    src_counter, camp_counter, ref_counter = Counter(), Counter(), Counter()
    for r in recs:
        f = r.get("fields", {})
        uniq.add(str(f.get("chat_id","")))
        src = (f.get("utm_source") or "").strip() or "(none)"
        camp= (f.get("utm_campaign") or "").strip() or "(none)"
        ref = (f.get("ref") or "").strip()
        src_counter[src]+=1; camp_counter[camp]+=1
        if ref: ref_counter[ref]+=1
    return total, len({u for u in uniq if u}), src_counter, camp_counter, ref_counter

def at_get_last_report() -> Optional[Dict]:
    recs = at_list_all(AIRTABLE_REPORTS)
    if not recs: return None
    recs.sort(key=lambda r: r.get("fields", {}).get("date", ""), reverse=True)
    return recs[0]

def at_create_report(date_str: str, downloads_total: int, unique_users: int,
                     channel_member_count: int, channel_delta: int) -> Optional[str]:
    fields = {"date": date_str, "downloads_total": downloads_total, "unique_users": unique_users,
              "channel_member_count": channel_member_count, "channel_delta": channel_delta}
    return at_create(AIRTABLE_REPORTS, fields)

# ──────────────── Каталог гидов ─────────────────────────
def guides_keyboard(records: List[Dict]) -> InlineKeyboardMarkup:
    rows = []
    for r in records:
        rid = r["id"]; name = r.get("fields", {}).get("name", "Без названия")
        rows.append([InlineKeyboardButton(name, callback_data=f"g|{rid}")])
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton("Каталог пуст", callback_data="noop")]])

async def open_guides(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    recs = at_list_guides_active()
    if not recs:
        if update.callback_query:
            await update.callback_query.edit_message_text(TEXT_GUIDES_EMPTY)
        else:
            await update.message.reply_text(TEXT_GUIDES_EMPTY)
        return
    if update.callback_query:
        await update.callback_query.edit_message_text("Выбери гайд:", reply_markup=guides_keyboard(recs))
    else:
        await update.message.reply_text("Выбери гайд:", reply_markup=guides_keyboard(recs))

async def pick_guide(update: Update, context: ContextTypes.DEFAULT_TYPE, rec_id: str) -> None:
    q = update.callback_query; await q.answer()
    user = q.from_user; chat_id = q.message.chat_id
    sub = await is_subscribed(user.id, context)
    if sub is not True:
        await q.edit_message_text(TEXT_NEED_SUB, reply_markup=subscribe_keyboard()); return
    guide = at_get(AIRTABLE_GUIDES, rec_id)
    if not guide:
        await q.edit_message_text("Этот гайд временно недоступен."); return
    fields = guide.get("fields", {})
    guide_name = fields.get("name", "Гайд")
    file_id = fields.get("file_id")
    try:
        await q.edit_message_text(f"Отправляю «{guide_name}» 📥"); await asyncio.sleep(2)
        await send_pdf(chat_id, context, file_id=file_id); await asyncio.sleep(2)
    except Exception as e:
        logger.exception("Send guide failed: %s", e)
        await notify_error(context, f"Send guide failed: {e}")
        await q.message.reply_text("Не получилось отправить файл. Попробуй позже.")
        return
    utm = context.user_data.get("utm", {})
    ref = utm.get("ref")
    if ref: at_record_referral(inviter_chat_id=ref, invited_chat_id=str(user.id))
    rec_id = at_upsert_today_lead(user, None, None, utm, "guide_menu",
                                  context.user_data.get("start_param_raw"),
                                  True, guide_name, file_id, ref)
    context.user_data["lead_record_id"] = rec_id or None
    context.user_data["awaiting_name"] = True
    context.user_data["awaiting_email"] = False
    await q.message.reply_text(TEXT_ASK_NAME, reply_markup=skip_keyboard("name"))

# ──────────────── Основной сценарий ─────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    context.user_data["utm"] = parse_utm(args)
    context.user_data["start_param_raw"] = " ".join(args) if args else None
    first = args[0].lower() if args else ""
    if first in ("guide", "гайд") or first.startswith("guide"):
        await guide_flow(update, context, trigger="start"); return
    if update.message:
        await update.message.reply_text(TEXT_WELCOME, reply_markup=start_keyboard())

async def guide_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, trigger: str) -> None:
    user = update.effective_user; chat_id = update.effective_chat.id
    sub = await is_subscribed(user.id, context)
    if sub is True:
        await update.effective_message.reply_text(TEXT_ALREADY_SUB); await asyncio.sleep(2)
        await send_pdf(chat_id, context); await asyncio.sleep(2)
        utm = context.user_data.get("utm", {}); ref = utm.get("ref")
        if ref: at_record_referral(inviter_chat_id=ref, invited_chat_id=str(user.id))
        rec_id = at_upsert_today_lead(user, None, None, utm, trigger,
                                      context.user_data.get("start_param_raw"), True,
                                      DEFAULT_GUIDE_NAME, PDF_FILE_ID, ref)
        context.user_data["lead_record_id"]=rec_id or None
        context.user_data["awaiting_name"]=True; context.user_data["awaiting_email"]=False
        await update.effective_message.reply_text(TEXT_ASK_NAME, reply_markup=skip_keyboard("name"))
    elif sub is False:
        await update.effective_message.reply_text(TEXT_NEED_SUB, reply_markup=subscribe_keyboard())
    else:
        await update.effective_message.reply_text(TEXT_ERROR)

# ──────────────── Скипы / сбор имени и e-mail ───────────
async def on_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    if q.data == "skip_name":
        context.user_data["awaiting_name"]=False; context.user_data["awaiting_email"]=True
        await q.message.reply_text(TEXT_ASK_EMAIL, reply_markup=skip_keyboard("email"))
    elif q.data == "skip_email":
        context.user_data["awaiting_email"]=False
        await q.message.reply_text(TEXT_SKIPPED)

async def collector(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    if context.user_data.get("awaiting_name"):
        context.user_data["display_name"]=text
        context.user_data["awaiting_name"]=False; context.user_data["awaiting_email"]=True
        rec_id = context.user_data.get("lead_record_id")
        if rec_id: at_patch(AIRTABLE_TABLE, rec_id, {"display_name": text})
        await update.message.reply_text(TEXT_ASK_EMAIL, reply_markup=skip_keyboard("email")); return
    if context.user_data.get("awaiting_email"):
        if not EMAIL_RE.match(text):
            await update.message.reply_text(TEXT_EMAIL_BAD, reply_markup=skip_keyboard("email")); return
        context.user_data["awaiting_email"]=False
        rec_id = context.user_data.get("lead_record_id")
        if rec_id: at_patch(AIRTABLE_TABLE, rec_id, {"email": text})
        await update.message.reply_text(TEXT_EMAIL_OK); return

async def keyword_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    low = (update.message.text or "").lower()
    if "гайд" in low:
        context.user_data.setdefault("utm", {})
        context.user_data.setdefault("start_param_raw", None)
        await guide_flow(update, context, trigger="keyword")

# ──────────────── /resend и /reflink ────────────────────
async def resend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lead = at_last_lead_for_user(update.effective_user.id)
    if not lead:
        await update.message.reply_text("Пока не вижу скачиваний. Напиши «гайд» или открой меню гидов.")
        return
    f = lead.get("fields", {})
    name = f.get("guide_name") or DEFAULT_GUIDE_NAME
    fid  = f.get("guide_file_id") or PDF_FILE_ID
    await update.message.reply_text(f"Повторно отправляю «{name}» 📥"); await asyncio.sleep(2)
    await send_pdf(update.effective_chat.id, context, file_id=fid)

async def reflink(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    link = deep_link(f"guide__ref={uid}")
    await update.message.reply_text(f"Твоя персональная ссылка:\n{link}")

# ──────────────── Тур заявка ────────────────────────────
async def tour(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["tour_stage"]="region"
    await update.message.reply_text(TEXT_TOUR_START)

async def tour_collector(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    stage = context.user_data.get("tour_stage")
    txt = (update.message.text or "").strip()
    if stage=="region":
        context.user_data["tour_region"]=txt
        context.user_data["tour_stage"]="dates"
        await update.message.reply_text(TEXT_TOUR_DATES); return
    if stage=="dates":
        context.user_data["tour_dates"]=txt
        context.user_data["tour_stage"]="budget"
        await update.message.reply_text(TEXT_TOUR_BUDGET); return
    if stage=="budget":
        context.user_data["tour_budget"]=txt
        context.user_data["tour_stage"]="pax"
        await update.message.reply_text(TEXT_TOUR_PAX); return
    if stage=="pax":
        context.user_data["tour_pax"]=txt
        context.user_data["tour_stage"]="contact"
        await update.message.reply_text(TEXT_TOUR_CONTACT); return
    if stage=="contact":
        context.user_data["tour_contact"]=txt
        context.user_data["tour_stage"]=None
        fields = {
            "timestamp": datetime.datetime.now(tz).isoformat(),
            "date": today_iso(),
            "chat_id": str(update.effective_user.id),
            "username": update.effective_user.username or "",
            "region": context.user_data.get("tour_region",""),
            "dates": context.user_data.get("tour_dates",""),
            "budget": context.user_data.get("tour_budget",""),
            "pax": context.user_data.get("tour_pax",""),
            "contact": context.user_data.get("tour_contact",""),
        }
        if not at_create(AIRTABLE_TOUR, fields):
            await notify_error(context, "Не удалось записать TourRequest в Airtable")
        await update.message.reply_text(TEXT_TOUR_DONE)
        try:
            msg = ("🗺️ Новая заявка на тур:\n"
                   f"Регион: {fields['region']}\nДаты: {fields['dates']}\nБюджет: {fields['budget']}\n"
                   f"Гостей: {fields['pax']}\nКонтакт: {fields['contact']}\n"
                   f"От: @{fields['username']} ({fields['chat_id']})")
            await context.bot.send_message(chat_id=REPORT_CHAT_ID, text=msg)
        except Exception as e:
            logger.exception("notify tour to admin failed: %s", e)

# ──────────────── Админ-команды ────────────────────────
async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"Ваш Telegram ID: {update.effective_user.id}")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not (ADMIN_USER_ID and update.effective_user.id == ADMIN_USER_ID):
        await update.message.reply_text(TEXT_STATS_DENIED); return
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID):
        await update.message.reply_text(TEXT_NO_AT); return
    total, unique, src_c, camp_c, ref_c = at_count_today_metrics()
    try:
        members = await context.bot.get_chat_member_count(CHANNEL_ID)
    except Exception:
        members = -1
    def fmt(c: Counter): return ", ".join([f"{k}: {v}" for k,v in c.most_common(3)]) or "—"
    text = (f"Сегодня:\n— скачиваний: {total}\n— уникальных: {unique}\n"
            f"— подписчики канала: {members if members>=0 else 'N/A'}\n"
            f"— топ источников: {fmt(src_c)}\n— топ кампаний: {fmt(camp_c)}\n— топ рефереров: {fmt(ref_c)}")
    await update.message.reply_text(text)

async def report_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not (ADMIN_USER_ID and update.effective_user.id == ADMIN_USER_ID): return
    await do_daily_report(context)

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not (ADMIN_USER_ID and update.effective_user.id == ADMIN_USER_ID):
        await update.message.reply_text(TEXT_STATS_DENIED); return
    try:
        members = await context.bot.get_chat_member_count(CHANNEL_ID)
        tg_ok = True
    except Exception:
        tg_ok = False; members = -1
    at_ok = bool(at_headers()) and (len(at_list_guides_active()) >= 0)
    msg = (f"HEALTH:\n— Telegram: {'OK' if tg_ok else 'FAIL'} (subs={members if members>=0 else 'N/A'})\n"
           f"— Airtable: {'OK' if at_ok else 'FAIL'}\n"
           f"— Report chat: {REPORT_CHAT_ID}\n"
           f"— TZ: {REPORT_TZ} @ {REPORT_TIME_HOUR:02d}:{REPORT_TIME_MIN:02d}")
    await update.message.reply_text(msg)

# ──────────────── Дневной отчёт ────────────────────────
async def do_daily_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID):
        logger.warning("Airtable not configured; skip report."); return
    total, unique, src_c, camp_c, ref_c = at_count_today_metrics()
    try:
        members = await context.bot.get_chat_member_count(CHANNEL_ID)
    except Exception as e:
        logger.exception("get_chat_member_count failed: %s", e); members = -1
    last = at_get_last_report()
    last_count = int(last.get("fields", {}).get("channel_member_count", 0)) if last else 0
    delta = (members - last_count) if (members >= 0) else 0
    at_create_report(today_iso(), total, unique, members, delta)
    def fmt(c: Counter): return ", ".join([f"{k}: {v}" for k,v in c.most_common(3)]) or "—"
    text = (f"📊 Отчёт за {today_iso()}:\n"
            f"— Скачиваний: <b>{total}</b>\n— Уникальных: <b>{unique}</b>\n"
            f"— Подписчики канала: <b>{members if members>=0 else 'N/A'}</b>{'' if members<0 else f' (Δ {delta:+d})'}\n"
            f"— Топ источников: <b>{fmt(src_c)}</b>\n— Топ кампаний: <b>{fmt(camp_c)}</b>\n"
            f"— Топ рефереров: <b>{fmt(ref_c)}</b>")
    try:
        await context.bot.send_message(chat_id=REPORT_CHAT_ID, text=text, parse_mode="HTML")
    except Exception as e:
        logger.exception("send report failed: %s", e)

# ──────────────── Callback-и (кнопки) ───────────────────
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    data = q.data or ""
    if data == "check_sub":
        await q.answer()
        user = q.from_user; chat_id = q.message.chat_id
        sub = await is_subscribed(user.id, context)
        if sub is True:
            await q.edit_message_text(TEXT_ALREADY_SUB); await asyncio.sleep(2)
            await send_pdf(chat_id, context); await asyncio.sleep(2)
            utm = context.user_data.get("utm", {}); ref = utm.get("ref")
            if ref: at_record_referral(inviter_chat_id=ref, invited_chat_id=str(user.id))
            rec_id = at_upsert_today_lead(user, None, None, utm, "button_check",
                                          context.user_data.get("start_param_raw"), True,
                                          DEFAULT_GUIDE_NAME, PDF_FILE_ID, ref)
            context.user_data["lead_record_id"]=rec_id or None
            context.user_data["awaiting_name"]=True; context.user_data["awaiting_email"]=False
            await q.message.reply_text(TEXT_ASK_NAME, reply_markup=skip_keyboard("name"))
        elif sub is False:
            await q.edit_message_text(TEXT_NEED_SUB, reply_markup=subscribe_keyboard())
        else:
            await q.edit_message_text(TEXT_ERROR)
    elif data == "open_guides":
        await open_guides(update, context)
    elif data.startswith("g|"):
        await pick_guide(update, context, data.split("|",1)[1])
    elif data.startswith("skip_"):
        await on_skip(update, context)
    else:
        await q.answer()

# ──────────────── Админ-хендлер для получения file_id ──
async def _debug_fileid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пришли боту PDF-документ — он вернёт file_id (только для ADMIN_USER_ID)."""
    if update.effective_user.id != ADMIN_USER_ID:
        return
    if update.message and update.message.document:
        doc = update.message.document
        await update.message.reply_text(
            f"file_id:\n{doc.file_id}\nfile_unique_id:\n{doc.file_unique_id}"
        )

# ──────────────── MAIN ─────────────────────────────────
def main() -> None:
    if not BOT_TOKEN: raise RuntimeError("Не задан BOT_TOKEN (ENV).")
    app = Application.builder().token(BOT_TOKEN).build()

    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("resend", resend))
    app.add_handler(CommandHandler("reflink", reflink))
    app.add_handler(CommandHandler("tour", tour))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("report_now", report_now))
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CommandHandler("guides", open_guides))  # по запросу командой

    # Callback-и
    app.add_handler(CallbackQueryHandler(on_callback))

    # Сообщения (порядок важен)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, collector), group=0)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, keyword_handler), group=1)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, tour_collector), group=2)

    # Админ-хендлер file_id (обрабатывает документы)
    app.add_handler(MessageHandler(filters.Document.ALL, _debug_fileid))

    # Дневной отчёт
    from datetime import time as dtime
    app.job_queue.run_daily(
        do_daily_report,
        time=dtime(hour=REPORT_TIME_HOUR, minute=REPORT_TIME_MIN, tzinfo=tz),
        name="daily_report_job"
    )

    logger.info("Бот запущен. Ожидаю события…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
