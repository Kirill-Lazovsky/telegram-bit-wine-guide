# -*- coding: utf-8 -*-
"""
WineGuide bot.
Функции:
- "гайд"/deep-link -> проверка подписки -> PDF -> сбор имени/e-mail -> лог в Airtable
- каталог гидов (таблица Guides)
- /resend, /reflink, /tour
- /whoami (для любого), /health /report_now /stats /atping /pingpdf (для админа)
- ежедневный отчёт в REPORT_CHAT_ID
"""

import os, re, json, asyncio, logging, datetime
from typing import Dict, List, Optional, Tuple, Set
from collections import Counter

import requests
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember, BotCommand
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# ---------- LOGGING ----------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("wineguide")

# ---------- ENV ----------
BOT_TOKEN       = os.getenv("BOT_TOKEN")
BOT_USERNAME    = os.getenv("BOT_USERNAME", "MyWineBot")
CHANNEL_ID      = os.getenv("CHANNEL_ID", "@lazovsky_kirill")

ADMIN_USER_ID   = int(os.getenv("ADMIN_USER_ID", "0") or "0")
REPORT_CHAT_ID  = int(os.getenv("REPORT_CHAT_ID", str(ADMIN_USER_ID or 0)) or "0")

PDF_FILE_ID     = os.getenv("PDF_FILE_ID")  # ← важно: так и называется
PDF_PATH        = os.getenv("PDF_PATH", "Kak_vyibrat_horoshee_vino_v_magazine_restorane_ili_na_podarok_.pdf")
DEFAULT_GUIDE   = os.getenv("DEFAULT_GUIDE_NAME", "Основной гайд")

AIRTABLE_API    = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE   = os.getenv("AIRTABLE_BASE_ID")
T_LEADS         = os.getenv("AIRTABLE_TABLE_NAME", "Leads")
T_REPORTS       = os.getenv("AIRTABLE_REPORTS_TABLE", "Reports")
T_GUIDES        = os.getenv("AIRTABLE_GUIDES_TABLE", "Guides")
T_REF           = os.getenv("AIRTABLE_REFERRALS_TABLE", "Referrals")
T_TOUR          = os.getenv("AIRTABLE_TOUR_TABLE", "TourRequests")

TZ_NAME         = os.getenv("REPORT_TZ", "Europe/Paris")
try:
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(TZ_NAME)
except Exception:
    tz = datetime.timezone.utc
TIME_HOUR       = int(os.getenv("REPORT_TIME_HOUR", "20") or "20")
TIME_MIN        = int(os.getenv("REPORT_TIME_MINUTE", "0") or "0")

# ---------- TEXTS ----------
TXT_WELCOME   = "Привет! Нажми кнопку ниже — и я пришлю PDF-гайд после проверки подписки 👇"
TXT_ALREADY   = "Подписка подтверждена! Отправляю гайд 📥"
TXT_NEEDSUB   = "Похоже, ты ещё не подписан(а) на канал.\nПодпишись и вернись — я жду! 😉"
TXT_ERROR     = "Не смог проверить подписку. Убедись, что я админ канала, и попробуй ещё раз."
TXT_SENT      = "Готово! Сохрани гайд себе 📎"
TXT_ASK_NAME  = "Можно 10 секунд? Как к тебе обращаться? 🙂\n\nНапиши имя одним сообщением или нажми «Пропустить»."
TXT_ASK_MAIL  = "Спасибо! Оставь e-mail, чтобы получать мои короткие винные подсказки ✉️\n\nНапиши e-mail или нажми «Пропустить»."
TXT_MAIL_OK   = "Принял! Если надо — всегда можно написать /start ещё раз. 🍷"
TXT_MAIL_BAD  = "Кажется, это не похоже на e-mail. Попробуй ещё раз или нажми «Пропустить»."
TXT_SKIPPED   = "Ок, пропускаем. Спасибо!"
TXT_GUIDES_EMPTY = "Каталог гидов скоро пополнится. Загляни чуть позже 📝"
TXT_NO_AT     = "⚠️ Airtable не настроен. Проверь AIRTABLE_API_KEY и AIRTABLE_BASE_ID."
EMAIL_RE      = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

BTN_SUB = "Подписаться"; BTN_CHECK = "Проверить подписку"
BTN_GET = "Получить гайд"; BTN_MORE = "📚 Ещё гиды"; BTN_SKIP = "Пропустить"

# ---------- UI ----------
def deep_link(extra="guide") -> str:
    return f"https://t.me/{BOT_USERNAME}?start={extra}"

def kb_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(BTN_GET, url=deep_link("guide"))],
        [InlineKeyboardButton(BTN_MORE, callback_data="open_guides")]
    ])

def kb_subscribe() -> InlineKeyboardMarkup:
    url = f"https://t.me/{str(CHANNEL_ID).replace('@','')}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(BTN_SUB, url=url)],
        [InlineKeyboardButton(BTN_CHECK, callback_data="check_sub")]
    ])

def kb_skip(tag: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(BTN_SKIP, callback_data=f"skip_{tag}")]])

def parse_utm(args: List[str]) -> Dict[str,str]:
    if not args: return {}
    raw = " ".join(args); parts = raw.split("__"); kv={}
    for token in parts[1:]:
        if "=" in token:
            k,v = token.split("=",1); kv[k.strip()]=v.strip()
    if kv: kv["_raw"]=raw
    return kv

# ---------- ADMIN NOTIFY ----------
_last_alert=0.0
async def notify_error(ctx: ContextTypes.DEFAULT_TYPE, text: str, throttle=600):
    global _last_alert
    now = datetime.datetime.now().timestamp()
    if now - _last_alert < throttle: return
    _last_alert = now
    try:
        if REPORT_CHAT_ID:
            await ctx.bot.send_message(REPORT_CHAT_ID, f"⚠️ Ошибка: {text[:3500]}")
    except Exception as e:
        log.exception("notify_error failed: %s", e)

# ---------- TG HELPERS ----------
async def is_subscribed(uid: int, ctx: ContextTypes.DEFAULT_TYPE) -> Optional[bool]:
    try:
        m: ChatMember = await ctx.bot.get_chat_member(CHANNEL_ID, uid)
        return m.status in ("member", "administrator", "creator")
    except Exception as e:
        log.exception("get_chat_member failed: %s", e); return None

async def send_pdf_robust(chat_id: int, ctx, file_id: Optional[str], path: Optional[str]) -> None:
    # сначала пытаемся по file_id; при ошибке — с диска
    if file_id:
        try:
            await ctx.bot.send_document(chat_id=chat_id, document=file_id, caption=TXT_SENT)
            return
        except Exception:
            pass
    if path:
        with open(path, "rb") as f:
            await ctx.bot.send_document(chat_id=chat_id, document=f,
                                        filename=os.path.basename(path), caption=TXT_SENT)
    else:
        raise RuntimeError("Нет PDF_FILE_ID и PDF_PATH")

# ---------- AIRTABLE ----------
def at_headers() -> Dict[str,str]:
    if not AIRTABLE_API or not AIRTABLE_BASE: return {}
    return {"Authorization": f"Bearer {AIRTABLE_API}", "Content-Type": "application/json"}

def at_url(table: str) -> str: return f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{table}"

def at_create(table: str, fields: Dict) -> Optional[str]:
    try:
        r = requests.post(at_url(table), headers=at_headers(), json={"fields": fields}, timeout=20)
        if r.status_code in (200,201): return r.json().get("id")
        log.error("Airtable create %s: %s", r.status_code, r.text)
    except Exception as e: log.exception("Airtable create exception: %s", e)
    return None

def at_patch(table: str, record_id: str, fields: Dict) -> bool:
    try:
        r = requests.patch(f"{at_url(table)}/{record_id}", headers=at_headers(), json={"fields": fields}, timeout=20)
        if r.status_code in (200,201): return True
        log.error("Airtable patch %s: %s", r.status_code, r.text)
    except Exception as e: log.exception("Airtable patch exception: %s", e)
    return False

def at_get(table: str, rec_id: str) -> Optional[Dict]:
    try:
        r = requests.get(f"{at_url(table)}/{rec_id}", headers=at_headers(), timeout=15)
        if r.status_code==200: return r.json()
        log.error("Airtable get %s: %s", r.status_code, r.text)
    except Exception as e: log.exception("Airtable get exception: %s", e)
    return None

def at_list_all(table: str, formula: Optional[str]=None) -> List[Dict]:
    params = {}; 
    if formula: params["filterByFormula"]=formula
    out=[]; offset=None; headers=at_headers()
    if not headers: return out
    while True:
        if offset: params["offset"]=offset
        r = requests.get(at_url(table), headers=headers, params=params, timeout=30)
        if r.status_code!=200:
            log.error("Airtable list %s: %s", r.status_code, r.text); break
        data=r.json(); out+=data.get("records",[]); offset=data.get("offset")
        if not offset: break
    return out

# ---------- LEADS & REPORTS ----------
def today() -> str: return datetime.datetime.now(tz).date().isoformat()

def lead_fields(user, display_name, email, utm, trigger, start_raw, subscribed,
                guide_name, guide_file_id, ref) -> Dict:
    now=datetime.datetime.now(tz)
    return {
        "timestamp": now.isoformat(), "date": now.date().isoformat(),
        "chat_id": str(user.id), "username": user.username or "",
        "first_name": user.first_name or "", "last_name": user.last_name or "",
        "display_name": display_name or "", "email": email or "",
        "utm_source": utm.get("utm_source",""), "utm_medium": utm.get("utm_medium",""),
        "utm_campaign": utm.get("utm_campaign",""), "utm_content": utm.get("utm_content",""),
        "utm_term": utm.get("utm_term",""), "start_param_raw": start_raw or "",
        "trigger": trigger, "subscribed": subscribed,
        "guide_name": guide_name, "guide_file_id": guide_file_id or "", "ref": ref or ""
    }

def at_find_today_lead(chat_id: int) -> Optional[Dict]:
    return (at_list_all(T_LEADS, f"AND({{chat_id}}='{chat_id}', {{date}}='{today()}')") or [None])[0]

def at_upsert_today_lead(user, display_name, email, utm, trigger, start_raw, subscribed,
                         guide_name, guide_file_id, ref) -> str:
    existing = at_find_today_lead(user.id)
    fields = lead_fields(user, display_name, email, utm, trigger, start_raw, subscribed, guide_name, guide_file_id, ref)
    if existing:
        rec_id = existing["id"]
        update = {"timestamp": fields["timestamp"], "guide_name": guide_name,
                  "guide_file_id": guide_file_id or "", "trigger": trigger}
        for k in ("display_name","email","utm_source","utm_medium","utm_campaign","utm_content","utm_term","start_param_raw","ref"):
            v=fields.get(k); old=existing.get("fields",{}).get(k)
            if v and v!=old: update[k]=v
        at_patch(T_LEADS, rec_id, update); return rec_id
    else:
        return at_create(T_LEADS, fields) or ""

def at_record_ref(inviter: str, invited: str):
    if not inviter or inviter==invited: return
    if at_list_all(T_REF, f"AND({{inviter_chat_id}}='{inviter}', {{invited_chat_id}}='{invited}')"): return
    at_create(T_REF, {"date": today(), "inviter_chat_id": str(inviter), "invited_chat_id": str(invited)})

def last_lead(chat_id: int) -> Optional[Dict]:
    recs=at_list_all(T_LEADS, f"{{chat_id}}='{chat_id}'")
    if not recs: return None
    recs.sort(key=lambda r: r.get("fields",{}).get("timestamp",""), reverse=True); return recs[0]

def guides_active() -> List[Dict]:
    return at_list_all(T_GUIDES, "{is_active}")

def count_today() -> Tuple[int,int,Counter,Counter,Counter]:
    recs=at_list_all(T_LEADS, f"{{date}}='{today()}'")
    total=len(recs); uniq=set(); src=Counter(); camp=Counter(); refc=Counter()
    for r in recs:
        f=r.get("fields",{})
        uniq.add(str(f.get("chat_id","")))
        src[(f.get("utm_source") or "").strip() or "(none)"]+=1
        camp[(f.get("utm_campaign") or "").strip() or "(none)"]+=1
        if f.get("ref"): refc[str(f.get("ref"))]+=1
    return total, len(uniq), src, camp, refc

def last_report() -> Optional[Dict]:
    recs=at_list_all(T_REPORTS)
    if not recs: return None
    recs.sort(key=lambda r: r.get("fields",{}).get("date",""), reverse=True); return recs[0]

def create_report(date, d_total, d_unique, members, delta) -> Optional[str]:
    return at_create(T_REPORTS, {
        "date": date, "downloads_total": d_total, "unique_users": d_unique,
        "channel_member_count": members, "channel_delta": delta
    })

# ---------- FLOW ----------
def kb_guides(records: List[Dict]) -> InlineKeyboardMarkup:
    rows=[[InlineKeyboardButton(r.get("fields",{}).get("name","Без названия"), callback_data=f"g|{r['id']}")] for r in records]
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton("Каталог пуст", callback_data="noop")]])

def utm_from_ctx(ctx): return ctx.user_data.get("utm", {})
def start_raw(ctx): return ctx.user_data.get("start_param_raw")

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args or []
    ctx.user_data["utm"] = parse_utm(args)
    ctx.user_data["start_param_raw"] = " ".join(args) if args else None
    first = args[0].lower() if args else ""
    if first in ("guide","гайд") or first.startswith("guide"):
        await guide_flow(update, ctx, "start"); return
    if update.message:
        await update.message.reply_text(TXT_WELCOME, reply_markup=kb_start())

async def guide_flow(update: Update, ctx: ContextTypes.DEFAULT_TYPE, trigger: str):
    user=update.effective_user; chat_id=update.effective_chat.id
    sub = await is_subscribed(user.id, ctx)
    if sub is True:
        await update.effective_message.reply_text(TXT_ALREADY); await asyncio.sleep(2)
        await send_pdf_robust(chat_id, ctx, PDF_FILE_ID, PDF_PATH); await asyncio.sleep(2)
        utm = utm_from_ctx(ctx); ref = utm.get("ref")
        if ref: at_record_ref(ref, str(user.id))
        rec_id = at_upsert_today_lead(user, None, None, utm, trigger, start_raw(ctx), True, DEFAULT_GUIDE, PDF_FILE_ID, ref)
        if not rec_id: await notify_error(ctx, "Airtable: failed to log lead (guide_flow)")
        ctx.user_data["lead_record_id"]=rec_id or None
        ctx.user_data["awaiting_name"]=True; ctx.user_data["awaiting_email"]=False
        await update.effective_message.reply_text(TXT_ASK_NAME, reply_markup=kb_skip("name"))
    elif sub is False:
        await update.effective_message.reply_text(TXT_NEEDSUB, reply_markup=kb_subscribe())
    else:
        await update.effective_message.reply_text(TXT_ERROR)

async def open_guides(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    recs=guides_active()
    if not recs: await q.edit_message_text(TXT_GUIDES_EMPTY); return
    await q.edit_message_text("Выбери гайд:", reply_markup=kb_guides(recs))

async def pick_guide(update: Update, ctx: ContextTypes.DEFAULT_TYPE, rec_id: str):
    q=update.callback_query; await q.answer()
    user=q.from_user; chat_id=q.message.chat_id
    sub=await is_subscribed(user.id, ctx)
    if sub is not True:
        await q.edit_message_text(TXT_NEEDSUB, reply_markup=kb_subscribe()); return
    guide=at_get(T_GUIDES, rec_id)
    if not guide: await q.edit_message_text("Этот гайд временно недоступен."); return
    fields=guide.get("fields",{}); name=fields.get("name","Гайд"); fid=fields.get("file_id")
    await q.edit_message_text(f"Отправляю «{name}» 📥"); await asyncio.sleep(2)
    await send_pdf_robust(chat_id, ctx, fid, None); await asyncio.sleep(2)
    utm=utm_from_ctx(ctx); ref=utm.get("ref"); if ref: at_record_ref(ref, str(user.id))
    rec_id2=at_upsert_today_lead(user, None, None, utm, "guide_menu", start_raw(ctx), True, name, fid, ref)
    if not rec_id2: await notify_error(ctx, "Airtable: failed to log lead (pick_guide)")
    ctx.user_data["lead_record_id"]=rec_id2 or None; ctx.user_data["awaiting_name"]=True; ctx.user_data["awaiting_email"]=False
    await q.message.reply_text(TXT_ASK_NAME, reply_markup=kb_skip("name"))

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; data=q.data or ""
    if data=="check_sub":
        await q.answer(); user=q.from_user; chat_id=q.message.chat_id
        sub = await is_subscribed(user.id, ctx)
        if sub is True:
            await q.edit_message_text(TXT_ALREADY); await asyncio.sleep(2)
            await send_pdf_robust(chat_id, ctx, PDF_FILE_ID, PDF_PATH); await asyncio.sleep(2)
            utm=utm_from_ctx(ctx); ref=utm.get("ref"); if ref: at_record_ref(ref, str(user.id))
            rec_id=at_upsert_today_lead(user, None, None, utm, "button_check", start_raw(ctx), True, DEFAULT_GUIDE, PDF_FILE_ID, ref)
            if not rec_id: await notify_error(ctx, "Airtable: failed to log lead (check_sub)")
            ctx.user_data["lead_record_id"]=rec_id or None; ctx.user_data["awaiting_name"]=True; ctx.user_data["awaiting_email"]=False
            await q.message.reply_text(TXT_ASK_NAME, reply_markup=kb_skip("name"))
        elif sub is False:
            await q.edit_message_text(TXT_NEEDSUB, reply_markup=kb_subscribe())
        else:
            await q.edit_message_text(TXT_ERROR)
    elif data=="open_guides":
        await open_guides(update, ctx)
    elif data.startswith("g|"):
        await pick_guide(update, ctx, data.split("|",1)[1])
    elif data.startswith("skip_"):
        await on_skip(update, ctx)
    else:
        await q.answer()

# ---------- Collect name/email ----------
async def on_skip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    if q.data=="skip_name":
        ctx.user_data["awaiting_name"]=False; ctx.user_data["awaiting_email"]=True
        await q.message.reply_text(TXT_ASK_MAIL, reply_markup=kb_skip("email"))
    elif q.data=="skip_email":
        ctx.user_data["awaiting_email"]=False; await q.message.reply_text(TXT_SKIPPED)

async def collector(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text=(update.message.text or "").strip()
    if ctx.user_data.get("awaiting_name"):
        ctx.user_data["display_name"]=text; ctx.user_data["awaiting_name"]=False; ctx.user_data["awaiting_email"]=True
        rec_id=ctx.user_data.get("lead_record_id"); 
        if rec_id: at_patch(T_LEADS, rec_id, {"display_name": text})
        await update.message.reply_text(TXT_ASK_MAIL, reply_markup=kb_skip("email")); return
    if ctx.user_data.get("awaiting_email"):
        if not EMAIL_RE.match(text):
            await update.message.reply_text(TXT_MAIL_BAD, reply_markup=kb_skip("email")); return
        ctx.user_data["awaiting_email"]=False
        rec_id=ctx.user_data.get("lead_record_id"); 
        if rec_id: at_patch(T_LEADS, rec_id, {"email": text})
        await update.message.reply_text(TXT_MAIL_OK); return

async def keyword_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "гайд" in (update.message.text or "").lower():
        ctx.user_data.setdefault("utm", {}); ctx.user_data.setdefault("start_param_raw", None)
        await guide_flow(update, ctx, "keyword")

# ---------- User commands ----------
async def whoami(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Ваш Telegram ID: {update.effective_user.id}")

async def resend(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lead=last_lead(update.effective_user.id)
    if not lead:
        await update.message.reply_text("Пока не вижу скачиваний. Напиши «гайд» или открой каталог."); return
    f=lead.get("fields",{}); name=f.get("guide_name") or DEFAULT_GUIDE; fid=f.get("guide_file_id") or PDF_FILE_ID
    await update.message.reply_text(f"Повторно отправляю «{name}» 📥"); await asyncio.sleep(2)
    await send_pdf_robust(update.effective_chat.id, ctx, fid, PDF_PATH if fid is None else None)

async def reflink(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    link = deep_link(f"guide__ref={update.effective_user.id}")
    await update.message.reply_text(f"Твоя персональная ссылка:\n{link}")

# ---------- Admin helpers ----------
def is_admin(u: Update) -> bool:
    return ADMIN_USER_ID and u.effective_user.id == ADMIN_USER_ID

async def health(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        members = await ctx.bot.get_chat_member_count(CHANNEL_ID); tg_ok=True
    except Exception: members=-1; tg_ok=False
    at_ok = bool(at_headers())
    if at_ok:
        try: _=guides_active()
        except Exception: at_ok=False
    await update.message.reply_text(
        f"HEALTH:\n— Telegram: {'OK' if tg_ok else 'FAIL'} (subs={members if members>=0 else 'N/A'})\n"
        f"— Airtable: {'OK' if at_ok else 'FAIL'}\n"
        f"— Report chat: {REPORT_CHAT_ID}\n— TZ: {getattr(tz,'key','UTC')} @ {TIME_HOUR:02d}:{TIME_MIN:02d}"
    )

async def stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    if not at_headers(): await update.message.reply_text(TXT_NO_AT); return
    total, uniq, src, camp, refc = count_today()
    try: members = await ctx.bot.get_chat_member_count(CHANNEL_ID)
    except Exception: members=-1
    fmt=lambda c: ", ".join([f"{k}: {v}" for k,v in c.most_common(3)]) or "—"
    await update.message.reply_text(
        f"Сегодня:\n— скачиваний: {total}\n— уникальных: {uniq}\n"
        f"— подписчики канала: {members if members>=0 else 'N/A'}\n"
        f"— топ источников: {fmt(src)}\n— топ кампаний: {fmt(camp)}\n— топ рефереров: {fmt(refc)}"
    )

async def report_now(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    await do_daily_report(ctx)

async def atping(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    headers_ok = bool(at_headers())
    try: recs = guides_active(); msg=f"Airtable headers: {'OK' if headers_ok else 'MISSING'}\nGuides records: {len(recs)}"
    except Exception as e: msg=f"Airtable error: {type(e).__name__}: {e}"
    await update.message.reply_text(msg)

async def pingpdf(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    exists = os.path.exists(PDF_PATH); size = os.path.getsize(PDF_PATH) if exists else 0
    info = f"PDF_FILE_ID={'set' if PDF_FILE_ID else 'not set'}\nPDF_PATH={os.path.basename(PDF_PATH)}\nexists={exists}\nsize={size} bytes"
    try:
        await send_pdf_robust(update.effective_chat.id, ctx, PDF_FILE_ID, PDF_PATH)
    except Exception as e:
        info += f"\nОшибка отправки: {e}"
    await update.message.reply_text(info)

# ---------- Daily report ----------
async def do_daily_report(ctx: ContextTypes.DEFAULT_TYPE):
    if not at_headers(): log.warning("Airtable not configured"); return
    total, uniq, src, camp, refc = count_today()
    try: members = await ctx.bot.get_chat_member_count(CHANNEL_ID)
    except Exception as e: log.exception("get_chat_member_count failed: %s", e); members=-1
    lr = last_report(); last_cnt = int(lr.get("fields",{}).get("channel_member_count",0)) if lr else 0
    delta = (members - last_cnt) if members>=0 else 0
    create_report(today(), total, uniq, members, delta)
    fmt=lambda c: ", ".join([f"{k}: {v}" for k,v in c.most_common(3)]) or "—"
    text=(f"📊 Отчёт за {today()}:\n— Скачиваний: <b>{total}</b>\n— Уникальных: <b>{uniq}</b>\n"
          f"— Подписчики канала: <b>{members if members>=0 else 'N/A'}</b>{'' if members<0 else f' (Δ {delta:+d})'}\n"
          f"— Топ источников: <b>{fmt(src)}</b>\n— Топ кампаний: <b>{fmt(camp)}</b>\n— Топ рефереров: <b>{fmt(refc)}</b>")
    try: await ctx.bot.send_message(REPORT_CHAT_ID, text, parse_mode="HTML")
    except Exception as e: log.exception("send report failed: %s", e)

# ---------- Error handler ----------
async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled exception", exc_info=ctx.error)
    try:
        await notify_error(ctx, f"{type(ctx.error).__name__}: {ctx.error}")
    except Exception:
        pass

# ---------- MAIN ----------
def main():
    if not BOT_TOKEN: raise RuntimeError("BOT_TOKEN не задан")
    app = Application.builder().token(BOT_TOKEN).build()

    # команды в меню Telegram (удобно видеть, что /whoami существует)
    app.bot.set_my_commands([
        BotCommand("start","Начать"),
        BotCommand("whoami","Показать мой Telegram ID"),
        BotCommand("resend","Прислать последний гайд"),
        BotCommand("reflink","Персональная реф-ссылка"),
        BotCommand("tour","Заявка на винный тур"),
        BotCommand("health","(админ) Статус"),
        BotCommand("stats","(админ) Статистика за сегодня"),
        BotCommand("report_now","(админ) Прислать отчёт"),
        BotCommand("atping","(админ) Проверить Airtable"),
        BotCommand("pingpdf","(админ) Проверить PDF"),
    ])

    # команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("resend", resend))
    app.add_handler(CommandHandler("reflink", reflink))
    app.add_handler(CommandHandler("tour", lambda u,c: c.application.create_task(tour(u,c)) if True else None))  # просто чтобы не забыть

    # админ
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("report_now", report_now))
    app.add_handler(CommandHandler("atping", atping))
    app.add_handler(CommandHandler("pingpdf", pingpdf))

    # callbacks
    app.add_handler(CallbackQueryHandler(on_callback))

    # сообщения
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, collector), group=0)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, keyword_handler), group=1)

    # ежедневный отчёт
    from datetime import time as dtime
    app.job_queue.run_daily(do_daily_report, time=dtime(hour=TIME_HOUR, minute=TIME_MIN, tzinfo=tz), name="daily_report")

    app.add_error_handler(on_error)

    log.info("Bot is up.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

# простой tour (чтоб не потерять команду)
async def tour(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Напиши регион интереса (Бордо, Шампань, Прованс)…")
    ctx.user_data["tour_stage"]="region"

if __name__ == "__main__":
    main()
