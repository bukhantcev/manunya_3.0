#main.py
import os
import uuid
import time
import re
import asyncio
import aiohttp
import ssl
import certifi
import urllib.parse

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    URLInputFile,
)
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
YANDEX_TOKEN = os.getenv("YANDEX_TOKEN")

# Разрешенные группы: "-1001..., -1002..."
ALLOWED_GROUP_IDS = [
    int(x.strip()) for x in os.getenv("ALLOWED_GROUP_IDS", "").split(",") if x.strip()
]

BESTUSER_IDS = [
    int(x.strip()) for x in os.getenv("BESTUSER_IDS", "").split(",") if x.strip()
]
BESTUSER_USERNAMES = [
    (x.strip().lstrip("@").lower())
    for x in os.getenv("BESTUSER_USERNAMES", "").split(",")
    if x.strip()
]

MATERIALS_PATH = "/materials"
API_BASE = "https://cloud-api.yandex.net/v1/disk"

SSL_CTX = ssl.create_default_context(cafile=certifi.where())

AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(
    total=None,
    connect=60,
    sock_connect=60,
    sock_read=900,  # 15 минут
)

router = Router()

# callback -> путь на диске
PATH_CACHE: dict[str, str] = {}

# кеш членства, чтобы не дергать API на каждый клик
# user_id -> (expires_at, allowed_bool)
MEMBER_CACHE: dict[int, tuple[float, bool]] = {}
MEMBER_CACHE_TTL_SEC = int(os.getenv("MEMBER_CACHE_TTL_SEC", "60"))  # 60 сек

# Google Sheets (публичная таблица через API key)
GSHEETS_API_KEY = os.getenv("GSHEETS_API_KEY", "").strip()
GSHEETS_SPREADSHEET_ID = os.getenv("GSHEETS_SPREADSHEET_ID", "").strip()
GSHEETS_RANGE = os.getenv("GSHEETS_RANGE", "Sheet1!A:Z").strip()

# кеш таблицы, чтобы не дергать Google на каждый запрос
# (expires_at, values)
GSHEETS_CACHE: tuple[float, list[list[str]]] | None = None
GSHEETS_CACHE_TTL_SEC = int(os.getenv("GSHEETS_CACHE_TTL_SEC", "60"))


def cache_path(path: str) -> str:
    key = uuid.uuid4().hex[:16]
    PATH_CACHE[key] = path
    return key


def get_cached_path(key: str) -> str:
    return PATH_CACHE.get(key, MATERIALS_PATH)


def depth_from_materials(path: str) -> int:
    p = path.strip("/")
    m = MATERIALS_PATH.strip("/")
    if p == m:
        return 0
    if not p.startswith(m + "/"):
        return 0
    rest = p[len(m) + 1 :]
    return 1 + rest.count("/")


def parent_path(path: str) -> str:
    if path.rstrip("/") == MATERIALS_PATH.rstrip("/"):
        return MATERIALS_PATH
    p = path.rstrip("/").rsplit("/", 1)[0]
    return p if p else "/"


def kb_for_dirs(path: str, dirs: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    if path.rstrip("/") != MATERIALS_PATH.rstrip("/"):
        back_id = cache_path(parent_path(path))
        rows.append([InlineKeyboardButton(text="⬅️ назад", callback_data=f"nav:{back_id}")])

    for d in dirs:
        pid = cache_path(d["path"])
        rows.append([InlineKeyboardButton(text=d.get("name", ""), callback_data=f"nav:{pid}")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def yd_list(path: str):
    headers = {"Authorization": f"OAuth {YANDEX_TOKEN}"}
    params = {"path": path, "limit": 200}
    async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as s:
        async with s.get(f"{API_BASE}/resources", headers=headers, params=params, ssl=SSL_CTX) as r:
            r.raise_for_status()
            data = await r.json()

    items = data.get("_embedded", {}).get("items", [])
    dirs = sorted([x for x in items if x.get("type") == "dir"], key=lambda x: x.get("name", "").lower())
    files = sorted([x for x in items if x.get("type") == "file"], key=lambda x: x.get("name", "").lower())
    return dirs, files


async def yd_download_url(path: str) -> str:
    headers = {"Authorization": f"OAuth {YANDEX_TOKEN}"}
    params = {"path": path}
    async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as s:
        async with s.get(f"{API_BASE}/resources/download", headers=headers, params=params, ssl=SSL_CTX) as r:
            r.raise_for_status()
            data = await r.json()
    return data["href"]


async def yd_read_text(path: str, max_bytes: int = 128_000) -> str:
    url = await yd_download_url(path)
    async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as s:
        async with s.get(url, ssl=SSL_CTX) as r:
            r.raise_for_status()
            data = await r.content.read(max_bytes)
    return data.decode("utf-8", errors="ignore")


def link_title_from_filename(name: str) -> str:
    # link_Имя.txt -> Имя
    base = name[:-4]  # drop .txt
    return base[5:] if base.lower().startswith("link_") else base



def extract_url(text: str) -> str | None:
    if not text:
        return None
    # поддержка формата .url (InternetShortcut)
    m = re.search(r"^URL\s*=\s*(https?://\S+)\s*$", text, flags=re.IGNORECASE | re.MULTILINE)
    if m:
        return m.group(1).strip()
    m = re.search(r"https?://\S+", text)
    return m.group(0).strip() if m else None


# --- Google Sheets helpers ---
async def gsheets_get_values() -> list[list[str]]:
    """Читает диапазон из Google Sheets через Sheets API v4 (требуется публичная таблица + API key)."""
    global GSHEETS_CACHE

    if not (GSHEETS_API_KEY and GSHEETS_SPREADSHEET_ID and GSHEETS_RANGE):
        raise RuntimeError("Нужны GSHEETS_API_KEY, GSHEETS_SPREADSHEET_ID, GSHEETS_RANGE в .env")

    now = time.time()
    if GSHEETS_CACHE and GSHEETS_CACHE[0] > now:
        return GSHEETS_CACHE[1]

    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{GSHEETS_SPREADSHEET_ID}"
        f"/values/{urllib.parse.quote(GSHEETS_RANGE, safe='!$')}"
    )
    params = {"key": GSHEETS_API_KEY, "majorDimension": "ROWS"}

    async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as s:
        async with s.get(url, params=params, ssl=SSL_CTX) as r:
            if r.status == 403:
                # почти всегда значит: таблица не публичная для чтения по API key
                txt = await r.text()
                raise RuntimeError(
                    "Google Sheets вернул 403. Сделай таблицу публичной для чтения или используй service account. "
                    f"Детали: {txt[:500]}"
                )
            if r.status >= 400:
                txt = await r.text()
                raise RuntimeError(f"Google Sheets HTTP {r.status}: {txt[:800]}")
            data = await r.json()

    values = data.get("values") or []
    # нормализуем в строки
    norm: list[list[str]] = [[str(c) for c in row] for row in values]
    GSHEETS_CACHE = (now + GSHEETS_CACHE_TTL_SEC, norm)
    return norm


def _find_col_idx(headers: list[str], *variants: str) -> int | None:
    h = [(x or "").strip().lower() for x in headers]
    for v in variants:
        v2 = v.strip().lower()
        if v2 in h:
            return h.index(v2)
    return None


async def lookup_dmx_by_socket_number(socket_number: str) -> str | None:
    """Ищет строку, где 'номер розетки' == socket_number и возвращает значение из колонки 'DMX адрес'."""
    values = await gsheets_get_values()
    if not values:
        return None

    headers = values[0]
    idx_socket = _find_col_idx(headers, "номер розетки", "розетка", "socket", "outlet", "номер")
    idx_dmx = _find_col_idx(headers, "dmx адрес", "dmx", "адрес dmx", "dmx address")

    # если нет заголовков — пробуем дефолт: A=socket, B=dmx
    if idx_socket is None or idx_dmx is None:
        idx_socket, idx_dmx = 0, 1

    target = str(socket_number).strip().lower()
    for row in values[1:]:
        if idx_socket >= len(row):
            continue
        cell = (row[idx_socket] or "").strip().lower()
        # поддержка значений вида 14Н / 14Р / STM 14Н
        if cell == target or cell.endswith(target) or target in cell:
            return (row[idx_dmx].strip() if idx_dmx < len(row) else "") or None

    return None


def is_bestuser(user) -> bool:
    if not user:
        return False
    if BESTUSER_IDS and user.id in set(BESTUSER_IDS):
        return True
    if BESTUSER_USERNAMES:
        uname = (user.username or "").lower()
        if uname and uname in set(BESTUSER_USERNAMES):
            return True
    return False


async def fetch_quote_ru() -> str:
    url = "http://api.forismatic.com/api/1.0/"
    data = {"method": "getQuote", "format": "text", "lang": "ru"}

    timeout = aiohttp.ClientTimeout(total=7)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.post(url, data=data) as r:
            text = (await r.text()).strip()

    return text or "Иногда лучше промолчать. Но не сегодня."


async def send_link_files(message: Message, files: list[dict]) -> list[dict]:
    """Отправляет все link_*.txt как отдельные сообщения. Возвращает files без этих link-файлов."""
    link_files = [
        f for f in files
        if (f.get("name") or "").lower().startswith("link_") and (f.get("name") or "").lower().endswith(".txt")
    ]

    if not link_files:
        return files

    # отправляем ссылки в отдельных сообщениях
    for lf in link_files:
        name = lf.get("name", "link")
        title = link_title_from_filename(name)
        try:
            text = await yd_read_text(lf["path"])
            url = extract_url(text)
            if not url:
                await message.answer(f"{title}\n(ссылка не найдена)")
            else:
                await message.answer(f"{title}\n{url}")
        except Exception as e:
            await message.answer(f"{title}\n(ошибка чтения ссылки: {e})")

    # убираем link_*.txt из списка файлов, чтобы не отправлять их документом
    link_paths = {lf.get("path") for lf in link_files}
    return [f for f in files if f.get("path") not in link_paths]


async def filter_nonempty_dirs(dirs: list[dict]) -> list[dict]:
    """Оставляет только папки, в которых есть хотя бы один файл или подпапка."""
    if not dirs:
        return []

    sem = asyncio.Semaphore(10)

    async def has_any(d: dict) -> bool:
        async with sem:
            try:
                sub_dirs, sub_files = await yd_list(d["path"])
                return bool(sub_dirs or sub_files)
            except Exception:
                # если не смогли проверить — скрываем, чтобы не показывать мусор
                return False

    flags = await asyncio.gather(*[has_any(d) for d in dirs])
    return [d for d, ok in zip(dirs, flags) if ok]


def chat_is_allowed(chat_id: int) -> bool:
    # Разрешено: любая группа из списка
    return chat_id in set(ALLOWED_GROUP_IDS)


async def user_is_member_of_any_allowed_group(bot: Bot, user_id: int) -> bool:
    # кеш
    now = time.time()
    cached = MEMBER_CACHE.get(user_id)
    if cached and cached[0] > now:
        return cached[1]

    if not ALLOWED_GROUP_IDS:
        MEMBER_CACHE[user_id] = (now + MEMBER_CACHE_TTL_SEC, True)
        return True

    allowed = False
    for gid in ALLOWED_GROUP_IDS:
        try:
            member = await bot.get_chat_member(chat_id=gid, user_id=user_id)
            if member.status not in {"left", "kicked"}:
                allowed = True
                break
        except TelegramBadRequest:
            # бот не в группе или нет прав — считаем что не можем подтвердить членство
            continue
        except Exception:
            continue

    MEMBER_CACHE[user_id] = (now + MEMBER_CACHE_TTL_SEC, allowed)
    return allowed


async def ensure_allowed_context(message_or_call, bot: Bot) -> bool:
    """
    Правила:
    - В разрешенной группе: доступ есть всем участникам этой группы (и вообще любому, но мы все равно проверяем членство)
    - В личке: доступ есть только тем, кто состоит хотя бы в одной разрешенной группе
    """
    user_id = message_or_call.from_user.id
    chat_id = message_or_call.chat.id if isinstance(message_or_call, Message) else message_or_call.message.chat.id
    chat_type = message_or_call.chat.type if isinstance(message_or_call, Message) else message_or_call.message.chat.type

    # если пишут в группе/супергруппе — разрешаем только если это одна из allowed групп
    if chat_type in {"group", "supergroup"}:
        if not chat_is_allowed(chat_id):
            return False
        # и дополнительно: пользователь должен быть участником (актуально при "переслано" и т.п.)
        return await user_is_member_of_any_allowed_group(bot, user_id)

    # если личка — только участникам allowed групп
    if chat_type == "private":
        return await user_is_member_of_any_allowed_group(bot, user_id)

    return False



@router.message(Command("go"))
async def cmd_go(message: Message, bot: Bot):
    ok = await ensure_allowed_context(message, bot)
    if not ok:
        await message.answer("Нет доступа.")
        return

    try:
        dirs, files = await yd_list(MATERIALS_PATH)

        # если есть link_*.txt — шлем ссылки отдельными сообщениями и не отправляем эти txt документами
        files = await send_link_files(message, files)
    except Exception as e:
        await message.answer(f"Я.Диск ошибка: {e}")
        return

    # сначала отправляем файлы из текущей папки
    for f in files:
        try:
            url = await yd_download_url(f["path"])
            await message.answer_document(URLInputFile(url, filename=f["name"]))
            await asyncio.sleep(0.3)
        except Exception as e:
            await message.answer(f"Не смог отправить {f['name']}: {e}")

    # затем показываем папки кнопками
    if dirs:
        dirs = await filter_nonempty_dirs(dirs)
        if dirs:
            await message.answer("materials:", reply_markup=kb_for_dirs(MATERIALS_PATH, dirs))
        else:
            await message.answer("В /materials нет папок")


# /id command handler
@router.message(Command("id"))
async def cmd_id(message: Message):
    await message.reply(str(message.chat.id))


@router.message(F.text)
async def on_cc(message: Message, bot: Bot):
    if not message.text:
        return

    # STM 567, STM 14Н, STM 14Р (русские буквы тоже)
    m = re.match(r"^\s*stm\s+([0-9]+[A-Za-zА-Яа-яЁё]*)\s*$", message.text, flags=re.IGNORECASE)
    if m:
        ok = await ensure_allowed_context(message, bot)
        if not ok:
            return
        num = m.group(1).strip()
        try:
            dmx = await lookup_dmx_by_socket_number(num)
            if dmx:
                await message.answer(f"Номер канала {num} - DMX адрес - {dmx}")
            else:
                await message.answer(f"Номер канала {num} - DMX адрес - не найден")
        except Exception as e:
            await message.answer(f"Ошибка Google Sheets: {e}")
        return

    if message.text.strip().lower() != "чч":
        return

    # только bestuser
    if not is_bestuser(message.from_user):
        return

    # доступ по тем же правилам (группа из списка или личка участника)
    ok = await ensure_allowed_context(message, bot)
    if not ok:
        return

    quote = await fetch_quote_ru()

    # всегда в ЛС
    try:
        await bot.send_message(chat_id=message.from_user.id, text=quote)
    except TelegramForbiddenError:
        # пользователь не открыл ЛС с ботом
        try:
            await message.answer("Открой ЛС с ботом и нажми /start")
        except Exception:
            pass
    except Exception:
        pass


@router.callback_query(F.data.startswith("nav:"))
async def on_nav(call: CallbackQuery, bot: Bot):
    ok = await ensure_allowed_context(call, bot)
    if not ok:
        await call.answer("Нет доступа", show_alert=True)
        return

    key = call.data.split(":", 1)[1]
    path = get_cached_path(key)

    # важно: отвечаем на callback сразу, иначе Telegram протухнет пока качаем
    await call.answer("Загружаю…")

    try:
        dirs, files = await yd_list(path)
    except Exception as e:
        await call.message.answer(f"Ошибка: {e}")
        await call.answer("Я.Диск ошибка", show_alert=True)
        return

    if dirs:
        dirs = await filter_nonempty_dirs(dirs)

    # если есть link_*.txt — шлем ссылки отдельными сообщениями и не отправляем эти txt документами
    files = await send_link_files(call.message, files)

    # UI: обновляем сообщение-меню (папки)
    title = "materials" if path.rstrip("/") == MATERIALS_PATH.rstrip("/") else path.split("/")[-1]
    if dirs:
        try:
            await call.message.edit_text(f"{title}:", reply_markup=kb_for_dirs(path, dirs))
        except TelegramBadRequest:
            # если сообщение уже нельзя редактировать — просто отправим новое
            await call.message.answer(f"{title}", reply_markup=kb_for_dirs(path, dirs))
    else:
        # если папок нет — все равно показываем "назад", если мы не в корне
        kb = kb_for_dirs(path, [])
        try:
            await call.message.edit_text(f"{title}", reply_markup=kb)
        except TelegramBadRequest:
            await call.message.answer(f"{title}", reply_markup=kb)

    # Данные: отправляем файлы из текущей папки
    if not files:
        return

    await call.message.answer("Отправляю файлы…")
    for f in files:
        try:
            url = await yd_download_url(f["path"])
            await call.message.answer_document(URLInputFile(url, filename=f["name"]))
            await asyncio.sleep(0.3)
        except Exception as e:
            await call.message.answer(f"Не смог отправить {f['name']}: {e}")


async def main():
    if not BOT_TOKEN or not YANDEX_TOKEN:
        raise SystemExit("Нужны BOT_TOKEN и YANDEX_TOKEN в .env")

    if not ALLOWED_GROUP_IDS:
        raise SystemExit("Нужен ALLOWED_GROUP_IDS в .env (через запятую)")

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())