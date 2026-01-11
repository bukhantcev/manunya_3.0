#main.py
import os
import uuid
import time
import re
import asyncio
import aiohttp
import ssl
import certifi

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
from aiogram.exceptions import TelegramBadRequest

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
YANDEX_TOKEN = os.getenv("YANDEX_TOKEN")

# Разрешенные группы: "-1001..., -1002..."
ALLOWED_GROUP_IDS = [
    int(x.strip()) for x in os.getenv("ALLOWED_GROUP_IDS", "").split(",") if x.strip()
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