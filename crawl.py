"""Собирает карты пользователей из seed_users.db и сохраняет их в senkuro_cards.db.

Примеры запуска:
    python crawl.py                 # первые 20 пользователей (безопасный тест)
    python crawl.py --limit 500     # 500 пользователей
    python crawl.py --retry-errors  # заново попробовать тех, у кого была ошибка

Можно остановить в любой момент через Ctrl+C: прогресс сохранён,
при следующем запуске сбор продолжится с того же места.
Пароли, Cookies и токены не используются.
"""
import argparse
import asyncio
import sqlite3
import sys
import time
from datetime import datetime, timezone

import httpx

API_URL = "https://api.senkuro.com/graphql"
QUERY_HASH = "e426e266f581825cf223ad2d30e133e95404113093576629481fdda992d47016"
HEADERS = {
    "content-type": "application/json",
    "accept": "application/graphql-response+json, application/json",
    "app-id": "4026531840100",
    "app-version": "171026",
    "origin": "https://senkuro.com",
    "referer": "https://senkuro.com/",
}
SEARCH_HASH = "43cdedb6f6d2c0bba04815c4e6822558c37f655976cb6bc722361f71905bf5fd"
PAGE_LIMIT = 10  # столько результатов отдаёт поиск за один запрос
ALPHABET = list("абвгдеёжзийклмнопрстуфхцчшщъыьэюя") + list("abcdefghijklmnopqrstuvwxyz") + list("0123456789")
SEED_DB = "seed_users.db"
CARDS_DB = "senkuro_cards.db"
MAX_RETRIES = 5
MIN_DELAY = 0.05  # абсолютный минимум
FAST_BELOW = 0.25  # быстрее этого — «быстрый режим»: любой 429 или 403 сразу останавливает сбор
CURRENT_DELAY = 0.5
LAST_SHOWN = None
SAFE_DELAY = 1.0  # после ответа 429 пауза возвращается к этому значению
THROTTLED = False
MAX_CONSECUTIVE_FAILS = 3  # после стольких подряд неудачных пользователей сбор останавливается


class CrawlError(Exception):
    pass


class StopCrawl(Exception):
    """Сервер просит остановиться или закрыл доступ: сбор прекращается, пользователь остаётся в очереди."""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def live_delay(default: float) -> float:
    """Если рядом лежит delay.txt с числом (например 0.1), берём паузу из него. Можно менять во время сбора."""
    global LAST_SHOWN
    d = default
    try:
        with open("delay.txt", encoding="utf-8") as f:
            d = float(f.read().strip().replace(",", "."))
    except (OSError, ValueError):
        pass
    d = max(d, MIN_DELAY)
    if d != LAST_SHOWN:
        LAST_SHOWN = d
        print(f"  [пауза между запросами: {d} с]")
    return d


class RateLimiter:
    """Общий светофор для всех потоков: запросы стартуют не чаще, чем раз в `delay` секунд."""

    def __init__(self, default_delay: float):
        self.default = default_delay
        self.next_slot = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        global CURRENT_DELAY
        async with self.lock:
            d = live_delay(self.default)
            if THROTTLED:
                d = max(d, SAFE_DELAY)
            CURRENT_DELAY = d
            loop_now = asyncio.get_running_loop().time()
            start = max(loop_now, self.next_slot)
            self.next_slot = start + d
        if start > loop_now:
            await asyncio.sleep(start - loop_now)


def init_cards_db(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS cards (
            card_id     TEXT PRIMARY KEY,
            slug        TEXT,
            title_ru    TEXT,
            title_en    TEXT,
            search_text TEXT,   -- все названия в нижнем регистре, для поиска
            rank        TEXT,
            explicit    INTEGER,
            animated    INTEGER
        );
        CREATE TABLE IF NOT EXISTS user_cards (
            number     TEXT NOT NULL,   -- числовой ID пользователя из ссылки на профиль
            card_id    TEXT NOT NULL,
            modifier   INTEGER NOT NULL DEFAULT 0,
            shard      INTEGER NOT NULL DEFAULT 0,
            quantity   INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (number, card_id, modifier, shard)
        );
        CREATE INDEX IF NOT EXISTS idx_user_cards_card ON user_cards(card_id);
        CREATE INDEX IF NOT EXISTS idx_cards_rank ON cards(rank);
        """
    )


def init_seed_db(db: sqlite3.Connection) -> None:
    cols = {r[1] for r in db.execute("PRAGMA table_info(seed_users)")}
    if "error" not in cols:
        db.execute("ALTER TABLE seed_users ADD COLUMN error TEXT")
    if "cards_count" not in cols:
        db.execute("ALTER TABLE seed_users ADD COLUMN cards_count INTEGER")
    if "chunk_id" not in cols:    # кусок, выданный помощнику: такие записи свой сбор не трогает
        db.execute("ALTER TABLE seed_users ADD COLUMN chunk_id TEXT")
    if "leased_at" not in cols:
        db.execute("ALTER TABLE seed_users ADD COLUMN leased_at TEXT")
    db.commit()


def init_search_queue(db: sqlite3.Connection, discover: bool) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS search_queue (
        query TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending',
        found INTEGER, new_users INTEGER, checked_at TEXT)""")
    qcols = {r[1] for r in db.execute("PRAGMA table_info(search_queue)")}
    if "chunk_id" not in qcols:
        db.execute("ALTER TABLE search_queue ADD COLUMN chunk_id TEXT")
    if "leased_at" not in qcols:
        db.execute("ALTER TABLE search_queue ADD COLUMN leased_at TEXT")
    if discover and not db.execute("SELECT 1 FROM search_queue LIMIT 1").fetchone():
        db.executemany("INSERT OR IGNORE INTO search_queue (query) VALUES (?)",
                       [(a + b,) for a in ALPHABET for b in ALPHABET])
    db.commit()


async def post_with_retry(client, limiter: RateLimiter, payload: dict) -> dict:
    global THROTTLED
    last = "нет ответа"
    for attempt in range(MAX_RETRIES):
        await limiter.wait()
        try:
            resp = await client.post(API_URL, headers=HEADERS, json=payload)
        except httpx.HTTPError as e:
            last = f"сетевая ошибка: {e}"
        else:
            if resp.status_code == 403:
                raise StopCrawl("HTTP 403: доступ закрыт. Остановлено.")
            if resp.status_code == 429 and CURRENT_DELAY < FAST_BELOW:
                raise StopCrawl("HTTP 429 на быстрой скорости: сервер просит реже. Остановлено; "
                                "увеличь паузу (delay.txt или --delay) и запусти снова через час-два.")
            if resp.status_code in (429, 500, 502, 503, 504):
                last = f"HTTP {resp.status_code}"
                if resp.status_code == 429 and not THROTTLED:
                    THROTTLED = True
                    print("Сервер просит реже: дальше пауза не меньше 1 секунды.")
                retry_after = resp.headers.get("retry-after", "")
                if retry_after.isdigit():
                    await asyncio.sleep(min(int(retry_after), 120))
                    continue
            elif resp.is_error:
                raise CrawlError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            else:
                try:
                    return resp.json()
                except ValueError:
                    raise CrawlError(f"ответ не JSON: {resp.text[:200]}")
        await asyncio.sleep(min(2 ** attempt * 2, 60))
    raise CrawlError(f"не удалось после {MAX_RETRIES} попыток ({last})")


async def search_users(client, limiter: "RateLimiter", query: str) -> list[dict]:
    payload = {"operationName": "search", "variables": {"query": query, "type": "USER"},
               "extensions": {"persistedQuery": {"sha256Hash": SEARCH_HASH, "version": 1}}}
    data = await post_with_retry(client, limiter, payload)
    if "errors" in data:
        raise CrawlError(f"ошибка API: {str(data['errors'])[:200]}")
    result = (data.get("data") or {}).get("search")
    if result is None:
        raise CrawlError("в ответе нет поля search")
    return [e["node"] for e in result.get("edges", []) if e.get("node")]


def add_users(db: sqlite3.Connection, nodes: list[dict]) -> int:
    new = 0
    for n in nodes:
        number = str(n.get("slug") or "").lstrip("@")
        if not number.isdigit() or not n.get("id") or n.get("bot"):
            continue
        new += db.execute("INSERT OR IGNORE INTO seed_users (number, user_id, name) VALUES (?,?,?)",
                          (number, n["id"], n.get("name"))).rowcount
    return new


def build_payload(user_id: str, after):
    return {
        "operationName": "fetchUserCards",
        "variables": {
            "after": after,
            "animated": None,
            "color": {"include": []},
            "explicit": None,
            "modifier": {"exclude": []},
            "orderDirection": "DESC",
            "orderField": "CREATED_AT",
            "rank": {"exclude": [], "include": []},
            "userId": user_id,
        },
        "extensions": {"persistedQuery": {"sha256Hash": QUERY_HASH, "version": 1}},
    }


async def fetch_all_cards(client, limiter: RateLimiter, user_id: str) -> list[dict]:
    """Возвращает все карты пользователя (все страницы)."""
    nodes, after = [], None
    while True:
        data = await post_with_retry(client, limiter, build_payload(user_id, after))
        if "errors" in data:
            raise CrawlError(f"ошибка API: {str(data['errors'])[:200]}")
        result = (data.get("data") or {}).get("userCards")
        if result is None:
            raise CrawlError("в ответе нет поля userCards")
        nodes.extend(e["node"] for e in result.get("edges", []))
        page = result.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            return nodes
        after = page.get("endCursor")
        if not after:
            raise CrawlError("hasNextPage=true, но нет endCursor")


def titles_of(card: dict):
    ru = en = None
    all_titles = []
    for t in card.get("titles") or []:
        content = (t.get("content") or "").strip()
        if not content:
            continue
        all_titles.append(content.casefold())
        if t.get("lang") == "RU" and ru is None:
            ru = content
        if t.get("lang") == "EN" and en is None:
            en = content
    return ru, en, "|".join(dict.fromkeys(all_titles))


def save_user(cards_db: sqlite3.Connection, number: str, nodes: list[dict]) -> int:
    cards_db.execute("DELETE FROM user_cards WHERE number = ?", (number,))
    for node in nodes:
        card = node.get("card") or {}
        card_id = card.get("id")
        if not card_id:
            continue
        ru, en, search_text = titles_of(card)
        cards_db.execute(
            """INSERT INTO cards (card_id, slug, title_ru, title_en, search_text, rank, explicit, animated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(card_id) DO UPDATE SET
                 slug=excluded.slug, title_ru=excluded.title_ru, title_en=excluded.title_en,
                 search_text=excluded.search_text, rank=excluded.rank,
                 explicit=excluded.explicit, animated=excluded.animated""",
            (card_id, card.get("slug"), ru, en, search_text, card.get("rank"),
             int(bool(card.get("explicit"))), int(bool(card.get("animated")))),
        )
        cards_db.execute(
            """INSERT INTO user_cards (number, card_id, modifier, shard, quantity)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(number, card_id, modifier, shard)
               DO UPDATE SET quantity = quantity + excluded.quantity""",
            (number, card_id, int(node.get("modifier") or 0),
             int(bool(node.get("shard"))), int(node.get("quantity") or 1)),
        )
    cards_db.commit()
    return len(nodes)


async def crawl(limit: int, delay: float, retry_errors: bool, workers: int, client,
                discover: bool = False, max_depth: int = 3, max_queries: int = 100000,
                discover_workers: int = 2,
                seed_path: str = SEED_DB, cards_path: str = CARDS_DB) -> None:
    seed = sqlite3.connect(seed_path)
    cards_db = sqlite3.connect(cards_path)
    init_seed_db(seed)
    init_cards_db(cards_db)
    init_search_queue(seed, discover)
    if retry_errors:
        seed.execute("UPDATE seed_users SET status='pending' WHERE status='error'")
        seed.commit()

    pending = lambda: seed.execute("SELECT COUNT(*) FROM seed_users WHERE status='pending'").fetchone()[0]
    queries_left = seed.execute("SELECT COUNT(*) FROM search_queue WHERE status='pending'").fetchone()[0] if discover else 0
    print(f"Пользователей в очереди: {pending()}. Лимит запуска: {limit}. Потоков: {workers}. Пауза: {delay} с.")
    if discover:
        print(f"Поиск новых пользователей: включён (запросов в очереди: {queries_left}, "
              f"глубина до {max_depth} символов, потоков поиска: {discover_workers}).")

    limiter = RateLimiter(delay)
    in_progress: set[str] = set()
    q_in_progress: set[str] = set()
    state = {"done": 0, "fails_in_row": 0, "processed": 0, "stop": False, "workers_done": False,
             "discovering": discover, "active_discoverers": discover_workers if discover else 0,
             "queries": 0, "new_users": 0}

    def claim_user():
        rows = seed.execute("SELECT number, user_id, name FROM seed_users WHERE status='pending' AND chunk_id IS NULL "
                            "ORDER BY number LIMIT ?", (len(in_progress) + 1,)).fetchall()
        return next((r for r in rows if r[0] not in in_progress), None)

    def claim_query():
        rows = seed.execute("SELECT query FROM search_queue WHERE status='pending' AND chunk_id IS NULL "
                            "ORDER BY length(query), rowid LIMIT ?", (len(q_in_progress) + 1,)).fetchall()
        return next((r[0] for r in rows if r[0] not in q_in_progress), None)

    def register_failure() -> None:
        state["fails_in_row"] += 1
        if state["fails_in_row"] >= MAX_CONSECUTIVE_FAILS:
            print(f"\n{MAX_CONSECUTIVE_FAILS} ошибки подряд. Останавливаюсь, "
                  "чтобы не нагружать сайт. Попробуй позже.")
            state["stop"] = True

    async def worker() -> None:
        while not state["stop"]:
            if state["processed"] + len(in_progress) >= limit:
                return
            row = claim_user()
            if row is None:
                if state["discovering"]:   # поиск ещё может добавить новых пользователей
                    await asyncio.sleep(1)
                    continue
                return
            number, user_id, name = row
            in_progress.add(number)
            try:
                nodes = await fetch_all_cards(client, limiter, user_id)
                count = save_user(cards_db, number, nodes)
                seed.execute(
                    "UPDATE seed_users SET status='done', checked_at=?, error=NULL, cards_count=? "
                    "WHERE number=?", (now(), count, number))
                seed.commit()
                state["done"] += 1
                state["fails_in_row"] = 0
                state["processed"] += 1
                print(f"[{state['processed']}] {name or number}: {count} карт")
            except StopCrawl as e:
                if not state["stop"]:
                    print(f"\nСТОП - {e}")
                state["stop"] = True
                return
            except CrawlError as e:
                cards_db.rollback()
                seed.execute("UPDATE seed_users SET status='error', checked_at=?, error=? WHERE number=?",
                             (now(), str(e)[:300], number))
                seed.commit()
                state["processed"] += 1
                print(f"[{state['processed']}] {name or number}: ОШИБКА - {e}")
                register_failure()
            finally:
                in_progress.discard(number)

    async def discoverer() -> None:
        try:
            while not state["stop"] and not state["workers_done"] and state["queries"] < max_queries:
                q = claim_query()
                if q is None:
                    if q_in_progress:       # другой поток поиска ещё может добавить запросы
                        await asyncio.sleep(1)
                        continue
                    return
                q_in_progress.add(q)
                try:
                    nodes = await search_users(client, limiter, q)
                    new = add_users(seed, nodes)
                    seed.execute("UPDATE search_queue SET status='done', found=?, new_users=?, checked_at=? WHERE query=?",
                                 (len(nodes), new, now(), q))
                    if len(q) < max_depth and len(nodes) >= PAGE_LIMIT:
                        seed.executemany("INSERT OR IGNORE INTO search_queue (query) VALUES (?)",
                                         [(q + c,) for c in ALPHABET])
                    seed.commit()
                    state["queries"] += 1
                    state["new_users"] += new
                    state["fails_in_row"] = 0
                    print(f"[поиск] «{q}»: найдено {len(nodes)}, новых {new}")
                except StopCrawl as e:
                    if not state["stop"]:
                        print(f"\nСТОП - {e}")
                    state["stop"] = True
                    return
                except CrawlError as e:
                    seed.rollback()
                    seed.execute("UPDATE search_queue SET status='error', checked_at=? WHERE query=?", (now(), q))
                    seed.commit()
                    print(f"[поиск] «{q}»: ОШИБКА - {e}")
                    register_failure()
                finally:
                    q_in_progress.discard(q)
        finally:
            state["active_discoverers"] -= 1
            if state["active_discoverers"] <= 0:
                state["discovering"] = False

    async def run_workers() -> None:
        try:
            await asyncio.gather(*(worker() for _ in range(workers)))
        finally:
            if workers > 0:          # в режиме «только поиск» потоков сбора карт нет
                state["workers_done"] = True

    try:
        await asyncio.gather(run_workers(), *(discoverer() for _ in range(discover_workers if discover else 0)))
    finally:
        left = pending()
        seed.close()
        cards_db.close()
    print(f"Готово. Собрано пользователей: {state['done']}. Поисковых запросов: {state['queries']}, "
          f"новых пользователей из поиска: {state['new_users']}. Осталось в очереди: {left}.")


async def run(args) -> None:
    async with httpx.AsyncClient(timeout=30) as client:
        await crawl(args.limit, args.delay, args.retry_errors, args.workers, client,
                    args.discover, args.max_depth, args.max_queries, args.discover_workers)


def main() -> None:
    ap = argparse.ArgumentParser(description="Сбор карт пользователей Senkuro")
    ap.add_argument("--limit", type=int, default=20, help="сколько пользователей обработать (по умолчанию 20)")
    ap.add_argument("--delay", type=float, default=0.5,
                    help="пауза между запросами, секунды (не меньше 0.05; по умолчанию 0.5; "
                         "можно менять на ходу через файл delay.txt). Это общий темп для всех потоков")
    ap.add_argument("--workers", type=int, default=4, help="сколько пользователей качать одновременно (1-10, по умолчанию 4)")
    ap.add_argument("--discover", action="store_true",
                    help="параллельно искать новых пользователей через поиск и сразу собирать их карты")
    ap.add_argument("--max-depth", type=int, default=3,
                    help="до скольких символов удлинять поисковый запрос, если пришло 10 результатов (2-5, по умолчанию 3)")
    ap.add_argument("--max-queries", type=int, default=100000, help="потолок поисковых запросов за запуск")
    ap.add_argument("--discover-workers", type=int, default=2, help="потоков поиска (1-3, по умолчанию 2)")
    ap.add_argument("--only-discover", action="store_true",
                    help="только искать новых пользователей, карты не собирать")
    ap.add_argument("--retry-errors", action="store_true", help="повторить пользователей с ошибкой")
    args = ap.parse_args()
    if args.delay < MIN_DELAY:
        sys.exit("Пауза меньше 0.05 секунды не разрешена.")
    if args.only_discover:
        args.discover = True
        args.workers = 0
    elif not 1 <= args.workers <= 10:
        sys.exit("--workers должен быть от 1 до 10.")
    if not 2 <= args.max_depth <= 5:
        sys.exit("--max-depth должен быть от 2 до 5.")
    if not 1 <= args.discover_workers <= 3:
        sys.exit("--discover-workers должен быть от 1 до 3.")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nОстановлено вручную. Прогресс сохранён (недокачанные пользователи останутся в очереди).")


if __name__ == "__main__":
    main()
