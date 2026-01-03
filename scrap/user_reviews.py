import asyncio
import csv
import os
import random
import re
import sqlite3
from typing import Optional

import aiohttp
from selectolax.parser import HTMLParser

DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "database2.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


def init_schema():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_reviews (
            user_id TEXT NOT NULL,
            film_id TEXT NOT NULL,
            rating REAL,
            liked INTEGER,
            review TEXT,
            PRIMARY KEY (user_id, film_id)
        )
        """
    )
    conn.commit()
    conn.close()


def convert_stars_to_number(star_str: Optional[str]) -> Optional[float]:
    if not star_str:
        return None
    return star_str.count("★") + star_str.count("½") * 0.5


def clean_film_url(raw_href: Optional[str]) -> Optional[str]:
    if not raw_href or "/film/" not in raw_href:
        return None
    return raw_href[raw_href.find("/film/") :]


def sample_pages(max_page: int) -> list[int]:
    pages = set()
    zones = [
        (1, min(5, max_page), 2),
        (6, min(30, max_page), 5),
        (31, max_page, 2),
    ]
    for start, end, k in zones:
        if start <= end:
            pages |= set(random.sample(range(start, end + 1), min(k, end - start + 1)))
    return list(pages)


async def fetch(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return None
            return await r.text()
    except Exception:
        return None


def parse_diary(html: str):
    tree = HTMLParser(html)
    rows = tree.css(".diary-entry-row")

    for row in rows:
        film_a = row.css_first(".name a")
        rating_el = row.css_first(".rating")

        yield {
            "film_href": film_a.attributes.get("href") if film_a else None,
            "rating": rating_el.text(strip=True) if rating_el else None,
            "liked": bool(row.css_first(".icon-liked")),
            "reviewed": bool(row.css_first(".icon-review")),
        }


def parse_review(html: str) -> Optional[str]:
    tree = HTMLParser(html)
    body = tree.css_first(".js-review-body")
    if not body:
        return None

    text = body.text()
    return re.sub(r"^\s+", "", text)


async def user_worker(name, queue_users, session, db_queue, sem):
    while True:
        try:
            uid = queue_users.get_nowait()
        except asyncio.QueueEmpty:
            return

        print(f"[worker {name}] user {uid}")
        await scrape_user(session, db_queue, uid, sem)
        queue_users.task_done()


async def db_writer(queue: asyncio.Queue):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    counter = 0

    while True:
        item = await queue.get()
        if item is None:
            break
        print(f"[db_writer] Inserting {item['user_id']} | {item['film_id']}")

        conn.execute(
            """
            INSERT OR IGNORE INTO user_reviews
            (user_id, film_id, rating, liked, review)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                item["user_id"],
                item["film_id"],
                item["rating"],
                int(item["liked"]),
                item["review"],
            ),
        )
        counter += 1
        if counter % 100 == 0:
            conn.commit()

        queue.task_done()

    conn.commit()
    conn.close()


async def scrape_user(session, queue, user_id: str, sem: asyncio.Semaphore):
    page = 1

    while True:
        diary_url = f"https://letterboxd.com{user_id}diary/films/page/{page}/"

        async with sem:
            html = await fetch(session, diary_url)

        if not html:
            continue

        tree = HTMLParser(html)
        if tree.css_first(".diary-entry-row") is None:
            print("No more entries.")
            break

        for entry in parse_diary(html):
            film_id = clean_film_url(entry["film_href"])
            if not film_id:
                continue

            review_text = None

            if entry["reviewed"]:
                review_url = f"https://letterboxd.com{entry['film_href']}"
                async with sem:
                    review_html = await fetch(session, review_url)
                if review_html:
                    review_text = parse_review(review_html)

            await queue.put(
                {
                    "user_id": user_id,
                    "film_id": film_id,
                    "rating": convert_stars_to_number(entry["rating"]),
                    "liked": entry["liked"],
                    "review": review_text,
                }
            )
        page += 1


def load_user_ids(csv_path: str) -> list[str]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return [r["user_id"] for r in csv.DictReader(f) if r["user_id"]]


async def run():
    os.makedirs(DATA_DIR, exist_ok=True)
    init_schema()

    user_ids = load_user_ids(os.path.join(DATA_DIR, "users_clone.csv"))

    sem = asyncio.Semaphore(20)  # HTTP concurrency
    db_queue = asyncio.Queue(maxsize=1000)
    user_queue = asyncio.Queue()

    for uid in user_ids:
        user_queue.put_nowait(uid)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        db_task = asyncio.create_task(db_writer(db_queue))

        workers = [
            asyncio.create_task(user_worker(i, user_queue, session, db_queue, sem))
            for i in range(5)
        ]

        await asyncio.gather(*workers)
        await db_queue.join()

        await db_queue.put(None)
        await db_task


if __name__ == "__main__":
    asyncio.run(run())
