import random
import re
from typing import Optional

import aiohttp
from selectolax.parser import HTMLParser

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


def convert_stars_to_number(star_str: Optional[str]) -> Optional[float]:
    if not star_str:
        return None
    return star_str.count("★") + star_str.count("½") * 0.5


def clean_film_url(raw_href: Optional[str]) -> Optional[str]:
    if not raw_href or "/film/" not in raw_href:
        return None
    return raw_href[raw_href.find("/film/") :]


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
    rows = tree.css(".griditem")

    for row in rows:
        component = row.css_first(".react-component")
        viewing_data = row.css_first(".poster-viewingdata")
        attrs = component.attributes
        film_a = attrs.get("data-item-link")
        rating_el = viewing_data.css_first(".rating")
        like_icon = bool(viewing_data.css_first(".icon-liked"))

        yield {
            "film_href": film_a,
            "rating": rating_el.text(strip=True) if rating_el else None,
            "liked": like_icon,
        }


async def scrape_user(session, user_id: str, page):
    datas = []

    diary_url = f"https://letterboxd.com{user_id}films/page/{page}/"

    html = await fetch(session, diary_url)

    if not html:
        return {}

    for entry in parse_diary(html):
        film_id = clean_film_url(entry["film_href"])
        if not film_id:
            continue

        data = {
            "user_id": user_id,
            "film_id": film_id,
            "rating": convert_stars_to_number(entry["rating"]),
            "liked": entry["liked"],
        }
        datas.append(data)
    return datas


async def get_user_diary_page(session: aiohttp.ClientSession, user_id: str, page: int):
    formatted_uid = f"/{user_id}/"
    return await scrape_user(session, formatted_uid, page)
