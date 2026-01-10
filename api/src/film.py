import asyncio
import json
import re
import time

import aiohttp
from bs4 import BeautifulSoup


async def fetch_html(session, url):
    try:
        async with session.get(url, timeout=60) as response:
            if response.status == 200:
                return await response.text()
            return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def extract_text(element):
    """Helper untuk mengambil teks bersih jika elemen ditemukan."""
    return element.get_text(strip=True) if element else None


def parse_film_data(html, film_id):
    soup = BeautifulSoup(html, "html.parser")
    data = {
        "id": film_id,
        "name": None,
        "year": None,
        "director": None,
        "tagline": None,
        "synopsis": None,
        "poster": None,
        "casts": None,
        "genres": None,
        "themes": None,
        "duration": None,
    }

    details_head = soup.select_one(".details")
    if details_head:
        data["name"] = extract_text(details_head.select_one("h1"))

    data["year"] = extract_text(soup.select_one(".releasedate"))
    data["director"] = extract_text(soup.select_one(".contributor"))
    data["tagline"] = extract_text(soup.select_one(".tagline"))
    data["synopsis"] = extract_text(soup.select_one(".truncate"))

    cast_links = soup.select(".cast-list .text-slug")
    if cast_links:
        casts = [c.get_text(strip=True) for c in cast_links]
        data["casts"] = ", ".join(casts)

    genre_lists = soup.select("#tab-genres .text-sluglist")

    if len(genre_lists) > 0:
        g_links = genre_lists[0].select("a")
        data["genres"] = ", ".join([l.get_text(strip=True) for l in g_links])

    if len(genre_lists) > 1:
        t_links = genre_lists[1].select("a")
        data["themes"] = ", ".join([l.get_text(strip=True) for l in t_links])

    if not data["poster"]:
        try:
            script_tag = soup.find("script", type="application/ld+json")
            if script_tag:
                json_content = script_tag.string.replace("/* <![CDATA[ */", "").replace(
                    "/* ]]> */", ""
                )
                ld_data = json.loads(json_content)
                if "image" in ld_data:
                    data["poster"] = ld_data["image"]
        except Exception:
            pass

    try:
        dur_el = soup.select_one(".text-footer")
        if dur_el:
            dur_text = dur_el.get_text(strip=True)
            match = re.search(r"(\d+)\s+mins", dur_text)
            if match:
                data["duration"] = match.group(1)
            else:
                for p in dur_text.split():
                    if p.isdigit():
                        data["duration"] = p
                        break
    except Exception:
        pass

    return data


async def get_film_by_id(film_id):
    url = f"https://letterboxd.com{film_id}"
    start_time = time.time()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        html = await fetch_html(session, url)

        if not html:
            return None

        data = parse_film_data(html, film_id)
        return data
