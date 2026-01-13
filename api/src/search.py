from playwright.async_api import async_playwright


async def run(context, query):
    page = await context.new_page()
    results = []

    await page.goto(
        f"https://letterboxd.com/search/films/{query}",
        wait_until="domcontentloaded",
    )
    items = page.locator(".search-result")
    await items.first.wait_for(state="visible")
    count = await items.count()

    for i in range(count):
        data = items.nth(i).locator("article > div").first
        await data.wait_for(state="visible")
        title = await data.get_attribute("data-item-name")
        film_id = await data.get_attribute("data-item-link")
        poster = data.locator("div > img")
        poster_film = None

        if poster:
            poster_film = await poster.get_attribute("srcset")

        results.append(
            {
                "title": title.strip() if title else None,
                "film_id": film_id,
                "poster": poster_film,
            }
        )

    return results


async def get_film_by_name(query):
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch()
        context = await browser.new_context()
        result = await run(context, query)
        await browser.close()
        return result
