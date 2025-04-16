import asyncio
import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from db import save_to_db
import time
import os

BASE_URL = "https://torgi.eltorg.org/admin/TendProcUserSRO2.aspx?page={page}&db=&region=&search=&sortType=1&sortDir=desc&adv=0&buy=on&sell=on&getReq=on&endReq=on&endProc=on&open=on&close=on&cancell=on&invite="

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

DB_CONFIG = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "qwerty"),
    "database": os.getenv("DB_NAME", "torgi_lots"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
}


TABLE_NAME = "lots"
MAX_CONCURRENT_REQUESTS = 4

sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


async def fetch_page(session, page):
    url = BASE_URL.format(page=page)
    async with sem:
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                return await response.text()
            return ""


def parse_lots(html):
    soup = BeautifulSoup(html, "html.parser")
    lots = soup.find_all("div", class_="main-info")

    result = []
    for lot in lots:
        title_tag = lot.find("h2")
        title = title_tag.get_text(strip=True) if title_tag else "Без названия"

        price_tag = lot.find("span", class_="price-initial")
        if price_tag:
            price = price_tag.get_text(strip=True).replace("Начальная цена:", "").strip().replace("\xa0", " ")
        else:
            price = "Цена не указана"

        result.append({"title": title, "price": price})
    return result



def extract_visible_pages(html):
    soup = BeautifulSoup(html, "html.parser")
    pager_div = soup.find("div", class_="bottom-pager")
    pages = set()
    has_next = False

    if pager_div:
        for a_tag in pager_div.find_all("a"):
            text = a_tag.get_text(strip=True)
            href = a_tag.get("href", "")

            if text.isdigit():
                try:
                    pages.add(int(text))
                except ValueError:
                    pass
            elif "Следующая" in text:
                has_next = True

    return pages, has_next


async def worker(queue, session, pool, seen_pages):
    while True:
        page = await queue.get()
        if page in seen_pages:
            queue.task_done()
            continue

        seen_pages.add(page)
        html = await fetch_page(session, page)

        if not html:
            queue.task_done()
            continue

        lots = parse_lots(html)
        if lots:
            await save_to_db(pool, lots)

        new_pages, has_next = extract_visible_pages(html)

        for new_page in new_pages:
            if new_page not in seen_pages:
                queue.put_nowait(new_page)

        if has_next and (page + 1) not in seen_pages:
            queue.put_nowait(page + 1)

        queue.task_done()

start = time.time()

async def main():
    queue = asyncio.Queue()
    seen_pages = set()
    queue.put_nowait(1)

    async with aiohttp.ClientSession() as session, asyncpg.create_pool(**DB_CONFIG) as pool:
        workers = [
            asyncio.create_task(worker(queue, session, pool, seen_pages))
            for _ in range(MAX_CONCURRENT_REQUESTS)
        ]

        await queue.join()

        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    print("Все страницы обработаны!")



if __name__ == "__main__":
    asyncio.run(main())

    end = time.time() - start
    print(end)
