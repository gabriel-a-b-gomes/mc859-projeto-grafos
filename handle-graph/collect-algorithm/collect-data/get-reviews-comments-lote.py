import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import time

MAX_CONCURRENT_TASKS = 15 

async def scrape_page(context, url):
    page = await context.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff}", lambda route: route.abort())
    
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        await page.wait_for_load_state("networkidle") 

        last_height = 0
        while True:
            new_height = await page.evaluate("""() => {
                return document.body ? document.body.scrollHeight : 0;
            }""")
            
            if new_height == 0 or new_height == last_height:
                break
                
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1.0) 
            last_height = new_height
        
        return await page.content()
    except Exception as e:
        print(f"Erro em {url}: {e}")
        return None
    finally:
        await page.close()

async def process_user(steam_id, browser_instance, semaphore):
    async with semaphore:
        start_time = time.time()
        context = await browser_instance.new_context()
        
        url_reviews = f"https://steamcommunity.com/profiles/{steam_id}/reviews/"
        url_comments = f"https://steamcommunity.com/profiles/{steam_id}/allcomments/"
        
        html_reviews, html_comments = await asyncio.gather(
            scrape_page(context, url_reviews),
            scrape_page(context, url_comments)
        )
        
        reviews_data = parse_reviews(html_reviews) if html_reviews else []
        comments_data = parse_comments(html_comments) if html_comments else []
        
        await context.close()
        
        duration = time.time() - start_time
        print(f"--- Finalizado: {steam_id} em {duration:.2f}s")
        
        return {"id": steam_id, "reviews": reviews_data, "comments": comments_data}

def parse_reviews(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = []
    review_cards = soup.find_all('div', class_='review_box')
    for card in review_cards:
        link_el = card.select_one('a[href*="/app/"]') 
        app_id = link_el['href'].rstrip('/').split('/')[-1] if link_el else "Unknown"
        date_el = card.find('div', class_='posted')
        text_el = card.find('div', class_='content')
        items.append({
            "game": app_id,
            "date": date_el.get_text(strip=True) if date_el else "Unknown Date",
            "review": text_el.get_text(strip=True) if text_el else "No Text",
            "judgement": "Recommend" if "thumbsUp" in str(card) else "Not Recommend"
        })
    return items

def parse_comments(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = []
    comment_blocks = soup.find_all('div', class_='commentthread_comment')
    for comm in comment_blocks:
        items.append({
            "author": comm.find('bdi').get_text(strip=True) if comm.find('bdi') else "Unknown",
            "date": comm.find('span', class_='commentthread_comment_timestamp').get_text(strip=True),
            "comment": comm.find('div', class_='commentthread_comment_text').get_text(strip=True)
        })
    return items

async def run_batch(ids):
    async with async_playwright() as p:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

        browser = await p.chromium.launch(headless=True)
        tasks = [process_user(uid, browser, semaphore) for uid in ids]
        results = await asyncio.gather(*tasks)
        await browser.close()
        return results

if __name__ == "__main__":
    lista_ids = []
    chunk_size = 100
    
    print(f"Iniciando coleta de {len(lista_ids)} usuários...")

    for i in range(0, len(lista_ids), chunk_size):
        batch = lista_ids[i:i+chunk_size]
        print(f"\n=== PROCESSANDO LOTE {i//chunk_size + 1} ===")
        
        batch_results = asyncio.run(run_batch(batch))
        
        with open("steam_data_lote_2.jsonl", "a", encoding="utf-8") as f:
            for user_entry in batch_results:
                f.write(json.dumps(user_entry, ensure_ascii=False) + "\n")