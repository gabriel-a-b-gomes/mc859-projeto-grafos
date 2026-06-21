import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import time

async def scrape_steam_data(steam_id, mode="reviews"):
  async with async_playwright() as p:
    browser = await p.chromium.launch(headless=True)
    page = await browser.new_page()
    
    if mode == "reviews":
      url = f"https://steamcommunity.com/profiles/{steam_id}/reviews/"
    else:
      url = f"https://steamcommunity.com/profiles/{steam_id}/allcomments/"
        
    await page.goto(url)

    last_height = await page.evaluate("document.body.scrollHeight")
    
    print(f"Scrolling to fetch {mode}...")
    while True:
      await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
      
      await asyncio.sleep(2) 
      
      new_height = await page.evaluate("document.body.scrollHeight")
      if new_height == last_height:
        break
      last_height = new_height

    content = await page.content()
    await browser.close()
    
    soup = BeautifulSoup(content, 'html.parser')
    items = []

    if mode == "reviews":
      review_cards = soup.find_all('div', class_='review_box')
      for card in review_cards:
        link_el = card.select_one('a[href*="/app/"]') 
        
        app_id = "Unknown"
        if link_el and 'href' in link_el.attrs:
          url_parts = link_el['href'].rstrip('/').split('/')
          app_id = url_parts[-1]

        date_el = card.find('div', class_='posted')
        text_el = card.find('div', class_='content')
        
        items.append({
          "game": app_id,
          "date": date_el.get_text(strip=True) if date_el else "Unknown Date",
          "review": text_el.get_text(strip=True) if text_el else "No Text",
          "judgement": "Recommend" if "thumbsUp" in str(card) else "Not Recommend"
        })
    else:
        comment_blocks = soup.find_all('div', class_='commentthread_comment')
        for comm in comment_blocks:
            items.append({
              "author": comm.find('bdi').get_text(strip=True) if comm.find('bdi') else "Unknown",
              "date": comm.find('span', class_='commentthread_comment_timestamp').get_text(strip=True),
              "comment": comm.find('div', class_='commentthread_comment_text').get_text(strip=True)
            })

    return items

def get_reviews_comments(ids):
  data = {}
  
  for id in ids:
    print(f"Starting collection for ID: {id}") 
    reviews = asyncio.run(scrape_steam_data(id, mode="reviews"))

    comments = asyncio.run(scrape_steam_data(id, mode="comments"))

    data[id] = { "reviews": reviews, "comments": comments }

    print(f"Finished {id}. Waiting before next user...")

  return data

data = get_reviews_comments(["76561198320338168", "76561198802351715"])

with open("steam_data.json", "w", encoding="utf-8") as f:
  json.dump(data, f, ensure_ascii=False, indent=4)

print("Data saved to steam_data.json")