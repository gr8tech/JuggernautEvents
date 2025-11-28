import sys
from bs4 import BeautifulSoup

import asyncio
import re
from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright
from markdownify import markdownify as md
from playwright_stealth import stealth, Stealth
from queue import Queue 
import random
import pika
import aio_pika
import json
import urllib.parse
import requests
import os
import aiomysql

app = FastAPI()
playwright = None
browser = None
context = None

sticky_port = 10000

try:
    CONTEXT_COUNT = int(os.environ.get('CONTEXT_COUNT', 1))
    CONTEXT_PAGES = int(os.environ.get('CONTEXT_PAGES', 1))
except:
     CONTEXT_COUNT = 1
     CONTEXT_PAGES = 1


SAVE_URL = "http://n8n:5678/webhook/save_event_html"


RABBITMQ_DEFAULT_USER=os.environ.get('RABBITMQ_DEFAULT_USER', None)
RABBITMQ_DEFAULT_PASS=os.environ.get('RABBITMQ_DEFAULT_PASS', None)
RABBITMQ_HOST=os.environ.get('RABBITMQ_HOST', None)
PROXY_USER=os.environ.get('PROXY_USER', None) 
PROXY_PASS=os.environ.get('PROXY_PASS', None) 
SAVE_USER=os.environ.get('SAVE_USER', None) 
SAVE_PASS=os.environ.get('SAVE_PASS', None)
MYSQL_DATABASE=os.environ.get('MYSQL_DATABASE', None)
MYSQL_USER=os.environ.get('MYSQL_USER', None)
MYSQL_PASSWORD=os.environ.get('MYSQL_PASSWORD', None)
MYSQL_HOST=os.environ.get('MYSQL_HOST', None)

if (not RABBITMQ_DEFAULT_USER or not RABBITMQ_DEFAULT_PASS or not RABBITMQ_HOST or not PROXY_USER 
    or not PROXY_PASS or not SAVE_USER or not SAVE_PASS
    or not MYSQL_DATABASE or not MYSQL_USER or not MYSQL_PASSWORD
    or not MYSQL_HOST):  
    print("❌ Missing one or more required environment variables:")
    print("   - RABBITMQ_DEFAULT_USER")
    print("   - RABBITMQ_DEFAULT_PASS")
    print("   - RABBITMQ_HOST")
    print("   - PROXY_USER")
    print("   - PROXY_PASS")
    print("   - SAVE_USER")
    print("   - SAVE_PASS")
    print("   - MYSQL_DATABASE")
    print("   - MYSQL_USER")
    print("   - MYSQL_PASSWORD")
    print("   - MYSQL_HOST")
    exit(1)

pool = None

async def init_pool():
    global pool
    pool = await aiomysql.create_pool(
        host=MYSQL_HOST,
        port=3306,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DATABASE,
        minsize=1,
        maxsize=CONTEXT_COUNT*CONTEXT_PAGES
    )


def extract_visible_text(html: str):
    """Extract visible text blocks, ignoring scripts, styles, and layout noise."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove non-content tags
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "svg", "meta", "link"]):
        tag.decompose()

    # Gather text from common content elements
    blocks = []
    for tag in soup.find_all(["p", "div", "section", "article", "li", "span", "td"]):
        text = tag.get_text(" ", strip=True)
        if text:
            blocks.append(text)
    return blocks

def score_event_relevance(text: str) -> int:
    """Heuristically score how 'event-like' a text block is."""
    patterns = [
        r"\b\d{1,2}\s?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b",  # date
        r"\b\d{4}-\d{2}-\d{2}\b",  # ISO date
        r"\b\d{1,2}(:|.)\d{2}\b",  # time
        r"\b(event|concert|meeting|conference|festival|webinar|workshop)\b",  # event keywords
        r"\bvenue|location|place|address|city|hall|center|theatre|arena\b",  # location
        r"\bRSVP|tickets|register|join us\b",  # participation
    ]
    return sum(bool(re.search(p, text, re.I)) for p in patterns)

def get_relevant_sections(blocks, threshold=1):
    """Filter and return blocks that look relevant."""
    scored = [(b, score_event_relevance(b)) for b in blocks]
    relevant = [b for b, s in scored if s >= threshold]

    # Deduplicate similar lines and join
    seen = set()
    unique_relevant = []
    for line in relevant:
        key = line.lower().strip()
        if key not in seen:
            seen.add(key)
            unique_relevant.append(line)

    return "\n\n".join(unique_relevant)

def truncate_text(text: str, max_chars=5000):
    """Prevent oversize payloads."""
    return text[:max_chars]

def clean_html_for_extraction(html: str) -> str:
    """Full pipeline: HTML → clean event text"""
    blocks = extract_visible_text(html)
    relevant = get_relevant_sections(blocks)
    return truncate_text(relevant)

async def get_one_message(channel, queue):
    try:
        message = await queue.get(timeout=5)  # 5 second poll
        return message
    except aio_pika.exceptions.QueueEmpty:
        return None
    except:
        return None

async def save_content(data):
    global pool
    if pool is None:
        print("Database pool not initialized")
        return False
    event_id = data.get("event_id", None)
    markdown = data.get("markdown", None)
    html = data.get("html", None)   
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute( "UPDATE events SET event_raw_html=%s, event_raw_markdown=%s WHERE id=%s", (html, markdown, event_id))
                await conn.commit()
                return True
            except Exception as e:
                print(f"Error saving to database: {e}")
    return False

async def block_unwanted(route, request):
    blocked_types = ["image", "media", "video", "font"]
    if request.resource_type in blocked_types:
        await route.abort()
    else:
        await route.continue_()

async def process_message(message, page):
    try:
        await page.goto("about:blank")
        data = json.loads(message.body.decode())
        event_id = data.get('events_id', None)
        event_url = data.get('event_url', None)
        if not event_id or not event_url:
            print("Invalid message data")
            return

        try:
            decoded_url = urllib.parse.unquote(event_url)
            print(f"Worker processing event_id: {event_id}, event_url: {decoded_url}")
            await page.route("**/*", block_unwanted)
            await page.goto(decoded_url, timeout=120000)  # 60s timeout
            content = await page.content()
            clean_md = md(content)
            clean_md = re.sub(r'\n\s*\n+', '\n\n', clean_md).strip()
            filtered_html = clean_html_for_extraction(content)
            return {"event_id": event_id, "url": decoded_url, "markdown": clean_md, "html": content, "filtered": filtered_html}
        except Exception as e:
            print(f"Worker {sticky_port} error: {e}")
            return {"event_id": event_id, "error": str(e)}

    except Exception as e:
        print(f"Error parsing message: {e}")
        return {"event_id": event_id, "error": str(e)}
    finally:
        try:
            await message.ack()
        except Exception as ack_err:
            print(f"FAILED TO ACK MESSAGE: {ack_err}")

async def worker(page):
    global pool

    if pool is None:
        print("Database pool not initialized")
        return

    try:
        connection = await aio_pika.connect_robust(
            f"amqp://{RABBITMQ_DEFAULT_USER}:{RABBITMQ_DEFAULT_PASS}@{RABBITMQ_HOST}/%2F"
        )
        channel = await connection.channel()
        queue = await channel.declare_queue("get_html", durable=True)
    except Exception as e:
        print("RabbitMQ init failed:", e)
        return

    # MAIN LOOP
    while True:
        try:
            message = await get_one_message(channel, queue)

            if message is None:
                await asyncio.sleep(0.5)
                continue
            
            extracted_data = await process_message(message, page)

            if extracted_data:
                try:
                    print("Saving data")
                    result = await save_content(extracted_data)

                    if not result:
                        print(f"Worker {1} failed to save event_id {extracted_data.get('event_id')}")
                        continue

                    # trim
                    for key in ("filtered", "html", "markdown", "url"):
                        extracted_data.pop(key, None)

                    save_response = await asyncio.to_thread(
                        requests.post,
                        SAVE_URL,
                        auth=(SAVE_USER, SAVE_PASS),
                        json=extracted_data,
                        timeout=90,
                    )

                    if save_response.status_code != 200:
                        print(
                            f"Worker save failed: event_id={extracted_data.get('event_id')} status={save_response.status_code}"
                        )

                except Exception as e:
                    print(f"Worker save error: {e}")
                    continue

        except Exception as e:
            print(f"Worker crashed in loop: {e}")
            await asyncio.sleep(1)

async def create_contexts(browser):
    global sticky_port
    contexts = []
    for _ in range(CONTEXT_COUNT):
            port = sticky_port
            context = await browser.new_context(
                    ignore_https_errors=True,
                    proxy={
                        "server": f"premium.residential.proxyrack.net:{port}",
                        "username": PROXY_USER,
                        "password": PROXY_PASS
                    }
                )
            contexts.append(context)
            sticky_port += 1
    return contexts

async def start():
    global pool
    await init_pool()
    playwright = await Stealth().use_async(async_playwright()).manager.start()
    
    # Launch browser
    browser = await playwright.chromium.launch(headless=False,
        args=[
        "--ignore-certificate-errors",
        ])
    
    workers = []
    contexts = await create_contexts(browser)
    for _ in range(CONTEXT_PAGES):
        for ctx in contexts:
            page = await ctx.new_page()
            workers.append(asyncio.create_task(worker(page)))

    await asyncio.gather(*workers)

    if pool is not None:
        pool.close()
        await pool.wait_closed()

asyncio.run(start())
