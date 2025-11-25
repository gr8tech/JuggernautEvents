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

app = FastAPI()
playwright = None
browser = None
context = None

sticky_port_lock = asyncio.Lock()
sticky_port = 10000

WORKER_COUNT = 20
SAVE_URL = "http://dev.bonomalim.com:5678/webhook/save_event_html"


RABBITMQ_DEFAULT_USER=os.environ.get('RABBITMQ_DEFAULT_USER', None)
RABBITMQ_DEFAULT_PASS=os.environ.get('RABBITMQ_DEFAULT_PASS', None)
RABBITMQ_HOST=os.environ.get('RABBITMQ_HOST', None)
PROXY_USER=os.environ.get('PROXY_USER', None) 
PROXY_PASS=os.environ.get('PROXY_PASS', None) 
SAVE_USER=os.environ.get('SAVE_USER', None) 
SAVE_PASS=os.environ.get('SAVE_PASS', None)

if (not RABBITMQ_DEFAULT_USER or not RABBITMQ_DEFAULT_PASS or not RABBITMQ_HOST or not PROXY_USER or not PROXY_PASS or not SAVE_USER or not SAVE_PASS):  
    print("❌ Missing one or more required environment variables:")
    print("   - RABBITMQ_DEFAULT_USER")
    print("   - RABBITMQ_DEFAULT_PASS")
    print("   - RABBITMQ_HOST")
    print("   - PROXY_USER")
    print("   - PROXY_PASS")
    print("   - SAVE_USER")
    print("   - SAVE_PASS")
    exit(1)


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


async def block_unwanted(route, request):
    blocked_types = ["image", "media", "video", "font"]
    if request.resource_type in blocked_types:
        await route.abort()
    else:
        await route.continue_()

async def process_message(message, context):
    try:
        data = json.loads(message.body.decode())
        event_id = data.get('events_id', None)
        event_url = data.get('event_url', None)
        if not event_id or not event_url:
            print("Invalid message data")
            await message.ack()
            return
        
        print(f"Worker processing event_id: {event_id}, event_url: {event_url}")
        page = await context.new_page()
        try:
            decoded_url = urllib.parse.unquote(event_url)
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
        finally:
            await message.ack()
            try:
                await page.close()
            except Exception as ex:
                print(ex)
    except Exception as e:
        print(f"Error parsing message: {e}")
        await message.ack()
        return {"event_id": event_id, "error": str(e)}

async def  worker(browser):
    global sticky_port

    connection = await aio_pika.connect_robust(f"amqp://{RABBITMQ_DEFAULT_USER}:{RABBITMQ_DEFAULT_PASS}@{RABBITMQ_HOST}/%2F")
    channel = await connection.channel()

    queue = await channel.declare_queue("get_html", durable=True)

    context = None
    async with sticky_port_lock:
        context = await browser.new_context(ignore_https_errors=True,
                                                proxy={
                                                    "server": f"premium.residential.proxyrack.net:{sticky_port}",
                                                    "username": f"{PROXY_USER}",
                                                    "password": f"{PROXY_PASS}"
                                                    }
                                                )
        sticky_port += 1
    if not context:
        print("Failed to create context with proxy")
        return

    print(f"Worker {sticky_port-1} starting")
    while True:
        message = await get_one_message(channel, queue)
        if message is None:
            # no messages → pause worker
            await asyncio.sleep(0.5)
            continue

        extracted_data = await process_message(message, context)
        if extracted_data:
            print(f"Worker {sticky_port-1} processed message, saving data...")
            save_url = SAVE_URL
            try:
                save_response = requests.post(save_url, 
                    auth=(SAVE_USER, SAVE_PASS),
                    json=extracted_data,
                    timeout=30)
                if save_response.status_code == 200:
                    print(f"Worker {sticky_port-1} successfully saved event_id: {extracted_data.get('event_id')}")
                else:
                    print(f"Worker {sticky_port-1} failed to save event_id: {extracted_data.get('event_id')}, status: {save_response.status_code}")
            except Exception as e:
                print(f"Worker {sticky_port-1} error saving event_id: {extracted_data.get('event_id')}, error: {e}")

async def start():
    playwright = await Stealth().use_async(async_playwright()).manager.start()
    
    # Launch browser
    browser = await playwright.chromium.launch(headless=False,
        args=[
        "--ignore-certificate-errors",
        ])
    workers = [asyncio.create_task(worker(browser)) for _ in range(WORKER_COUNT)]

    await asyncio.gather(*workers)

    while True:
        await asyncio.sleep(1)  

asyncio.run(start())
