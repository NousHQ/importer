import os
import logging
import hashlib
import json
import asyncio
import random
import aiofiles
import aiohttp
from playwright.async_api import async_playwright, Page
from config import settings

from indexer import indexer
from utils import get_weaviate_client, convert_user_id, get_supabase
# from playwright_stealth import stealth_async

# Set logging
log = logging.getLogger(__name__)

# Global maps of downloads

# Tracing parameters
tracing_enabled = False
tracing_filepath = "trace.zip"

def _limit_concurrency(coroutines, max_simultaneous):
    ''' Limit number of coroutines executing simultaneously '''
    semaphore = asyncio.Semaphore(max_simultaneous)

    async def with_concurrency_limit(coroutine):
        async with semaphore:
            return await coroutine

    return [with_concurrency_limit(coroutine) for coroutine in coroutines]


async def async_download_url_dicts(url_dict_l, log_filepath, tracing,
                                    user_id,
                                    out_dir=".",
                                    batch_size = 50,
                                    max_simultaneous_downloads=25,
                                    timeout=None,
                                    headers=None):

    async with aiofiles.open(log_filepath, 'w', encoding='utf-8') as log_fd:
        # Process input URL dictionaries on batches of given size
        # NOTE: This is done to periodically close playwright context
        #       so that the used memory is released.
        #       The downside is that until a batch ends, another cannot start
        for i in range(0, len(url_dict_l), batch_size):
            log.info("Processing URL dict batch [%d,%d]" % (i,i+batch_size-1))
            batch_l = url_dict_l[i:i+batch_size]
            async with async_playwright() as p:
                device_list = list(p.devices.keys())
                random_device = random.choice(device_list)
                device_info = p.devices[random_device]

                browser_type = p.chromium
                browser = await browser_type.launch(
                                    proxy = {
                                        "server": "https://brd.superproxy.io:22225",
                                        "username": "brd-customer-hl_3251280c-zone-data_center",
                                        "password": "03kqenast9p6"
                                        # "username": "brd-customer-hl_3251280c-zone-isp",
                                        # "password": "e786cyuj4mn0"
                                    },
                                    args=['--disable-blink-features=TrustedDOMTypes'],
                                    headless=True,
                                    downloads_path=out_dir
                                )

                context = await browser.new_context(accept_downloads=False,
                                                    # locale='en-US',
                                                    extra_http_headers=headers,
                                                    **device_info)
                # Start tracing before creating / navigating a page.
                if tracing:
                    await context.tracing.start(screenshots=False,
                                                snapshots=False,
                                                sources=True)

                download_futures = []
                for url_dict in batch_l:
                    download_futures.append(
                        async_download_url_in_context(
                            context=context,
                            url_dict=url_dict,
                            out_dir=out_dir,
                            timeout=timeout,
                        )
                    )
                for future in asyncio.as_completed(_limit_concurrency(
                        download_futures, max_simultaneous_downloads)):
                    data, pageContent = await future
                    client = get_weaviate_client()
                    supabase = get_supabase()
                    document = dict()
                    document["url"] = data.get('url')
                    document["content"] = pageContent
                    document["title"] = data.get('title')
                    # indexer(document, user_id, client)
                    response = supabase.from_("all_saved").insert(
                         {
                            "user_id": convert_user_id(user_id),
                            "url": data.get('url'),
                            "title": data.get('title')
                        }
                    ).execute()

                    log.info(f"[!] Imported {data.get('url')}")
                    line = json.dumps(data, sort_keys=True, default=str)
                    await log_fd.write("%s\n" % line)
                    await log_fd.flush()

                if tracing:
                    await context.tracing.stop(path = tracing_filepath)

                # Close
                await context.close()
                await browser.close()
                print("[!] Cleaning the browser")

async def extract_with_readability(page, webhash, out_dir):
    filename = "%s.text" % webhash
    filepath = os.path.join(out_dir, filename)
    readability_path = "node_modules/@mozilla/readability/Readability.js"
    page_text = ""
    with open(readability_path, "r") as fd:
        readability_script = fd.read()
    # Inject the Readability library into the page
    await page.evaluate(f"{readability_script}")
    # Use the Readability library in the page context
    readability_response = await page.evaluate(
        """() => {
            var documentClone = document.cloneNode(true);
            return (new Readability(documentClone)).parse();
        }"""
    )
    if readability_response:
        page_text = readability_response.get('textContent', "")
        async with aiofiles.open(filepath, "w") as download_fd:
            await download_fd.write(page_text.strip())
        
        return page_text

async def extract_raw_content(page, webhash, out_dir):
    filename = "%s.text" % webhash
    filepath = os.path.join(out_dir, filename)
    pageContent = await page.evaluate('() => document.body.innerText')
    if pageContent is None:
        return ""
    else:
        return pageContent


async def async_download_url_in_context(context,
                                        out_dir=".",
                                        get_text=True,
                                        timeout=None,
                                        url_dict=None
                                    ):
    url = url_dict.get('url')
    data = url_dict or {}
    pageContent = ""
    log.debug(f"[!] Started import: {url}")
    # Set timeout (in milliseconds)
    if timeout is not None:
        timeout=timeout*1000
    else:
        timeout=0

    # Visit URL
    max_retries = 3  # Set the maximum number of retries

    for i in range(max_retries):
        try:
            page: Page = await context.new_page()
            response = await page.goto(url, timeout=timeout, wait_until='domcontentloaded')
            # If the page loading is successful, break the loop
            break

        except asyncio.TimeoutError as e:
            log.info(u"Timeout downloading %s with exception: %s" % (url, e))
            data['download_status'] = -1
            data['error'] = f"TimeoutError: {str(e)}"

        except Exception as e:
            log.info(u"Failed to establish session %s with exception: %s" % (url, e))
            if 'response' in locals() and hasattr(response, 'status'):
                data['download_status'] = response.status
            else:
                data['download_status'] = -2
            data['error'] = f"ConnectionError: {str(e)}"
            # If this was the last retry, return the data
            if i == max_retries - 1:
                return data, pageContent

            # Otherwise, log the retry and continue the loop
            log.info(f"Retrying to establish session for {url}. Retry {i+1} of {max_retries}")
    # Check status code

    data['download_status'] = response.status
    webhash = hashlib.sha256(url.encode('utf-8')).hexdigest()
    data['download_sha256'] = webhash

    content_type = response.headers.get("content-type", None)
    if not content_type or not any(content_type.startswith(x) for x in ['text/html', 'text/plain']):
        log.warning("Non-HTML file %s using Chrome from %s"
                      %  (content_type, url))
        data['download_status'] = -3
        data['error'] = "Non-HTML file"
        return data, pageContent

    pageContent = ""
    if page.is_closed():
        log.info("Page is closed")
        data['download_status'] = -2
        data['error'] = "Closed page"
        return data, pageContent

    try: 
        pageContent = await extract_with_readability(page, webhash, out_dir)
        if not pageContent or pageContent.strip() == "":
            pageContent = await extract_raw_content(page, webhash, out_dir)
    except Exception as e:
        log.info(f"Failed to extract page content for {url} with exception: {e}")
    
    num_bytes = len(pageContent)

    log.info(f"Extracted pageContent for {url} with length: {num_bytes}")

    data['download_size'] = num_bytes

    await page.close()
    return data, pageContent