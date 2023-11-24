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
from utils import get_weaviate_client, convert_user_id
# from playwright_stealth import stealth_async

# Set logging
log = logging.getLogger(__name__)

# Global maps of downloads
downloaded_files = {}

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
        # NOTE: This is done to periodically close playwright context
        #       so that the used memory is released.
        #       The downside is that until a batch ends, another cannot start
        for i in range(0, len(url_dict_l), batch_size):
            log.info("Processing URL dict batch [%d,%d]" % (i,i+batch_size-1))
            batch_l = url_dict_l[i:i+batch_size]
            async with async_playwright() as p:
                # USING BROWSERLESS
                # browser = await p.chromium.connect_over_cdp(
                #     "wss://chrome.browserless.io?token=0a1c3d65-cd15-4175-8ba0-91e7e2d478f0"
                # )
                # context = browser.contexts[0]
                # context.accept_downloads = False
                # await context.set_extra_http_headers(headers)

                # USING LOCAL
                device_list = list(p.devices.keys())
                random_device = random.choice(device_list)
                device_info = p.devices[random_device]

                browser_type = p.chromium
                browser = await browser_type.launch(
                                  args=['--disable-blink-features=TrustedDOMTypes'],
                                  headless=True,
                                  downloads_path=out_dir)


                context = await browser.new_context(accept_downloads=False,
                                                    # locale='en-US',
                                                    extra_http_headers=headers,
                                                    **device_info)
                # Start tracing before creating / navigating a page.
                if tracing:
                    await context.tracing.start(screenshots=False,
                                                snapshots=False,
                                                sources=True)

                # Create futures
                download_futures = []
                async with aiohttp.ClientSession() as session:
                    for url_dict in batch_l:
                        download_futures.append(
                            async_download_url_in_context(
                                context, url_dict['url'], session,
                                user_id=user_id,
                                url_dict=url_dict,
                                out_dir=out_dir,
                                timeout=timeout,
                            )
                        )

                # Run futures and print them as they complete
                for future in asyncio.as_completed(_limit_concurrency(
                        download_futures, max_simultaneous_downloads)):
                    # Wait for URL visit to complete
                    data = await future
                    # Output log info
                    line = json.dumps(data, sort_keys=True, default=str)
                    await log_fd.write("%s\n" % line)
                    await log_fd.flush()

                # Stop tracing and export it into a zip archive.
                if tracing:
                    await context.tracing.stop(path = tracing_filepath)

                # Close
                await context.close()
                await browser.close()
                print("[!] Cleaning the browser")


async def async_download_url_in_context(context, url, user_id,
                                    session,
                                    out_dir=".",
                                    get_text=True,
                                    timeout=None,
                                    url_dict=None
                                ):
    data = url_dict or {}

    log.debug("Downloading: %s" % url)

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
            data['download_status'] = -1
            data['error'] = f"ConnectionError: {str(e)}"

            # If this was the last retry, return the data
            if i == max_retries - 1:
                return data

            # Otherwise, log the retry and continue the loop
            log.info(f"Retrying to establish session for {url}. Retry {i+1} of {max_retries}")
    # Check status code
    data['download_status'] = response.status
    title = await page.title()
    data['title'] = title if title else url
    webhash = hashlib.sha256(url.encode('utf-8')).hexdigest()
    data['download_sha256'] = webhash


    # Download of non-HTML content is problematic because Chrome may
    # fail to download the content,
    # download it with an impossible to predict name, or
    # wrap text content in some basic HTML that modifies the correct hash
    # Thus, we set a special error and will download it with another method
    content_type = response.headers.get("content-type", None)
    if not content_type or not any(content_type.startswith(x) for x in ['text/html', 'text/plain']):
        log.warning("Non-HTML file %s using Chrome from %s"
                      %  (content_type, url))
        data['download_status'] = -3
        data['error'] = "Non-HTML file"
        return data


    # Obtain redirection chain
    if (response.request.redirected_from):
        redirects = [response.url]
        prev_req = response.request.redirected_from
        while prev_req:
            redirects.insert(0, prev_req.url)
            prev_req = prev_req.redirected_from
        data['download_redirects'] = redirects

    async def extract_with_readability(page, webhash, out_dir):
        filename = "%s.text" % webhash
        filepath = os.path.join(out_dir, filename)
        readability_path = "node_modules/@mozilla/readability/Readability.js"
        page_text = None
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
            page_text = readability_response.get('textContent', None)
            if page_text:
                # print(page_text)
                async with aiofiles.open(filepath, "w") as download_fd:
                    await download_fd.write(page_text.strip())
                
                log.info(u"Downloaded %s into %s" % (url, filepath))
                return page_text
        return None

    async def extract_raw_content(page, webhash, out_dir):
        filename = "%s.text" % webhash
        filepath = os.path.join(out_dir, filename)
        pageContent = await page.evaluate('() => document.body.innerText')
        # bin_content = pageContent.encode('utf-8')
        # print(bin_content)
        async with aiofiles.open(filepath, "wb") as download_fd:
            await download_fd.write(pageContent)
        log.info(u"Downloaded %s into %s" % (url, filepath))
        return pageContent

    # Usage
    pageContent = ""
    try:
        pageContent = await extract_with_readability(page, webhash, out_dir)
        # print(pageContent)
    except Exception as e:
        log.info(f"Failed to extract readability data. Extracting raw page content")
        if page.is_closed():
            log.info("Page is closed")
            data['download_status'] = -2
            data['error'] = "Closed page"
        else:
            try:
                pageContent = await extract_raw_content(page, webhash, out_dir)
            except Exception as e:
                log.info(u"Failed to obtain content from %s with exception: %s" % (url, e))
                data['download_status'] = -2
                data['error'] = f"NavigationError: {str(e)}"


    document = dict()
    client = get_weaviate_client()
    document["url"] = url
    document["content"] = pageContent
    document["title"] = data['title']
    indexer(document, user_id, client)

    data = {
        "user_id": convert_user_id(user_id),
        "url": url,
        "title": data['title']
    }
    headers = {
        "apikey": settings.SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {settings.SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    try:
        async with session.post('https://mzaeulqqzucqbiovkgzg.supabase.co/rest/v1/all_saved', headers=headers, data=json.dumps(data)) as response:
            print(await response.text())
            logging.info("[!] Inserted into DB:")
    except Exception as e:
        logging.info("[!] Failed to insert into DB:", e)

    # try:
    #     response = supabase.from_("all_saved").insert(data).execute()
    #     print(response)
    #     logging.info("[!] Inserted into DB:")
    # except Exception as e:
    #     logging.info("[!] Failed to insert into DB:", e)

    # Compute SHA256 hash of downloaded document and url
    num_bytes = len(pageContent)
    data['download_size'] = num_bytes

    filename = "%s.download" % webhash
    filepath =  os.path.join(out_dir, filename)

    await page.close()
    return data


# async def async_download_url_dict(context, url_dict, user_id, out_dir=".",
#                                   timeout=None):
#     ''' Download URL in dictionary and add download fields to dictionary '''
#     # Check we have a URL
#     url = url_dict.get('url', None)
#     if url is None:
#         log.warning('No URL field in %s' % url_dict)
#         return url_dict

#     # Download URL
#     d = await async_download_url_in_context(context, url, user_id=user_id,
#                                   url_dict=url_dict,
#                                   out_dir=out_dir,
#                                   timeout=timeout
#                                 )
#     return d

