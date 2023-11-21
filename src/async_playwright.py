import os
import logging
import hashlib
import json
import asyncio
import random
import aiofiles
from playwright.async_api import async_playwright
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


async def async_download_url_in_context(context, url,
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
    try:
        page = await context.new_page()
        # await stealth_async(page)
        response = await page.goto(url, timeout=timeout, wait_until='networkidle')

        # try:
        #     await page.wait_for_navigation(timeout=500)
        # except asyncio.TimeoutError:
        #     pass

        await page.wait_for_load_state("domcontentloaded", timeout=timeout)

    except asyncio.TimeoutError as e:
        log.info(u"Timeout downloading %s with exception: %s" % (
                  url, e))
        data['download_status'] = -1
        data['error'] = f"TimeoutError: {str(e)}"

    except Exception as e:
        log.info(u"Failed to establish session %s with exception: %s" % (
                  url, e))
        data['download_status'] = -1
        data['error'] = f"ConnectionError: {str(e)}"
        return data

    # Check status code
    data['download_status'] = response.status
    # if (not response.ok):
    #     log.info("Failed to download %s with status code: %d" %
    #                   (url, response.status))
    #     return data

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

    # Get page content
    try:
        if page.is_closed():
            log.info("Page is closed")
            data['download_status'] = -2
            data['error'] = "Closed page"
            return data
        pageContent = await page.evaluate('() => document.body.innerText')
        pageTitle = await page.evaluate('() => document.title')
        bin_content = pageContent.encode('utf-8')
        num_bytes = len(bin_content)
        data['title'] = pageTitle
        if num_bytes == 0:
            log.info(f"Empty page content for {url}")
            data['download_status'] = -1
            data['error'] = "Empty page content"
            return data
    except Exception as e:
        log.info(u"Failed to obtain content from %s with exception: %s" %
                    (url, e))
        data['download_status'] = -2
        data['error'] = f"NavigationError: {str(e)}"
        return data

    # Compute SHA256 hash of downloaded document and url
    url = url.encode('utf-8')
    webhash = hashlib.sha256(url).hexdigest()
    data['download_sha256'] = webhash
    data['download_size'] = num_bytes

    # Save file
    # screenshot = "%s.png" % webhash
    # await page.screenshot(path=os.path.join(out_dir, screenshot), full_page=True)
    filename = "%s.download" % webhash
    filepath =  os.path.join(out_dir, filename)

    # async with aiofiles.open(filepath, "wb") as download_fd:
    #     await download_fd.write(bin_content)
    # log.info(u"Downloaded %d bytes from %s into %s" % (num_bytes, url,
    #                                                     filepath))


    # Extract text with Readability.js
    if get_text:
        filename = "%s.text" % webhash
        filepath =  os.path.join(out_dir, filename)
        try:
            readability_path = "node_modules/@mozilla/readability/Readability.js"
            with open(readability_path, "r") as fd:
                readability_script = fd.read()
            # Inject the Readability library into the page
            await page.evaluate(f"""
                {readability_script}
            """)

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
                    async with aiofiles.open(filepath, "w") as download_fd:
                        await download_fd.write(page_text.strip())
                    log.info(u"Page text from %s into %s" % (url, filepath))
        except Exception as e:
            data['error'] = str(e)
            log.info(u"Failed to extract text from %s with exception: %s" %
                        (url, e))
            async with aiofiles.open(filepath, "wb") as download_fd:
                await download_fd.write(bin_content)
                log.info(u"Downloaded %d bytes from %s into %s" % (num_bytes, url, filepath))


    await page.close()
    return data


async def async_download_url_dict(context, url_dict, out_dir=".",
                                  timeout=None):
    ''' Download URL in dictionary and add download fields to dictionary '''
    # Check we have a URL
    url = url_dict.get('url', None)
    if url is None:
        log.warning('No URL field in %s' % url_dict)
        return url_dict

    # Download URL
    d = await async_download_url_in_context(context, url,
                                  url_dict = url_dict,
                                  out_dir=out_dir,
                                  timeout=timeout)
    return d


async def async_download_url_dicts(url_dict_l, log_filepath, tracing,
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
                for url_dict in batch_l:
                    download_futures.append(
                        async_download_url_dict(context, url_dict,
                                                out_dir=out_dir,
                                                timeout=timeout
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