# from fastapi import (FastAPI, Request, BackgroundTasks)
import asyncio
import json
import sys
import os
import logging
from supabase import Client, create_client
import weaviate
from config import settings
from indexer import indexer
# from schemas import (Payload, Link, Bookmark, Record)
from typing import List, Optional

import warnings

warnings.filterwarnings("ignore")

# Create an instance of the FastAPI class
# app = FastAPI()

# Add a route to the app
default_out_dir = "./results"
os.makedirs(default_out_dir, exist_ok=True)

default_rest = 10
default_timeout = 60
# default_out_filepath = os.path.join(, "downloads.jsonl")

log = logging.getLogger(__name__)
formatter = logging.Formatter(u'%(message)s')
handler_stderr = logging.StreamHandler(sys.stderr)
handler_stderr.setFormatter(formatter)
root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(handler_stderr)

supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)

def convert_user_id(user_id: str):
    if "-" in user_id:
        return user_id.replace("-", "_")
    elif "_" in user_id:
        return user_id.replace("_", "-")
    else:
        return user_id


def extract_urls(payload: dict) -> List[str]:
    urls = []

    def traverse_links(links):
        for link in links:
            if link['url']:
                urls.append(link['url'])

            if link['links']:
                traverse_links(link['links'])

    # for bookmark in payload.record.bookmarks:
    for bookmark in payload['record']['bookmarks']:
        traverse_links(bookmark['links'])

    return urls

def read_text_urls(url_l: list):
    ''' Read one URL per line and return list of 
        dictionaries with url field '''
    url_dicts = []
    for line in url_l:
        url = line.strip()
        d = { 'url' : url }
        url_dicts.append(d)

    # Return URL dictionaries
    return url_dicts

async def worker(url_l: List, user_id: str):
    user_id = convert_user_id(user_id)
    url_dict_l = read_text_urls(url_l)
    num_urls = len(url_dict_l)
    logging.info("Read %d URLs" % (num_urls))
    user_dir = os.path.join(default_out_dir, user_id)
    user_out_dir = os.path.join(user_dir, "out")
    os.makedirs(user_out_dir, exist_ok=True)

    log_file = os.path.join(user_dir, "downloads.jsonl")
    
    import async_playwright

    fun = async_playwright.async_download_url_dicts(url_dict_l,
                                                    log_file,
                                                    user_id=user_id,
                                                    tracing=True,
                                                    out_dir=user_out_dir,
                                                    timeout=default_timeout,
                                                )
    await fun


def importer(webhookData: dict):
    # print("Importing data")
    print(webhookData)
    url_l = extract_urls(webhookData)
    user_id = webhookData['record']['user_id']
    asyncio.run(worker(url_l, user_id))