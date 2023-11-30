# from fastapi import (FastAPI, Request, BackgroundTasks)
import asyncio
import json
import sys
import os
import logging
import weaviate
from config import settings
from indexer import indexer
# from schemas import (Payload, Link, Bookmark, Record)
from typing import List, Optional

import warnings

from utils import get_supabase

warnings.filterwarnings("ignore")

# Create an instance of the FastAPI class
# app = FastAPI()

# Add a route to the app
default_out_dir = "./results"
os.makedirs(default_out_dir, exist_ok=True)

default_rest = 10
default_timeout = 60
# default_out_filepath = os.path.join(, "downloads.jsonl")

# log = logging.getLogger(__name__)
# formatter = logging.Formatter(u'%(message)s')
# handler_stderr = logging.StreamHandler(sys.stderr)
# handler_stderr.setFormatter(formatter)
# root = logging.getLogger()
# root.setLevel(logging.INFO)
# root.addHandler(handler_stderr)


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
                urls.append({'url': link['url'], 'title': link['name']})

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

async def worker(url_dict_l: dict, user_id: str):
    user_id = convert_user_id(user_id)
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
                                                    tracing=False,
                                                    out_dir=user_out_dir,
                                                    timeout=default_timeout,
                                                )
    await fun


def importer(webhookData: dict):
    # print("Importing data")
    url_dict_l = extract_urls(webhookData)
    user_id = webhookData['record']['user_id']
    job_id = webhookData['record']['id']
    asyncio.run(worker(url_dict_l, user_id))
    supabase = get_supabase()
    supabase.from_("imported_bookmarks").update({"is_job_finished": True}).eq("id", job_id).execute()