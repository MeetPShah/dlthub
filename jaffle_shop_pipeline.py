import os
import time
import threading
import dlt
from dlt import resource, source
from urllib.parse import urljoin
import requests
import logging
from typing import Iterator, Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE = "https://jaffle-shop.scalevector.ai/api/v1"
NAIVE_PAGE_SIZE = 100
FAST_PAGE_SIZE = 1000

try:
    from dlt.sources.helpers.rest_client import RESTClient
except Exception:
    RESTClient = None

def _make_client():
    if RESTClient is not None:
        return RESTClient(base_url=API_BASE)
    return requests.Session()

def _get_json(c, url_or_path, params=None):
    if RESTClient is not None and isinstance(c, RESTClient):
        try:
            r = c.get(url_or_path, params=params)
            try:
                return r.json() if hasattr(r, "json") else r
            except Exception:
                return r
        except AttributeError:
            r = c.request("GET", url_or_path, params=params)
            try:
                return r.json() if hasattr(r, "json") else r
            except Exception:
                return r
    if isinstance(c, requests.Session):
        u = url_or_path if url_or_path.startswith("http") else urljoin(API_BASE, url_or_path)
        r = c.get(u, params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    u = url_or_path if url_or_path.startswith("http") else urljoin(API_BASE, url_or_path)
    r = requests.get(u, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def _extract_items(payload):
    if isinstance(payload, list):
        return payload, None, None, None
    if isinstance(payload, dict):
        for k in ("data", "items", "results", "records"):
            if k in payload and isinstance(payload[k], list):
                next_url = payload.get("next") or (payload.get("links") or {}).get("next")
                total_pages = payload.get("total_pages")
                page = payload.get("page")
                return payload[k], next_url, page, total_pages
        next_url = payload.get("next") or (payload.get("links") or {}).get("next")
        total_pages = payload.get("total_pages")
        page = payload.get("page")
        keys_with_list = [v for v in payload.values() if isinstance(v, list)]
        if keys_with_list:
            return keys_with_list[0], next_url, page, total_pages
    return [], None, None, None

def _pages(endpoint, page_size):
    c = _make_client()
    page = 1
    url = endpoint
    params = {"page": page, "page_size": page_size}
    while True:
        payload = _get_json(c, url, params=params)
        items, next_url, page_val, total_pages = _extract_items(payload)
        if items:
            yield items
        if next_url:
            url = next_url
            params = None
        elif total_pages and page_val:
            page += 1
            if page > int(total_pages):
                break
            params = {"page": page, "page_size": page_size}
            url = endpoint
        else:
            if items and len(items) == page_size:
                page += 1
                params = {"page": page, "page_size": page_size}
                url = endpoint
                continue
            break

@resource(name="customers_fast", parallelized=True)
def customers_fast():
    for p in _pages("/customers", FAST_PAGE_SIZE):
        yield p

@resource(name="orders_fast", parallelized=True)
def orders_fast():
    for p in _pages("/orders", FAST_PAGE_SIZE):
        yield p

@resource(name="products_fast", parallelized=True)
def products_fast():
    for p in _pages("/products", FAST_PAGE_SIZE):
        yield p

@source(name="jaffle_shop_fast")
def jaffle_shop_fast():
    return customers_fast(), orders_fast(), products_fast()

def _set_workers(e, n, l):
    os.environ["EXTRACT__WORKERS"] = str(e)
    os.environ["NORMALIZE__WORKERS"] = str(n)
    os.environ["LOAD__WORKERS"] = str(l)

def run_pipeline():
    logger.info("Starting Jaffle Shop pipeline")
    
    # Set optimal worker configuration
    _set_workers(8, 4, 4)
    os.environ["DLT_WRITE_FINITE_FILES"] = "true"
    
    # Initialize pipeline
    destination = os.getenv("DLT_DESTINATION", "duckdb")
    dataset_name = os.getenv("DLT_DATASET", "jaffle_shop")
    
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_fast",
        destination=destination,
        dataset_name=dataset_name,
        progress="log"
    )

    # Run pipeline
    try:
        logger.info(f"Running pipeline with destination={destination}, dataset={dataset_name}")
        info = pipeline.run(jaffle_shop_fast())
        logger.info("Pipeline completed successfully")
        logger.info(f"Loaded tables: {list(info.get('loads_ids', {}).keys())}")
        return info
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_pipeline()
