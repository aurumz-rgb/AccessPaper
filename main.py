import os
import json
import asyncio
from datetime import datetime
from threading import RLock
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import quote as url_quote
import xml.etree.ElementTree as ET
from datetime import date
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import re
import time
from functools import lru_cache
import weakref
import gc

load_dotenv()

REQUEST_TIMEOUT = 5
MAX_CONCURRENT_REQUESTS = 10
RATE_LIMIT_DELAY = 0.5

BASE_API_ENABLED = os.getenv("BASE_API_ENABLED") 
GOOGLE_BOOKS_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY", "")
UNPAYWALL_EMAIL = os.getenv("UNPAYWALL_EMAIL", "email@example.com")

API_RATE_LIMITS = {
    "crossref": 1.0,
    "openalex": 1.0,
    "semantic_scholar": 1.0,
    "unpaywall": 1.0,
    "base": 2.0,
    "zenodo": 1.0,
    "figshare": 1.0,
    "europepmc": 1.0,
    "arxiv": 1.0,
    "biorxiv": 1.0,
    "medrxiv": 1.0,
    "internetarchive": 1.0,
    "hal": 1.0,
    "plos": 1.0,
    "doaj": 1.0,
    "share": 1.0,
    "pubmed": 1.0,
    "dryad": 1.0,
    "openaire": 1.0,
    "wikidata": 1.0,
    "google_books": 1.0,
    "springer": 1.0,
    "elsevier": 1.0,
    "wiley": 1.0,
    "nature": 1.0,
    "science": 1.0,
    "jstor": 1.0,
    "ssrn": 1.0,
    "repec": 1.0,
    "citeseerx": 1.0,
    "researchgate": 1.0,
    "chemrxiv": 1.0,
    "f1000": 1.0,
    "elife": 1.0,
    "cell": 1.0,
    "frontiers": 1.0,
    "mdpi": 1.0,
    "hindawi": 1.0,
    "copernicus": 1.0,
    "iop": 1.0,
    "aps": 1.0,
    "aip": 1.0,
    "rsc": 1.0,
    "acs": 1.0,
    "ieee": 1.0,
    "acm": 1.0,
    "pmc": 1.0,
}

last_request_time = {api: 0 for api in API_RATE_LIMITS}

app = FastAPI()

@app.on_event("startup")
async def on_startup():
    try:
        print("App startup")
        app.state.client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=MAX_CONCURRENT_REQUESTS),
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True
        )
    except Exception as e:
        print(f"Error on startup: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    try:
        print("App shutdown")
        await app.state.client.aclose()
    except Exception as e:
        print(f"Error on shutdown: {e}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://accesspaper.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def merge_metadata(base: dict, new: dict) -> dict:
    if not base:
        return new or {}
    if not new:
        return base

    for key, val in new.items():
        if val and not base.get(key):
            base[key] = val

    if "authors" in new and new["authors"]:
        existing_names = {a.get("name") for a in base.get("authors", []) if a.get("name")}
        for author in new["authors"]:
            if author.get("name") not in existing_names:
                base.setdefault("authors", []).append(author)

    return base

def quote(text: Optional[str]) -> str:
    return url_quote(text or "")

async def rate_limit(api_name: str):
    current_time = time.time()
    last_time = last_request_time.get(api_name, 0)
    time_since_last = current_time - last_time
    
    if time_since_last < API_RATE_LIMITS.get(api_name, RATE_LIMIT_DELAY):
        await asyncio.sleep(API_RATE_LIMITS.get(api_name, RATE_LIMIT_DELAY) - time_since_last)
    
    last_request_time[api_name] = time.time()

async def verify_pdf_url(url: str, client: httpx.AsyncClient) -> bool:
    try:
        head_response = await client.head(url, timeout=REQUEST_TIMEOUT)
        content_type = head_response.headers.get("content-type", "").lower()
        content_disposition = head_response.headers.get("content-disposition", "").lower()
        
        if "pdf" in content_type or "pdf" in content_disposition or url.lower().endswith(".pdf"):
            return True
        return False
    except Exception:
        return False

async def extract_pdf_from_page(page_url: str, client: httpx.AsyncClient) -> Optional[str]:
    try:
        response = await client.get(page_url, timeout=REQUEST_TIMEOUT)
        content = response.text
        
        patterns = [
            r'href=["\']([^"\']*\.pdf)["\']',
            r'["\']([^"\']*\.pdf)["\']',
            r'href=["\']([^"\']*\?download=1)["\']',
            r'href=["\']([^"\']*\/download)["\']',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match.startswith("/"):
                    base_url = page_url.split("/")[0] + "//" + page_url.split("/")[2]
                    match = base_url + match
                elif not match.startswith("http"):
                    base_url = page_url.rsplit("/", 1)[0]
                    match = base_url + "/" + match
                
                if await verify_pdf_url(match, client):
                    return match
        
        return None
    except Exception:
        return None

async def get_crossref_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("crossref")
    print(f"[Crossref] Fetching metadata for DOI: {doi}")
    url = f"https://api.crossref.org/works/{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json().get("message", {})
        authors = data.get("author", [])
        author_list = []
        for a in authors:
            affiliations = a.get("affiliation") or []
            affiliation_name = affiliations[0].get("name") if affiliations else ""
            author_list.append({
                "name": f"{a.get('given', '')} {a.get('family', '')}".strip(),
                "affiliation": affiliation_name
            })
        return {
            "title": data.get("title", [""])[0],
            "authors": author_list,
            "corresponding_email": None,
            "journal": data.get("container-title", [""])[0],
            "year": data.get("created", {}).get("date-parts", [[None]])[0][0]
        }
    except Exception as e:
        print(f"[Crossref] metadata fetch error: {e}")
        return None

async def get_openalex_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("openalex")
    print(f"[OpenAlex] Fetching metadata for DOI: {doi}")
    url = f"https://api.openalex.org/works/https://doi.org/{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        authorships = data.get("authorships", [])
        authors = [{"name": a.get("author", {}).get("display_name", ""), "affiliation": ""} for a in authorships]
        return {
            "title": data.get("title"),
            "authors": authors,
            "corresponding_email": None,
            "journal": data.get("host_venue", {}).get("display_name"),
            "year": data.get("publication_year")
        }
    except Exception as e:
        print(f"[OpenAlex] metadata fetch error: {e}")
        return None

async def get_semantic_scholar_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("semantic_scholar")
    print(f"[Semantic Scholar] Fetching metadata for DOI: {doi}")
    url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{quote(doi)}?fields=title,authors,journal,year"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        authors = [{"name": a.get("name", ""), "affiliation": ""} for a in data.get("authors", [])]
        return {
            "title": data.get("title"),
            "authors": authors,
            "corresponding_email": None,
            "journal": data.get("journal", {}).get("name"),
            "year": data.get("year")
        }
    except httpx.HTTPStatusError as e:
        print(f"[Semantic Scholar] HTTP error: {e}")
        return None
    except Exception as e:
        print(f"[Semantic Scholar] metadata fetch error: {e}")
        return None

async def get_pubmed_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("pubmed")
    print(f"[PubMed] Fetching metadata for DOI: {doi}")
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={quote(doi)}[DOI]&retmode=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        idlist = data.get("esearchresult", {}).get("idlist", [])
        if not idlist:
            print("[PubMed] No PMID found for DOI")
            return None
        pmid = idlist[0]
        summary_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id={pmid}&retmode=json"
        r2 = await client.get(summary_url, timeout=REQUEST_TIMEOUT)
        r2.raise_for_status()
        summary = r2.json()
        doc = summary.get("result", {}).get(pmid, {})
        metadata = {
            "title": doc.get("title"),
            "authors": [{"name": a.get("name")} for a in doc.get("authors", [])] if doc.get("authors") else [],
            "pubdate": doc.get("pubdate"),
        }
        print(f"[PubMed] Metadata fetched: {metadata}")
        return metadata
    except Exception as e:
        print(f"[PubMed] Fetch error: {e}")
        return None

async def get_doaj_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("doaj")
    print(f"[DOAJ] Fetching metadata for DOI: {doi}")
    url = f"https://doaj.org/api/v2/search/articles/doi:{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            print("[DOAJ] No results found")
            return None
        article = results[0].get("bibjson", {})
        metadata = {
            "title": article.get("title"),
            "authors": [{"name": a.get("name")} for a in article.get("author", [])],
            "corresponding_email": None,
            "journal": article.get("journal", {}).get("title"),
            "year": article.get("year"),
        }
        return metadata
    except Exception as e:
        print(f"[DOAJ] Fetch error: {e}")
        return None

async def get_dryad_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("dryad")
    print(f"[Dryad] Fetching metadata for DOI: {doi}")
    url = f"https://datadryad.org/api/v2/package/{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 404:
            print("[Dryad] No data found (404)")
            return None
        r.raise_for_status()
        data = r.json()
        metadata = {
            "title": data.get("title"),
            "authors": [{"name": a.get("full_name")} for a in data.get("authors", [])],
            "year": data.get("publication_year"),
        }
        print(f"[Dryad] Metadata fetched: {metadata}")
        return metadata
    except Exception as e:
        print(f"[Dryad] Fetch error: {e}")
        return None

async def get_openaire_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("openaire")
    print(f"[OpenAIRE] Fetching metadata for DOI: {doi}")
    url = f"https://api.openaire.eu/search/publications?doi={quote(doi)}&format=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        results = data.get("result", {}).get("results", [])
        if not results:
            print("[OpenAIRE] No results found")
            return None
        item = results[0]
        metadata = {
            "title": item.get("title"),
            "authors": [{"name": a} for a in item.get("authors", [])],
            "year": item.get("publicationYear"),
        }
        print(f"[OpenAIRE] Metadata fetched: {metadata}")
        return metadata
    except Exception as e:
        print(f"[OpenAIRE] Fetch error: {e}")
        return None

async def get_internetarchive_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("internetarchive")
    print(f"[Internet Archive] Fetching metadata for DOI: {doi}")
    url = f"https://archive.org/metadata/{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        metadata = {
            "title": data.get("metadata", {}).get("title"),
            "authors": [{"name": a} for a in data.get("metadata", {}).get("creator", [])] if isinstance(data.get("metadata", {}).get("creator"), list) else [],
        }
        print(f"[Internet Archive] Metadata fetched: {metadata}")
        return metadata
    except Exception as e:
        print(f"[Internet Archive] Fetch error: {e}")
        return None

async def get_wikidata_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("wikidata")
    print(f"[Wikidata SPARQL] Fetching metadata for DOI: {doi}")
    query = f"""
    SELECT ?item ?itemLabel WHERE {{
      ?item wdt:P356 "{doi}".
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    url = "https://query.wikidata.org/sparql"
    headers = {"Accept": "application/sparql-results+json"}
    try:
        r = await client.get(url, params={"query": query}, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        bindings = data.get("results", {}).get("bindings", [])
        if not bindings:
            print("[Wikidata SPARQL] No results found")
            return None
        item = bindings[0].get("itemLabel", {}).get("value")
        metadata = {"title": item, "authors": []}
        print(f"[Wikidata SPARQL] Metadata fetched: {metadata}")
        return metadata
    except Exception as e:
        print(f"[Wikidata SPARQL] Fetch error: {e}")
        return None

async def get_google_books_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("google_books")
    print(f"[Google Books] Fetching metadata for DOI: {doi}")
    if not GOOGLE_BOOKS_API_KEY:
        print("[Google Books] API key missing, skipping")
        return None
    
    url = f"https://www.googleapis.com/books/v1/volumes?q=doi:{quote(doi)}&key={GOOGLE_BOOKS_API_KEY}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        if not items:
            print("[Google Books] No items found")
            return None
        volume_info = items[0].get("volumeInfo", {})
        metadata = {
            "title": volume_info.get("title"),
            "authors": [{"name": a} for a in volume_info.get("authors", [])],
            "publishedDate": volume_info.get("publishedDate"),
        }
        print(f"[Google Books] Metadata fetched: {metadata}")
        return metadata
    except httpx.HTTPStatusError as e:
        print(f"[Google Books] HTTP error: {e}")
        return None
    except Exception as e:
        print(f"[Google Books] Fetch error: {e}")
        return None

async def get_pdf_url_from_doi(doi: str, client: httpx.AsyncClient) -> Dict[str, str]:
    print(f"[DOI] Fetching for DOI: {doi}", flush=True)
    doi_url = f"https://doi.org/{doi}"
    result = {"pdf_url": None, "publisher_url": None}

    try:
        resp = await client.get(doi_url, follow_redirects=True, timeout=REQUEST_TIMEOUT)
        final_url = str(resp.url)
        content_type = resp.headers.get("content-type", "").lower()

        if final_url.lower().endswith(".pdf") or "pdf" in content_type:
            if await verify_pdf_url(final_url, client):
                result["pdf_url"] = final_url
            else:
                result["publisher_url"] = final_url
        else:
            result["publisher_url"] = final_url

        if "arxiv.org" in final_url and "/abs/" in final_url:
            pdf_url = final_url.replace("/abs/", "/pdf/") + ".pdf"
            if await verify_pdf_url(pdf_url, client):
                result["pdf_url"] = pdf_url
        
        if not result["pdf_url"] and result["publisher_url"]:
            pdf_url = await extract_pdf_from_page(result["publisher_url"], client)
            if pdf_url:
                result["pdf_url"] = pdf_url

    except Exception as e:
        print(f"[PDF Check] Error checking DOI: {e}", flush=True)

    return result

async def get_unpaywall_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("unpaywall")
    print(f"[Unpaywall] Fetching PDF for DOI: {doi}")
    url = f"https://api.unpaywall.org/v2/{quote(doi)}?email={UNPAYWALL_EMAIL}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        
        loc = data.get("best_oa_location")
        if loc:
            pdf_url = loc.get("url_for_pdf")
            if pdf_url and await verify_pdf_url(pdf_url, client):
                print(f"[Unpaywall] PDF URL found in best_oa_location: {pdf_url}")
                return {"pdf_url": pdf_url, "host_type": loc.get("host_type"), "source": "Unpaywall"}
            elif pdf_url:
                direct_pdf = await extract_pdf_from_page(pdf_url, client)
                if direct_pdf:
                    print(f"[Unpaywall] Direct PDF extracted from page: {direct_pdf}")
                    return {"pdf_url": direct_pdf, "host_type": loc.get("host_type"), "source": "Unpaywall"}

        oa_locations = data.get("oa_locations", [])
        for location in oa_locations:
            pdf_url = location.get("url_for_pdf")
            if pdf_url and await verify_pdf_url(pdf_url, client):
                print(f"[Unpaywall] PDF URL found in oa_locations: {pdf_url}")
                return {"pdf_url": pdf_url, "host_type": location.get("host_type"), "source": "Unpaywall"}
            elif pdf_url:
                direct_pdf = await extract_pdf_from_page(pdf_url, client)
                if direct_pdf:
                    print(f"[Unpaywall] Direct PDF extracted from page: {direct_pdf}")
                    return {"pdf_url": direct_pdf, "host_type": location.get("host_type"), "source": "Unpaywall"}

        print("[Unpaywall] No valid PDF link found in any location")
    except Exception as e:
        print(f"[Unpaywall] PDF fetch error: {e}")
    return None

async def get_europepmc_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("europepmc")
    print(f"[EuropePMC] Fetching PDF for DOI: {doi}")
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=doi:{quote(doi)}&format=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        results = r.json().get("resultList", {}).get("result", [])

        for result in results:
            full_text_urls = result.get("fullTextUrlList", {}).get("fullTextUrl", [])
            for full_text_url in full_text_urls:
                if full_text_url.get("documentStyle") == "pdf" and full_text_url.get("availability") == "OPEN_ACCESS":
                    pdf_link = full_text_url.get("url")
                    if await verify_pdf_url(pdf_link, client):
                        host_type = (
                            "EuropePMC Preprints"
                            if result.get("pubType", "").lower() == "preprint"
                            else "EuropePMC"
                        )
                        print(f"[EuropePMC] PDF URL found: {pdf_link} (Type: {host_type})")
                        return {"pdf_url": pdf_link, "host_type": host_type, "source": host_type}
                    else:
                        direct_pdf = await extract_pdf_from_page(pdf_link, client)
                        if direct_pdf:
                            host_type = (
                                "EuropePMC Preprints"
                                if result.get("pubType", "").lower() == "preprint"
                                else "EuropePMC"
                            )
                            print(f"[EuropePMC] Direct PDF extracted from page: {direct_pdf} (Type: {host_type})")
                            return {"pdf_url": direct_pdf, "host_type": host_type, "source": host_type}

        print("[EuropePMC] No valid PDF link found")
    except Exception as e:
        print(f"[EuropePMC] PDF fetch error: {e}")
    return None

async def get_base_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("base")
    print(f"[BASE] Fetching PDF for DOI: {doi}")

    url = f"https://api.base-search.net/beta/search?q=doi:{quote(doi)}&format=json&limit=1"
    headers = {"Authorization": f"Bearer {BASE_API_ENABLED}"}

    try:
        r = await client.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        records = data.get("records", [])
        for record in records:
            for link in record.get("links", []):
                url_link = link.get("url", "")
                if link.get("type") == "fulltext" and ".pdf" in url_link.lower(): 
                    if await verify_pdf_url(url_link, client):
                        print(f"[BASE] PDF URL found: {url_link}")
                        return {"pdf_url": url_link, "host_type": "BASE", "source": "BASE"}
                    else:
                        direct_pdf = await extract_pdf_from_page(url_link, client)
                        if direct_pdf:
                            print(f"[BASE] Direct PDF extracted from page: {direct_pdf}")
                            return {"pdf_url": direct_pdf, "host_type": "BASE", "source": "BASE"}

        print("[BASE] No valid PDF link found in response")
    except httpx.HTTPStatusError as e:
        print(f"[BASE] HTTP error: {e}")
    except Exception as e:
        print(f"[BASE] PDF fetch error: {e}")

    return None

async def get_zenodo_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("zenodo")
    print(f"[Zenodo] Fetching PDF for DOI: {doi}")
    url = f"https://zenodo.org/api/records/?q=doi:{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        hits = r.json().get("hits", {}).get("hits", [])
        for hit in hits:
            for f in hit.get("files", []):
                pdf_link = f.get("links", {}).get("self", "")
                if pdf_link.lower().endswith(".pdf") and await verify_pdf_url(pdf_link, client):
                    print(f"[Zenodo] PDF URL found: {pdf_link}")
                    return {"pdf_url": pdf_link, "host_type": "Zenodo", "source": "Zenodo"}
        print("[Zenodo] No valid PDF link found")
    except Exception as e:
        print(f"[Zenodo] PDF fetch error: {e}")
    return None

async def get_figshare_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("figshare")
    print(f"[Figshare] Fetching PDF for DOI: {doi}")
    url = f"https://api.figshare.com/v2/articles/search?search_for={quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        for item in items:
            for f in item.get("files", []):
                if f.get("name", "").lower().endswith(".pdf"):
                    download_url = f.get("download_url")
                    if download_url and await verify_pdf_url(download_url, client):
                        print(f"[Figshare] PDF URL found: {download_url}")
                        return {"pdf_url": download_url, "host_type": "Figshare", "source": "Figshare"}
        print("[Figshare] No valid PDF link found")
    except httpx.HTTPStatusError as e:
        print(f"[Figshare] HTTP error: {e}")
    except Exception as e:
        print(f"[Figshare] PDF fetch error: {e}")
    return None

async def get_arxiv_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("arxiv")
    arxiv_prefix = "10.48550/arXiv."
    if not doi.startswith(arxiv_prefix):
        print("[ArXiv] DOI not arXiv prefix, skipping")
        return None
    arxiv_id = doi[len(arxiv_prefix):]
    pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[ArXiv] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "ArXiv", "source": "ArXiv"}
        else:
            print("[ArXiv] PDF not found")
    except Exception as e:
        print(f"[ArXiv] PDF fetch error: {e}")
    return None

async def get_biorxiv_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("biorxiv")
    if not doi.startswith("10.1101"):
        print("[bioRxiv] DOI not bioRxiv prefix, skipping")
        return None

    pdf_url = f"https://www.biorxiv.org/content/{doi}.full.pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[bioRxiv] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "bioRxiv", "source": "bioRxiv"}
        else:
            print("[bioRxiv] PDF not found")
    except Exception as e:
        print(f"[bioRxiv] PDF fetch error: {e}")
    return None

async def get_medrxiv_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("medrxiv")
    if not doi.startswith("10.1101"):
        print("[medRxiv] DOI not medRxiv prefix, skipping")
        return None

    pdf_url = f"https://www.medrxiv.org/content/{doi}.full.pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[medRxiv] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "medRxiv", "source": "medRxiv"}
        else:
            print("[medRxiv] PDF not found")
    except Exception as e:
        print(f"[medRxiv] PDF fetch error: {e}")
    return None

async def get_chemrxiv_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("chemrxiv")
    if not doi.startswith("10.26434"):
        print("[ChemRxiv] DOI not ChemRxiv prefix, skipping")
        return None

    pdf_url = f"https://chemrxiv.org/engage/api-gateway/chemrxiv/assets/file/{doi}/content"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[ChemRxiv] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "ChemRxiv", "source": "ChemRxiv"}
        else:
            print("[ChemRxiv] PDF not found")
    except Exception as e:
        print(f"[ChemRxiv] PDF fetch error: {e}")
    return None

async def get_f1000_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("f1000")
    if not doi.startswith("10.12688"):
        print("[F1000] DOI not F1000 prefix, skipping")
        return None

    pdf_url = f"https://f1000research.com/articles/{doi.split('/')[-1]}/pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[F1000] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "F1000", "source": "F1000"}
        else:
            print("[F1000] PDF not found")
    except Exception as e:
        print(f"[F1000] PDF fetch error: {e}")
    return None

async def get_elife_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("elife")
    if not doi.startswith("10.7554"):
        print("[eLife] DOI not eLife prefix, skipping")
        return None

    pdf_url = f"https://elifesciences.org/articles/{doi.split('/')[-1]}/pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[eLife] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "eLife", "source": "eLife"}
        else:
            print("[eLife] PDF not found")
    except Exception as e:
        print(f"[eLife] PDF fetch error: {e}")
    return None

async def get_cell_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("cell")
    if not doi.startswith("10.1016"):
        print("[Cell] DOI not Cell Press prefix, skipping")
        return None

    pdf_url = f"https://www.cell.com/article/{doi}/pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[Cell] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "Cell", "source": "Cell"}
        else:
            print("[Cell] PDF not found")
    except Exception as e:
        print(f"[Cell] PDF fetch error: {e}")
    return None

async def get_frontiers_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("frontiers")
    if not doi.startswith("10.3389"):
        print("[Frontiers] DOI not Frontiers prefix, skipping")
        return None

    pdf_url = f"https://www.frontiersin.org/articles/{doi}/pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[Frontiers] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "Frontiers", "source": "Frontiers"}
        else:
            print("[Frontiers] PDF not found")
    except Exception as e:
        print(f"[Frontiers] PDF fetch error: {e}")
    return None

async def get_mdpi_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("mdpi")
    if not doi.startswith("10.3390"):
        print("[MDPI] DOI not MDPI prefix, skipping")
        return None

    pdf_url = f"https://www.mdpi.com/{doi.split('/')[-1]}/pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[MDPI] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "MDPI", "source": "MDPI"}
        else:
            print("[MDPI] PDF not found")
    except Exception as e:
        print(f"[MDPI] PDF fetch error: {e}")
    return None

async def get_hindawi_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("hindawi")
    if not doi.startswith("10.1155"):
        print("[Hindawi] DOI not Hindawi prefix, skipping")
        return None

    pdf_url = f"https://downloads.hindawi.com/journals/{doi.split('/')[-2]}/{doi.split('/')[-1]}.pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[Hindawi] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "Hindawi", "source": "Hindawi"}
        else:
            print("[Hindawi] PDF not found")
    except Exception as e:
        print(f"[Hindawi] PDF fetch error: {e}")
    return None

async def get_copernicus_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("copernicus")
    if not doi.startswith("10.5194"):
        print("[Copernicus] DOI not Copernicus prefix, skipping")
        return None

    pdf_url = f"https://{doi.split('/')[-2]}.copernicus.org/articles/{doi.split('/')[-1]}.pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[Copernicus] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "Copernicus", "source": "Copernicus"}
        else:
            print("[Copernicus] PDF not found")
    except Exception as e:
        print(f"[Copernicus] PDF fetch error: {e}")
    return None

async def get_iop_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("iop")
    if not doi.startswith("10.1088"):
        print("[IOP] DOI not IOP prefix, skipping")
        return None

    pdf_url = f"https://iopscience.iop.org/article/{doi}/pdf"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[IOP] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "IOP", "source": "IOP"}
        else:
            print("[IOP] PDF not found")
    except Exception as e:
        print(f"[IOP] PDF fetch error: {e}")
    return None

async def get_aps_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("aps")
    if not doi.startswith("10.1103"):
        print("[APS] DOI not APS prefix, skipping")
        return None

    pdf_url = f"https://journals.aps.org/{doi.split('/')[-2]}/pdf/{doi.split('/')[-1]}"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[APS] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "APS", "source": "APS"}
        else:
            print("[APS] PDF not found")
    except Exception as e:
        print(f"[APS] PDF fetch error: {e}")
    return None

async def get_aip_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("aip")
    if not doi.startswith("10.1063"):
        print("[AIP] DOI not AIP prefix, skipping")
        return None

    pdf_url = f"https://aip.scitation.org/doi/pdf/{doi}"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[AIP] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "AIP", "source": "AIP"}
        else:
            print("[AIP] PDF not found")
    except Exception as e:
        print(f"[AIP] PDF fetch error: {e}")
    return None

async def get_rsc_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("rsc")
    if not doi.startswith("10.1039"):
        print("[RSC] DOI not RSC prefix, skipping")
        return None

    pdf_url = f"https://pubs.rsc.org/en/content/articlepdf/{doi}"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[RSC] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "RSC", "source": "RSC"}
        else:
            print("[RSC] PDF not found")
    except Exception as e:
        print(f"[RSC] PDF fetch error: {e}")
    return None

async def get_acs_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("acs")
    if not doi.startswith("10.1021"):
        print("[ACS] DOI not ACS prefix, skipping")
        return None

    pdf_url = f"https://pubs.acs.org/doi/pdf/{doi}"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[ACS] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "ACS", "source": "ACS"}
        else:
            print("[ACS] PDF not found")
    except Exception as e:
        print(f"[ACS] PDF fetch error: {e}")
    return None

async def get_ieee_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("ieee")
    if not doi.startswith("10.1109"):
        print("[IEEE] DOI not IEEE prefix, skipping")
        return None

    pdf_url = f"https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber={doi.split('/')[-1]}"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[IEEE] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "IEEE", "source": "IEEE"}
        else:
            print("[IEEE] PDF not found")
    except Exception as e:
        print(f"[IEEE] PDF fetch error: {e}")
    return None

async def get_acm_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("acm")
    if not doi.startswith("10.1145"):
        print("[ACM] DOI not ACM prefix, skipping")
        return None

    pdf_url = f"https://dl.acm.org/doi/pdf/{doi}"

    try:
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[ACM] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "ACM", "source": "ACM"}
        else:
            print("[ACM] PDF not found")
    except Exception as e:
        print(f"[ACM] PDF fetch error: {e}")
    return None

async def get_springer_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("springer")
    print(f"[Springer] Fetching PDF for DOI: {doi}")
    url = f"https://link.springer.com/content/pdf/{quote(doi)}.pdf"
    try:
        r = await client.head(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[Springer] PDF URL found: {url}")
            return {"pdf_url": url, "host_type": "Springer", "source": "Springer"}
        else:
            article_url = f"https://link.springer.com/article/{quote(doi)}"
            direct_pdf = await extract_pdf_from_page(article_url, client)
            if direct_pdf:
                print(f"[Springer] Direct PDF extracted from page: {direct_pdf}")
                return {"pdf_url": direct_pdf, "host_type": "Springer", "source": "Springer"}
            
            print("[Springer] PDF not found")
    except Exception as e:
        print(f"[Springer] PDF fetch error: {e}")
    return None

async def get_elsevier_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("elsevier")
    print(f"[Elsevier] Fetching PDF for DOI: {doi}")
    url = f"https://www.sciencedirect.com/science/article/pii/{quote(doi)}"
    try:
        direct_pdf = await extract_pdf_from_page(url, client)
        if direct_pdf:
            print(f"[Elsevier] Direct PDF extracted from page: {direct_pdf}")
            return {"pdf_url": direct_pdf, "host_type": "Elsevier", "source": "Elsevier"}
        
        print("[Elsevier] No valid PDF link found")
    except Exception as e:
        print(f"[Elsevier] PDF fetch error: {e}")
    return None

async def get_wiley_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("wiley")
    print(f"[Wiley] Fetching PDF for DOI: {doi}")
    url = f"https://onlinelibrary.wiley.com/doi/pdfdirect/{quote(doi)}"
    try:
        r = await client.head(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[Wiley] PDF URL found: {url}")
            return {"pdf_url": url, "host_type": "Wiley", "source": "Wiley"}
        else:
            article_url = f"https://onlinelibrary.wiley.com/doi/{quote(doi)}"
            direct_pdf = await extract_pdf_from_page(article_url, client)
            if direct_pdf:
                print(f"[Wiley] Direct PDF extracted from page: {direct_pdf}")
                return {"pdf_url": direct_pdf, "host_type": "Wiley", "source": "Wiley"}
            
            print("[Wiley] PDF not found")
    except Exception as e:
        print(f"[Wiley] PDF fetch error: {e}")
    return None

async def get_nature_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("nature")
    print(f"[Nature] Fetching PDF for DOI: {doi}")
    url = f"https://www.nature.com/articles/{quote(doi)}.pdf"
    try:
        r = await client.head(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[Nature] PDF URL found: {url}")
            return {"pdf_url": url, "host_type": "Nature", "source": "Nature"}
        else:
            article_url = f"https://www.nature.com/articles/{quote(doi)}"
            direct_pdf = await extract_pdf_from_page(article_url, client)
            if direct_pdf:
                print(f"[Nature] Direct PDF extracted from page: {direct_pdf}")
                return {"pdf_url": direct_pdf, "host_type": "Nature", "source": "Nature"}
            
            print("[Nature] PDF not found")
    except Exception as e:
        print(f"[Nature] PDF fetch error: {e}")
    return None

async def get_science_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("science")
    print(f"[Science] Fetching PDF for DOI: {doi}")
    url = f"https://www.science.org/doi/pdf/{quote(doi)}"
    try:
        r = await client.head(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[Science] PDF URL found: {url}")
            return {"pdf_url": url, "host_type": "Science", "source": "Science"}
        else:
            article_url = f"https://www.science.org/doi/{quote(doi)}"
            direct_pdf = await extract_pdf_from_page(article_url, client)
            if direct_pdf:
                print(f"[Science] Direct PDF extracted from page: {direct_pdf}")
                return {"pdf_url": direct_pdf, "host_type": "Science", "source": "Science"}
            
            print("[Science] PDF not found")
    except Exception as e:
        print(f"[Science] PDF fetch error: {e}")
    return None

async def get_jstor_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("jstor")
    print(f"[JSTOR] Fetching PDF for DOI: {doi}")
    url = f"https://www.jstor.org/action/doBasicSearch?Query={quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        content = r.text
        
        patterns = [
            r'href=["\']([^"\']*\.pdf)["\']',
            r'["\']([^"\']*\.pdf)["\']',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match.startswith("/"):
                    base_url = "https://www.jstor.org"
                    match = base_url + match
                elif not match.startswith("http"):
                    base_url = "https://www.jstor.org"
                    match = base_url + "/" + match
                
                if await verify_pdf_url(match, client):
                    print(f"[JSTOR] PDF URL found: {match}")
                    return {"pdf_url": match, "host_type": "JSTOR", "source": "JSTOR"}
        
        print("[JSTOR] No valid PDF link found")
    except Exception as e:
        print(f"[JSTOR] PDF fetch error: {e}")
    return None

async def get_ssrn_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("ssrn")
    print(f"[SSRN] Fetching PDF for DOI: {doi}")
    url = f"https://papers.ssrn.com/sol3/Delivery.cfm/{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        content = r.text
        
        patterns = [
            r'href=["\']([^"\']*\.pdf)["\']',
            r'["\']([^"\']*\.pdf)["\']',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match.startswith("/"):
                    base_url = "https://papers.ssrn.com"
                    match = base_url + match
                elif not match.startswith("http"):
                    base_url = "https://papers.ssrn.com"
                    match = base_url + "/" + match
                
                if await verify_pdf_url(match, client):
                    print(f"[SSRN] PDF URL found: {match}")
                    return {"pdf_url": match, "host_type": "SSRN", "source": "SSRN"}
        
        print("[SSRN] No valid PDF link found")
    except Exception as e:
        print(f"[SSRN] PDF fetch error: {e}")
    return None

async def get_repec_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("repec")
    print(f"[RePEc] Fetching PDF for DOI: {doi}")
    url = f"https://api.repec.org/cgibin/getref?doi={quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        content = r.text
        
        patterns = [
            r'href=["\']([^"\']*\.pdf)["\']',
            r'["\']([^"\']*\.pdf)["\']',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match.startswith("/"):
                    base_url = "https://ideas.repec.org"
                    match = base_url + match
                elif not match.startswith("http"):
                    base_url = "https://ideas.repec.org"
                    match = base_url + "/" + match
                
                if await verify_pdf_url(match, client):
                    print(f"[RePEc] PDF URL found: {match}")
                    return {"pdf_url": match, "host_type": "RePEc", "source": "RePEc"}
        
        print("[RePEc] No valid PDF link found")
    except Exception as e:
        print(f"[RePEc] PDF fetch error: {e}")
    return None

async def get_pmc_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("pmc")
    print(f"[PMC] Fetching PDF for DOI: {doi}")
    url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids={quote(doi)}&format=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        records = data.get("records", [])
        if not records:
            print("[PMC] No PMC ID found for DOI")
            return None
        
        pmc_id = records[0].get("pmcid")
        if not pmc_id:
            print("[PMC] No PMC ID found in record")
            return None
        
        pdf_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/pdf/{pmc_id}.pdf"
        if await verify_pdf_url(pdf_url, client):
            print(f"[PMC] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "PMC", "source": "PMC"}
        
        pdf_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/pdf"
        if await verify_pdf_url(pdf_url, client):
            print(f"[PMC] Alternative PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "PMC", "source": "PMC"}
        
        article_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}"
        direct_pdf = await extract_pdf_from_page(article_url, client)
        if direct_pdf:
            print(f"[PMC] Direct PDF extracted from page: {direct_pdf}")
            return {"pdf_url": direct_pdf, "host_type": "PMC", "source": "PMC"}
        
        print("[PMC] No valid PDF link found")
    except Exception as e:
        print(f"[PMC] PDF fetch error: {e}")
    return None

async def get_citeseerx_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("citeseerx")
    print(f"[CiteSeerX] Fetching PDF for DOI: {doi}")
    url = f"http://citeseerx.ist.psu.edu/search?q={quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        content = r.text
        
        patterns = [
            r'href=["\']([^"\']*\.pdf)["\']',
            r'["\']([^"\']*\.pdf)["\']',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match.startswith("/"):
                    base_url = "http://citeseerx.ist.psu.edu"
                    match = base_url + match
                elif not match.startswith("http"):
                    base_url = "http://citeseerx.ist.psu.edu"
                    match = base_url + "/" + match
                
                if await verify_pdf_url(match, client):
                    print(f"[CiteSeerX] PDF URL found: {match}")
                    return {"pdf_url": match, "host_type": "CiteSeerX", "source": "CiteSeerX"}
        
        print("[CiteSeerX] No valid PDF link found")
    except Exception as e:
        print(f"[CiteSeerX] PDF fetch error: {e}")
    return None

async def get_researchgate_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("researchgate")
    print(f"[ResearchGate] Fetching PDF for DOI: {doi}")
    url = f"https://www.researchgate.net/publication/{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        content = r.text
        
        patterns = [
            r'href=["\']([^"\']*\.pdf)["\']',
            r'["\']([^"\']*\.pdf)["\']',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match.startswith("/"):
                    base_url = "https://www.researchgate.net"
                    match = base_url + match
                elif not match.startswith("http"):
                    base_url = "https://www.researchgate.net"
                    match = base_url + "/" + match
                
                if await verify_pdf_url(match, client):
                    print(f"[ResearchGate] PDF URL found: {match}")
                    return {"pdf_url": match, "host_type": "ResearchGate", "source": "ResearchGate"}
        
        print("[ResearchGate] No valid PDF link found")
    except Exception as e:
        print(f"[ResearchGate] PDF fetch error: {e}")
    return None

async def get_plos_pdf_and_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("plos")
    url = f"http://api.plos.org/search?q=doi:{quote(doi)}&fl=id,title,author,publication_date,journal&wt=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        if not docs:
            print("[PLOS] No results found")
            return None
        doc = docs[0]
        
        article_id = doc.get("id")
        if not article_id:
            print("[PLOS] No article ID found")
            return None
            
        journal = doc.get("journal")
        if not journal:
            journal = "plosone"
            
        pdf_url = f"https://journals.plos.org/{journal}/article/file?id={article_id}&type=printable"
        
        try:
            pdf_response = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
            if pdf_response.status_code != 200:
                print(f"[PLOS] PDF URL not accessible: {pdf_url}, status: {pdf_response.status_code}")
                pdf_url = f"https://journals.plos.org/{journal}/article/file?id={article_id}&type=full"
                pdf_response = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
                if pdf_response.status_code != 200:
                    print(f"[PLOS] Alternative PDF URL not accessible: {pdf_url}, status: {pdf_response.status_code}")
                    return None
        except Exception as e:
            print(f"[PLOS] Error checking PDF URL: {e}")
            return None
            
        print(f"[PLOS] PDF URL found: {pdf_url}")
        metadata = {
            "title": doc.get("title"),
            "authors": [{"name": a} for a in doc.get("author", [])],
            "corresponding_email": None,
            "journal": journal,
            "year": doc.get("publication_date", "")[:4],
        }
        return {"pdf_url": pdf_url, "host_type": "PLOS", "source": "PLOS", "metadata": metadata}
    except Exception as e:
        print(f"[PLOS] Fetch error: {e}")
    return None

async def get_share_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, str]]:
    await rate_limit("share")
    print(f"[Share API] Fetching PDF for DOI: {doi}")
    base_url = "https://share.osf.io/api/v2/search/"
    params = {
        "q": f"doi:{doi}",
        "page[size]": 5
    }
    try:
        r = await client.get(base_url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        results = data.get('data', [])
        for item in results:
            attrs = item.get('attributes', {})

            sources = attrs.get('sources', [])
            for source in sources:
                url = source.get('url')
                if url and url.lower().endswith('.pdf') and await verify_pdf_url(url, client):
                    print(f"[Share API] PDF URL found in sources: {url}")
                    return {"pdf_url": url, "host_type": "Share API", "source": "Share"}
                elif url and url.lower().endswith('.pdf'):
                    direct_pdf = await extract_pdf_from_page(url, client)
                    if direct_pdf:
                        print(f"[Share API] Direct PDF extracted from sources: {direct_pdf}")
                        return {"pdf_url": direct_pdf, "host_type": "Share API", "source": "Share"}

            fulltext_url = attrs.get('fulltext')
            if fulltext_url and fulltext_url.lower().endswith('.pdf') and await verify_pdf_url(fulltext_url, client):
                print(f"[Share API] PDF URL found in fulltext: {fulltext_url}")
                return {"pdf_url": fulltext_url, "host_type": "Share API", "source": "Share"}
            elif fulltext_url and fulltext_url.lower().endswith('.pdf'):
                direct_pdf = await extract_pdf_from_page(fulltext_url, client)
                if direct_pdf:
                    print(f"[Share API] Direct PDF extracted from fulltext: {direct_pdf}")
                    return {"pdf_url": direct_pdf, "host_type": "Share API", "source": "Share"}

            links = attrs.get('links', {})
            for key in ['pdf', 'html']:
                link_url = links.get(key)
                if link_url and link_url.lower().endswith('.pdf') and await verify_pdf_url(link_url, client):
                    print(f"[Share API] PDF URL found in links[{key}]: {link_url}")
                    return {"pdf_url": link_url, "host_type": "Share API", "source": "Share"}
                elif link_url and link_url.lower().endswith('.pdf'):
                    direct_pdf = await extract_pdf_from_page(link_url, client)
                    if direct_pdf:
                        print(f"[Share API] Direct PDF extracted from links[{key}]: {direct_pdf}")
                        return {"pdf_url": direct_pdf, "host_type": "Share API", "source": "Share"}

        print("[Share API] No valid PDF link found")
    except Exception as e:
        print(f"[Share API] PDF fetch error: {e}")
    return None

async def get_internetarchive_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("internetarchive")
    print(f"[Internet Archive] Fetching PDF for DOI: {doi}")
    url = f"https://archive.org/advancedsearch.php?q=doi:{quote(doi)}&fl[]=identifier&fl[]=title&fl[]=downloads&fl[]=mediatype&output=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        results = r.json().get("response", {}).get("docs", [])
        for doc in results:
            identifier = doc.get("identifier")
            if identifier:
                pdf_url = f"https://archive.org/download/{identifier}/{identifier}.pdf"
                head = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
                if head.status_code == 200:
                    print(f"[Internet Archive] PDF URL found: {pdf_url}")
                    return {"pdf_url": pdf_url, "host_type": "Internet Archive", "source": "Internet Archive"}
        print("[Internet Archive] No valid PDF found")
    except Exception as e:
        print(f"[Internet Archive] PDF fetch error: {e}")
    return None

async def get_hal_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("hal")
    print(f"[HAL] Fetching PDF for DOI: {doi}")
    url = f"https://api.archives-ouvertes.fr/search/?q=doiId_s:{quote(doi)}&fl=doiId_s,uri_s,fileMain_s,title_s,authFullName_s&wt=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        docs = data.get("response", {}).get("docs", [])
        if not docs:
            print("[HAL] No results found")
            return None
        
        doc = docs[0]
        pdf_url = doc.get("fileMain_s")
        if pdf_url:
            if await verify_pdf_url(pdf_url, client):
                print(f"[HAL] PDF URL found: {pdf_url}")
                return {"pdf_url": pdf_url, "host_type": "HAL", "source": "HAL"}
            else:
                direct_pdf = await extract_pdf_from_page(pdf_url, client)
                if direct_pdf:
                    print(f"[HAL] Direct PDF extracted from page: {direct_pdf}")
                    return {"pdf_url": direct_pdf, "host_type": "HAL", "source": "HAL"}
        
        print("[HAL] No valid PDF link found")
    except Exception as e:
        print(f"[HAL] PDF fetch error: {e}")
    return None

async def get_openaire_pdf_and_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("openaire")
    print(f"[OpenAIRE] Fetching PDF and metadata for DOI: {doi}")
    url = f"https://api.openaire.eu/search/publications?doi:{quote(doi)}&format=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            print("[OpenAIRE] No results found")
            return None
        pub = results[0]
        fulltexts = pub.get("result", {}).get("fulltexts", [])
        for ft in fulltexts:
            if "url" in ft and ft.get("mediaType", "").lower() == "application/pdf":
                pdf_url = ft["url"]
                if await verify_pdf_url(pdf_url, client):
                    print(f"[OpenAIRE] PDF URL found: {pdf_url}")
                    metadata = {
                        "title": pub.get("result", {}).get("title"),
                        "authors": [{"name": a.get("name")} for a in pub.get("result", {}).get("creators", [])],
                        "corresponding_email": None,
                        "journal": pub.get("result", {}).get("publisher"),
                        "year": pub.get("result", {}).get("publicationYear"),
                    }
                    return {"pdf_url": pdf_url, "host_type": "OpenAIRE", "source": "OpenAIRE", "metadata": metadata}
                else:
                    direct_pdf = await extract_pdf_from_page(pdf_url, client)
                    if direct_pdf:
                        print(f"[OpenAIRE] Direct PDF extracted from page: {direct_pdf}")
                        metadata = {
                            "title": pub.get("result", {}).get("title"),
                            "authors": [{"name": a.get("name")} for a in pub.get("result", {}).get("creators", [])],
                            "corresponding_email": None,
                            "journal": pub.get("result", {}).get("publisher"),
                            "year": pub.get("result", {}).get("publicationYear"),
                        }
                        return {"pdf_url": direct_pdf, "host_type": "OpenAIRE", "source": "OpenAIRE", "metadata": metadata}
        print("[OpenAIRE] No valid PDF found")
    except httpx.HTTPStatusError as e:
        print(f"[OpenAIRE] HTTP error: {e}")
    except Exception as e:
        print(f"[OpenAIRE] PDF fetch error: {e}")
    return None

async def get_doaj_metadata_and_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    await rate_limit("doaj")
    url = f"https://doaj.org/api/v2/search/articles/doi:{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            print("[DOAJ] No results found")
            return None
        article = results[0].get("bibjson", {})
        pdf_url = None
        for link in article.get("link", []):
            if link.get("url") and link.get("content_type") == "application/pdf":
                pdf_url = link["url"]
                break
        if pdf_url:
            if await verify_pdf_url(pdf_url, client):
                print(f"[DOAJ] PDF URL found: {pdf_url}")
                metadata = {
                    "title": article.get("title"),
                    "authors": [{"name": a.get("name")} for a in article.get("author", [])],
                    "corresponding_email": None,
                    "journal": article.get("journal", {}).get("title"),
                    "year": article.get("year"),
                }
                return {"pdf_url": pdf_url, "host_type": "DOAJ", "source": "DOAJ", "metadata": metadata}
            else:
                direct_pdf = await extract_pdf_from_page(pdf_url, client)
                if direct_pdf:
                    print(f"[DOAJ] Direct PDF extracted from page: {direct_pdf}")
                    metadata = {
                        "title": article.get("title"),
                        "authors": [{"name": a.get("name")} for a in article.get("author", [])],
                        "corresponding_email": None,
                        "journal": article.get("journal", {}).get("title"),
                        "year": article.get("year"),
                    }
                    return {"pdf_url": direct_pdf, "host_type": "DOAJ", "source": "DOAJ", "metadata": metadata}
        print("[DOAJ] No PDF found")
    except Exception as e:
        print(f"[DOAJ] Fetch error: {e}")
    return None

PDF_SOURCES_PRIORITY = [
    "arxiv", "biorxiv", "medrxiv", "chemrxiv", "f1000", "elife",
    "unpaywall", "europepmc", "pmc", "zenodo", "figshare", "doaj", "openaire",
    "plos", "frontiers", "mdpi", "hindawi", "copernicus", "elife", "f1000",
    "base", "hal", "internetarchive",
    "doi",
    "springer", "elsevier", "wiley", "nature", "science", "cell",
    "iop", "aps", "aip", "rsc", "acs", "ieee", "acm",
    "researchgate", "ssrn", "repec", "citeseerx",
    "jstor", "share",
]

METADATA_SOURCES_PRIORITY = [
    "crossref", "openalex", "semantic_scholar", "pubmed",
    "openaire", "doaj", "dryad", "internetarchive",
    "wikidata", "google_books",
]

PDF_SOURCE_FUNCTIONS = {
    "doi": get_pdf_url_from_doi,
    "unpaywall": get_unpaywall_pdf,
    "europepmc": get_europepmc_pdf,
    "base": get_base_pdf,
    "zenodo": get_zenodo_pdf,
    "figshare": get_figshare_pdf,
    "arxiv": get_arxiv_pdf,
    "biorxiv": get_biorxiv_pdf,
    "medrxiv": get_medrxiv_pdf,
    "chemrxiv": get_chemrxiv_pdf,
    "f1000": get_f1000_pdf,
    "elife": get_elife_pdf,
    "cell": get_cell_pdf,
    "frontiers": get_frontiers_pdf,
    "mdpi": get_mdpi_pdf,
    "hindawi": get_hindawi_pdf,
    "copernicus": get_copernicus_pdf,
    "iop": get_iop_pdf,
    "aps": get_aps_pdf,
    "aip": get_aip_pdf,
    "rsc": get_rsc_pdf,
    "acs": get_acs_pdf,
    "ieee": get_ieee_pdf,
    "acm": get_acm_pdf,
    "springer": get_springer_pdf,
    "elsevier": get_elsevier_pdf,
    "wiley": get_wiley_pdf,
    "nature": get_nature_pdf,
    "science": get_science_pdf,
    "jstor": get_jstor_pdf,
    "plos": get_plos_pdf_and_metadata,
    "ssrn": get_ssrn_pdf,
    "repec": get_repec_pdf,
    "pmc": get_pmc_pdf,
    "citeseerx": get_citeseerx_pdf,
    "researchgate": get_researchgate_pdf,
    "share": get_share_pdf,
    "internetarchive": get_internetarchive_pdf,
    "hal": get_hal_pdf,
    "openaire": get_openaire_pdf_and_metadata,
    "doaj": get_doaj_metadata_and_pdf,
}

METADATA_SOURCE_FUNCTIONS = {
    "crossref": get_crossref_metadata,
    "openalex": get_openalex_metadata,
    "semantic_scholar": get_semantic_scholar_metadata,
    "pubmed": get_pubmed_metadata,
    "openaire": get_openaire_metadata,
    "doaj": get_doaj_metadata,
    "dryad": get_dryad_metadata,
    "internetarchive": get_internetarchive_metadata,
    "wikidata": get_wikidata_metadata,
    "google_books": get_google_books_metadata,
    "plos": get_plos_pdf_and_metadata,
}

def check_and_increment_google_books() -> bool:
    return True

@app.post("/api/search")
async def search(data: dict):
    doi = data.get("doi")
    if not doi:
        raise HTTPException(status_code=400, detail="DOI is required")

    try:
        client = app.state.client
        
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
        async def limited_fetch(source_name, fetch_func, *args, **kwargs):
            async with semaphore:
                try:
                    return await fetch_func(*args, **kwargs)
                except asyncio.CancelledError:
                    print(f"[{source_name}] Task cancelled")
                    return None
                except Exception as e:
                    print(f"[{source_name}] Error: {e}")
                    return None
        
        pdf_tasks = []
        for source in PDF_SOURCES_PRIORITY:
            if source in PDF_SOURCE_FUNCTIONS:
                task = asyncio.create_task(
                    limited_fetch(source, PDF_SOURCE_FUNCTIONS[source], doi, client)
                )
                pdf_tasks.append((source, task))
        
        metadata_tasks = []
        for source in METADATA_SOURCES_PRIORITY:
            if source in METADATA_SOURCE_FUNCTIONS:
                task = asyncio.create_task(
                    limited_fetch(source, METADATA_SOURCE_FUNCTIONS[source], doi, client)
                )
                metadata_tasks.append((source, task))
        
        pdf_result = None
        metadata = None
        
        for source_name, task in pdf_tasks:
            try:
                result = await task
                if result and result.get("pdf_url"):
                    pdf_result = {
                        "pdf_url": result["pdf_url"],
                        "host_type": result.get("host_type", source_name),
                        "source": result.get("source", source_name),
                    }
                    print(f"[Found PDF] from {source_name}: {pdf_result['pdf_url']}")
                    
                    if result.get("metadata"):
                        metadata = result["metadata"]
                    
                    break
            except Exception as e:
                print(f"[{source_name}] Error: {e}")
        
        if pdf_result:
            for _, task in pdf_tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
        
        try:
            active_metadata_tasks = [task for _, task in metadata_tasks if not task.done()]
            
            if active_metadata_tasks:
                done, pending = await asyncio.wait(
                    active_metadata_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=3.0
                )
                
                for task in done:
                    try:
                        result = task.result()
                        if result:
                            if metadata:
                                metadata = merge_metadata(metadata, result)
                            else:
                                metadata = result
                            break
                    except Exception:
                        pass
                
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                
                if not metadata:
                    still_active_tasks = [task for _, task in metadata_tasks if not task.done()]
                    
                    if still_active_tasks:
                        done, pending = await asyncio.wait(
                            still_active_tasks,
                            timeout=2.0
                        )
                        
                        for task in done:
                            try:
                                result = task.result()
                                if result:
                                    if metadata:
                                        metadata = merge_metadata(metadata, result)
                                    else:
                                        metadata = result
                                    break
                            except Exception:
                                pass
                        
                        for task in pending:
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
        except Exception as e:
            print(f"[Search API Error] {e}")
        
        if metadata is None:
            metadata = {}
            no_meta_message = "No metadata found"
        else:
            no_meta_message = None
        
        title = metadata.get("title", "Unknown Title")
        authors = metadata.get("authors", [])
        author_name = "Unknown"
        if authors and isinstance(authors, list) and isinstance(authors[0], dict):
            author_name = authors[0].get("name", "Unknown")
        
        gc.collect()
        
        if pdf_result:
            return {
                "message": "Paper found!" if metadata else no_meta_message,
                "pdf_link": pdf_result["pdf_url"],
                "host_type": pdf_result.get("host_type", "unknown"),
                "source": pdf_result.get("source", "unknown"),
                "metadata": metadata,
            }
        else:
            return {
                "message": no_meta_message or "Couldn't find paper.",
                "metadata": metadata,
            }

    except Exception as e:
        print(f"[Search API Error] {e}")
        raise HTTPException(status_code=500, detail=str(e))