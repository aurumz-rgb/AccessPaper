import os
import json
import asyncio
from datetime import datetime
from threading import RLock
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, List
from urllib.parse import quote as url_quote
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv


app = FastAPI()

load_dotenv()

REQUEST_TIMEOUT = 5  # seconds

BASE_API_ENABLED = os.getenv("BASE_API_ENABLED", "")
CORE_API_KEY = os.getenv("CORE_API_KEY", "")
GOOGLE_BOOKS_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY", "")
UNPAYWALL_EMAIL = os.getenv("UNPAYWALL_EMAIL")

# Crossref / Paper processing tracking
_lock = RLock()
_processed_dois = set()


def quote(text: Optional[str]) -> str:
    return url_quote(text or "")

async def verify_pdf(url: str, client: httpx.AsyncClient) -> bool:
    try:
        r = await client.head(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False



async def get_crossref_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
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



async def get_pdf_url_from_doi(doi: str, client: httpx.AsyncClient) -> Dict[str, str]:
    """
    Returns a dictionary with:
    - pdf_url: direct PDF link if available
    - publisher_url: publisher page URL
    """
    print(f"[DOI] Fetching for DOI: {doi}", flush=True)
    doi_url = f"https://doi.org/{doi}"
    result = {"pdf_url": None, "publisher_url": None}

    try:
        resp = await client.get(doi_url, follow_redirects=True, timeout=REQUEST_TIMEOUT)
        final_url = str(resp.url)
        content_type = resp.headers.get("content-type", "").lower()

        if final_url.lower().endswith(".pdf") or "pdf" in content_type:
            result["pdf_url"] = final_url
        else:
            result["publisher_url"] = final_url

        if "arxiv.org" in final_url and "/abs/" in final_url:
            result["pdf_url"] = final_url.replace("/abs/", "/pdf/") + ".pdf"

    except Exception as e:
        print(f"[PDF Check] Error checking DOI: {e}", flush=True)

    return result




async def get_openalex_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
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
    except Exception as e:
        print(f"[Semantic Scholar] metadata fetch error: {e}")
        return None



async def get_unpaywall_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[Unpaywall] Fetching PDF for DOI: {doi}")
    url = f"https://api.unpaywall.org/v2/{quote(doi)}?email={UNPAYWALL_EMAIL}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        
        # First try best_oa_location
        loc = data.get("best_oa_location")
        if loc:
            pdf_url = loc.get("url_for_pdf")
            if pdf_url:
                print(f"[Unpaywall] PDF URL found in best_oa_location: {pdf_url}")
                return {"pdf_url": pdf_url, "host_type": loc.get("host_type"), "source": "Unpaywall"}

        # If best_oa_location missing or no pdf_url, check all oa_locations
        oa_locations = data.get("oa_locations", [])
        for location in oa_locations:
            pdf_url = location.get("url_for_pdf")
            if pdf_url:
                print(f"[Unpaywall] PDF URL found in oa_locations: {pdf_url}")
                return {"pdf_url": pdf_url, "host_type": location.get("host_type"), "source": "Unpaywall"}

        print("[Unpaywall] No valid PDF link found in any location")
    except Exception as e:
        print(f"[Unpaywall] PDF fetch error: {e}")
    return None



async def get_core_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[CORE] Fetching PDF for DOI: {doi}")
    if not CORE_API_KEY:
        print("[CORE] CORE API key missing, skipping")
        return None
    
    url = f"https://api.core.ac.uk/v3/search/works/?apiKey={CORE_API_KEY}&q=doi:{quote(doi)}"

    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = await r.json()
        results = data.get("results", [])
        if not results:
            print("[CORE] No results found")
            return None
        
        for item in results:
            full_text_urls = item.get("fullTextUrl", [])
            if not full_text_urls:
                continue
            for url_data in full_text_urls:
                url_ = url_data.get("url")
                if url_ and url_.lower().endswith(".pdf"):
                    # Use the global verify_pdf function
                    if await verify_pdf(url_, client):
                        print(f"[CORE] PDF URL verified: {url_}")
                        return {"pdf_url": url_, "host_type": "CORE", "source": "CORE"}
                    else:
                        print(f"[CORE] PDF URL not reachable: {url_}")
        
        print("[CORE] No valid PDF link found in results")
    except Exception as e:
        print(f"[CORE] PDF fetch error: {e}")
    return None



async def get_zenodo_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[Zenodo] Fetching PDF for DOI: {doi}")
    url = f"https://zenodo.org/api/records/?q=doi:{quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        hits = r.json().get("hits", {}).get("hits", [])
        for hit in hits:
            for f in hit.get("files", []):
                pdf_link = f.get("links", {}).get("self", "")
                if pdf_link.lower().endswith(".pdf"):
                    print(f"[Zenodo] PDF URL found: {pdf_link}")
                    return {"pdf_url": pdf_link, "host_type": "Zenodo", "source": "Zenodo"}
        print("[Zenodo] No valid PDF link found")
    except Exception as e:
        print(f"[Zenodo] PDF fetch error: {e}")
    return None

async def get_figshare_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
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
                    if download_url:
                        print(f"[Figshare] PDF URL found: {download_url}")
                        return {"pdf_url": download_url, "host_type": "Figshare", "source": "Figshare"}
        print("[Figshare] No valid PDF link found")
    except Exception as e:
        print(f"[Figshare] PDF fetch error: {e}")
    return None

async def get_europepmc_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[EuropePMC] Fetching PDF for DOI: {doi}")
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:{quote(doi)}&format=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        results = r.json().get("resultList", {}).get("result", [])
        for result in results:
            full_text_urls = result.get("fullTextUrlList", {}).get("fullTextUrl", [])
            for full_text_url in full_text_urls:
                if (
                    full_text_url.get("documentStyle") == "pdf"
                    and full_text_url.get("availability") == "OPEN_ACCESS"
                ):
                    pdf_link = full_text_url.get("url")
                    print(f"[EuropePMC] PDF URL found: {pdf_link}")
                    return {"pdf_url": pdf_link, "host_type": "EuropePMC", "source": "EuropePMC"}
        print("[EuropePMC] No valid PDF link found")
    except Exception as e:
        print(f"[EuropePMC] PDF fetch error: {e}")
    return None


BASE_API_ENABLED = True

async def get_base_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[BASE] Fetching PDF for DOI: {doi}")
    if not BASE_API_ENABLED:
        print("[BASE] Skipped: API access not enabled (IP not whitelisted)")
        return None

    url = (
        f"https://api.base-search.net/cgi-bin/BaseAPI.dll?"
        f"operation=searchRetrieve&query=doi={quote(doi)}&maximumRecords=1&recordSchema=dc"
    )
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        r.raise_for_status()

        root = ET.fromstring(r.content)
        ns = {"dc": "http://purl.org/dc/elements/1.1/"}

        for elem in root.iterfind(".//dc:identifier", ns):
            if elem.text and elem.text.lower().endswith(".pdf"):
                print(f"[BASE] PDF URL found: {elem.text}")
                return {"pdf_url": elem.text, "host_type": "BASE", "source": "BASE"}

        print("[BASE] No valid PDF link found in response")
    except Exception as e:
        print(f"[BASE] PDF fetch error: {e}")

    return None


async def get_openaire_pdf_and_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[OpenAIRE] Fetching PDF and metadata for DOI: {doi}")
    url = f"https://api.openaire.eu/search/publications?doi={quote(doi)}&format=json"
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
                print(f"[OpenAIRE] PDF URL found: {pdf_url}")
                metadata = {
                    "title": pub.get("result", {}).get("title"),
                    "authors": [{"name": a.get("name")} for a in pub.get("result", {}).get("creators", [])],
                    "corresponding_email": None,
                    "journal": pub.get("result", {}).get("publisher"),
                    "year": pub.get("result", {}).get("publicationYear"),
                }
                return {"pdf_url": pdf_url, "host_type": "OpenAIRE", "source": "OpenAIRE", "metadata": metadata}
        print("[OpenAIRE] No valid PDF found")
    except Exception as e:
        print(f"[OpenAIRE] PDF fetch error: {e}")
    return None


# 2. DOAJ metadata & PDF fetch
async def get_doaj_metadata_and_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
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
            print(f"[DOAJ] PDF URL found: {pdf_url}")
            metadata = {
                "title": article.get("title"),
                "authors": [{"name": a.get("name")} for a in article.get("author", [])],
                "corresponding_email": None,
                "journal": article.get("journal", {}).get("title"),
                "year": article.get("year"),
            }
            return {"pdf_url": pdf_url, "host_type": "DOAJ", "source": "DOAJ", "metadata": metadata}
        print("[DOAJ] No PDF found")
    except Exception as e:
        print(f"[DOAJ] Fetch error: {e}")
    return None

# 3. ArXiv PDF fetch
async def get_arxiv_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    # DOI for ArXiv papers usually start with "10.48550/arXiv."
    # Extract arXiv id from DOI if possible
    arxiv_prefix = "10.48550/arXiv."
    if not doi.startswith(arxiv_prefix):
        print("[ArXiv] DOI not arXiv prefix, skipping")
        return None
    arxiv_id = doi[len(arxiv_prefix):]
    pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    try:
        # Check if PDF exists
        r = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            print(f"[ArXiv] PDF URL found: {pdf_url}")
            return {"pdf_url": pdf_url, "host_type": "ArXiv", "source": "ArXiv"}
        else:
            print("[ArXiv] PDF not found")
    except Exception as e:
        print(f"[ArXiv] PDF fetch error: {e}")
    return None

# 4. bioRxiv PDF fetch via unofficial API (RSS feed)
async def get_biorxiv_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    # bioRxiv DOIs have prefix 10.1101
    if not doi.startswith("10.1101"):
        print("[bioRxiv] DOI not bioRxiv prefix, skipping")
        return None
    # Construct URL to try PDF
    pdf_url = doi.replace("doi.org", "biorxiv.org/content") + ".full.pdf"
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

# 5. medRxiv PDF fetch (same logic as bioRxiv)
async def get_medrxiv_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    if not doi.startswith("10.1101"):
        print("[medRxiv] DOI not medRxiv prefix, skipping")
        return None
    pdf_url = doi.replace("doi.org", "medrxiv.org/content") + ".full.pdf"
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

# 6. Internet Archive PDF fetch (search metadata + check for pdf)
async def get_internetarchive_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
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
                # Check PDF existence
                head = await client.head(pdf_url, timeout=REQUEST_TIMEOUT)
                if head.status_code == 200:
                    print(f"[Internet Archive] PDF URL found: {pdf_url}")
                    return {"pdf_url": pdf_url, "host_type": "Internet Archive", "source": "Internet Archive"}
        print("[Internet Archive] No valid PDF found")
    except Exception as e:
        print(f"[Internet Archive] PDF fetch error: {e}")
    return None


# 7. PLOS API for metadata and PDFs
async def get_plos_pdf_and_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    url = f"http://api.plos.org/search?q=doi:{quote(doi)}&fl=id,title,author,publication_date,journal&wt=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        if not docs:
            print("[PLOS] No results found")
            return None
        doc = docs[0]
        # PLOS PDFs are usually at this pattern
        pdf_url = f"http://journals.plos.org/plosone/article/file?id={quote(doi)}&type=printable"
        print(f"[PLOS] PDF URL found: {pdf_url}")
        metadata = {
            "title": doc.get("title"),
            "authors": [{"name": a} for a in doc.get("author", [])],
            "corresponding_email": None,
            "journal": doc.get("journal"),
            "year": doc.get("publication_date", "")[:4],
        }
        return {"pdf_url": pdf_url, "host_type": "PLOS", "source": "PLOS", "metadata": metadata}
    except Exception as e:
        print(f"[PLOS] Fetch error: {e}")
    return None




# 2. Crossref Event Data - Metadata only, no API key required
async def get_crossref_event_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[Crossref Event Data] Fetching event metadata for DOI: {doi}")
    url = f"https://api.eventdata.crossref.org/v1/events?obj-id={quote(doi)}"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        metadata = {"title": None, "authors": []}
        if data.get("message", {}).get("events"):
            metadata["title"] = f"Events count: {len(data['message']['events'])}"
        print(f"[Crossref Event Data] Metadata fetched: {metadata}")
        return metadata
    except Exception as e:
        print(f"[Crossref Event Data] Fetch error: {e}")
        return None


# 3. NLM PubMed/PMC Metadata - no key, IP whitelist (simple metadata fetch)
async def get_nlm_pubmed_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[NLM PubMed] Fetching metadata for DOI: {doi}")
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={quote(doi)}[DOI]&retmode=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        idlist = data.get("esearchresult", {}).get("idlist", [])
        if not idlist:
            print("[NLM PubMed] No PMID found for DOI")
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
        print(f"[NLM PubMed] Metadata fetched: {metadata}")
        return metadata
    except Exception as e:
        print(f"[NLM PubMed] Fetch error: {e}")
        return None


# 4. Dryad Metadata (open access)
async def get_dryad_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
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



# 5. CORE Text Mining Metadata + PDF (API key required)
async def get_core_pdf_and_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    if not CORE_API_KEY:
        print("[CORE] API key missing, skipping")
        return None
    url = f"https://api.core.ac.uk/v3/search/works?query=doi:{quote(doi)}"
    headers = {"Authorization": f"Bearer {CORE_API_KEY}"}
    try:
        r = await client.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        docs = data.get("results", [])
        if not docs:
            return None
        doc = docs[0]
        metadata = {
            "title": doc.get("title"),
            "authors": [{"name": a.get("name")} for a in doc.get("authors", [])],
        }
        pdf_url = doc.get("downloadUrl")
        result = {"metadata": metadata}
        if pdf_url:
            result.update({"pdf_url": pdf_url, "host_type": "CORE", "source": "CORE"})
            print(f"[CORE] PDF URL found: {pdf_url}")
        return result
    except Exception as e:
        print(f"[CORE] Fetch error: {e}")
        return None


# 6. OpenAIRE Datasets/Projects Metadata (no key)
async def get_openaire_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
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


# 7. Internet Archive Scholar Metadata (no key)
async def get_internetarchive_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
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


# 8. Europe PMC Grants (no key) — typically metadata only
async def get_europepmc_grants_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[Europe PMC Grants] Fetching metadata for DOI: {doi}")
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:{quote(doi)}+AND+grants&format=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        results = data.get("resultList", {}).get("result", [])
        if not results:
            print("[Europe PMC Grants] No results found")
            return None
        item = results[0]
        metadata = {
            "title": item.get("title"),
            "authors": [{"name": a} for a in item.get("authorString", "").split(", ")],
        }
        print(f"[Europe PMC Grants] Metadata fetched: {metadata}")
        return metadata
    except Exception as e:
        print(f"[Europe PMC Grants] Fetch error: {e}")
        return None


# 15. Wikidata SPARQL Query (no key)
async def get_wikidata_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
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


# 16. Google Books API (API key required)
async def get_google_books_metadata(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[Google Books] Fetching metadata for DOI: {doi}")
    if not GOOGLE_BOOKS_API_KEY:
        print("[Google Books] API key missing, skipping")
        return None
    if not check_and_increment_google_books():
        print("[Google Books] Daily query limit reached, skipping")
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
    except Exception as e:
        print(f"[Google Books] Fetch error: {e}")
        return None


# --- Your existing new PDF fetchers from question ---
async def get_europepmc_preprints_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    print(f"[EuropePMC Preprints] Fetching PDF for DOI: {doi}")
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:{quote(doi)}+AND+PUB_TYPE:preprint&format=json"
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        results = r.json().get("resultList", {}).get("result", [])
        for result in results:
            full_text_urls = result.get("fullTextUrlList", {}).get("fullTextUrl", [])
            for full_text_url in full_text_urls:
                if (
                    full_text_url.get("documentStyle") == "pdf"
                    and full_text_url.get("availability") == "OPEN_ACCESS"
                ):
                    pdf_link = full_text_url.get("url")
                    print(f"[EuropePMC Preprints] PDF URL found: {pdf_link}")
                    return {"pdf_url": pdf_link, "host_type": "EuropePMC Preprints", "source": "EuropePMC Preprints"}
        print("[EuropePMC Preprints] No valid PDF link found")
    except Exception as e:
        print(f"[EuropePMC Preprints] PDF fetch error: {e}")
    return None



async def get_share_pdf(doi: str, client: httpx.AsyncClient) -> Optional[Dict[str, str]]:
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

            # Check 'sources' for PDF URLs
            sources = attrs.get('sources', [])
            for source in sources:
                url = source.get('url')
                if url and url.lower().endswith('.pdf'):
                    print(f"[Share API] PDF URL found in sources: {url}")
                    return {"pdf_url": url, "host_type": "Share API", "source": "Share"}

            # Check 'fulltext' field
            fulltext_url = attrs.get('fulltext')
            if fulltext_url and fulltext_url.lower().endswith('.pdf'):
                print(f"[Share API] PDF URL found in fulltext: {fulltext_url}")
                return {"pdf_url": fulltext_url, "host_type": "Share API", "source": "Share"}

            # Check 'links' dictionary
            links = attrs.get('links', {})
            for key in ['pdf', 'html']:
                link_url = links.get(key)
                if link_url and link_url.lower().endswith('.pdf'):
                    print(f"[Share API] PDF URL found in links[{key}]: {link_url}")
                    return {"pdf_url": link_url, "host_type": "Share API", "source": "Share"}

        print("[Share API] No valid PDF link found")
    except Exception as e:
        print(f"[Share API] PDF fetch error: {e}")
    return None


@app.post("/api/search")
async def search(data: dict):
    doi = data.get("doi")
    if not doi:
        raise HTTPException(status_code=400, detail="DOI is required")

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:

            # PDF fetchers list (independent)
            pdf_tasks = [
                get_pdf_url_from_doi(doi, client),
                get_unpaywall_pdf(doi, client),
                get_europepmc_pdf(doi, client),
                get_core_pdf(doi, client),
                get_base_pdf(doi, client),
                get_zenodo_pdf(doi, client),
                get_figshare_pdf(doi, client),
                get_europepmc_preprints_pdf(doi, client),
                get_arxiv_pdf(doi, client),
                get_biorxiv_pdf(doi, client),
                get_medrxiv_pdf(doi, client),
                get_internetarchive_pdf(doi, client),
            ]

            # Metadata fetchers list (may also contain PDF)
            metadata_tasks = [
                get_crossref_metadata(doi, client),
                get_openalex_metadata(doi, client),
                get_semantic_scholar_metadata(doi, client),
                get_openaire_metadata(doi, client),
                get_doaj_metadata_and_pdf(doi, client),
                get_plos_pdf_and_metadata(doi, client),
                get_crossref_event_metadata(doi, client),
                get_nlm_pubmed_metadata(doi, client),
                get_dryad_metadata(doi, client),
                get_core_pdf_and_metadata(doi, client),
                get_internetarchive_metadata(doi, client),
                get_europepmc_grants_metadata(doi, client),
                get_wikidata_metadata(doi, client),
                get_google_books_metadata(doi, client),
            ]

            # Create asyncio tasks
            pdf_tasks = [asyncio.create_task(t) for t in pdf_tasks]
            metadata_tasks = [asyncio.create_task(t) for t in metadata_tasks]

            pdf_result = None
            metadata = None

            # 1️⃣ Wait for first PDF from independent fetchers
            while pdf_tasks:
                done, pending = await asyncio.wait(pdf_tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    try:
                        result = task.result()
                    except Exception:
                        result = None

                    if result and result.get("pdf_url"):
                        pdf_result = {**result, "independent": True}  # mark as independent
                        # cancel remaining independent PDF tasks
                        for t in pending:
                            t.cancel()
                        pdf_tasks = []
                        break

                pdf_tasks = [t for t in pending if not t.cancelled()]
                if pdf_result:
                    break

            # 2️⃣ Give metadata tasks 4s to finish if PDF found
            if pdf_result:
                try:
                    done, pending = await asyncio.wait(metadata_tasks, timeout=4)
                    for task in done:
                        try:
                            result = task.result()
                        except Exception:
                            result = None

                        if result:
                            if result.get("source") == "crossref_event":
                                continue

                            # Merge metadata if not set
                            if isinstance(result, dict) and "metadata" in result:
                                if result["metadata"].get("title") and not metadata:
                                    metadata = result["metadata"]
                                # Merge PDF from metadata only if no independent PDF
                                if result.get("pdf_url") and not pdf_result.get("independent"):
                                    pdf_result = {
                                        "pdf_url": result["pdf_url"],
                                        "host_type": result.get("host_type"),
                                        "source": result.get("source"),
                                        "independent": False,
                                    }
                            elif result.get("title") and not metadata:
                                metadata = result
                            elif result.get("pdf_url") and not pdf_result.get("independent"):
                                pdf_result = {
                                    "pdf_url": result["pdf_url"],
                                    "host_type": result.get("host_type"),
                                    "source": result.get("source"),
                                    "independent": False,
                                }
                    for t in pending:
                        t.cancel()
                except asyncio.TimeoutError:
                    metadata = None
            else:
                # If no independent PDF found → still try metadata fully (6s)
                done, pending = await asyncio.wait(metadata_tasks, timeout=6)
                for task in done:
                    try:
                        result = task.result()
                    except Exception:
                        result = None
                    if result and result.get("source") != "crossref_event":
                        if isinstance(result, dict) and "metadata" in result:
                            if result["metadata"].get("title") and not metadata:
                                metadata = result["metadata"]
                            if result.get("pdf_url") and not pdf_result:
                                pdf_result = {
                                    "pdf_url": result["pdf_url"],
                                    "host_type": result.get("host_type"),
                                    "source": result.get("source"),
                                    "independent": False,
                                }
                        elif result.get("title") and not metadata:
                            metadata = result
                        elif result.get("pdf_url") and not pdf_result:
                            pdf_result = {
                                "pdf_url": result["pdf_url"],
                                "host_type": result.get("host_type"),
                                "source": result.get("source"),
                                "independent": False,
                            }
                for t in pending:
                    t.cancel()

            # Fallback metadata
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
