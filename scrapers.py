"""Live multi-source scrapers for Turkish public sector jobs.

Sources (in order):
1. SBB Kamu İlan Portalı            https://kamuilan.sbb.gov.tr
2. Cumhurbaşkanlığı Kariyer Kapısı  https://isealimkariyerkapisi.cbiko.gov.tr
3. T.C. Resmî Gazete                https://www.resmigazete.gov.tr
4. İŞKUR                            https://www.iskur.gov.tr (via esube listing)

Each adapter is best-effort: on any network error, layout change, or empty
response it returns [] and reports its status. The pipeline merges results
across all adapters and dedupes by fingerprint(institution|title|city).
"""
from __future__ import annotations

import re
import asyncio
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.5",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
TIMEOUT = httpx.Timeout(30.0, connect=15.0)

CITIES = {
    "İSTANBUL": "İstanbul", "ANKARA": "Ankara", "İZMİR": "İzmir", "BURSA": "Bursa",
    "MARDİN": "Mardin", "ANTALYA": "Antalya", "DİYARBAKIR": "Diyarbakır",
    "KONYA": "Konya", "ADANA": "Adana", "SAMSUN": "Samsun", "TRABZON": "Trabzon",
    "GAZİANTEP": "Gaziantep", "KAYSERİ": "Kayseri", "ESKİŞEHİR": "Eskişehir",
    "ŞANLIURFA": "Şanlıurfa", "HATAY": "Hatay", "ERZURUM": "Erzurum", "VAN": "Van",
    "MUĞLA": "Muğla", "BATMAN": "Batman", "SİVAS": "Sivas", "ORDU": "Ordu",
    "MALATYA": "Malatya", "KOCAELİ": "Kocaeli", "TEKİRDAĞ": "Tekirdağ",
}

DEGREE_KEYWORDS = {
    "Ortaöğretim": ["lise", "ortaöğretim"],
    "Ön Lisans": ["ön lisans", "önlisans", "meslek yüksekokul"],
    "Lisans": ["lisans", "üniversite"],
}


# ---------- Helpers ----------
def fingerprint(institution: str, title: str, city: str) -> str:
    key = f"{institution.strip().lower()}|{title.strip().lower()[:80]}|{city.strip().lower()}"
    return "job-" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def infer_city(text: str) -> str:
    up = text.upper()
    for k, v in CITIES.items():
        if k in up:
            return v
    return "Türkiye Geneli"


def infer_degree(text: str) -> str:
    low = text.lower()
    for level, kws in DEGREE_KEYWORDS.items():
        if any(k in low for k in kws):
            return level
    return "Lisans"


def infer_institution(text: str) -> Tuple[str, str]:
    aliases = [
        ("Sağlık Bakanlığı", "SB"), ("Milli Eğitim Bakanlığı", "MEB"),
        ("Adalet Bakanlığı", "ADB"), ("İçişleri Bakanlığı", "İB"),
        ("Millî Savunma Bakanlığı", "MSB"), ("Diyanet İşleri Başkanlığı", "DİB"),
        ("Emniyet Genel Müdürlüğü", "EGM"), ("Jandarma Genel Komutanlığı", "JGK"),
        ("Sahil Güvenlik", "SGK"), ("SGK", "SGK"),
        ("TCDD", "TCDD"), ("PTT", "PTT"), ("TÜİK", "TÜİK"),
        ("Türkiye İstatistik Kurumu", "TÜİK"),
        ("Karayolları Genel Müdürlüğü", "KGM"),
        ("Orman Genel Müdürlüğü", "OGM"),
        ("Sosyal Güvenlik Kurumu", "SGK"),
        ("Strateji ve Bütçe", "SBB"), ("Cumhurbaşkanlığı", "CBK"),
        ("İller Bankası", "İLBANK"), ("TBMM", "TBMM"),
        ("TBB", "TBB"), ("Türkiye Barolar Birliği", "TBB"),
        ("Ticaret Bakanlığı", "TB"), ("Devlet Personel", "DPB"),
        ("Basın İlan Kurumu", "BİK"), ("Kızılay", "TKZ"),
        ("Belediyesi", "BLD"),
    ]
    for name, short in aliases:
        if name.lower() in text.lower():
            return name, short
    # generic university
    m = re.search(r"([A-ZÇĞİÖŞÜ][a-zçğıöşü]+)\s+Üniversitesi", text)
    if m:
        return f"{m.group(1)} Üniversitesi", "ÜNV"
    return "Kamu Kurumu", "KMU"


def extract_kpss(text: str) -> Tuple[int, str]:
    """Returns (min_score, score_type)."""
    score_type = "Lisans P3"
    low = text.lower()
    if "p94" in low or "ortaöğretim" in low or "lise" in low:
        score_type = "Ortaöğretim P94"
    elif "p93" in low or "ön lisans" in low:
        score_type = "Ön Lisans P93"
    elif "p3" in low or "lisans" in low:
        score_type = "Lisans P3"
    # numeric min
    m = re.search(r"(?:en az|asgari|min\.?)\s*(\d{2}(?:[.,]\d)?)\s*puan", low)
    if not m:
        m = re.search(r"kpss[^0-9]{0,25}(\d{2}(?:[.,]\d)?)", low)
    if m:
        try:
            return int(float(m.group(1).replace(",", "."))), score_type
        except Exception:
            pass
    return 0, score_type


def extract_deadline(text: str) -> str:
    # DD.MM.YYYY or DD/MM/YYYY
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
    if m:
        try:
            d = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)), tzinfo=timezone.utc)
            return d.isoformat()
        except Exception:
            pass
    # default: +21 days
    return (datetime.now(timezone.utc) + timedelta(days=21)).isoformat()


def extract_quota(text: str) -> int:
    m = re.search(r"(\d{1,4})\s*(?:kişi|kadro|kontenjan|personel)", text.lower())
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return 0


def normalize_job(*, title: str, source: str, official_url: str,
                  raw_text: Optional[str] = None,
                  city: Optional[str] = None) -> Dict[str, Any]:
    ctx = f"{title} {raw_text or ''}"
    institution, short = infer_institution(ctx)
    city_norm = city or infer_city(ctx)
    degree = infer_degree(ctx)
    min_kpss, score_type = extract_kpss(ctx)
    return {
        "id": fingerprint(institution, title, city_norm),
        "title": title.strip()[:200],
        "institution": institution,
        "institution_short": short,
        "source": source,
        "sources": [source],
        "city": city_norm,
        "quota": extract_quota(ctx),
        "deadline": extract_deadline(ctx),
        "min_kpss": min_kpss,
        "score_type": score_type,
        "required_degree": degree,
        "target_majors": [],
        "min_age": None,
        "max_age": None,
        "gender": None,
        "required_license": None,
        "require_security_card": None,
        "require_disability": None,
        "required_certificates": None,
        "contract_type": "İlanda belirtilmiştir",
        "viewers": 0,
        "applicants": 0,
        "official_url": official_url,
    }


# ---------- Adapters ----------
async def scrape_sbb(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Cumhurbaşkanlığı Strateji ve Bütçe Başkanlığı Kamu İlan Portalı."""
    url = "https://kamuilan.sbb.gov.tr/"
    resp = await client.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    jobs: List[Dict[str, Any]] = []
    # Portal typically renders each ilan inside a table row or list item with an <a>
    for a in soup.select("a[href]"):
        title = a.get_text(" ", strip=True)
        href = a.get("href", "")
        if not title or len(title) < 20:
            continue
        low = title.lower()
        if not any(k in low for k in ("alım", "alim", "personel", "sözleşmeli", "işçi", "memur", "kadro")):
            continue
        full = href if href.startswith("http") else f"https://kamuilan.sbb.gov.tr/{href.lstrip('/')}"
        jobs.append(normalize_job(title=title, source="SBB", official_url=full))
        if len(jobs) >= 50:
            break
    return jobs


async def scrape_kariyer_kapisi(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """isealimkariyerkapisi.cbiko.gov.tr — SPA. Best-effort HTML parse."""
    url = "https://isealimkariyerkapisi.cbiko.gov.tr/"
    resp = await client.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    jobs: List[Dict[str, Any]] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 12:
            continue
        if "IlanDetay" not in href and "ilan" not in href.lower():
            continue
        full = href if href.startswith("http") else f"https://isealimkariyerkapisi.cbiko.gov.tr/{href.lstrip('/')}"
        jobs.append(normalize_job(title=title, source="Kariyer Kapısı", official_url=full))
        if len(jobs) >= 50:
            break
    return jobs


async def scrape_resmi_gazete(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Resmi Gazete — today's fihrist → 'Çeşitli İlanlar' page → PDF entries.
    Each entry is an anchor with institution name as text and PDF href.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fihrist = f"https://www.resmigazete.gov.tr/fihrist?tarih={today}"
    resp = await client.get(fihrist)
    resp.raise_for_status()
    resp.encoding = "windows-1254"
    soup = BeautifulSoup(resp.text, "lxml")

    # find the "Çeşitli İlânlar" section link (kısım 4)
    cesitli_url: Optional[str] = None
    for a in soup.find_all("a", href=True):
        t = a.get_text(" ", strip=True)
        if "Çeşitli" in t and "İlân" in t or ("cesitli" in t.lower() and "ilan" in t.lower()):
            cesitli_url = a["href"] if a["href"].startswith("http") else f"https://www.resmigazete.gov.tr{a['href']}"
            break
    if not cesitli_url:
        # fallback: guessed pattern kısım 4
        yyyy_mm = datetime.now(timezone.utc).strftime("%Y/%m")
        ymd = datetime.now(timezone.utc).strftime("%Y%m%d")
        cesitli_url = f"https://www.resmigazete.gov.tr/ilanlar/eskiilanlar/{yyyy_mm}/{ymd}-4.htm"

    resp2 = await client.get(cesitli_url)
    if resp2.status_code >= 400:
        return []
    resp2.encoding = "windows-1254"
    s2 = BeautifulSoup(resp2.text, "lxml")

    jobs: List[Dict[str, Any]] = []
    for a in s2.find_all("a", href=True):
        title = a.get_text(" ", strip=True)
        if len(title) < 20:
            continue
        href = a["href"]
        if not href.lower().endswith(".pdf"):
            continue
        # Clean multi-space
        title = re.sub(r"\s+", " ", title)
        # Only keep entries that look like personnel or academic postings
        low = title.lower()
        if not any(k in low for k in (
            "başkanlığından", "müdürlüğünden", "rektörlüğünden", "üniversitesi",
            "belediye", "bakanlığı", "genel müdürlüğü", "personel", "öğretim", "alım", "alim",
        )):
            continue
        base = cesitli_url.rsplit("/", 1)[0]
        pdf_url = href if href.startswith("http") else f"{base}/{href.lstrip('/')}"
        jobs.append(normalize_job(title=title[:200], source="Resmi Gazete", official_url=pdf_url))
        if len(jobs) >= 50:
            break
    return jobs


async def scrape_iskur(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """İŞKUR — açık iş ilanları listeleme sayfası JS-rendered SPA.
    We attempt a plain-HTTP fetch; if the DOM is not populated (typical),
    the adapter returns []. On the server side this is reported in
    /api/scrape/status with last_error='no_ilan_found (js-rendered)'.
    Real production integration requires İŞKUR resmi web servisi (SOAP) or
    a headless browser worker — outside scope of this preview.
    """
    url = "https://esube.iskur.gov.tr/istihdam/AcikIsIlanAra.aspx?mid=10436"
    resp = await client.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    jobs: List[Dict[str, Any]] = []
    # Search for any table-like structure
    rows = soup.find_all("tr")
    for tr in rows:
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if len(cells) < 3:
            continue
        line = " | ".join(cells)
        low = line.lower()
        if not any(k in low for k in ("kamu", "belediye", "bakanlık", "genel müdürlük", "memur", "sözleşmeli", "başkanlığı")):
            continue
        title = next((c for c in cells if len(c) > 15), None)
        if not title:
            continue
        link_el = tr.select_one("a[href]")
        href = link_el["href"] if link_el else "https://esube.iskur.gov.tr/istihdam/AcikIsIlanAra.aspx?mid=10436"
        full = href if href.startswith("http") else f"https://esube.iskur.gov.tr/istihdam/{href.lstrip('/')}"
        jobs.append(normalize_job(title=title[:200], source="İŞKUR", raw_text=line, official_url=full))
        if len(jobs) >= 50:
            break
    if not jobs:
        raise RuntimeError("iskur_no_ilan_found (JS-rendered SPA — production needs SOAP webservice or headless browser)")
    return jobs


ADAPTERS = [
    ("SBB", scrape_sbb),
    ("Kariyer Kapısı", scrape_kariyer_kapisi),
    ("Resmi Gazete", scrape_resmi_gazete),
    ("İŞKUR", scrape_iskur),
]


async def run_all_scrapers() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Runs all adapters concurrently. Returns (deduped_jobs, per_source_status)."""
    status: Dict[str, Dict[str, Any]] = {}
    async with httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT, follow_redirects=True, verify=False) as client:
        tasks = {name: asyncio.create_task(fn(client)) for name, fn in ADAPTERS}
        results: Dict[str, List[Dict[str, Any]]] = {}
        for name, task in tasks.items():
            try:
                jobs = await task
                results[name] = jobs
                status[name] = {
                    "ok": True, "count": len(jobs),
                    "last_success": datetime.now(timezone.utc).isoformat(),
                    "last_error": None,
                }
            except Exception as e:
                results[name] = []
                status[name] = {
                    "ok": False, "count": 0,
                    "last_success": None,
                    "last_error": f"{type(e).__name__}: {str(e)[:200]}",
                }
                logger.warning("scraper %s failed: %s", name, e)

    # Dedup + merge sources
    merged: Dict[str, Dict[str, Any]] = {}
    for name, jobs in results.items():
        for j in jobs:
            key = j["id"]
            if key in merged:
                existing = merged[key]
                if name not in existing["sources"]:
                    existing["sources"].append(name)
                # prefer record with more populated fields (higher quota etc.)
                if j.get("quota", 0) > existing.get("quota", 0):
                    existing.update({k: v for k, v in j.items() if k not in ("sources",)})
                    if name not in existing["sources"]:
                        existing["sources"].append(name)
            else:
                merged[key] = j
    return list(merged.values()), status
