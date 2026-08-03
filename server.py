from fastapi import FastAPI, APIRouter, HTTPException
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import os
import logging
from pathlib import Path
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import httpx

from scrapers import run_all_scrapers

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = "mongodb+srv://komisermevlutcan_db_user:In29TJfjfLe7owA1@emrekhan.mongodb.net/?retryWrites=true&w=majority"
client = AsyncIOMotorClient(mongo_url)
db = client["kpss_app"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- MODELLER ----------
class Job(BaseModel):
    id: str
    title: str
    institution: str
    institution_short: str
    source: str
    sources: List[str] = []
    city: str
    quota: int
    deadline: str
    min_kpss: int
    score_type: str
    required_degree: str
    target_majors: List[str] = []
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    gender: Optional[str] = None
    required_license: Optional[List[str]] = None
    require_security_card: Optional[str] = None
    require_disability: Optional[bool] = None
    required_certificates: Optional[List[str]] = None
    contract_type: str
    viewers: int = 0
    applicants: int = 0
    official_url: str

class UserProfile(BaseModel):
    device_token: str
    kpss_score: int
    degree: str
    target_city: Optional[str] = "Türkiye Geneli"

class Announcement(BaseModel):
    id: str
    title: str
    subtitle: str
    icon: str
    tint: str

class Meta(BaseModel):
    cities: List[str]
    majors: List[str]
    certificates: List[str]
    licenses: List[str]

class SourceStatus(BaseModel):
    ok: bool
    count: int
    last_success: Optional[str] = None
    last_error: Optional[str] = None

class ScrapeStatus(BaseModel):
    last_refresh: Optional[str] = None
    total_jobs: int
    sources: Dict[str, SourceStatus]

# ---------- SABİT LİSTELER (MOBİL UYGULAMA İÇİN) ----------
CITIES = [
    "Adana", "Adıyaman", "Afyon", "Ağrı", "Amasya", "Ankara", "Antalya", "Aydın",
    "Balıkesir", "Bilecik", "Bingöl", "Bitlis", "Bolu", "Burdur", "Bursa",
    "Çanakkale", "Çankırı", "Çorum", "Denizli", "Diyarbakır", "Edirne", "Elazığ",
    "Erzincan", "Erzurum", "Eskişehir", "Gaziantep", "Giresun", "Gümüşhane", "Hakkari",
    "Hatay", "Isparta", "İçel", "İstanbul", "İzmir", "Kars", "Kastamonu", "Kayseri",
    "Kırklareli", "Kırşehir", "Kocaeli", "Konya", "Kütahya", "Malatya", "Manisa",
    "K.Maraş", "Mardin", "Muğla", "Muş", "Nevşehir", "Niğde", "Ordu", "Rize",
    "Sakarya", "Samsun", "Siirt", "Sinop", "Sivas", "Tekirdağ", "Tokat", "Trabzon",
    "Tunceli", "Şanlıurfa", "Uşak", "Van", "Yozgat", "Zonguldak", "Türkiye Geneli",
]
CERTIFICATES = [
    "Hijyen", "Bilgisayar İşletmenliği", "İngilizce Sertifikası", "İş Sağlığı ve Güvenliği",
    "İlk Yardım", "MEB Onaylı Eğitici Eğitimi", "Aşçılık Belgesi", "Kaynakçılık",
    "Forklift Operatörlüğü", "Kalorifer Ateşçiliği", "Bilgisayar Programcılığı",
    "Muhasebe", "Web Tasarım", "Grafik Tasarım", "SRC 1", "SRC 2", "SRC 3", "SRC 4",
    "ISO 9001", "Pedagojik Formasyon",
]
MAJORS = [
    "Coğrafya", "Tarih", "Türk Dili ve Edebiyatı", "Sınıf Öğretmenliği",
    "Matematik", "İngilizce Öğretmenliği", "İngiliz Dili ve Edebiyatı", "Hemşirelik",
    "İşletme", "İktisat", "Bilgisayar Mühendisliği", "Bilgisayar Programcılığı",
    "Elektrik-Elektronik", "Makine Mühendisliği", "Kamu Yönetimi", "Hukuk", "Sosyoloji",
    "Psikoloji", "Ziraat", "Tarım Teknolojisi", "Veterinerlik", "Mimarlık",
    "Şehir ve Bölge Planlama", "İnşaat Mühendisliği", "Sağlık Yönetimi", "Sosyal Hizmet",
    "İstatistik", "İlahiyat", "Orman Mühendisliği", "Muhasebe", "Bankacılık",
    "Tıbbi Dokümantasyon", "Büro Yönetimi", "Eczacılık", "İletişim", "Gazetecilik",
    "Halkla İlişkiler",
]
LICENSES = ["A", "B", "C", "D", "E", "F", "G", "M", "T"]

ANNOUNCEMENTS: List[Announcement] = [
    Announcement(id="a1", title="Kariyer Kapısı", subtitle="Cumhurbaşkanlığı İnsan Kaynakları Ofisi resmi başvuru portalı", icon="briefcase-outline", tint="brand"),
    Announcement(id="a2", title="Resmi Gazete", subtitle="Her gün yayımlanan ilan bölümü otomatik taranıyor", icon="newspaper-outline", tint="success"),
    Announcement(id="a3", title="SBB Kamu İlan Portalı", subtitle="Tüm kamu personel ilanları tek noktada", icon="business-outline", tint="warning"),
    Announcement(id="a4", title="İŞKUR", subtitle="Kamu işçi alım ilanları buradan takip edilir", icon="people-outline", tint="info"),
]

# ---------- BİLDİRİM VE EŞLEŞTİRME MOTORU ----------
async def send_push_notification(token: str, title: str, message: str):
    if not token or not token.startswith("ExponentPushToken"):
        return
    
    message_data = {
        "to": token,
        "sound": "default",
        "title": title,
        "body": message,
    }
    async with httpx.AsyncClient() as push_client:
        try:
            await push_client.post('https://exp.host/--/api/v2/push/send', json=message_data)
            logger.info(f"Bildirim Gönderildi: {token[:15]}... -> {title}")
        except Exception as e:
            logger.error(f"Bildirim hatası: {e}")

async def match_and_notify_users(new_jobs):
    users = await db.users.find({}).to_list(1000)
    for job in new_jobs:
        for user in users:
            if user.get("kpss_score", 0) >= job.get("min_kpss", 0) and user.get("degree") == job.get("required_degree"):
                if user.get("target_city") == "Türkiye Geneli" or user.get("target_city") == job.get("city"):
                    title = "🎉 Sana Uygun Yeni Bir İlan Var!"
                    body = f"{job['institution']} - {job['title']} (Min KPSS: {job['min_kpss']})"
                    await send_push_notification(user.get("device_token"), title, body)

# ---------- YENİLEME (REFRESH) ALTYAPISI ----------
async def refresh_jobs() -> int:
    jobs, status = await run_all_scrapers()

    stamped_at = datetime.now(timezone.utc).isoformat()
    for j in jobs:
        j["_updated_at"] = stamped_at

    await db.jobs.delete_many({})
    if jobs:
        await db.jobs.insert_many(jobs)
        await match_and_notify_users(jobs)

    await db.metadata.update_one(
        {"_id": "refresh"},
        {"$set": {
            "last_refresh": stamped_at,
            "total_jobs": len(jobs),
            "sources": status,
        }},
        upsert=True,
    )
    logger.info("refresh_jobs: %d unique jobs stored; per-source=%s",
                len(jobs), {k: v.get("count") for k, v in status.items()})
    return len(jobs)

scheduler: Optional[AsyncIOScheduler] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler
    try:
        await refresh_jobs()
    except Exception as e:
        logger.warning("initial refresh failed: %s", e)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(refresh_jobs, "interval", hours=6, id="refresh_jobs")
    scheduler.start()
    yield
    if scheduler:
        scheduler.shutdown(wait=False)
    client.close()

app = FastAPI(lifespan=lifespan)
api_router = APIRouter(prefix="/api")
PROJECTION = {"_id": 0}

# ---------- API ENDPOINTLERİ ----------
@api_router.get("/")
async def root():
    return {"service": "kpss-api", "status": "ok"}

@api_router.post("/users/profile")
async def save_user_profile(user: UserProfile):
    await db.users.update_one(
        {"device_token": user.device_token},
        {"$set": user.dict()},
        upsert=True
    )
    return {"status": "success", "message": "Profil kaydedildi"}

@api_router.get("/jobs", response_model=List[Job])
async def list_jobs(city: Optional[str] = None, source: Optional[str] = None, limit: int = 500):
    query: dict = {}
    if city: query["city"] = city
    if source: query["source"] = source
    docs = await db.jobs.find(query, PROJECTION).to_list(limit)
    return docs

@api_router.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str):
    doc = await db.jobs.find_one({"id": job_id}, PROJECTION)
    if not doc:
        raise HTTPException(status_code=404, detail="Job not found")
    return doc

@api_router.post("/jobs/refresh")
async def refresh_jobs_endpoint():
    count = await refresh_jobs()
    meta = await db.metadata.find_one({"_id": "refresh"}, PROJECTION) or {}
    return {
        "count": count,
        "last_refresh": meta.get("last_refresh"),
        "sources": meta.get("sources", {}),
    }

@api_router.get("/scrape/status", response_model=ScrapeStatus)
async def scrape_status():
    meta = await db.metadata.find_one({"_id": "refresh"}, PROJECTION) or {}
    return ScrapeStatus(
        last_refresh=meta.get("last_refresh"),
        total_jobs=meta.get("total_jobs", 0),
        sources={k: SourceStatus(**v) for k, v in (meta.get("sources") or {}).items()},
    )

@api_router.get("/announcements", response_model=List[Announcement])
async def list_announcements():
    return ANNOUNCEMENTS

@api_router.get("/meta", response_model=Meta)
async def get_meta():
    return Meta(cities=CITIES, majors=MAJORS, certificates=CERTIFICATES, licenses=LICENSES)

@api_router.get("/health")
async def health():
    meta = await db.metadata.find_one({"_id": "refresh"}, PROJECTION) or {}
    count = await db.jobs.count_documents({})
    return {"ok": True, "jobs": count, **meta}

app.include_router(api_router)
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
