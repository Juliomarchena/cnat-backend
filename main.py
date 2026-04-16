"""
CNAT - Centro Nacional de Alerta de Tsunamis
Backend de ingesta de datos en tiempo real
MICROHELP © 2026
"""

import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

import httpx
import feedparser
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cnat")

# ─── Config ───
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # service_role key for writes
supabase: Client = None

HTTP_TIMEOUT = 30
scheduler = AsyncIOScheduler()


# ─── Helpers ───
def make_id(*parts):
    raw = "-".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def classify_severity(mag, depth):
    if mag >= 7.5 and depth <= 60:
        return "critical"
    if mag >= 7.0 and depth <= 100:
        return "warning"
    if mag >= 6.5 and depth <= 70:
        return "warning"
    if mag >= 6.0 and depth <= 100:
        return "moderate"
    if mag >= 4.5:
        return "moderate"
    return "normal"


async def log_fetch(source_id, status, records=0, error=None, duration_ms=0):
    try:
        supabase.table("fetch_log").insert({
            "source_id": source_id,
            "status": status,
            "records_fetched": records,
            "error_message": error,
            "duration_ms": duration_ms
        }).execute()
    except Exception as e:
        logger.error(f"Error logging fetch for {source_id}: {e}")


async def update_source_status(source_id, status="active"):
    try:
        supabase.table("sources").update({
            "status": status,
            "last_fetch": datetime.now(timezone.utc).isoformat()
        }).eq("id", source_id).execute()
    except Exception as e:
        logger.error(f"Error updating source {source_id}: {e}")


# ═══════════════════════════════════════════
#  FETCHERS - Fuentes con API/datos estructurados
# ═══════════════════════════════════════════

# ─── 1. USGS Earthquakes (API JSON GeoJSON) ───
async def fetch_usgs():
    source_id = "usgs"
    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            # 2.5+ magnitude, last day
            r = await client.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson")
            r.raise_for_status()
            data = r.json()

        records = []
        for feature in data.get("features", []):
            props = feature["properties"]
            coords = feature["geometry"]["coordinates"]
            eq_id = feature.get("id", make_id("usgs", props.get("time")))
            mag = props.get("mag") or 0
            depth = coords[2] if len(coords) > 2 else 0

            records.append({
                "id": eq_id,
                "source_id": source_id,
                "magnitude": round(float(mag), 1),
                "depth_km": round(float(depth), 1),
                "latitude": round(float(coords[1]), 6),
                "longitude": round(float(coords[0]), 6),
                "place": props.get("place", ""),
                "event_time": datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc).isoformat(),
                "tsunami_flag": props.get("tsunami", 0),
                "alert_level": props.get("alert"),
                "severity": classify_severity(float(mag), float(depth)),
                "raw_data": json.dumps(props),
            })

        if records:
            supabase.table("earthquakes").upsert(records, on_conflict="id").execute()

        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "success", len(records), duration_ms=duration)
        await update_source_status(source_id)
        logger.info(f"USGS: {len(records)} earthquakes fetched")

    except Exception as e:
        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
        await update_source_status(source_id, "error")
        logger.error(f"USGS error: {e}")


# ─── 2. PTWC Tsunami Alerts (ATOM XML Feed) ───
async def fetch_ptwc():
    source_id = "ptwc"
    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get("https://www.tsunami.gov/events/xml/PHEBAtom.xml")
            r.raise_for_status()

        feed = feedparser.parse(r.text)
        records = []

        for entry in feed.entries:
            alert_id = make_id("ptwc", entry.get("id", entry.get("title", "")))
            title = entry.get("title", "")
            summary = entry.get("summary", "")
            updated = entry.get("updated", "")

            # Determine severity from title
            severity = "info"
            alert_type = "INFORMACION"
            title_upper = title.upper()
            if "WARNING" in title_upper or "ALARMA" in title_upper:
                severity = "critical"
                alert_type = "ALARMA"
            elif "WATCH" in title_upper or "ADVISORY" in title_upper or "ALERTA" in title_upper:
                severity = "warning"
                alert_type = "ALERTA"

            records.append({
                "id": alert_id,
                "source_id": source_id,
                "alert_type": alert_type,
                "title": title[:500] if title else "PTWC Bulletin",
                "message": summary[:2000] if summary else "",
                "issued_at": updated or datetime.now(timezone.utc).isoformat(),
                "severity": severity,
                "raw_data": json.dumps({"title": title, "links": [l.get("href") for l in entry.get("links", [])]}),
            })

        # Also check NTWC feed
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r2 = await client.get("https://www.tsunami.gov/events/xml/PAAQAtom.xml")
            if r2.status_code == 200:
                feed2 = feedparser.parse(r2.text)
                for entry in feed2.entries:
                    alert_id = make_id("ntwc", entry.get("id", entry.get("title", "")))
                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    severity = "info"
                    alert_type = "INFORMACION"
                    title_upper = title.upper()
                    if "WARNING" in title_upper:
                        severity = "critical"
                        alert_type = "ALARMA"
                    elif "WATCH" in title_upper or "ADVISORY" in title_upper:
                        severity = "warning"
                        alert_type = "ALERTA"

                    records.append({
                        "id": alert_id,
                        "source_id": source_id,
                        "alert_type": alert_type,
                        "title": title[:500] if title else "NTWC Bulletin",
                        "message": summary[:2000] if summary else "",
                        "issued_at": entry.get("updated", datetime.now(timezone.utc).isoformat()),
                        "severity": severity,
                        "raw_data": json.dumps({"title": title}),
                    })

        if records:
            supabase.table("tsunami_alerts").upsert(records, on_conflict="id").execute()

        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "success", len(records), duration_ms=duration)
        await update_source_status(source_id)
        logger.info(f"PTWC: {len(records)} alerts fetched")

    except Exception as e:
        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
        await update_source_status(source_id, "error")
        logger.error(f"PTWC error: {e}")


# ─── 3. NDBC DART Buoy Data (Text files) ───
async def fetch_ndbc_dart():
    source_id = "ndbc"
    start = time.time()
    # Key DART stations near Peru/Pacific
    dart_stations = ["32412", "32413", "32411", "43412", "43413", "32301", "32302"]
    total_records = 0

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            for station_id in dart_stations:
                try:
                    url = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.dart"
                    r = await client.get(url)
                    if r.status_code != 200:
                        continue

                    lines = r.text.strip().split("\n")
                    if len(lines) < 3:
                        continue

                    # Parse DART format: #YY MM DD hh mm ss T   HEIGHT
                    readings = []
                    for line in lines[2:12]:  # Last 10 readings
                        parts = line.split()
                        if len(parts) < 8:
                            continue
                        try:
                            yy, mm, dd, hh, mn, ss = parts[0:6]
                            t_type = parts[6]  # 1=scheduled, 2=event
                            height = float(parts[7])
                            year = 2000 + int(yy) if int(yy) < 100 else int(yy)
                            reading_time = datetime(year, int(mm), int(dd), int(hh), int(mn), int(ss), tzinfo=timezone.utc)

                            readings.append({
                                "station_id": station_id,
                                "reading_time": reading_time.isoformat(),
                                "water_level_mm": height,
                                "anomaly_mm": 0,  # Would need baseline to compute
                                "is_event_mode": t_type == "2",
                            })
                        except (ValueError, IndexError):
                            continue

                    if readings:
                        supabase.table("buoy_readings").insert(readings).execute()
                        total_records += len(readings)

                        # Update station status
                        is_event = any(r["is_event_mode"] for r in readings)
                        status = "alert" if is_event else "normal"
                        supabase.table("buoy_stations").update({"status": status}).eq("id", station_id).execute()

                except Exception as e:
                    logger.warning(f"DART station {station_id} error: {e}")
                    continue

        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "success", total_records, duration_ms=duration)
        await update_source_status(source_id)
        logger.info(f"NDBC DART: {total_records} readings from {len(dart_stations)} stations")

    except Exception as e:
        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
        await update_source_status(source_id, "error")
        logger.error(f"NDBC error: {e}")


# ─── 4. IRIS FDSN Earthquakes ───
async def fetch_iris():
    source_id = "iris"
    start = time.time()
    try:
        now = datetime.now(timezone.utc)
        start_time = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
        url = f"https://service.iris.edu/fdsnws/event/1/query?format=text&orderby=time&limit=50&minmag=4&starttime={start_time}"

        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get(url)
            r.raise_for_status()

        lines = r.text.strip().split("\n")
        records = []
        for line in lines[1:]:  # Skip header
            parts = line.split("|")
            if len(parts) < 12:
                continue
            try:
                eq_id = make_id("iris", parts[0].strip())
                event_time = parts[1].strip()
                lat = float(parts[2].strip())
                lon = float(parts[3].strip())
                depth = float(parts[4].strip())
                mag = float(parts[10].strip())
                place = parts[12].strip() if len(parts) > 12 else ""

                records.append({
                    "id": eq_id,
                    "source_id": source_id,
                    "magnitude": round(mag, 1),
                    "depth_km": round(depth, 1),
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "place": place,
                    "event_time": event_time,
                    "severity": classify_severity(mag, depth),
                })
            except (ValueError, IndexError):
                continue

        if records:
            supabase.table("earthquakes").upsert(records, on_conflict="id").execute()

        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "success", len(records), duration_ms=duration)
        await update_source_status(source_id)
        logger.info(f"IRIS: {len(records)} earthquakes fetched")

    except Exception as e:
        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
        await update_source_status(source_id, "error")
        logger.error(f"IRIS error: {e}")


# ─── 5. Chile Alerta API (Sismos Chile + Tsunamis) ───
async def fetch_chile():
    source_id = "csn_chile"
    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            # Últimos sismos Chile
            r = await client.get("https://chilealerta.com/api/query/?user=demo&select=ultimos_sismos_chile&limit=30")
            r.raise_for_status()
            data = r.json()

        records = []
        sismos = data.get("ultimos_sismos_Chile", [])
        for s in sismos:
            eq_id = make_id("chile", s.get("id", s.get("utc_time")))
            mag = float(s.get("magnitude", 0))
            depth = float(s.get("depth", 0))
            records.append({
                "id": eq_id,
                "source_id": source_id,
                "magnitude": round(mag, 1),
                "depth_km": round(depth, 1),
                "latitude": round(float(s.get("latitude", 0)), 6),
                "longitude": round(float(s.get("longitude", 0)), 6),
                "place": s.get("reference", ""),
                "event_time": s.get("utc_time", "").replace("/", "-"),
                "severity": classify_severity(mag, depth),
            })

        if records:
            supabase.table("earthquakes").upsert(records, on_conflict="id").execute()

        # Also fetch tsunami bulletins
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r2 = await client.get("https://chilealerta.com/api/query/?user=demo&select=tsunami_chile")
            if r2.status_code == 200:
                tsu_data = r2.json()
                for key, val in tsu_data.items():
                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict) and item.get("title"):
                                alert_id = make_id("snam", item.get("title", ""))
                                supabase.table("tsunami_alerts").upsert([{
                                    "id": alert_id,
                                    "source_id": "snam",
                                    "alert_type": "INFORMACION",
                                    "title": item.get("title", "")[:500],
                                    "message": item.get("description", "")[:2000],
                                    "issued_at": item.get("date", datetime.now(timezone.utc).isoformat()),
                                    "severity": "info",
                                }], on_conflict="id").execute()

        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "success", len(records), duration_ms=duration)
        await update_source_status(source_id)
        logger.info(f"Chile: {len(records)} earthquakes fetched")

    except Exception as e:
        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
        await update_source_status(source_id, "error")
        logger.error(f"Chile error: {e}")


# ─── 6. BBC Mundo RSS (Noticias) ───
async def fetch_news_rss():
    rss_sources = [
        ("bbc", "https://feeds.bbci.co.uk/mundo/rss.xml"),
    ]
    keywords = ["tsunami", "sismo", "terremoto", "earthquake", "volcán", "erupción", "maremoto",
                "meteorito", "asteroide", "nivel del mar", "sea level", "deslizamiento", "landslide"]

    for source_id, url in rss_sources:
        start = time.time()
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                r = await client.get(url)
                r.raise_for_status()

            feed = feedparser.parse(r.text)
            records = []

            for entry in feed.entries[:50]:
                title = entry.get("title", "")
                summary = entry.get("summary", "")
                combined = (title + " " + summary).lower()

                # Filter for relevant news
                is_relevant = any(kw in combined for kw in keywords)
                if not is_relevant:
                    continue

                news_id = make_id(source_id, entry.get("link", title))
                relevance = "warning" if any(kw in combined for kw in ["tsunami", "terremoto", "alarma"]) else "info"

                records.append({
                    "id": news_id,
                    "source_id": source_id,
                    "title": title[:500],
                    "summary": summary[:1000],
                    "url": entry.get("link", ""),
                    "published_at": entry.get("published", datetime.now(timezone.utc).isoformat()),
                    "relevance": relevance,
                })

            if records:
                supabase.table("news").upsert(records, on_conflict="id").execute()

            duration = int((time.time() - start) * 1000)
            await log_fetch(source_id, "success", len(records), duration_ms=duration)
            await update_source_status(source_id)
            logger.info(f"News {source_id}: {len(records)} relevant articles")

        except Exception as e:
            duration = int((time.time() - start) * 1000)
            await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
            await update_source_status(source_id, "error")
            logger.error(f"News {source_id} error: {e}")


# ─── 7. Sea Level Monitoring UNESCO ───
async def fetch_sea_level():
    source_id = "sea_level"
    start = time.time()
    try:
        # Get Pacific stations
        url = "https://www.ioc-sealevelmonitoring.org/service.php?query=stationlist&showall=all&output=json"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get(url)
            r.raise_for_status()
            stations = r.json()

        # Filter for Pacific/South American stations
        pacific_stations = []
        for s in stations:
            try:
                lon = float(s.get("lon", 0))
                lat = float(s.get("lat", 0))
                # Pacific coast of South America
                if -120 <= lon <= -60 and -60 <= lat <= 20:
                    pacific_stations.append(s)
            except (ValueError, TypeError):
                continue

        # Limit to first 20 stations
        for station in pacific_stations[:20]:
            station_id = make_id("sl", station.get("code", ""))
            try:
                supabase.table("buoy_stations").upsert([{
                    "id": f"sl_{station.get('code', station_id)}",
                    "name": station.get("location", "Unknown"),
                    "country": station.get("country", ""),
                    "latitude": round(float(station.get("lat", 0)), 6),
                    "longitude": round(float(station.get("lon", 0)), 6),
                    "station_type": "tide_gauge",
                    "source_id": source_id,
                    "status": "normal",
                }], on_conflict="id").execute()
            except Exception:
                continue

        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "success", len(pacific_stations[:20]), duration_ms=duration)
        await update_source_status(source_id)
        logger.info(f"Sea Level: {len(pacific_stations[:20])} Pacific stations registered")

    except Exception as e:
        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
        await update_source_status(source_id, "error")
        logger.error(f"Sea Level error: {e}")


# ═══════════════════════════════════════════
#  MAIN FETCH ORCHESTRATOR
# ═══════════════════════════════════════════
async def fetch_all():
    logger.info("═══ Starting full data fetch cycle ═══")
    await fetch_usgs()
    await fetch_ptwc()
    await fetch_ndbc_dart()
    await fetch_iris()
    await fetch_chile()
    await fetch_news_rss()
    logger.info("═══ Fetch cycle complete ═══")


async def fetch_slow():
    """Less frequent fetches (every 30 min)"""
    await fetch_sea_level()


# ═══════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    global supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase connected")

    # Run initial fetch
    await fetch_all()
    await fetch_slow()

    # Schedule recurring fetches
    scheduler.add_job(fetch_all, "interval", minutes=5, id="fetch_all")
    scheduler.add_job(fetch_slow, "interval", minutes=30, id="fetch_slow")
    scheduler.start()
    logger.info("Scheduler started: fetch every 5 min")

    yield

    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(
    title="CNAT - Sistema de Alerta Temprana de Tsunamis",
    description="Backend de ingesta de datos para el Centro Nacional de Alerta de Tsunamis - DHN Marina de Guerra del Perú",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "system": "CNAT - Centro Nacional de Alerta de Tsunamis",
        "provider": "MICROHELP",
        "status": "operational",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "fetch_now": "/fetch",
            "sources": "/api/sources",
            "earthquakes": "/api/earthquakes",
            "alerts": "/api/alerts",
            "buoys": "/api/buoys",
        }
    }


@app.get("/health")
async def health():
    try:
        result = supabase.table("sources").select("id", count="exact").execute()
        return {"status": "healthy", "sources_count": result.count, "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.post("/fetch")
async def trigger_fetch():
    """Manually trigger a fetch cycle"""
    await fetch_all()
    return {"status": "fetch completed", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/sources")
async def get_sources():
    result = supabase.table("sources").select("*").order("source_type").execute()
    return result.data


@app.get("/api/earthquakes")
async def get_earthquakes(limit: int = 50, min_mag: float = 0):
    result = (supabase.table("earthquakes")
              .select("*")
              .gte("magnitude", min_mag)
              .order("event_time", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/alerts")
async def get_alerts(limit: int = 20):
    result = (supabase.table("tsunami_alerts")
              .select("*")
              .order("issued_at", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/buoys")
async def get_buoys():
    result = supabase.table("buoy_stations").select("*").execute()
    return result.data


@app.get("/api/buoy-readings/{station_id}")
async def get_buoy_readings(station_id: str, limit: int = 100):
    result = (supabase.table("buoy_readings")
              .select("*")
              .eq("station_id", station_id)
              .order("reading_time", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/thresholds")
async def get_thresholds():
    result = supabase.table("alert_thresholds").select("*").order("priority").execute()
    return result.data


@app.get("/api/news")
async def get_news(limit: int = 20):
    result = (supabase.table("news")
              .select("*")
              .order("published_at", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/fetch-log")
async def get_fetch_log(limit: int = 50):
    result = (supabase.table("fetch_log")
              .select("*")
              .order("fetched_at", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/dashboard")
async def get_dashboard():
    """Consolidated endpoint for the frontend dashboard"""
    earthquakes = (supabase.table("earthquakes")
                   .select("*")
                   .order("event_time", desc=True)
                   .limit(50)
                   .execute()).data

    alerts = (supabase.table("tsunami_alerts")
              .select("*")
              .order("issued_at", desc=True)
              .limit(10)
              .execute()).data

    buoys = supabase.table("buoy_stations").select("*").execute().data

    sources = supabase.table("sources").select("*").execute().data

    thresholds = supabase.table("alert_thresholds").select("*").order("priority").execute().data

    news = (supabase.table("news")
            .select("*")
            .order("published_at", desc=True)
            .limit(10)
            .execute()).data

    # KPIs
    critical_count = sum(1 for e in earthquakes if e.get("severity") == "critical")
    warning_count = sum(1 for e in earthquakes if e.get("severity") == "warning")
    alert_buoys = sum(1 for b in buoys if b.get("status") in ("alert", "warning"))
    sources_online = sum(1 for s in sources if s.get("status") == "active")

    return {
        "kpis": {
            "total_earthquakes": len(earthquakes),
            "critical_count": critical_count,
            "warning_count": warning_count,
            "active_alerts": len(alerts),
            "alert_buoys": alert_buoys,
            "total_buoys": len(buoys),
            "sources_online": sources_online,
            "total_sources": len(sources),
            "risk_level": "ALTO" if critical_count > 0 else "MEDIO" if warning_count > 0 else "BAJO",
        },
        "earthquakes": earthquakes,
        "alerts": alerts,
        "buoys": buoys,
        "sources": sources,
        "thresholds": thresholds,
        "news": news,
        "last_update": datetime.now(timezone.utc).isoformat(),
    }
