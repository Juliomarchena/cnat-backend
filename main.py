"""
CNAT - Centro Nacional de Alerta de Tsunamis
Backend de ingesta de datos en tiempo real
MICROHELP © 2026

Incluye: Mapa Mareográfico del Pacífico (IOC/SLSMF API v2)

============================================================
FASE 1 (19/05/2026): Clasificación oficial DHN
- Se agrega dhn_classifier (módulo nuevo)
- Cada sismo se clasifica también según la matriz oficial DHN
- Campo severity legacy se mantiene para compatibilidad

FASE 2 (22/05/2026): Escucha Social Inteligente
- news_raw: almacena noticias crudas con fuente, fecha y hora exactas
- news_summaries: resúmenes diarios generados por Claude API
- Scrapers RSS ampliados: BBC, NYT Español, Washington Post
- Scheduler diario para generación automática de resumen
- ARIA/VIGÍA consulta news_summaries en sus reportes
- fetch_mode en sources: realtime | daily | pending
============================================================
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
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from auth import get_current_user, require_admin, load_jwks
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from supabase import create_client, Client
from dotenv import load_dotenv

# ─── [FASE 1] Import del clasificador oficial DHN ───
from dhn_classifier import classify_dhn, dhn_to_severity

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cnat")

# ─── Config ───
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
IOC_API_KEY  = os.getenv("IOC_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
supabase: Client = None

HTTP_TIMEOUT = 30
scheduler = AsyncIOScheduler()

# ─── IOC API v2 Config ───
IOC_V2_BASE = "https://api.ioc-sealevelmonitoring.org/v2"

def get_ioc_headers():
    return {"X-API-KEY": IOC_API_KEY, "Accept": "application/json"}

# ─── Filtro Pacífico ───
PACIFIC_ZONES = [
    {"lon_min": -180, "lon_max": -70, "lat_min": -60, "lat_max": 65},
    {"lon_min": 100,  "lon_max": 180, "lat_min": -60, "lat_max": 65},
]

def is_in_pacific(lat: float, lon: float) -> bool:
    for zone in PACIFIC_ZONES:
        if (zone["lon_min"] <= lon <= zone["lon_max"] and
            zone["lat_min"] <= lat <= zone["lat_max"]):
            return True
    return False


# ─── Helpers ───
def make_id(*parts):
    raw = "-".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def classify_severity(mag, depth):
    """
    Funcion legacy de clasificacion por severity.
    [FASE 1]: Se mantiene para compatibilidad con frontend existente.
    La clasificacion oficial DHN se hace ahora en classify_dhn() del modulo dhn_classifier.
    """
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


# ─── [FASE 1] Helper para construir record con clasificacion DHN ───
def build_earthquake_record(eq_id, source_id, magnitude, depth_km, latitude, longitude,
                            place, event_time, tsunami_flag=0, alert_level=None,
                            raw_data=None):
    mag   = float(magnitude)
    depth = float(depth_km)
    lat   = float(latitude)
    lon   = float(longitude)

    dhn_result = classify_dhn(mag, depth, lat, lon)

    record = {
        "id":         eq_id,
        "source_id":  source_id,
        "magnitude":  round(mag, 1),
        "depth_km":   round(depth, 1),
        "latitude":   round(lat, 6),
        "longitude":  round(lon, 6),
        "place":      place or "",
        "event_time": event_time,
        "severity":   classify_severity(mag, depth),
        "dhn_level":  dhn_result["dhn_level"],
        "dhn_reason": dhn_result["dhn_reason"],
        "is_local":   dhn_result["is_local"],
    }

    if tsunami_flag is not None:
        record["tsunami_flag"] = tsunami_flag
    if alert_level is not None:
        record["alert_level"] = alert_level
    if raw_data is not None:
        record["raw_data"] = raw_data

    return record


async def log_fetch(source_id, status, records=0, error=None, duration_ms=0):
    try:
        supabase.table("fetch_log").insert({
            "source_id":       source_id,
            "status":          status,
            "records_fetched": records,
            "error_message":   error,
            "duration_ms":     duration_ms
        }).execute()
    except Exception as e:
        logger.error(f"Error logging fetch for {source_id}: {e}")


async def update_source_status(source_id, status="active"):
    try:
        supabase.table("sources").update({
            "status":     status,
            "last_fetch": datetime.now(timezone.utc).isoformat()
        }).eq("id", source_id).execute()
    except Exception as e:
        logger.error(f"Error updating source {source_id}: {e}")


# ═══════════════════════════════════════════
#  FETCHERS - Fuentes con API/datos estructurados
# ═══════════════════════════════════════════

# ─── 1. USGS Earthquakes ───
async def fetch_usgs():
    source_id = "usgs"
    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojson")
            r.raise_for_status()
            data = r.json()

        records = []
        for feature in data.get("features", []):
            props  = feature["properties"]
            coords = feature["geometry"]["coordinates"]
            eq_id  = feature.get("id", make_id("usgs", props.get("time")))
            mag    = props.get("mag") or 0
            depth  = coords[2] if len(coords) > 2 else 0

            record = build_earthquake_record(
                eq_id=eq_id, source_id=source_id,
                magnitude=mag, depth_km=depth,
                latitude=coords[1], longitude=coords[0],
                place=props.get("place", ""),
                event_time=datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc).isoformat(),
                tsunami_flag=props.get("tsunami", 0),
                alert_level=props.get("alert"),
                raw_data=json.dumps(props),
            )
            records.append(record)

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


# ─── 2. PTWC Tsunami Alerts ───
async def fetch_ptwc():
    source_id = "ptwc"
    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get("https://www.tsunami.gov/events/xml/PHEBAtom.xml")
            r.raise_for_status()

        feed    = feedparser.parse(r.text)
        records = []

        for entry in feed.entries:
            alert_id    = make_id("ptwc", entry.get("id", entry.get("title", "")))
            title       = entry.get("title", "")
            summary     = entry.get("summary", "")
            updated     = entry.get("updated", "")
            severity    = "info"
            alert_type  = "INFORMACION"
            title_upper = title.upper()

            if "WARNING" in title_upper or "ALARMA" in title_upper:
                severity = "critical"; alert_type = "ALARMA"
            elif "WATCH" in title_upper or "ADVISORY" in title_upper or "ALERTA" in title_upper:
                severity = "warning"; alert_type = "ALERTA"

            records.append({
                "id": alert_id, "source_id": source_id,
                "alert_type": alert_type,
                "title":   title[:500] if title else "PTWC Bulletin",
                "message": summary[:2000] if summary else "",
                "issued_at": updated or datetime.now(timezone.utc).isoformat(),
                "severity": severity,
                "raw_data": json.dumps({"title": title, "links": [l.get("href") for l in entry.get("links", [])]}),
            })

        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r2 = await client.get("https://www.tsunami.gov/events/xml/PAAQAtom.xml")
            if r2.status_code == 200:
                feed2 = feedparser.parse(r2.text)
                for entry in feed2.entries:
                    alert_id    = make_id("ntwc", entry.get("id", entry.get("title", "")))
                    title       = entry.get("title", "")
                    summary     = entry.get("summary", "")
                    severity    = "info"; alert_type = "INFORMACION"
                    title_upper = title.upper()
                    if "WARNING" in title_upper:
                        severity = "critical"; alert_type = "ALARMA"
                    elif "WATCH" in title_upper or "ADVISORY" in title_upper:
                        severity = "warning"; alert_type = "ALERTA"

                    records.append({
                        "id": alert_id, "source_id": source_id,
                        "alert_type": alert_type,
                        "title":   title[:500] if title else "NTWC Bulletin",
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


# ─── 3. NDBC DART Buoy Data ───
async def fetch_ndbc_dart():
    source_id     = "ndbc"
    start         = time.time()
    dart_stations = ["32412", "32413", "32411", "43412", "43413", "32301", "32302"]
    total_records = 0

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            for station_id in dart_stations:
                try:
                    url = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.dart"
                    r   = await client.get(url)
                    if r.status_code != 200:
                        continue

                    lines = r.text.strip().split("\n")
                    if len(lines) < 3:
                        continue

                    readings = []
                    for line in lines[2:12]:
                        parts = line.split()
                        if len(parts) < 8:
                            continue
                        try:
                            yy, mm, dd, hh, mn, ss = parts[0:6]
                            t_type = parts[6]
                            height = float(parts[7])
                            year   = 2000 + int(yy) if int(yy) < 100 else int(yy)
                            reading_time = datetime(year, int(mm), int(dd), int(hh), int(mn), int(ss), tzinfo=timezone.utc)
                            readings.append({
                                "station_id":    station_id,
                                "reading_time":  reading_time.isoformat(),
                                "water_level_mm": height,
                                "anomaly_mm":    0,
                                "is_event_mode": t_type == "2",
                            })
                        except (ValueError, IndexError):
                            continue

                    if readings:
                        supabase.table("buoy_readings").insert(readings).execute()
                        total_records += len(readings)
                        is_event = any(r["is_event_mode"] for r in readings)
                        status   = "alert" if is_event else "normal"
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
    start     = time.time()
    try:
        now        = datetime.now(timezone.utc)
        start_time = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
        url        = f"https://service.iris.edu/fdsnws/event/1/query?format=text&orderby=time&limit=50&minmag=4&starttime={start_time}"

        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get(url)
            r.raise_for_status()

        lines   = r.text.strip().split("\n")
        records = []
        for line in lines[1:]:
            parts = line.split("|")
            if len(parts) < 12:
                continue
            try:
                eq_id      = make_id("iris", parts[0].strip())
                event_time = parts[1].strip()
                lat        = float(parts[2].strip())
                lon        = float(parts[3].strip())
                depth      = float(parts[4].strip())
                mag        = float(parts[10].strip())
                place      = parts[12].strip() if len(parts) > 12 else ""
                record     = build_earthquake_record(
                    eq_id=eq_id, source_id=source_id,
                    magnitude=mag, depth_km=depth,
                    latitude=lat, longitude=lon,
                    place=place, event_time=event_time,
                )
                records.append(record)
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


# ─── 5. Chile Alerta API ───
async def fetch_chile():
    source_id = "csn_chile"
    start     = time.time()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get("https://chilealerta.com/api/query/?user=demo&select=ultimos_sismos_chile&limit=30")
            r.raise_for_status()
            data = r.json()

        records = []
        sismos  = data.get("ultimos_sismos_Chile", [])
        for s in sismos:
            eq_id  = make_id("chile", s.get("id", s.get("utc_time")))
            mag    = float(s.get("magnitude", 0))
            depth  = float(s.get("depth", 0))
            record = build_earthquake_record(
                eq_id=eq_id, source_id=source_id,
                magnitude=mag, depth_km=depth,
                latitude=s.get("latitude", 0),
                longitude=s.get("longitude", 0),
                place=s.get("reference", ""),
                event_time=s.get("utc_time", "").replace("/", "-"),
            )
            records.append(record)

        if records:
            supabase.table("earthquakes").upsert(records, on_conflict="id").execute()

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
                                    "id":         alert_id,
                                    "source_id":  "snam",
                                    "alert_type": "INFORMACION",
                                    "title":      item.get("title", "")[:500],
                                    "message":    item.get("description", "")[:2000],
                                    "issued_at":  item.get("date", datetime.now(timezone.utc).isoformat()),
                                    "severity":   "info",
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


# ═══════════════════════════════════════════
#  [FASE 2] ESCUCHA SOCIAL — RSS NOTICIAS
#  Guarda en news_raw Y news (legacy)
#  Fuentes: BBC Mundo, NYT Español, Washington Post
# ═══════════════════════════════════════════

KEYWORDS_SEISMICOS = [
    "tsunami", "sismo", "terremoto", "earthquake", "volcán", "erupción",
    "maremoto", "meteorito", "asteroide", "nivel del mar", "sea level",
    "deslizamiento", "landslide", "alerta", "alarma", "temblor", "réplica",
    "aftershock", "seísmo", "tectónica", "magma", "lava", "ceniza volcánica",
]

RSS_SOURCES_DAILY = [
    ("bbc",  "https://feeds.bbci.co.uk/mundo/rss.xml",                      "BBC Mundo"),
    ("nyt",  "https://rss.nytimes.com/services/xml/rss/nyt/espanol.xml",     "NYT Español"),
    ("wapo", "https://feeds.washingtonpost.com/rss/world",                   "Washington Post"),
]


async def fetch_news_rss():
    """
    [FASE 2] Fetcher de noticias RSS diario.
    Guarda artículos relevantes en:
    - news_raw (nueva tabla con fuente, fecha/hora exactas)
    - news (tabla legacy — compatibilidad frontend)
    """
    for source_id, url, source_name in RSS_SOURCES_DAILY:
        start = time.time()
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                r = await client.get(url)
                r.raise_for_status()

            feed         = feedparser.parse(r.text)
            records_raw  = []   # → news_raw
            records_news = []   # → news (legacy)
            fetched_at   = datetime.now(timezone.utc).isoformat()

            for entry in feed.entries[:50]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "") or entry.get("description", "")
                combined = (title + " " + summary).lower()

                if not any(kw in combined for kw in KEYWORDS_SEISMICOS):
                    continue

                news_id   = make_id(source_id, entry.get("link", title))
                relevance = "warning" if any(
                    kw in combined for kw in ["tsunami", "terremoto", "alarma", "maremoto"]
                ) else "info"

                # Detectar palabras clave presentes
                found_keywords = [kw for kw in KEYWORDS_SEISMICOS if kw in combined]

                # Fecha de publicación
                published_raw = entry.get("published", entry.get("updated", ""))
                try:
                    import email.utils
                    published_dt = email.utils.parsedate_to_datetime(published_raw).isoformat()
                except Exception:
                    published_dt = fetched_at

                # ── news_raw ──
                records_raw.append({
                    "id":           news_id,
                    "source_id":    source_id,
                    "title":        title[:500],
                    "url":          entry.get("link", ""),
                    "published_at": published_dt,
                    "fetched_at":   fetched_at,
                    "content":      summary[:2000],
                    "keywords":     found_keywords[:10],
                    "relevance":    relevance,
                    "processed":    False,
                })

                # ── news legacy ──
                records_news.append({
                    "id":           news_id,
                    "source_id":    source_id,
                    "title":        title[:500],
                    "summary":      summary[:1000],
                    "url":          entry.get("link", ""),
                    "published_at": published_dt,
                    "relevance":    relevance,
                })

            if records_raw:
                supabase.table("news_raw").upsert(records_raw, on_conflict="id").execute()
            if records_news:
                supabase.table("news").upsert(records_news, on_conflict="id").execute()

            duration = int((time.time() - start) * 1000)
            await log_fetch(source_id, "success", len(records_raw), duration_ms=duration)
            await update_source_status(source_id)
            logger.info(f"[FASE 2] {source_name}: {len(records_raw)} artículos relevantes guardados en news_raw")

        except Exception as e:
            duration = int((time.time() - start) * 1000)
            await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
            await update_source_status(source_id, "error")
            logger.error(f"[FASE 2] News {source_id} ({source_name}) error: {e}")


# ═══════════════════════════════════════════
#  [FASE 2] RESUMEN DIARIO INTELIGENTE
#  Genera resumen con Claude y guarda en news_summaries
# ═══════════════════════════════════════════

async def generate_daily_news_summary():
    """
    [FASE 2] Genera un resumen diario de noticias relevantes usando Claude API.
    Lee las últimas 24h de news_raw, llama a Claude, guarda en news_summaries.
    Lo consume ARIA/VIGÍA para enriquecer sus reportes.
    """
    logger.info("📰 Generando resumen diario de noticias con Claude...")
    start = time.time()

    now         = datetime.now(timezone.utc)
    period_end  = now
    period_start = now - timedelta(hours=24)

    try:
        # 1. Leer artículos de las últimas 24h desde news_raw
        result = (
            supabase.table("news_raw")
            .select("title, content, url, published_at, source_id, keywords, relevance")
            .gte("fetched_at", period_start.isoformat())
            .order("published_at", desc=True)
            .limit(50)
            .execute()
        )
        articles = result.data or []

        if not articles:
            logger.info("📰 Sin artículos nuevos en las últimas 24h — resumen omitido")
            return

        # 2. Construir contexto para Claude
        articles_text = ""
        sources_used  = {}
        for i, a in enumerate(articles, 1):
            src = a.get("source_id", "desconocida")
            sources_used[src] = sources_used.get(src, 0) + 1
            pub = a.get("published_at", "")[:16].replace("T", " ")
            kws = ", ".join(a.get("keywords", [])[:5])
            articles_text += (
                f"\n[{i}] FUENTE: {src.upper()} | FECHA: {pub} | RELEVANCIA: {a.get('relevance','info').upper()}\n"
                f"TITULAR: {a.get('title','')}\n"
                f"RESUMEN: {a.get('content','')[:400]}\n"
                f"PALABRAS CLAVE: {kws}\n"
                f"URL: {a.get('url','')}\n"
            )

        sources_list = [{"source": k, "articles": v} for k, v in sources_used.items()]
        period_label = f"{period_start.strftime('%d/%m/%Y %H:%M')} – {period_end.strftime('%d/%m/%Y %H:%M')} UTC"

        prompt = f"""Eres VIGÍA, sistema de inteligencia de noticias del CNAT (Centro Nacional de Alerta de Tsunamis) de la Marina de Guerra del Perú.

Analiza los siguientes {len(articles)} artículos capturados en las últimas 24 horas ({period_label}) y genera un resumen ejecutivo ESTRICTAMENTE enfocado en riesgos sísmicos y oceánicos.

ARTÍCULOS:
{articles_text}

REGLAS ESTRICTAS:
1. SOLO desarrolla noticias directamente relacionadas con: sismos, terremotos, tsunamis, volcanes, erupciones, maremotos, nivel del mar, alertas oceánicas, actividad tectónica.
2. Si hay noticias de salud, política, economía, deportes u otros temas: menciónalas en UNA sola línea como "Se detectaron X noticias sin relevancia sísmica (salud pública, política, etc.)". NO las desarrolles.
3. Si NO hay noticias sísmicas relevantes: indícalo claramente en una línea: "Sin eventos sísmicos o tsunamis destacados en el período analizado."
4. Máximo 250 palabras en total.
5. Menciona fuente y fecha solo para eventos sísmicos.
6. Termina con una línea de conclusión operativa para el oficial de guardia.
7. Nivel de alerta: NORMAL si no hay eventos sísmicos, VIGILANCIA si hay sismos menores, ELEVADO si hay sismos M5+ o alertas de tsunami.

Formato de respuesta (JSON estricto, sin texto fuera del JSON):
{{
  "resumen": "texto del resumen ejecutivo",
  "nivel_alerta": "NORMAL|VIGILANCIA|ELEVADO",
  "highlights": [
    {{"evento": "descripción breve", "fuente": "nombre fuente", "fecha": "dd/mm/yyyy HH:MM", "url": "url"}}
  ],
  "conclusion_operativa": "una sola frase para el oficial de guardia"
}}""""""

        # 3. Llamar a Claude API
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      "claude-sonnet-4-20250514",
                    "max_tokens": 1500,
                    "messages":   [{"role": "user", "content": prompt}],
                }
            )
            response.raise_for_status()
            claude_data = response.json()

        raw_text = claude_data["content"][0]["text"].strip()

        # 4. Parsear JSON de respuesta
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            # Fallback: extraer JSON del texto
            import re
            match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            parsed = json.loads(match.group()) if match else {}

        resumen_text     = parsed.get("resumen", raw_text)
        nivel_alerta     = parsed.get("nivel_alerta", "NORMAL")
        highlights       = parsed.get("highlights", [])
        conclusion       = parsed.get("conclusion_operativa", "")

        # Agregar conclusión al final del resumen si existe
        if conclusion:
            resumen_text += f"\n\n📋 CONCLUSIÓN OPERATIVA: {conclusion}"

        # Mapear nivel a score numérico
        relevance_score_map = {"NORMAL": 3.0, "VIGILANCIA": 6.0, "ELEVADO": 9.0}
        relevance_score = relevance_score_map.get(nivel_alerta, 3.0)

        # 5. Guardar en news_summaries
        summary_id = make_id("summary", now.isoformat())
        supabase.table("news_summaries").upsert([{
            "id":              summary_id,
            "generated_at":    now.isoformat(),
            "period_start":    period_start.isoformat(),
            "period_end":      period_end.isoformat(),
            "summary_text":    resumen_text,
            "sources_used":    json.dumps(sources_list),
            "articles_count":  len(articles),
            "relevance_score": relevance_score,
            "created_by":      "vigia",
            "highlights":      json.dumps(highlights),
        }], on_conflict="id").execute()

        # 6. Marcar artículos como procesados
        article_ids = [a.get("id") for a in articles if a.get("id")]
        if article_ids:
            for art_id in article_ids:
                supabase.table("news_raw").update({"processed": True}).eq("id", art_id).execute()

        duration = int((time.time() - start) * 1000)
        logger.info(f"📰 [FASE 2] Resumen diario generado: {len(articles)} artículos → nivel {nivel_alerta} ({duration}ms)")

    except Exception as e:
        logger.error(f"📰 [FASE 2] Error generando resumen diario: {e}")


# ─── 7. Sea Level UNESCO (API v1 - legacy) ───
async def fetch_sea_level():
    source_id = "sea_level"
    start     = time.time()
    try:
        url = "https://www.ioc-sealevelmonitoring.org/service.php?query=stationlist&showall=all&output=json"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get(url)
            r.raise_for_status()
            stations = r.json()

        pacific_stations = []
        for s in stations:
            try:
                lon = float(s.get("lon", 0))
                lat = float(s.get("lat", 0))
                if -120 <= lon <= -60 and -60 <= lat <= 20:
                    pacific_stations.append(s)
            except (ValueError, TypeError):
                continue

        for station in pacific_stations[:20]:
            station_id = make_id("sl", station.get("code", ""))
            try:
                supabase.table("buoy_stations").upsert([{
                    "id":           f"sl_{station.get('code', station_id)}",
                    "name":         station.get("location", "Unknown"),
                    "country":      station.get("country", ""),
                    "latitude":     round(float(station.get("lat", 0)), 6),
                    "longitude":    round(float(station.get("lon", 0)), 6),
                    "station_type": "tide_gauge",
                    "source_id":    source_id,
                    "status":       "normal",
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
#  8. MAPA MAREOGRÁFICO - IOC API v2
# ═══════════════════════════════════════════

async def fetch_sea_level_stations_v2():
    source_id = "ioc_v2"
    start     = time.time()
    logger.info("🌊 Fetching IOC sea level stations v2 (Pacific filter)...")

    now_iso      = datetime.now(timezone.utc).isoformat()
    PERU_PRIORITY = [
        {"code": "call2", "name": "Callao",            "country": "Peru", "lat": -12.07, "lon": -77.17, "status": "online", "api_status": "Operational", "sensor_type": "prs", "operator": "DHN Peru", "source": "IOC-SLSMF-v2", "fetched_at": now_iso},
        {"code": "IsHor", "name": "Isla Hormiga, Lima","country": "Peru", "lat": -11.98, "lon": -77.73, "status": "online", "api_status": "Operational", "sensor_type": "prs", "operator": "DHN Peru", "source": "IOC-SLSMF-v2", "fetched_at": now_iso},
        {"code": "chim1", "name": "Chimbote",          "country": "Peru", "lat":  -9.08, "lon": -78.61, "status": "online", "api_status": "Operational", "sensor_type": "prs", "operator": "DHN Peru", "source": "IOC-SLSMF-v2", "fetched_at": now_iso},
        {"code": "pait",  "name": "Paita",             "country": "Peru", "lat":  -5.08, "lon": -81.11, "status": "online", "api_status": "Operational", "sensor_type": "prs", "operator": "DHN Peru", "source": "IOC-SLSMF-v2", "fetched_at": now_iso},
        {"code": "talr",  "name": "Talara",            "country": "Peru", "lat":  -4.58, "lon": -81.28, "status": "online", "api_status": "Operational", "sensor_type": "prs", "operator": "DHN Peru", "source": "IOC-SLSMF-v2", "fetched_at": now_iso},
        {"code": "mata",  "name": "Matarani",          "country": "Peru", "lat": -17.00, "lon": -72.11, "status": "online", "api_status": "Operational", "sensor_type": "prs", "operator": "DHN Peru", "source": "IOC-SLSMF-v2", "fetched_at": now_iso},
        {"code": "sanjn", "name": "San Juan",          "country": "Peru", "lat": -15.36, "lon": -75.16, "status": "online", "api_status": "Operational", "sensor_type": "prs", "operator": "DHN Peru", "source": "IOC-SLSMF-v2", "fetched_at": now_iso},
        {"code": "pdas",  "name": "Pisco / San Andres","country": "Peru", "lat": -13.72, "lon": -76.22, "status": "online", "api_status": "Operational", "sensor_type": "prs", "operator": "DHN Peru", "source": "IOC-SLSMF-v2", "fetched_at": now_iso},
        {"code": "ilo1",  "name": "Ilo",               "country": "Peru", "lat": -17.64, "lon": -71.34, "status": "online", "api_status": "Operational", "sensor_type": "prs", "operator": "DHN Peru", "source": "IOC-SLSMF-v2", "fetched_at": now_iso},
    ]
    peru_codes_lower = {s["code"].lower() for s in PERU_PRIORITY}

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(f"{IOC_V2_BASE}/stations", headers=get_ioc_headers())
            response.raise_for_status()
            stations_raw = response.json()

        pacific_stations = []
        for station in stations_raw:
            try:
                lat = float(station.get("Lat", 0))
                lon = float(station.get("Lon", 0))
                if not is_in_pacific(lat, lon):
                    continue

                code = station.get("Code", "")
                if code.lower() in peru_codes_lower:
                    continue

                sensors        = station.get("sensor", [])
                primary_sensor = sensors[0] if sensors else {}
                api_status     = station.get("status", "Unknown")
                is_online      = api_status == "Operational"

                last_time_str = primary_sensor.get("lasttime", "")
                if last_time_str and is_online:
                    try:
                        last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        cutoff    = datetime.now(timezone.utc) - timedelta(hours=6)
                        if last_time < cutoff:
                            is_online = False
                    except (ValueError, TypeError):
                        pass

                pacific_stations.append({
                    "code":          code,
                    "name":          station.get("Location", "Unknown"),
                    "country":       station.get("countryname", station.get("country", "")),
                    "lat":           lat,
                    "lon":           lon,
                    "status":        "online" if is_online else "offline",
                    "api_status":    api_status,
                    "last_value":    primary_sensor.get("lastvalue"),
                    "last_time":     last_time_str,
                    "sensor_type":   primary_sensor.get("sensor", ""),
                    "sensor_units":  primary_sensor.get("units", ""),
                    "performance":   primary_sensor.get("performance", ""),
                    "transmit_type": station.get("transmittype", ""),
                    "operator":      station.get("localoperator", ""),
                    "source":        "IOC-SLSMF-v2",
                    "fetched_at":    now_iso
                })

            except (ValueError, TypeError, KeyError) as e:
                logger.warning(f"Skipping station: {e}")
                continue

        all_stations = PERU_PRIORITY + pacific_stations
        logger.info(f"🌊 Pacific: {len(pacific_stations)} + {len(PERU_PRIORITY)} Peru = {len(all_stations)} total")

        if all_stations and supabase:
            supabase.table("sea_level_stations").delete().neq("code", "").execute()
            batch_size = 50
            for i in range(0, len(all_stations), batch_size):
                supabase.table("sea_level_stations").insert(all_stations[i:i+batch_size]).execute()
            logger.info(f"✅ {len(all_stations)} stations saved")

        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "success", len(pacific_stations), duration_ms=duration)
        return pacific_stations

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            logger.error("❌ IOC API: 401 Unauthorized")
        else:
            logger.error(f"❌ IOC API HTTP error: {e}")
        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
        return []
    except Exception as e:
        logger.error(f"❌ Error fetching sea level stations v2: {e}")
        duration = int((time.time() - start) * 1000)
        await log_fetch(source_id, "error", error=str(e), duration_ms=duration)
        return []


async def fetch_station_sea_data(station_code: str, hours: int = 24):
    logger.info(f"🌊 Fetching tide data: station={station_code}, hours={hours}")
    now      = datetime.now(timezone.utc)
    start_dt = now - timedelta(hours=hours)

    try:
        url = "http://www.ioc-sealevelmonitoring.org/service.php"
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params={
                "query":     "data",
                "code":      station_code,
                "timestart": start_dt.strftime("%Y-%m-%dT%H:%M"),
                "timeend":   now.strftime("%Y-%m-%dT%H:%M"),
                "format":    "json"
            })
            response.raise_for_status()
            raw_data = response.json()

        processed = []
        if isinstance(raw_data, list):
            for point in raw_data:
                try:
                    val = point.get("slevel")
                    if val is not None and val != "":
                        processed.append({
                            "timestamp": point.get("stime", ""),
                            "value":     round(float(val), 4),
                            "sensor":    point.get("sensor", ""),
                        })
                except (ValueError, TypeError):
                    continue

        logger.info(f"🌊 Station {station_code}: {len(processed)} data points")
        return processed

    except Exception as e:
        logger.error(f"❌ Error fetching tide data for {station_code}: {e}")
        return []


# ═══════════════════════════════════════════
#  ORCHESTRATORS
# ═══════════════════════════════════════════

async def fetch_all():
    logger.info("═══ Starting full data fetch cycle ═══")
    await fetch_usgs()
    await fetch_ptwc()
    await fetch_ndbc_dart()
    await fetch_iris()
    await fetch_chile()
    await fetch_news_rss()       # [FASE 2] BBC + NYT + WaPo → news_raw + news
    logger.info("═══ Fetch cycle complete ═══")


async def fetch_slow():
    """Less frequent fetches (every 30 min)"""
    await fetch_sea_level()
    await fetch_sea_level_stations_v2()


async def fetch_daily():
    """[FASE 2] Tareas diarias: resumen inteligente de noticias"""
    await generate_daily_news_summary()


# ═══════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    global supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase connected")

    await load_jwks()

    try:
        await fetch_all()
    except Exception as e:
        logger.error(f"⚠️ Initial fetch_all failed (non-fatal): {e}")

    try:
        await fetch_slow()
    except Exception as e:
        logger.error(f"⚠️ Initial fetch_slow failed (non-fatal): {e}")

    # Schedulers
    scheduler.add_job(fetch_all,   "interval", minutes=5,  id="fetch_all")
    scheduler.add_job(fetch_slow,  "interval", minutes=30, id="fetch_slow")
    # [FASE 2] Resumen diario a las 06:00 UTC (01:00 Lima)
    scheduler.add_job(fetch_daily, "cron", hour=6, minute=0, id="fetch_daily")
    scheduler.start()
    logger.info("Scheduler: fetch/5min | sea_level/30min | resumen_diario/06:00UTC")

    yield

    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(
    title="CNAT - Sistema de Alerta Temprana de Tsunamis",
    description="Backend de ingesta de datos — DHN Marina de Guerra del Perú | MICROHELP © 2026",
    version="2.2.0",  # [FASE 2] Escucha Social Inteligente
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://cnat-frontend.vercel.app",
        "https://cnat-frontend-git-main-juliomarchenas-projects.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════
#  ENDPOINTS
# ═══════════════════════════════════════════

@app.get("/")
async def root():
    return {
        "system":   "CNAT - Centro Nacional de Alerta de Tsunamis",
        "provider": "MICROHELP",
        "status":   "operational",
        "version":  "2.2.0",
        "phase":    "FASE 2 - Escucha Social Inteligente",
        "endpoints": {
            "health":           "/health",
            "fetch_now":        "/fetch",
            "fetch_summary":    "/fetch-summary",
            "sources":          "/api/sources",
            "earthquakes":      "/api/earthquakes",
            "earthquakes_local":"/api/earthquakes/local",
            "earthquakes_dhn":  "/api/earthquakes/by-dhn-level/{level}",
            "alerts":           "/api/alerts",
            "buoys":            "/api/buoys",
            "news":             "/api/news",
            "news_raw":         "/api/news/raw",
            "news_summary":     "/api/news/summary",
            "sealevel_stations":"/api/sealevel/stations",
            "sealevel_data":    "/api/sealevel/station/{code}",
        }
    }


@app.get("/health")
async def health():
    try:
        result = supabase.table("sources").select("id", count="exact").execute()
        return {
            "status":        "healthy",
            "sources_count": result.count,
            "timestamp":     datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/fetch")
async def trigger_fetch():
    await fetch_all()
    return {"status": "fetch completed", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/fetch-summary")
async def trigger_summary():
    """[FASE 2] Dispara manualmente la generación del resumen diario de noticias"""
    await generate_daily_news_summary()
    return {"status": "summary generated", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/sources")
async def get_sources(user: dict = Depends(get_current_user)):
    result = supabase.table("sources").select("*").order("source_type").execute()
    return result.data


@app.get("/api/earthquakes")
async def get_earthquakes(limit: int = 50, min_mag: float = 0, user: dict = Depends(get_current_user)):
    result = (supabase.table("earthquakes")
              .select("*")
              .gte("magnitude", min_mag)
              .order("event_time", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/earthquakes/local")
async def get_local_earthquakes(limit: int = 50, user: dict = Depends(get_current_user)):
    result = (supabase.table("earthquakes")
              .select("*")
              .eq("is_local", True)
              .order("event_time", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/earthquakes/by-dhn-level/{level}")
async def get_earthquakes_by_dhn_level(level: str, limit: int = 50, user: dict = Depends(get_current_user)):
    level_upper  = level.upper()
    valid_levels = {"INFORMACION", "ALERTA", "ALARMA", "NO_APLICA"}
    if level_upper not in valid_levels:
        return {"error": f"Nivel invalido. Validos: {', '.join(sorted(valid_levels))}"}
    result = (supabase.table("earthquakes")
              .select("*")
              .eq("dhn_level", level_upper)
              .order("event_time", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/alerts")
async def get_alerts(limit: int = 20, user: dict = Depends(get_current_user)):
    result = (supabase.table("tsunami_alerts")
              .select("*")
              .order("issued_at", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/buoys")
async def get_buoys(user: dict = Depends(get_current_user)):
    result = supabase.table("buoy_stations").select("*").execute()
    return result.data


@app.get("/api/buoy-readings/{station_id}")
async def get_buoy_readings(station_id: str, limit: int = 100, user: dict = Depends(get_current_user)):
    result = (supabase.table("buoy_readings")
              .select("*")
              .eq("station_id", station_id)
              .order("reading_time", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/thresholds")
async def get_thresholds(user: dict = Depends(get_current_user)):
    result = supabase.table("alert_thresholds").select("*").order("priority").execute()
    return result.data


@app.get("/api/news")
async def get_news(limit: int = 20, user: dict = Depends(get_current_user)):
    """Endpoint legacy — tabla news original"""
    result = (supabase.table("news")
              .select("*")
              .order("published_at", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/news/raw")
async def get_news_raw(limit: int = 50, source_id: str = None, user: dict = Depends(get_current_user)):
    """[FASE 2] Artículos crudos con fuente, fecha/hora y keywords"""
    query = supabase.table("news_raw").select("*").order("fetched_at", desc=True).limit(limit)
    if source_id:
        query = query.eq("source_id", source_id)
    result = query.execute()
    return result.data


@app.get("/api/news/summary")
async def get_news_summary(limit: int = 5, user: dict = Depends(get_current_user)):
    """[FASE 2] Resúmenes diarios generados por Claude/VIGÍA"""
    result = (supabase.table("news_summaries")
              .select("*")
              .order("generated_at", desc=True)
              .limit(limit)
              .execute())
    return result.data


@app.get("/api/news/summary/latest")
async def get_latest_summary(user: dict = Depends(get_current_user)):
    """[FASE 2] Último resumen diario — usado por ARIA/VIGÍA"""
    result = (supabase.table("news_summaries")
              .select("*")
              .order("generated_at", desc=True)
              .limit(1)
              .execute())
    if result.data:
        return result.data[0]
    return {"message": "No hay resúmenes disponibles aún. Ejecuta /fetch-summary para generar el primero."}


@app.get("/api/fetch-log")
async def get_fetch_log(limit: int = 50, user: dict = Depends(get_current_user)):
    result = (supabase.table("fetch_log")
              .select("*")
              .order("fetched_at", desc=True)
              .limit(limit)
              .execute())
    return result.data


# ═══════════════════════════════════════════
#  ENDPOINTS: MAPA MAREOGRÁFICO
# ═══════════════════════════════════════════

@app.get("/api/sealevel/stations")
async def get_sealevel_stations(user: dict = Depends(get_current_user)):
    try:
        result = supabase.table("sea_level_stations").select("*").execute()
        return {"stations": result.data, "count": len(result.data), "source": "IOC-SLSMF-v2"}
    except Exception as e:
        logger.warning(f"sea_level_stations query error: {e}")
        return {"stations": [], "count": 0, "source": "IOC-SLSMF-v2"}


@app.get("/api/sealevel/station/{code}")
async def get_station_data(code: str, hours: int = 24, user: dict = Depends(get_current_user)):
    data = await fetch_station_sea_data(code, hours)
    if data:
        values = [p["value"] for p in data]
        stats  = {
            "min":    round(min(values), 3),
            "max":    round(max(values), 3),
            "mean":   round(sum(values) / len(values), 3),
            "range":  round(max(values) - min(values), 3),
            "points": len(data),
        }
    else:
        stats = {}
    return {"code": code, "data": data, "stats": stats}


@app.get("/api/sealevel/station/{code}/metadata")
async def get_station_metadata(code: str, user: dict = Depends(get_current_user)):
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(f"{IOC_V2_BASE}/stations/{code}", headers=get_ioc_headers())
            response.raise_for_status()
            data = response.json()
        if isinstance(data, list) and len(data) > 0:
            return {"code": code, "metadata": data[0]}
        return {"code": code, "metadata": data}
    except Exception as e:
        logger.error(f"Metadata error for {code}: {e}")
        return {"code": code, "error": str(e)}


@app.get("/api/dashboard")
async def get_dashboard(user: dict = Depends(get_current_user)):
    """Consolidated endpoint for the frontend dashboard"""
    earthquakes = (supabase.table("earthquakes")
                   .select("*").order("event_time", desc=True).limit(500).execute()).data

    alerts = (supabase.table("tsunami_alerts")
              .select("*").order("issued_at", desc=True).limit(10).execute()).data

    buoys   = supabase.table("buoy_stations").select("*").execute().data
    sources = supabase.table("sources").select("*").execute().data

    thresholds = supabase.table("alert_thresholds").select("*").order("priority").execute().data

    news = (supabase.table("news")
            .select("*").order("published_at", desc=True).limit(10).execute()).data

    # [FASE 2] Último resumen diario para ARIA/VIGÍA
    try:
        summary_result = (supabase.table("news_summaries")
                          .select("id,generated_at,summary_text,relevance_score,articles_count,highlights,created_by")
                          .order("generated_at", desc=True).limit(1).execute())
        latest_summary = summary_result.data[0] if summary_result.data else None
    except Exception:
        latest_summary = None

    try:
        sl_stations = supabase.table("sea_level_stations").select("code,status", count="exact").execute()
        sl_count    = sl_stations.count or 0
        sl_online   = sum(1 for s in (sl_stations.data or []) if s.get("status") == "online")
    except Exception:
        sl_count = 0; sl_online = 0

    # KPIs legacy
    critical_count = sum(1 for e in earthquakes if e.get("severity") == "critical")
    warning_count  = sum(1 for e in earthquakes if e.get("severity") == "warning")
    alert_buoys    = sum(1 for b in buoys if b.get("status") in ("alert", "warning"))
    sources_online = sum(1 for s in sources if s.get("status") == "active")

    # [FASE 1] KPIs DHN
    dhn_alarma_count    = sum(1 for e in earthquakes if e.get("dhn_level") == "ALARMA")
    dhn_alerta_count    = sum(1 for e in earthquakes if e.get("dhn_level") == "ALERTA")
    dhn_informacion_count = sum(1 for e in earthquakes if e.get("dhn_level") == "INFORMACION")
    local_count         = sum(1 for e in earthquakes if e.get("is_local") is True)

    if dhn_alarma_count > 0:
        risk_level = "ALTO"
    elif dhn_alerta_count > 0:
        risk_level = "MEDIO"
    elif local_count > 0:
        risk_level = "BAJO-VIGILANCIA"
    else:
        risk_level = "BAJO"

    return {
        "kpis": {
            "total_earthquakes":      len(earthquakes),
            "critical_count":         critical_count,
            "warning_count":          warning_count,
            "active_alerts":          len(alerts),
            "alert_buoys":            alert_buoys,
            "total_buoys":            len(buoys),
            "sources_online":         sources_online,
            "total_sources":          len(sources),
            "sealevel_stations":      sl_count,
            "sealevel_online":        sl_online,
            "risk_level":             risk_level,
            "dhn_alarma_count":       dhn_alarma_count,
            "dhn_alerta_count":       dhn_alerta_count,
            "dhn_informacion_count":  dhn_informacion_count,
            "local_earthquakes_count": local_count,
        },
        "earthquakes":  earthquakes,
        "alerts":       alerts,
        "buoys":        buoys,
        "sources":      sources,
        "thresholds":   thresholds,
        "news":         news,
        "news_summary": latest_summary,   # [FASE 2] para ARIA/VIGÍA
        "last_update":  datetime.now(timezone.utc).isoformat(),
    }
