"""
igp_web.py
CNAT - Centro Nacional de Alerta de Tsunamis
Scraper: Web oficial IGP — ultimosismo.igp.gob.pe
MICROHELP © 2026

Lee los sismos reportados del sitio oficial del IGP/CENSIS.
Parsea y guarda en igp_tweets (misma tabla que Twitter stream).
Sin tokens, sin riesgos, fuente oficial del estado peruano.

DATOS QUE EXTRAE:
- Reporte ID (IGP/CENSIS/RS 2026-XXXX)
- Fecha y hora local
- Magnitud, profundidad
- Latitud, longitud
- Intensidad
- Referencia geográfica

USO EN main.py:
    from igp_web import fetch_igp_web
    scheduler.add_job(fetch_igp_web, "interval", minutes=5,
                      id="igp_web", args=[supabase])
    asyncio.create_task(fetch_igp_web(supabase))
"""

import re
import hashlib
import logging
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("cnat.igp_web")

IGP_URL     = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
IGP_BASE    = "https://ultimosismo.igp.gob.pe"
IGP_API     = "https://ultimosismo.igp.gob.pe/api/sismos"

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/html, */*",
    "Accept-Language": "es-PE,es;q=0.9",
    "Referer":         "https://ultimosismo.igp.gob.pe/",
}


def make_id(*parts):
    raw = "-".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def parse_fecha_igp(fecha_str: str):
    """
    Convierte '25/05/2026,16:52:44' o '25/05/2026 16:52:44' a ISO UTC.
    El IGP reporta en hora local Lima (UTC-5).
    """
    if not fecha_str:
        return datetime.now(timezone.utc).isoformat()
    try:
        fecha_str = fecha_str.strip().replace(",", " ")
        dt_local  = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M:%S")
        # Lima es UTC-5
        from datetime import timedelta
        dt_utc = dt_local.replace(tzinfo=timezone.utc) + timedelta(hours=5)
        return dt_utc.isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def parse_igp_record(row: dict) -> dict:
    """
    Parsea un registro JSON de la API del IGP.
    Campos esperados: id, fecha_local, magnitud, profundidad,
                      latitud, longitud, intensidad, referencia
    """
    reporte_id = str(row.get("id_reporte") or row.get("id") or "")
    fecha      = row.get("fecha_local") or row.get("fecha") or ""
    mag        = row.get("magnitud") or row.get("mag")
    prof       = row.get("profundidad") or row.get("prof")
    lat        = row.get("latitud") or row.get("lat")
    lon        = row.get("longitud") or row.get("lon")
    intens     = row.get("intensidad") or ""
    refer      = row.get("referencia") or row.get("lugar") or ""

    tweet_id = make_id("igp_web", reporte_id or fecha or str(mag))

    return {
        "id":           tweet_id,
        "tweet_id":     reporte_id,
        "author":       "@IGP_web",
        "raw_text":     f"IGP/CENSIS/RS {reporte_id}\nFecha: {fecha}\nMag: {mag} | Prof: {prof}km\nLat: {lat} | Lon: {lon}\nIntensidad: {intens}\nRef: {refer}",
        "published_at": parse_fecha_igp(fecha),
        "magnitude":    float(mag)  if mag  else None,
        "depth_km":     float(prof) if prof else None,
        "latitude":     float(lat)  if lat  else None,
        "longitude":    float(lon)  if lon  else None,
        "intensidad":   str(intens)[:100] if intens else None,
        "lugar":        str(refer)[:200]  if refer  else None,
        "reporte_id":   reporte_id,
        "source":       "igp_web",
    }


def parse_igp_html(html: str) -> list:
    """
    Fallback: parsea la tabla HTML del sitio IGP si la API no responde.
    Extrae los últimos sismos de la tabla de reportes.
    """
    records = []
    try:
        soup  = BeautifulSoup(html, "html.parser")
        tabla = soup.find("table")
        if not tabla:
            return records

        filas = tabla.find_all("tr")[1:]  # skip header
        for fila in filas[:20]:
            celdas = fila.find_all("td")
            if len(celdas) < 6:
                continue
            try:
                fecha  = celdas[0].get_text(strip=True)
                mag    = celdas[1].get_text(strip=True)
                prof   = celdas[2].get_text(strip=True)
                lat    = celdas[3].get_text(strip=True)
                lon    = celdas[4].get_text(strip=True)
                refer  = celdas[5].get_text(strip=True) if len(celdas) > 5 else ""

                tweet_id = make_id("igp_html", fecha, mag)
                records.append({
                    "id":           tweet_id,
                    "tweet_id":     tweet_id,
                    "author":       "@IGP_web",
                    "raw_text":     f"Fecha: {fecha} | Mag: {mag} | Prof: {prof}km\nLat: {lat} | Lon: {lon}\nRef: {refer}",
                    "published_at": parse_fecha_igp(fecha),
                    "magnitude":    float(mag)  if mag  else None,
                    "depth_km":     float(re.sub(r'[^\d.]', '', prof)) if prof else None,
                    "latitude":     float(lat)  if lat  else None,
                    "longitude":    float(lon)  if lon  else None,
                    "intensidad":   None,
                    "lugar":        refer[:200] if refer else None,
                    "reporte_id":   None,
                    "source":       "igp_web",
                })
            except (ValueError, IndexError):
                continue
    except Exception as e:
        logger.warning(f"IGP HTML parse error: {e}")
    return records


async def fetch_igp_web(supabase) -> int:
    """
    Fetch de sismos desde el sitio oficial IGP/CENSIS.
    Intenta la API JSON primero, luego HTML como fallback.
    Guarda en tabla igp_tweets.
    Retorna número de registros nuevos guardados.
    """
    records = []

    async with httpx.AsyncClient(timeout=30, headers=HEADERS, follow_redirects=True) as client:

        # ── Intento 1: API JSON del IGP ─────────────────────────────
        try:
            r = await client.get(IGP_API, params={"limit": 20, "year": 2026})
            if r.status_code == 200:
                data = r.json()
                # La API puede devolver lista o dict con "data"
                items = data if isinstance(data, list) else data.get("data", data.get("sismos", []))
                for item in items[:20]:
                    records.append(parse_igp_record(item))
                logger.info(f"🌐 IGP Web API: {len(records)} sismos obtenidos")
        except Exception as e:
            logger.warning(f"🌐 IGP API JSON falló: {e} — intentando HTML...")

        # ── Intento 2: HTML scraping ─────────────────────────────────
        if not records:
            try:
                r = await client.get(IGP_URL)
                if r.status_code == 200:
                    records = parse_igp_html(r.text)
                    logger.info(f"🌐 IGP Web HTML: {len(records)} sismos parseados")
                else:
                    logger.warning(f"🌐 IGP Web HTTP {r.status_code}")
            except Exception as e:
                logger.error(f"🌐 IGP Web HTML error: {e}")

        # ── Intento 3: API alternativa con año/mes ───────────────────
        if not records:
            try:
                now = datetime.now(timezone.utc)
                url = f"{IGP_BASE}/api/sismos/{now.year}/{now.month:02d}"
                r   = await client.get(url)
                if r.status_code == 200:
                    data  = r.json()
                    items = data if isinstance(data, list) else data.get("data", [])
                    for item in items[:20]:
                        records.append(parse_igp_record(item))
                    logger.info(f"🌐 IGP API alt: {len(records)} sismos")
            except Exception as e:
                logger.warning(f"🌐 IGP API alt error: {e}")

    # ── Guardar en Supabase ──────────────────────────────────────────
    if records:
        try:
            supabase.table("igp_tweets").upsert(
                records, on_conflict="id"
            ).execute()
            logger.info(f"🌐 IGP Web: {len(records)} sismos guardados en igp_tweets")
        except Exception as e:
            logger.error(f"🌐 IGP Web Supabase error: {e}")
    else:
        logger.info("🌐 IGP Web: sin datos nuevos")

    return len(records)
