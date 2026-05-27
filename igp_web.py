"""
igp_web.py - CORREGIDO
CNAT - Centro Nacional de Alerta de Tsunamis
Scraper: Web oficial IGP — ultimosismo.igp.gob.pe/evento/
MICROHELP © 2026

El sitio IGP es una SPA React — no hay tabla HTML parseable.
Estrategia: scraping de eventos individuales por número de reporte
secuencial (2026-0300, 0301, ..., hasta el más reciente).
"""

import re
import hashlib
import logging
from datetime import datetime, timezone, timedelta

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("cnat.igp_web")

IGP_BASE = "https://ultimosismo.igp.gob.pe"
HEADERS  = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "es-PE,es;q=0.9",
    "Referer":         "https://ultimosismo.igp.gob.pe/",
}

# Último reporte conocido — se actualiza dinámicamente en Supabase
_last_reporte_num = 314  # RS 2026-0314 fue el último conocido al 26/05/2026


def make_id(*parts):
    raw = "-".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def parse_fecha_igp(fecha_str: str) -> str:
    """Convierte '25/05/2026,16:52:44' a ISO UTC (Lima = UTC-5)."""
    if not fecha_str:
        return datetime.now(timezone.utc).isoformat()
    try:
        fecha_str = fecha_str.strip().replace(",", " ").replace("/", "/")
        dt_local  = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M:%S")
        dt_utc    = dt_local.replace(tzinfo=timezone.utc) + timedelta(hours=5)
        return dt_utc.isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def parse_evento_html(html: str, reporte_num: int) -> dict | None:
    """
    Parsea la página de un evento individual del IGP.
    Extrae datos del meta og:description o del texto visible.
    Formato conocido del og:description:
    'Magnitud: 6.5 | Profundidad: 129 km | Lat: -20.17 | Lon: -70.10'
    """
    soup = BeautifulSoup(html, "html.parser")

    # Intentar og:description primero (más confiable)
    og_desc = soup.find("meta", {"property": "og:description"})
    og_title = soup.find("meta", {"property": "og:title"})
    
    text = ""
    if og_desc and og_desc.get("content"):
        text = og_desc["content"]
    
    # También buscar en el body
    body_text = soup.get_text(" ", strip=True)
    full_text = text + " " + body_text

    # Extraer campos con regex
    mag   = re.search(r'[Mm]agnitud[:\s]+([\d\.]+)', full_text)
    prof  = re.search(r'[Pp]rofundidad[:\s]+([\d\.]+)', full_text)
    lat   = re.search(r'[Ll]at(?:itud)?[:\s]+([\-\d\.]+)', full_text)
    lon   = re.search(r'[Ll]on(?:gitud)?[:\s]+([\-\d\.]+)', full_text)
    intens = re.search(r'[Ii]ntensidad[:\s]+([^\|<\n]+)', full_text)
    refer  = re.search(r'[Rr]eferencia[:\s]+([^\|<\n]+)', full_text)
    fecha  = re.search(r'(\d{2}/\d{2}/\d{4}[,\s]+\d{2}:\d{2}:\d{2})', full_text)

    if not mag:
        return None  # No es una página de sismo válida

    reporte_id = f"2026-{reporte_num:04d}"
    tweet_id   = make_id("igp_web", reporte_id)

    mag_val   = float(mag.group(1))   if mag   else None
    prof_val  = float(prof.group(1))  if prof  else None
    lat_val   = float(lat.group(1))   if lat   else None
    lon_val   = float(lon.group(1))   if lon   else None
    intens_val = intens.group(1).strip()[:100] if intens else None
    refer_val  = refer.group(1).strip()[:200]  if refer  else None
    fecha_val  = parse_fecha_igp(fecha.group(1)) if fecha else datetime.now(timezone.utc).isoformat()

    raw = (f"IGP/CENSIS/RS {reporte_id}\n"
           f"Fecha: {fecha.group(1) if fecha else '?'}\n"
           f"Mag: {mag_val} | Prof: {prof_val}km\n"
           f"Lat: {lat_val} | Lon: {lon_val}\n"
           f"Intensidad: {intens_val}\nRef: {refer_val}")

    return {
        "id":           tweet_id,
        "tweet_id":     reporte_id,
        "author":       "@IGP_CENSIS_web",
        "raw_text":     raw,
        "published_at": fecha_val,
        "magnitude":    mag_val,
        "depth_km":     prof_val,
        "latitude":     lat_val,
        "longitude":    lon_val,
        "intensidad":   intens_val,
        "lugar":        refer_val,
        "reporte_id":   reporte_id,
        "source":       "igp_web",
    }


async def fetch_igp_web(supabase) -> int:
    """
    Scraping de eventos individuales del IGP.
    Busca los últimos 5 reportes desde el último conocido.
    Guarda nuevos en igp_tweets. Retorna count guardados.
    """
    global _last_reporte_num

    # ── Obtener el último número registrado en Supabase ──────────
    try:
        result = (supabase.table("igp_tweets")
                  .select("reporte_id")
                  .eq("source", "igp_web")
                  .order("published_at", desc=True)
                  .limit(1)
                  .execute())
        if result.data and result.data[0].get("reporte_id"):
            rid = result.data[0]["reporte_id"]  # e.g. "2026-0314"
            num = int(rid.split("-")[-1])
            _last_reporte_num = max(_last_reporte_num, num)
    except Exception:
        pass

    records  = []
    # Intentar los próximos 5 reportes desde el último conocido
    start    = _last_reporte_num + 1
    end      = _last_reporte_num + 6

    logger.info(f"🌐 IGP Web: buscando reportes 2026-{start:04d} a 2026-{end-1:04d}")

    async with httpx.AsyncClient(timeout=20, headers=HEADERS, follow_redirects=True) as client:
        for num in range(start, end):
            url = f"{IGP_BASE}/evento/2026-{num:04d}"
            try:
                r = await client.get(url)
                if r.status_code == 404:
                    logger.info(f"🌐 IGP: 2026-{num:04d} no existe aún")
                    break  # No hay más reportes nuevos
                if r.status_code == 200:
                    record = parse_evento_html(r.text, num)
                    if record:
                        records.append(record)
                        _last_reporte_num = num
                        logger.info(f"🌐 IGP: encontrado RS 2026-{num:04d} M{record['magnitude']}")
                    else:
                        logger.warning(f"🌐 IGP: 2026-{num:04d} sin datos parseables")
                        break
            except Exception as e:
                logger.warning(f"🌐 IGP Web 2026-{num:04d} error: {e}")
                break

    # ── Guardar en Supabase ──────────────────────────────────────
    if records:
        supabase.table("igp_tweets").upsert(records, on_conflict="id").execute()
        logger.info(f"🌐 IGP Web: {len(records)} reportes nuevos guardados")
    else:
        logger.info("🌐 IGP Web: sin reportes nuevos")

    return len(records)
