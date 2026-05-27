"""
igp_stream.py
CNAT - Centro Nacional de Alerta de Tsunamis
IGP/CENSIS — Twitter Polling (reemplaza Filtered Stream)
MICROHELP © 2026

Polling cada 5 minutos a GET /2/tweets/search/recent
Sin conexión persistente → sin 429 TooManyConnections.
Guarda tweets sísmicos en tabla igp_tweets.

USO EN main.py (sin cambios necesarios):
    from igp_stream import start_igp_stream
    asyncio.create_task(start_igp_stream(supabase))
"""

import re
import os
import asyncio
import hashlib
import logging
from datetime import datetime, timezone

import httpx

logger = logging.getLogger("cnat.igp")

TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")
POLL_INTERVAL        = 300  # 5 minutos
QUERY                = "from:Sismos_Peru_IGP (Magnitud OR Profundidad OR REPORTE OR sismo)"
MAX_RESULTS          = 10

# Último tweet_id procesado — evita duplicados sin consultar Supabase
_last_tweet_id = None


def _headers():
    return {
        "Authorization": f"Bearer {TWITTER_BEARER_TOKEN}",
        "Content-Type":  "application/json",
    }


def make_id(*parts):
    raw = "-".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def parse_igp_tweet(text: str) -> dict:
    """
    Parsea el formato estándar IGP/CENSIS:
    IGP/CENSIS/RS 2026-0314
    Magnitud: 3.8 | Profundidad: 62km
    Latitud: -14.24 | Longitud: -75.74
    Intensidad: II Ica
    Referencia: 19 km al S de Ica
    """
    result = {
        "magnitude":  None,
        "depth_km":   None,
        "latitude":   None,
        "longitude":  None,
        "intensidad": None,
        "lugar":      None,
        "reporte_id": None,
    }
    if not text:
        return result
    try:
        m = re.search(r'RS\s+([\d\-]+)', text)
        if m: result["reporte_id"] = m.group(1).strip()

        m = re.search(r'[Mm]ag(?:nitud)?[:\s]+([\d\.]+)', text)
        if m: result["magnitude"] = float(m.group(1))

        m = re.search(r'[Pp]rof(?:undidad)?[:\s]+([\d\.]+)', text)
        if m: result["depth_km"] = float(m.group(1))

        m = re.search(r'[Ll]at(?:itud)?[:\s]+([\-\d\.]+)', text)
        if m: result["latitude"] = float(m.group(1))

        m = re.search(r'[Ll]on(?:gitud)?[:\s]+([\-\d\.]+)', text)
        if m: result["longitude"] = float(m.group(1))

        m = re.search(r'[Ii]ntensidad[:\s]+([^\n]+)', text)
        if m: result["intensidad"] = m.group(1).strip()[:100]

        m = re.search(r'[Rr]ef(?:erencia)?[:\s]+([^\n]+)', text)
        if m: result["lugar"] = m.group(1).strip()[:200]

    except Exception as e:
        logger.warning(f"IGP parse error: {e}")
    return result


async def _fetch_recent_tweets(supabase) -> int:
    """
    Hace un GET /2/tweets/search/recent y guarda nuevos tweets.
    Retorna número de tweets nuevos guardados.
    """
    global _last_tweet_id

    if not TWITTER_BEARER_TOKEN:
        logger.warning("🐦 IGP: TWITTER_BEARER_TOKEN no configurado")
        return 0

    params = {
        "query":        QUERY,
        "max_results":  MAX_RESULTS,
        "tweet.fields": "created_at,author_id,text",
        "expansions":   "author_id",
        "user.fields":  "username",
    }
    if _last_tweet_id:
        params["since_id"] = _last_tweet_id

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                "https://api.twitter.com/2/tweets/search/recent",
                headers=_headers(),
                params=params,
            )

        if r.status_code == 429:
            logger.warning("🐦 IGP: rate limit — esperando próximo ciclo")
            return 0

        if r.status_code != 200:
            logger.error(f"🐦 IGP: HTTP {r.status_code} — {r.text[:200]}")
            return 0

        data     = r.json()
        tweets   = data.get("data", [])
        users    = {u["id"]: u["username"] for u in data.get("includes", {}).get("users", [])}

        if not tweets:
            logger.info("🐦 IGP: sin tweets nuevos")
            return 0

        records = []
        for tw in tweets:
            text      = tw.get("text", "")
            tw_id     = tw.get("id", "")
            author_id = tw.get("author_id", "")
            username  = users.get(author_id, "Sismos_Peru_IGP")
            created   = tw.get("created_at", datetime.now(timezone.utc).isoformat())

            parsed    = parse_igp_tweet(text)
            tweet_id  = make_id("tw", tw_id)

            records.append({
                "id":           tweet_id,
                "tweet_id":     tw_id,
                "author":       f"@{username}",
                "raw_text":     text[:1000],
                "published_at": created,
                "magnitude":    parsed["magnitude"],
                "depth_km":     parsed["depth_km"],
                "latitude":     parsed["latitude"],
                "longitude":    parsed["longitude"],
                "intensidad":   parsed["intensidad"],
                "lugar":        parsed["lugar"],
                "reporte_id":   parsed["reporte_id"],
                "source":       "twitter",
            })

        # Guardar en Supabase
        supabase.table("igp_tweets").upsert(records, on_conflict="id").execute()

        # Actualizar cursor — el más reciente es el primero
        _last_tweet_id = tweets[0]["id"]

        logger.info(f"🐦 IGP: {len(records)} tweet(s) nuevo(s) guardados")
        return len(records)

    except Exception as e:
        logger.error(f"🐦 IGP polling error: {e}")
        return 0


async def start_igp_stream(supabase_client):
    """
    Loop de polling cada 5 minutos.
    Reemplaza el Filtered Stream — sin conexión persistente.
    Compatible con el mismo import en main.py.
    """
    if not TWITTER_BEARER_TOKEN:
        logger.warning("🐦 IGP: TWITTER_BEARER_TOKEN no configurado — polling desactivado")
        return

    logger.info("🐦 IGP: polling iniciado (@Sismos_Peru_IGP cada 5 min)")

    # Primera consulta inmediata al arrancar
    await _fetch_recent_tweets(supabase_client)

    # Loop cada 5 minutos
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        await _fetch_recent_tweets(supabase_client)
