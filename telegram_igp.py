"""
telegram_igp.py
CNAT - Centro Nacional de Alerta de Tsunamis
Scraper: Canal Telegram @sismos_peru_igp
MICROHELP © 2026

Lee el canal público @sismos_peru_igp vía Telegram Bot API.
Parsea mensajes sísmicos y los guarda en la tabla igp_tweets
(misma tabla que el Filtered Stream de Twitter — fuente cruzada).

USO EN main.py:
    from telegram_igp import fetch_telegram_igp
    # En scheduler:
    scheduler.add_job(fetch_telegram_igp, "interval", minutes=5, id="telegram_igp",
                      args=[supabase])
    # En lifespan, arranque inicial:
    asyncio.create_task(fetch_telegram_igp(supabase))
"""

import re
import os
import logging
import hashlib
from datetime import datetime, timezone

import httpx

logger = logging.getLogger("cnat.telegram")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL   = "@sismos_peru_igp"
TELEGRAM_API       = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# ID del canal (se resuelve en el primer fetch)
_channel_id_cache = None


def make_id(*parts):
    raw = "-".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def parse_igp_message(text: str) -> dict:
    """
    Parsea el formato estándar del canal Telegram del IGP:

    REPORTE SÍSMICO
    IGP/CENSIS/RS 2026-0314
    Fecha y Hora Local: 26/05/2026 12:34:26
    Magnitud: 3.8 | Profundidad: 62km
    Latitud: -14.24 | Longitud: -75.74
    Intensidad: II Ica
    Referencia: 19 km al S de Ica, Ica - Ica
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
        logger.warning(f"Telegram parse error: {e}")

    return result


def is_seismic_message(text: str) -> bool:
    """Filtra solo mensajes sísmicos del canal."""
    if not text:
        return False
    keywords = [
        "reporte sísmico", "reporte sismico", "igp/censis",
        "magnitud", "profundidad", "latitud", "sismo"
    ]
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)


async def fetch_telegram_igp(supabase) -> int:
    """
    Fetch de mensajes recientes del canal @sismos_peru_igp.
    Guarda en tabla igp_tweets (misma que Twitter stream).
    Retorna número de mensajes nuevos guardados.
    """
    global _channel_id_cache

    if not TELEGRAM_BOT_TOKEN:
        logger.warning("📡 Telegram: TELEGRAM_BOT_TOKEN no configurado")
        return 0

    saved = 0

    try:
        async with httpx.AsyncClient(timeout=30) as client:

            # ── Paso 1: Resolver chat_id del canal ──────────────────────
            if not _channel_id_cache:
                r = await client.get(
                    f"{TELEGRAM_API}/getChat",
                    params={"chat_id": TELEGRAM_CHANNEL}
                )
                if r.status_code == 200:
                    chat_data = r.json()
                    if chat_data.get("ok"):
                        _channel_id_cache = chat_data["result"]["id"]
                        logger.info(f"📡 Telegram: canal {TELEGRAM_CHANNEL} = {_channel_id_cache}")
                    else:
                        logger.error(f"📡 Telegram getChat error: {chat_data}")
                        return 0
                else:
                    logger.error(f"📡 Telegram HTTP {r.status_code}")
                    return 0

            # ── Paso 2: Obtener últimas actualizaciones del canal ────────
            # Para canales públicos usamos getUpdates o forwardMessages
            # La forma más confiable es getUpdates con offset
            r = await client.get(
                f"{TELEGRAM_API}/getUpdates",
                params={
                    "limit":   20,
                    "timeout": 0,
                    "allowed_updates": ["channel_post"],
                }
            )

            if r.status_code != 200:
                logger.error(f"📡 Telegram getUpdates HTTP {r.status_code}")
                return 0

            data = r.json()
            if not data.get("ok"):
                logger.error(f"📡 Telegram getUpdates error: {data}")
                return 0

            updates = data.get("result", [])
            logger.info(f"📡 Telegram: {len(updates)} updates recibidos")

            records = []
            for update in updates:
                post = update.get("channel_post", {})
                if not post:
                    continue

                # Solo mensajes del canal IGP
                chat = post.get("chat", {})
                username = chat.get("username", "").lower()
                if "sismos_peru_igp" not in username and "igp_peru" not in username:
                    continue

                text = post.get("text", "") or post.get("caption", "")
                if not is_seismic_message(text):
                    continue

                msg_id     = post.get("message_id", 0)
                date_unix  = post.get("date", 0)
                published  = datetime.fromtimestamp(date_unix, tz=timezone.utc).isoformat()
                tweet_id   = make_id("tg", msg_id)

                parsed = parse_igp_message(text)

                records.append({
                    "id":           tweet_id,
                    "tweet_id":     str(msg_id),
                    "author":       f"@{chat.get('username', 'sismos_peru_igp')}",
                    "raw_text":     text[:1000],
                    "published_at": published,
                    "magnitude":    parsed["magnitude"],
                    "depth_km":     parsed["depth_km"],
                    "latitude":     parsed["latitude"],
                    "longitude":    parsed["longitude"],
                    "intensidad":   parsed["intensidad"],
                    "lugar":        parsed["lugar"],
                    "reporte_id":   parsed["reporte_id"],
                    "source":       "telegram",
                })

            if records:
                supabase.table("igp_tweets").upsert(
                    records, on_conflict="id"
                ).execute()
                saved = len(records)
                logger.info(f"📡 Telegram IGP: {saved} mensajes sísmicos guardados")
            else:
                logger.info("📡 Telegram IGP: sin mensajes sísmicos nuevos")

    except Exception as e:
        logger.error(f"📡 Telegram IGP error: {e}")

    return saved
