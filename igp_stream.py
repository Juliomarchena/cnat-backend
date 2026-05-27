"""
igp_stream.py
IGP/CENSIS — Twitter Filtered Stream
Marina de Guerra del Perú | MICROHELP © 2026

Módulo independiente que escucha en tiempo real los tweets
de @Sismos_Peru_IGP y los guarda en la tabla igp_tweets.

IMPORTANTE:
- Solo se activa cuando el IGP publica un tweet sísmico
- Heartbeats vacíos NO cuestan nada
- Costo estimado: ~$1.50/mes (300 tweets x $0.005)
- Reconexión automática si se cae el stream

USO EN main.py:
    from igp_stream import start_igp_stream
    # En lifespan, después de scheduler.start():
    asyncio.create_task(start_igp_stream(supabase))
"""

import re
import os
import json
import asyncio
import logging
from datetime import datetime, timezone

import httpx

logger = logging.getLogger("cnat.igp")

TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")


# ─── Headers Twitter API v2 ───
def _headers():
    return {
        "Authorization": f"Bearer {TWITTER_BEARER_TOKEN}",
        "Content-Type":  "application/json",
    }


# ─── Parsea el texto del tweet IGP ───
def parse_igp_tweet(text: str) -> dict:
    """
    Parsea el formato estándar del IGP/CENSIS:

    IGP/CENSIS/RS 2026-0293
    Fecha y Hora Local: 20/05/2026 07:14:12
    Magnitud: 3.6
    Profundidad: 60km
    Latitud: -14.24
    Longitud: -75.74
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
    try:
        m = re.search(r'RS\s+([\d\-]+)', text)
        if m: result["reporte_id"] = m.group(1).strip()

        m = re.search(r'[Mm]ag(?:nitud)?[:\s]+([\d\.]+)', text)
        if m: result["magnitude"] = float(m.group(1))

        m = re.search(r'[Pp]rof(?:undidad)?[:\s]+([\d\.]+)', text)
        if m: result["depth_km"] = float(m.group(1))

        m = re.search(r'[Ll]at(?:itud)?[:\s]+([\-\d\.]+)', text)
        if m: result["latitude"] = float(m.group(1))

        m = re.search(r'[Ll]ong(?:itud)?[:\s]+([\-\d\.]+)', text)
        if m: result["longitude"] = float(m.group(1))

        m = re.search(r'[Ii]ntensidad[:\s]+([^\n]+)', text)
        if m: result["intensidad"] = m.group(1).strip()[:100]

        m = re.search(r'[Rr]ef(?:erencia)?[:\s]+([^\n]+)', text)
        if m: result["lugar"] = m.group(1).strip()[:200]

    except Exception as e:
        logger.warning(f"parse_igp_tweet error: {e}")

    return result


# ─── Configura el filtro del stream ───
async def _setup_rule():
    """
    Regla quirúrgica: SOLO tweets de @Sismos_Peru_IGP
    que contengan palabras sísmicas clave.
    Ningún otro tweet pasará este filtro.
    """
    rule_value = 'from:Sismos_Peru_IGP (Magnitud OR Profundidad OR REPORTE OR sismo)'
    rule_tag   = 'IGP_CENSIS_SISMOS'

    try:
        async with httpx.AsyncClient(timeout=30) as client:

            # 1. Ver reglas existentes
            r        = await client.get(
                "https://api.twitter.com/2/tweets/search/stream/rules",
                headers=_headers()
            )
            existing = r.json().get("data", [])

            # 2. Eliminar reglas antiguas con el mismo tag
            ids_del = [x["id"] for x in existing if x.get("tag") == rule_tag]
            if ids_del:
                await client.post(
                    "https://api.twitter.com/2/tweets/search/stream/rules",
                    headers=_headers(),
                    json={"delete": {"ids": ids_del}}
                )
                logger.info(f"IGP Stream: {len(ids_del)} regla(s) antigua(s) eliminada(s)")

            # 3. Crear regla nueva
            r2     = await client.post(
                "https://api.twitter.com/2/tweets/search/stream/rules",
                headers=_headers(),
                json={"add": [{"value": rule_value, "tag": rule_tag}]}
            )
            result = r2.json()
            logger.info(f"IGP Stream: regla configurada → {result}")

    except Exception as e:
        logger.error(f"IGP _setup_rule error: {e}")


# ─── Stream principal ───
async def start_igp_stream(supabase_client):
    """
    Punto de entrada principal.
    Llamar desde main.py con:
        asyncio.create_task(start_igp_stream(supabase))

    Escucha indefinidamente con reconexión automática.
    Cada tweet sísmico del IGP se guarda en igp_tweets.
    """
    if not TWITTER_BEARER_TOKEN:
        logger.warning("IGP Stream: TWITTER_BEARER_TOKEN no configurado — stream desactivado")
        return
    
    # Esperar 15s para que la conexión anterior se cierre
    await asyncio.sleep(15)

    logger.info("🐦 IGP Stream: configurando regla de filtro...")
    await _setup_rule()

    stream_url = (
        "https://api.twitter.com/2/tweets/search/stream"
        "?tweet.fields=created_at,author_id,text"
        "&expansions=author_id"
        "&user.fields=username"
    )

    logger.info("🐦 IGP Stream: conectado — escuchando @Sismos_Peru_IGP en tiempo real...")

    while True:  # Reconexión automática
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream(
                    "GET", stream_url, headers=_headers()
                ) as response:

                    if response.status_code != 200:
                        body = await response.aread()
                        logger.error(f"IGP Stream HTTP {response.status_code}: {body[:200]}")
                        await asyncio.sleep(60)
                        continue

                    logger.info("🐦 IGP Stream: conexión activa — esperando sismos...")

                    async for line in response.aiter_lines():

                        # Heartbeat vacío — no cobra nada, ignorar
                        if not line.strip():
                            continue

                        try:
                            payload    = json.loads(line)
                            tweet      = payload.get("data", {})
                            tweet_id   = tweet.get("id",   "")
                            tweet_text = tweet.get("text", "")
                            created_at = tweet.get(
                                "created_at",
                                datetime.now(timezone.utc).isoformat()
                            )

                            if not tweet_id or not tweet_text:
                                continue

                            # Parsear campos sísmicos
                            parsed = parse_igp_tweet(tweet_text)

                            # Guardar en Supabase
                            supabase_client.table("igp_tweets").upsert([{
                                "id":           tweet_id,
                                "tweet_text":   tweet_text[:1000],
                                "published_at": created_at,
                                "magnitude":    parsed["magnitude"],
                                "depth_km":     parsed["depth_km"],
                                "latitude":     parsed["latitude"],
                                "longitude":    parsed["longitude"],
                                "intensidad":   parsed["intensidad"],
                                "lugar":        parsed["lugar"],
                                "reporte_id":   parsed["reporte_id"],
                            }], on_conflict="id").execute()

                            logger.info(
                                f"🐦 IGP Tweet guardado: "
                                f"M{parsed['magnitude']} | "
                                f"{parsed['lugar']} | "
                                f"RS:{parsed['reporte_id']}"
                            )

                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            logger.error(f"IGP tweet process error: {e}")
                            continue

        except httpx.ReadTimeout:
            logger.warning("🐦 IGP Stream: timeout — reconectando en 30s...")
            await asyncio.sleep(30)

        except httpx.ConnectError:
            logger.warning("🐦 IGP Stream: sin conexión — reconectando en 60s...")
            await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"🐦 IGP Stream error inesperado: {e} — reconectando en 60s...")
            await asyncio.sleep(60)
