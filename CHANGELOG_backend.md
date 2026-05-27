# CHANGELOG — cnat-backend
**Repositorio:** github.com/Juliomarchena/cnat-backend  
**URL producción:** https://cnat-backend-1.onrender.com  
**Stack:** FastAPI + Python + Supabase + Render (Docker)

---

## [v2.3.0] — 27/05/2026

### Agregado
- `igp_stream.py` — Módulo independiente Twitter Filtered Stream escuchando `@Sismos_Peru_IGP`. Parser regex para formato IGP/CENSIS. Guarda en tabla `igp_tweets`.
- `igp_web.py` — Scraper secuencial de eventos IGP por número `RS 2026-XXXX` en `ultimosismo.igp.gob.pe/evento/`. Detecta nuevos reportes automáticamente.
- `telegram_igp.py` — Scraper Bot API Telegram para `@sismos_peru_igp`. Bot creado: `@cnat_igp_monitor_bot`. ⚠️ No funcional — bot no puede leer canales de terceros.
- `main.py`: import `start_igp_stream`, `fetch_telegram_igp`, `fetch_igp_web`
- Scheduler: 2 jobs nuevos — `fetch_telegram_igp` (5min), `fetch_igp_web` (5min)
- `asyncio.create_task(start_igp_stream(supabase))` en lifespan
- `asyncio.sleep(15)` en `igp_stream.py` para evitar 429 en deploys
- Endpoint `GET /api/igp-tweets` — retorna últimos tweets IGP
- Endpoint `GET /api/telegram-igp` — dispara manualmente scraper Telegram
- Variable de entorno `TELEGRAM_BOT_TOKEN` en Render
- Variable de entorno `TWITTER_BEARER_TOKEN` regenerada (token nuevo 27/05/2026)

### Estado Twitter Stream
- ❌ **429 TooManyConnections persistente**. Causa: Render inicia nuevo proceso antes de cerrar el anterior. Tier pay-per-use permite solo 1 conexión simultánea.
- **Solución pendiente:** Reemplazar Stream por polling `GET /2/tweets/search/recent` cada 5min.

### Commits
| Commit | Descripción |
|--------|-------------|
| `a3bf533` | fix: indentacion lifespan y try igp_tweets - FASE 3 v2.3.0 |
| `8529fba` | feat: Telegram IGP scraper @sismos_peru_igp - FASE 3.1 |
| `4e741aa` | feat: IGP Web scraper oficial - ultimosismo.igp.gob.pe - FASE 3.2 |
| `ef50345` | fix: IGP Web scraper por evento individual RS 2026-XXXX |
| `3f1f91b` | fix: delay 15s en IGP stream para evitar 429 TooManyConnections |

---

## [v2.2.0] — 22/05/2026

### Agregado
- `ANTHROPIC_API_KEY` en variables de entorno Render
- `fetch_news_rss()` ampliado: BBC Mundo + NYT Español + Washington Post → `news_raw` + `news` legacy
- `generate_daily_news_summary()` — llama Claude API, genera resumen ejecutivo sísmico, guarda en `news_summaries`
- Scheduler diario: `generate_daily_news_summary()` a las 06:00 UTC (01:00 Lima)
- `GET /fetch-summary` — dispara manualmente el resumen VIGÍA
- `GET /api/news/raw` — artículos crudos con filtro por source_id
- `GET /api/news/summary/latest` — último resumen para ARIA/VIGÍA
- `/api/dashboard` actualizado: incluye `news_summary` en respuesta
- Prompt VIGÍA estricto: solo sismos/tsunamis/volcanes, máx 250 palabras, JSON estructurado

### Commits
| Commit | Descripción |
|--------|-------------|
| `2db1edd` | FASE 2: Escucha social - news_raw, news_summaries, NYT, WaPo, resumen Claude |
| `93209ba` | Fix: URL RSS NYT corregida |
| `006916d` | VIGÍA: prompt estricto - solo noticias sísmicas |
| `eeeb8e0` | Fix: corregir triple-quote duplicado en prompt VIGÍA |

---

## [v2.1.0] — 19-20/05/2026

### Agregado
- `dhn_classifier.py` — clasificador oficial DHN (INFORMACION / ALERTA / ALARMA / NO_APLICA)
- `build_earthquake_record()` — helper centralizado con clasificación DHN
- `GET /api/earthquakes/local` — sismos locales peruanos
- `GET /api/earthquakes/by-dhn-level/{level}` — filtro por nivel DHN
- Helper `apiFetch()` centralizado con JWT en frontend
- Corrección recursión RLS `cnat_users` con función `get_mi_role()` SECURITY DEFINER

### Commits
| Commit | Descripción |
|--------|-------------|
| múltiples | FASE 1: dhn_classifier, endpoints DHN, cron automático |

---

## Fuentes activas en producción

| Fuente | Estado | Notas |
|--------|--------|-------|
| USGS GeoJSON | ✅ | 315+ sismos/semana |
| PTWC Tsunami | ✅ | Alertas Pacífico |
| NDBC DART Boyas | ⚠️ | 3/7 activas. 4 con 404 |
| IRIS FDSN | ✅ | M4+ últimas 24h |
| Chile Alerta | ❌ | 302 redirect. API muerta |
| Sea Level UNESCO | ✅ | Legacy |
| IOC v2 API | ❌ | 401 Unauthorized. Key expirada |
| BBC Mundo RSS | ✅ | Noticias sísmicas |
| NYT Español RSS | ❌ | 404 permanente |
| Washington Post RSS | ✅ | 0 artículos relevantes actualmente |
| Twitter @Sismos_Peru_IGP | ❌ | 429 TooManyConnections |
| Telegram IGP | ❌ | 401 Unauthorized |
| IGP Web | ⚠️ | Conecta pero SPA React — HTML vacío |

---

## Pendientes prioritarios

1. 🔴 Reemplazar Twitter Stream → Polling `GET /2/tweets/search/recent` (20 min)
2. 🟢 Reuters América Latina RSS (30 min)
3. 🟢 CSN Chile API oficial `api.sismologia.cl` (30 min)
4. 🟢 NOAA Tsunami Watch RSS (30 min)
5. 🟡 IGP Web con Playwright headless (2 horas)
6. 🟡 BMKG Indonesia scraping (1 hora)
7. 🟡 JMA Japón API REST (1 hora)
8. 🟢 Verificar IDs NDBC actualizados (30 min)
9. 🟢 Renovar API key IOC mareógrafos (30 min)
