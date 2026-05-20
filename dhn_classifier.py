"""
CNAT - Clasificador oficial DHN
================================
Implementa la matriz oficial de boletines de la Dirección de Hidrografía
y Navegación (DHN) de la Marina de Guerra del Perú para la clasificación
de sismos según su potencial tsunamigénico.

Autor:    Julio Marchena - MICROHELP
Fecha:    19 de mayo de 2026
Versión:  1.0 (FASE 1 del plan de implementación CNAT)
Cliente:  DHN - Marina de Guerra del Perú

NIVELES OFICIALES DHN
---------------------
- INFORMACION  → Evento registrado, no genera tsunami
- ALERTA       → Probabilidad de generación de tsunami
- ALARMA       → Generación confirmada de tsunami

MATRIZ DE CLASIFICACIÓN (matriz oficial DHN)
--------------------------------------------

SISMO LOCAL (en el mar o hasta 60 km tierra adentro del territorio peruano)
+-----------+---------------------+----------------------+
| Magnitud  | Profundidad <= 60km | Profundidad > 60km   |
+-----------+---------------------+----------------------+
| 5.0 - 6.9 | INFORMACION         | INFORMACION          |
| 7.0 - 7.4 | ALERTA              | INFORMACION (vigil.) |
| >= 7.5    | ALARMA              | ALERTA               |
+-----------+---------------------+----------------------+

SISMO REGIONAL / LEJANO (fuera del territorio peruano, en cuenca Pacifico)
+-----------+---------------------+----------------------+
| Magnitud  | Profundidad <= 60km | Profundidad 60-100km |
+-----------+---------------------+----------------------+
| 7.0 - 7.9 | INFORMACION (vigil.)| (no aplica)          |
| 8.0 - 8.4 | ALERTA              | INFORMACION (vigil.) |
| >= 8.5    | ALARMA              | ALERTA               |
+-----------+---------------------+----------------------+
"""

# ============================================================
# CONSTANTES DE NIVELES OFICIALES DHN
# ============================================================
DHN_INFORMACION = "INFORMACION"
DHN_ALERTA = "ALERTA"
DHN_ALARMA = "ALARMA"
DHN_NO_APLICA = "NO_APLICA"  # Para sismos fuera del Pacifico o de baja magnitud

# ============================================================
# CONSTANTES GEOGRAFICAS - TERRITORIO PERUANO
# ============================================================
# Aproximacion rectangular del area de influencia peruana segun
# la definicion DHN: "en el mar o cerca de la costa, entre la fosa
# y 60 km tierra adentro".
#
# Rango latitudinal: desde frontera norte con Ecuador hasta frontera
# sur con Chile, incluyendo margen de seguridad.
PERU_LAT_NORTE = -3.0   # Frontera norte con Ecuador
PERU_LAT_SUR = -19.0    # Frontera sur con Chile

# Rango longitudinal: desde la fosa peruano-chilena (mar adentro)
# hasta 60 km tierra adentro desde la costa.
PERU_LON_OESTE = -85.0  # Aproximadamente a la altura de la fosa
PERU_LON_ESTE = -68.0   # Aproximadamente 60 km tierra adentro

# ============================================================
# FUNCION 1: DETECCION DE SISMO LOCAL
# ============================================================
def is_local_peru(latitude, longitude):
    """
    Determina si un sismo ocurrio en el area de influencia local del Peru.

    Segun la matriz DHN, se considera LOCAL cuando el epicentro esta
    en el mar o cerca de la costa, entre la fosa peruano-chilena y
    60 km tierra adentro.

    Args:
        latitude  (float): Latitud del epicentro (grados decimales).
        longitude (float): Longitud del epicentro (grados decimales).

    Returns:
        bool: True si el sismo es LOCAL para Peru, False en caso contrario.

    Ejemplos:
        >>> is_local_peru(-14.0, -75.7)  # Pampa de Tate (Ica)
        True
        >>> is_local_peru(38.3, 142.4)   # Tohoku, Japon
        False
        >>> is_local_peru(-33.4, -70.6)  # Santiago, Chile
        False
    """
    if latitude is None or longitude is None:
        return False

    try:
        lat = float(latitude)
        lon = float(longitude)
    except (TypeError, ValueError):
        return False

    return (
        PERU_LAT_SUR <= lat <= PERU_LAT_NORTE
        and PERU_LON_OESTE <= lon <= PERU_LON_ESTE
    )


# ============================================================
# FUNCION 2: CLASIFICACION DHN PARA SISMO LOCAL
# ============================================================
def _classify_local(magnitude, depth_km):
    """
    Aplica la matriz DHN para sismos locales (territorio peruano).

    Returns:
        tuple: (nivel_dhn, razon_legible)
    """
    # Magnitud minima para considerar relevante: 5.0
    if magnitude < 5.0:
        return (
            DHN_INFORMACION,
            f"Sismo local de magnitud baja (M{magnitude:.1f}). Sin potencial tsunamigénico."
        )

    # Rango 5.0 - 6.9: siempre boletin de INFORMACION
    if magnitude < 7.0:
        return (
            DHN_INFORMACION,
            f"Sismo local M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
            f"No genera tsunami (matriz DHN: rango 5.0-6.9)."
        )

    # Rango 7.0 - 7.4
    if magnitude < 7.5:
        if depth_km <= 60:
            return (
                DHN_ALERTA,
                f"Sismo local M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
                f"Probabilidad de generación de tsunami (matriz DHN: rango 7.0-7.4, prof. <=60km)."
            )
        else:
            return (
                DHN_INFORMACION,
                f"Sismo local M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
                f"Se intensifica vigilancia (matriz DHN: rango 7.0-7.4, prof. >60km)."
            )

    # Rango >= 7.5
    if depth_km <= 60:
        return (
            DHN_ALARMA,
            f"Sismo local M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
            f"Generación de tsunami confirmada (matriz DHN: rango >=7.5, prof. <=60km)."
        )
    else:
        return (
            DHN_ALERTA,
            f"Sismo local M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
            f"Probabilidad de generación de tsunami (matriz DHN: rango >=7.5, prof. >60km)."
        )


# ============================================================
# FUNCION 3: CLASIFICACION DHN PARA SISMO REGIONAL/LEJANO
# ============================================================
def _classify_regional(magnitude, depth_km):
    """
    Aplica la matriz DHN para sismos regionales o lejanos
    (fuera del territorio peruano).

    Returns:
        tuple: (nivel_dhn, razon_legible)
    """
    # Magnitud minima para considerar tsunamigenico desde lejos: 7.0
    if magnitude < 7.0:
        return (
            DHN_NO_APLICA,
            f"Sismo lejano M{magnitude:.1f}. Sin potencial tsunamigénico transoceánico."
        )

    # Rango 7.0 - 7.9
    if magnitude < 8.0:
        if depth_km <= 60:
            return (
                DHN_INFORMACION,
                f"Sismo lejano M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
                f"Se intensifica vigilancia (matriz DHN: rango 7.0-7.9, prof. <=60km)."
            )
        else:
            return (
                DHN_NO_APLICA,
                f"Sismo lejano M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
                f"Profundidad excesiva para tsunami transoceánico."
            )

    # Rango 8.0 - 8.4
    if magnitude < 8.5:
        if depth_km <= 60:
            return (
                DHN_ALERTA,
                f"Sismo lejano M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
                f"Probabilidad de generación de tsunami (matriz DHN: rango 8.0-8.4, prof. <=60km)."
            )
        elif depth_km <= 100:
            return (
                DHN_INFORMACION,
                f"Sismo lejano M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
                f"Se intensifica vigilancia (matriz DHN: rango 8.0-8.4, prof. 60-100km)."
            )
        else:
            return (
                DHN_NO_APLICA,
                f"Sismo lejano M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
                f"Profundidad excesiva para tsunami transoceánico."
            )

    # Rango >= 8.5
    if depth_km <= 60:
        return (
            DHN_ALARMA,
            f"Sismo lejano M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
            f"Generación de tsunami confirmada (matriz DHN: rango >=8.5, prof. <=60km)."
        )
    elif depth_km <= 100:
        return (
            DHN_ALERTA,
            f"Sismo lejano M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
            f"Probabilidad de generación de tsunami (matriz DHN: rango >=8.5, prof. 60-100km)."
        )
    else:
        return (
            DHN_INFORMACION,
            f"Sismo lejano M{magnitude:.1f}, profundidad {depth_km:.1f}km. "
            f"Se intensifica vigilancia (profundidad extrema)."
        )


# ============================================================
# FUNCION 4: CLASIFICACION PRINCIPAL (PUNTO DE ENTRADA)
# ============================================================
def classify_dhn(magnitude, depth_km, latitude=None, longitude=None):
    """
    Funcion principal de clasificacion segun la matriz oficial DHN.

    Esta funcion es el unico punto de entrada que se debe llamar
    desde el resto del codigo. Internamente decide si aplicar la
    matriz local o regional segun la ubicacion del epicentro.

    Args:
        magnitude (float): Magnitud del sismo (escala Richter o Mw).
        depth_km  (float): Profundidad del epicentro en kilometros.
        latitude  (float): Latitud del epicentro (opcional).
        longitude (float): Longitud del epicentro (opcional).

    Returns:
        dict: Diccionario con los siguientes campos:
            - dhn_level (str):  Nivel oficial DHN ("INFORMACION" | "ALERTA" | "ALARMA" | "NO_APLICA")
            - dhn_reason (str): Razon legible de la clasificacion
            - is_local (bool):  True si el sismo es LOCAL para Peru

    Ejemplo (sismo de Ica del 19/05/2026):
        >>> result = classify_dhn(5.8, 56.5, -14.0, -75.7)
        >>> result["dhn_level"]
        'INFORMACION'
        >>> result["is_local"]
        True
    """
    # Sanitizar entradas
    try:
        mag = float(magnitude) if magnitude is not None else 0.0
    except (TypeError, ValueError):
        mag = 0.0

    try:
        depth = float(depth_km) if depth_km is not None else 0.0
    except (TypeError, ValueError):
        depth = 0.0

    # Determinar si es LOCAL o REGIONAL
    is_local = is_local_peru(latitude, longitude)

    # Aplicar matriz correspondiente
    if is_local:
        dhn_level, dhn_reason = _classify_local(mag, depth)
    else:
        dhn_level, dhn_reason = _classify_regional(mag, depth)

    return {
        "dhn_level": dhn_level,
        "dhn_reason": dhn_reason,
        "is_local": is_local,
    }


# ============================================================
# FUNCION 5: HELPER PARA MAPEAR A 'severity' (RETROCOMPATIBILIDAD)
# ============================================================
def dhn_to_severity(dhn_level):
    """
    Convierte un nivel DHN al formato 'severity' usado historicamente
    por el frontend para no romper la compatibilidad.

    Mapeo:
        ALARMA       -> critical
        ALERTA       -> warning
        INFORMACION  -> moderate (si magnitud relevante) / normal
        NO_APLICA    -> normal
    """
    mapping = {
        DHN_ALARMA: "critical",
        DHN_ALERTA: "warning",
        DHN_INFORMACION: "moderate",
        DHN_NO_APLICA: "normal",
    }
    return mapping.get(dhn_level, "normal")
