"""
CNAT - Tests del clasificador DHN
=================================
Tests funcionales para validar la implementacion de la matriz oficial DHN.
Incluye casos historicos reales como el sismo de Ica del 19/05/2026.

Ejecucion:
    python test_dhn_classifier.py

Autor:    Julio Marchena - MICROHELP
Fecha:    19 de mayo de 2026
"""

import sys
from dhn_classifier import (
    classify_dhn,
    is_local_peru,
    DHN_INFORMACION,
    DHN_ALERTA,
    DHN_ALARMA,
    DHN_NO_APLICA,
)

# Contadores globales
passed = 0
failed = 0


def assert_eq(actual, expected, label):
    """Helper para imprimir resultados de forma legible."""
    global passed, failed
    if actual == expected:
        print(f"  [OK] {label}: {actual}")
        passed += 1
    else:
        print(f"  [FAIL] {label}: esperado={expected}, obtenido={actual}")
        failed += 1


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


# ============================================================
# SECCION 1: Pruebas de deteccion local vs regional
# ============================================================
section("1. DETECCION LOCAL vs REGIONAL")

print("\nCaso 1.1: Sismo de Ica (Pampa de Tate) - debe ser LOCAL")
assert_eq(is_local_peru(-14.0, -75.7), True, "Ica (-14.0, -75.7)")

print("\nCaso 1.2: Lima - debe ser LOCAL")
assert_eq(is_local_peru(-12.05, -77.04), True, "Lima (-12.05, -77.04)")

print("\nCaso 1.3: Frontera norte (Tumbes) - debe ser LOCAL")
assert_eq(is_local_peru(-3.5, -80.0), True, "Tumbes (-3.5, -80.0)")

print("\nCaso 1.4: Frontera sur (Tacna) - debe ser LOCAL")
assert_eq(is_local_peru(-18.0, -70.5), True, "Tacna (-18.0, -70.5)")

print("\nCaso 1.5: Santiago de Chile - debe ser REGIONAL")
assert_eq(is_local_peru(-33.4, -70.6), False, "Santiago (-33.4, -70.6)")

print("\nCaso 1.6: Tohoku Japon - debe ser REGIONAL")
assert_eq(is_local_peru(38.3, 142.4), False, "Tohoku (38.3, 142.4)")

print("\nCaso 1.7: Coordenadas nulas - debe ser False")
assert_eq(is_local_peru(None, None), False, "None, None")


# ============================================================
# SECCION 2: SISMO DE ICA 19/05/2026 (caso real)
# ============================================================
section("2. SISMO DE ICA - 19/05/2026 (CASO REAL)")

print("\nDatos del evento real:")
print("  Magnitud (USGS): 5.8")
print("  Profundidad:     56.5 km")
print("  Lat / Lon:       -14.0, -75.7")
print("  DHN emitio:      Boletin N 14-2026-1 (NO genera tsunami)")
print("  Resultado esperado: INFORMACION + es local")

result = classify_dhn(5.8, 56.5, -14.0, -75.7)
print(f"\nResultado del clasificador:")
print(f"  Nivel:    {result['dhn_level']}")
print(f"  Es local: {result['is_local']}")
print(f"  Razon:    {result['dhn_reason']}")

assert_eq(result["dhn_level"], DHN_INFORMACION, "Nivel DHN del sismo Ica")
assert_eq(result["is_local"], True, "Es local")


# ============================================================
# SECCION 3: MATRIZ LOCAL - Casos sinteticos
# ============================================================
section("3. MATRIZ LOCAL - Casos sinteticos")

# Coordenadas validas de Peru para los casos sinteticos
PERU_LAT = -12.0
PERU_LON = -77.0

print("\nCaso 3.1: Local M6.5 prof 30km -> INFORMACION")
r = classify_dhn(6.5, 30, PERU_LAT, PERU_LON)
assert_eq(r["dhn_level"], DHN_INFORMACION, "M6.5/30km local")

print("\nCaso 3.2: Local M7.0 prof 30km -> ALERTA")
r = classify_dhn(7.0, 30, PERU_LAT, PERU_LON)
assert_eq(r["dhn_level"], DHN_ALERTA, "M7.0/30km local")

print("\nCaso 3.3: Local M7.0 prof 80km -> INFORMACION (vigilancia)")
r = classify_dhn(7.0, 80, PERU_LAT, PERU_LON)
assert_eq(r["dhn_level"], DHN_INFORMACION, "M7.0/80km local")

print("\nCaso 3.4: Local M7.5 prof 30km -> ALARMA")
r = classify_dhn(7.5, 30, PERU_LAT, PERU_LON)
assert_eq(r["dhn_level"], DHN_ALARMA, "M7.5/30km local")

print("\nCaso 3.5: Local M8.0 prof 30km -> ALARMA")
r = classify_dhn(8.0, 30, PERU_LAT, PERU_LON)
assert_eq(r["dhn_level"], DHN_ALARMA, "M8.0/30km local")

print("\nCaso 3.6: Local M7.8 prof 80km -> ALERTA (>=7.5, prof>60)")
r = classify_dhn(7.8, 80, PERU_LAT, PERU_LON)
assert_eq(r["dhn_level"], DHN_ALERTA, "M7.8/80km local")


# ============================================================
# SECCION 4: MATRIZ REGIONAL - Casos sinteticos
# ============================================================
section("4. MATRIZ REGIONAL/LEJANA - Casos sinteticos")

# Coordenadas de Chile para sismos regionales
CHILE_LAT = -33.4
CHILE_LON = -70.6

# Coordenadas de Japon para sismos lejanos
JAPAN_LAT = 38.3
JAPAN_LON = 142.4

print("\nCaso 4.1: Chile M7.5 prof 30km -> INFORMACION (vigilancia)")
r = classify_dhn(7.5, 30, CHILE_LAT, CHILE_LON)
assert_eq(r["dhn_level"], DHN_INFORMACION, "M7.5/30km Chile")

print("\nCaso 4.2: Chile M8.0 prof 30km -> ALERTA")
r = classify_dhn(8.0, 30, CHILE_LAT, CHILE_LON)
assert_eq(r["dhn_level"], DHN_ALERTA, "M8.0/30km Chile")

print("\nCaso 4.3: Japon M8.5 prof 30km -> ALARMA")
r = classify_dhn(8.5, 30, JAPAN_LAT, JAPAN_LON)
assert_eq(r["dhn_level"], DHN_ALARMA, "M8.5/30km Japon")

print("\nCaso 4.4: Japon Tohoku M9.0 prof 30km -> ALARMA (caso historico 2011)")
r = classify_dhn(9.0, 30, JAPAN_LAT, JAPAN_LON)
assert_eq(r["dhn_level"], DHN_ALARMA, "M9.0/30km Japon (Tohoku)")

print("\nCaso 4.5: Japon M6.0 prof 30km -> NO_APLICA (magnitud insuficiente)")
r = classify_dhn(6.0, 30, JAPAN_LAT, JAPAN_LON)
assert_eq(r["dhn_level"], DHN_NO_APLICA, "M6.0/30km Japon")

print("\nCaso 4.6: Chile M8.2 prof 90km -> INFORMACION (vigilancia)")
r = classify_dhn(8.2, 90, CHILE_LAT, CHILE_LON)
assert_eq(r["dhn_level"], DHN_INFORMACION, "M8.2/90km Chile")


# ============================================================
# SECCION 5: CASOS LIMITE Y ROBUSTEZ
# ============================================================
section("5. CASOS LIMITE Y ROBUSTEZ")

print("\nCaso 5.1: Magnitud None -> debe manejar sin error")
r = classify_dhn(None, 30, PERU_LAT, PERU_LON)
assert_eq(r["dhn_level"], DHN_INFORMACION, "Magnitud None")

print("\nCaso 5.2: Profundidad None -> debe manejar sin error")
r = classify_dhn(6.0, None, PERU_LAT, PERU_LON)
assert_eq(r["dhn_level"], DHN_INFORMACION, "Profundidad None")

print("\nCaso 5.3: Sin coordenadas -> se trata como regional")
r = classify_dhn(7.5, 30, None, None)
assert_eq(r["is_local"], False, "Sin coordenadas")


# ============================================================
# RESUMEN FINAL
# ============================================================
print(f"\n{'='*60}")
print(f"  RESUMEN: {passed} pasaron, {failed} fallaron")
print('='*60)

if failed == 0:
    print("\nTODOS LOS TESTS PASARON - El clasificador esta listo para integrar.\n")
    sys.exit(0)
else:
    print(f"\nHAY {failed} TEST(S) FALLIDOS - Revisar el codigo.\n")
    sys.exit(1)
