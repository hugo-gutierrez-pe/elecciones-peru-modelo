"""
Scraper ONPE Segunda Vuelta 2026 — v2 (requests, sin Selenium)
===============================================================
Extrae datos a nivel provincial de la API de ONPE,
extrapola al 100% y proyecta el resultado nacional.

Uso: python scraper_segunda_vuelta.py
"""

import requests
import pandas as pd
import numpy as np
import time
import sys
from datetime import datetime

# ── Config ────────────────────────────────────────────────
BASE_URL = "https://resultadosegundavuelta.onpe.gob.pe"
ID_ELECCION = 10
AMBITO_NACIONAL = 1
AMBITO_EXTERIOR = 2
SLEEP = 0.3  # segundos entre requests para no saturar

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Referer": f"{BASE_URL}/main/resumen",
    "Origin": BASE_URL,
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/148.0.0.0 Safari/537.36",
}

session = requests.Session()
session.headers.update(HEADERS)


def api(path, params=None):
    """GET a /presentacion-backend/{path} y devuelve data."""
    r = session.get(f"{BASE_URL}/presentacion-backend/{path}", params=params)
    r.raise_for_status()
    body = r.json()
    if not body.get("success", False):
        raise RuntimeError(f"API error: {body.get('message', 'unknown')}")
    return body["data"]


def progress(msg):
    print(f"  {msg}", flush=True)


# ═══════════════════════════════════════════════════════════
# 1. DEPARTAMENTOS
# ═══════════════════════════════════════════════════════════
print("\n=== 1. OBTENIENDO DEPARTAMENTOS ===")
t0 = time.time()

deptos_raw = api("ubigeos/departamentos", {
    "idEleccion": ID_ELECCION,
    "idAmbitoGeografico": AMBITO_NACIONAL,
})
departamentos = [(d["ubigeo"], d["nombre"]) for d in deptos_raw]
print(f"  {len(departamentos)} departamentos obtenidos")
time.sleep(SLEEP)

# ═══════════════════════════════════════════════════════════
# 2. PROVINCIAS
# ═══════════════════════════════════════════════════════════
print("\n=== 2. OBTENIENDO PROVINCIAS ===")

provincias = []
for ubigeo_dep, nombre_dep in departamentos:
    try:
        provs_raw = api("ubigeos/provincias", {
            "idEleccion": ID_ELECCION,
            "idAmbitoGeografico": AMBITO_NACIONAL,
            "idUbigeoDepartamento": ubigeo_dep,
        })
        for p in provs_raw:
            provincias.append({
                "departamento": nombre_dep,
                "ubigeo_dep": ubigeo_dep,
                "ubigeo_prov": p["ubigeo"],
                "provincia": p["nombre"],
                "ambito": 1,
            })
        time.sleep(SLEEP)
    except Exception as e:
        print(f"  ERROR provincias {nombre_dep}: {e}")

df_provincias = pd.DataFrame(provincias)
print(f"  {len(df_provincias)} provincias obtenidas")

# ═══════════════════════════════════════════════════════════
# 3. VOTOS POR PROVINCIA
# ═══════════════════════════════════════════════════════════
print("\n=== 3. OBTENIENDO VOTOS POR PROVINCIA ===")

votos = []
n_prov = len(df_provincias)
for i, (_, row) in enumerate(df_provincias.iterrows()):
    try:
        data = api("eleccion-presidencial/participantes-ubicacion-geografica-nombre", {
            "tipoFiltro": "ubigeo_nivel_02",
            "idAmbitoGeografico": AMBITO_NACIONAL,
            "ubigeoNivel1": row["ubigeo_dep"],
            "ubigeoNivel2": row["ubigeo_prov"],
            "idEleccion": ID_ELECCION,
        })
        for fila in data:
            votos.append({
                "departamento": row["departamento"],
                "provincia": row["provincia"],
                "ubigeo_prov": row["ubigeo_prov"],
                "candidato": fila.get("nombreCandidato", ""),
                "partido": fila.get("nombreAgrupacionPolitica", ""),
                "votos": fila.get("totalVotosValidos", 0),
            })
        time.sleep(SLEEP)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n_prov} provincias procesadas", flush=True)
    except Exception as e:
        print(f"  ERROR votos {row['provincia']}: {e}")

df_votos = pd.DataFrame(votos)
print(f"  {len(df_votos)} registros de votos ({n_prov} provincias)")

# ═══════════════════════════════════════════════════════════
# 4. ACTAS POR PROVINCIA
# ═══════════════════════════════════════════════════════════
print("\n=== 4. OBTENIENDO ACTAS POR PROVINCIA ===")

actas = []
for i, (_, row) in enumerate(df_provincias.iterrows()):
    try:
        data = api("resumen-general/totales", {
            "idAmbitoGeografico": AMBITO_NACIONAL,
            "idEleccion": ID_ELECCION,
            "tipoFiltro": "ubigeo_nivel_02",
            "idUbigeoDepartamento": row["ubigeo_dep"],
            "idUbigeoProvincia": row["ubigeo_prov"],
        })
        actas.append({
            "departamento": row["departamento"],
            "provincia": row["provincia"],
            "ubigeo_prov": row["ubigeo_prov"],
            "avance_pct": data["actasContabilizadas"],
        })
        time.sleep(SLEEP)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n_prov} provincias procesadas", flush=True)
    except Exception as e:
        print(f"  ERROR actas {row['provincia']}: {e}")

df_actas = pd.DataFrame(actas)
print(f"  {len(df_actas)} registros de actas")

# ═══════════════════════════════════════════════════════════
# 5. EXTRANJERO
# ═══════════════════════════════════════════════════════════
print("\n=== 5. OBTENIENDO EXTRANJERO ===")

extranjero_regiones = [
    ("910000", "AFRICA"), ("920000", "AMERICA"),
    ("930000", "ASIA"), ("940000", "EUROPA"), ("950000", "OCEANIA"),
]

votos_ext = []
actas_ext = []

for ubigeo, nombre in extranjero_regiones:
    try:
        # votos
        data_v = api("eleccion-presidencial/participantes-ubicacion-geografica-nombre", {
            "tipoFiltro": "ubigeo_nivel_01",
            "idAmbitoGeografico": AMBITO_EXTERIOR,
            "ubigeoNivel1": ubigeo,
            "idEleccion": ID_ELECCION,
        })
        for fila in data_v:
            votos_ext.append({
                "departamento": f"EXTERIOR_{nombre}",
                "provincia": f"EXTERIOR_{nombre}",
                "ubigeo_prov": ubigeo,
                "candidato": fila.get("nombreCandidato", ""),
                "partido": fila.get("nombreAgrupacionPolitica", ""),
                "votos": fila.get("totalVotosValidos", 0),
            })

        # actas
        data_a = api("resumen-general/totales", {
            "idAmbitoGeografico": AMBITO_EXTERIOR,
            "idEleccion": ID_ELECCION,
            "tipoFiltro": "ubigeo_nivel_01",
            "idUbigeoDepartamento": ubigeo,
        })
        actas_ext.append({
            "departamento": f"EXTERIOR_{nombre}",
            "provincia": f"EXTERIOR_{nombre}",
            "ubigeo_prov": ubigeo,
            "avance_pct": data_a["actasContabilizadas"],
        })
        time.sleep(SLEEP)
    except Exception as e:
        print(f"  ERROR exterior {nombre}: {e}")

if votos_ext:
    df_ext = pd.DataFrame(votos_ext).merge(
        pd.DataFrame(actas_ext), on=["departamento", "provincia", "ubigeo_prov"]
    )
    print(f"  Extranjero: {len(df_ext)} registros, {len(actas_ext)} regiones")
else:
    df_ext = pd.DataFrame()
    print("  Extranjero: sin datos aún")

# ═══════════════════════════════════════════════════════════
# 6. MERGE Y EXTRAPOLACIÓN
# ═══════════════════════════════════════════════════════════
print("\n=== 6. MERGE Y EXTRAPOLACIÓN ===")

df_final = df_votos.merge(df_actas, on=["departamento", "provincia", "ubigeo_prov"])

if not df_ext.empty:
    df_total = pd.concat([df_final, df_ext], ignore_index=True)
else:
    df_total = df_final.copy()

# Limpiar avance_pct (evitar división por cero)
df_total["avance_pct"] = df_total["avance_pct"].replace(0, np.nan)

# Extrapolación
df_total["votos_estimados"] = df_total["votos"] / (df_total["avance_pct"] / 100)
df_total["votos_restantes"] = df_total["votos_estimados"] - df_total["votos"]

# Métricas regionales
totales_region = df_total.groupby("departamento")["votos_estimados"].sum().reset_index()
totales_region.columns = ["departamento", "total_region"]

df_total = df_total.merge(totales_region, on="departamento")
df_total["pct_region_actual"] = df_total["votos"] / df_total["total_region"] * 100
df_total["pct_region_proyectado"] = df_total["votos_estimados"] / df_total["total_region"] * 100

# ═══════════════════════════════════════════════════════════
# 7. RESUMEN NACIONAL
# ═══════════════════════════════════════════════════════════
print("\n=== 7. PROYECCIÓN NACIONAL ===")

# Filtrar solo candidatos (excluir votos nulos y blancos si se desea)
candidatos_validos = df_total[
    ~df_total["candidato"].isin(["", "VOTOS NULOS", "VOTOS EN BLANCO"])
]

# O también mostrar todo incluyendo nulos/blancos
df_nacional = df_total.groupby(["candidato", "partido"], as_index=False).agg(
    votos_actuales=("votos", "sum"),
    votos_estimados=("votos_estimados", "sum"),
    votos_restantes=("votos_restantes", "sum"),
)
df_nacional = df_nacional.sort_values("votos_estimados", ascending=False).reset_index(drop=True)

total_votos_est = df_nacional["votos_estimados"].sum()
df_nacional["pct_proyectado"] = df_nacional["votos_estimados"] / total_votos_est * 100

# ═══════════════════════════════════════════════════════════
# 8. GUARDAR CSVs
# ═══════════════════════════════════════════════════════════
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

out = {
    "provincias.csv": df_provincias,
    "votos_provincial.csv": df_votos,
    "actas_provincial.csv": df_actas,
    "resultado_provincial_completo.csv": df_total,
    "proyeccion_nacional.csv": df_nacional,
}

for fname, dframe in out.items():
    path = f"v2/output_{timestamp}_{fname}"
    dframe.to_csv(path, index=False)
    print(f"  Guardado: {path}")

# ═══════════════════════════════════════════════════════════
# 9. RESULTADO
# ═══════════════════════════════════════════════════════════
t_total = time.time() - t0
avance_medio = df_total["avance_pct"].mean()

print(f"\n{'═' * 60}")
print(f"  SEGUNDA VUELTA 2026 — PROYECCIÓN AL 100%")
print(f"  Avance promedio provincial: {avance_medio:.1f}%")
print(f"  Tiempo total: {t_total:.0f}s")
print(f"{'═' * 60}")

print(f"\n{'Candidato':<40} {'Partido':<25} {'Votos actuales':>14} {'Proyectado':>14} {'%':>7}")
print("-" * 105)
for _, row in df_nacional.iterrows():
    cand = row["candidato"] if row["candidato"] else row["partido"]
    print(
        f"{cand:<40} {row['partido']:<25} "
        f"{row['votos_actuales']:>14,.0f} "
        f"{row['votos_estimados']:>14,.0f} "
        f"{row['pct_proyectado']:>6.2f}%"
    )

print("-" * 105)
print(f"{'TOTAL':<40} {'':<25} {df_nacional['votos_actuales'].sum():>14,.0f} {df_nacional['votos_estimados'].sum():>14,.0f} {'100.00%':>7}")

# Ganador proyectado
top = df_nacional[df_nacional["candidato"] != ""].iloc[0]
second = df_nacional[df_nacional["candidato"] != ""].iloc[1]
diff = top["votos_estimados"] - second["votos_estimados"]

print(f"\n{'═' * 60}")
print(f"  🏆 GANADOR PROYECTADO: {top['candidato']}")
print(f"     {top['partido']}")
print(f"     {top['votos_estimados']:,.0f} votos estimados ({top['pct_proyectado']:.2f}%)")
print(f"     Ventaja sobre {second['candidato']}: {diff:,.0f} votos")
print(f"{'═' * 60}")

sys.exit(0)
