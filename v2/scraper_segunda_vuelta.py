"""
Scraper ONPE Segunda Vuelta 2026 — v2 distrital + paralelo + cache incremental
=============================================================================
Extrae datos a nivel distrital. Si un distrito falla, usa datos de la corrida anterior.
Cada ejecucion actualiza lo que puede y conserva el resto del cache.

Uso: python scraper_segunda_vuelta.py
"""

import io
import os
import requests
import pandas as pd
import numpy as np
import time
import sys
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ── Config ────────────────────────────────────────────────
BASE_URL = "https://resultadosegundavuelta.onpe.gob.pe"
ID_ELECCION = 10
AMBITO_NACIONAL = 1
AMBITO_EXTERIOR = 2
SLEEP = 0.3
MAX_WORKERS = 15

CACHE_DIR = "v2/cache"
CACHE_VOTOS = f"{CACHE_DIR}/cache_votos_distrital.csv"
CACHE_ACTAS = f"{CACHE_DIR}/cache_actas_distrital.csv"
CACHE_EXTERIOR = f"{CACHE_DIR}/cache_extranjero.csv"
CACHE_PROVINCIAS = f"{CACHE_DIR}/cache_provincias.csv"
CACHE_DISTRITOS = f"{CACHE_DIR}/cache_distritos.csv"

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

os.makedirs(CACHE_DIR, exist_ok=True)

_tlocal = threading.local()

def _session():
    if not hasattr(_tlocal, "session"):
        _tlocal.session = requests.Session()
        _tlocal.session.headers.update(HEADERS)
    return _tlocal.session

def api(path, params=None, retries=5):
    last_err = None
    for attempt in range(retries):
        try:
            r = _session().get(f"{BASE_URL}/presentacion-backend/{path}", params=params)
            r.raise_for_status()
            body = r.json()
            if body.get("success", False):
                return body["data"]
            raise RuntimeError(f"API error: {body.get('message', 'unknown')}")
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                wait = 2 * (attempt + 1)
                print(f"  Reintento {attempt + 2}/{retries} en {wait}s...", flush=True)
                time.sleep(wait)
    raise last_err

def load_cache(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    return None

def merge_cache(df_new, df_cache, key_col="ubigeo_dist"):
    """Combina datos nuevos con cache: nuevos toman precedencia, el resto del cache."""
    if df_cache is None or df_cache.empty:
        return df_new, 0
    if df_new is None or df_new.empty:
        return df_cache, len(df_cache)
    # Normalizar: zero-pad a 6 digitos para comparacion correcta
    for d in [df_new, df_cache]:
        d[key_col] = d[key_col].astype(str).str.zfill(6)
    fetched = set(df_new[key_col].unique())
    cache_remain = df_cache[~df_cache[key_col].isin(fetched)]
    n_cache = len(cache_remain)
    result = pd.concat([df_new, cache_remain], ignore_index=True)
    return result, n_cache

# ═══════════════════════════════════════════════════════════
# 0. CARGAR CACHE
# ═══════════════════════════════════════════════════════════
cache_votos = load_cache(CACHE_VOTOS)
cache_actas = load_cache(CACHE_ACTAS)
cache_ext = load_cache(CACHE_EXTERIOR)
cache_provincias = load_cache(CACHE_PROVINCIAS)
cache_distritos = load_cache(CACHE_DISTRITOS)

t0 = time.time()
n_cache_votos_used = 0
n_cache_actas_used = 0
api_ok = True

# ═══════════════════════════════════════════════════════════
# 1. DEPARTAMENTOS
# ═══════════════════════════════════════════════════════════
print("\n=== 1. OBTENIENDO DEPARTAMENTOS ===")
try:
    deptos_raw = api("ubigeos/departamentos", {
        "idEleccion": ID_ELECCION, "idAmbitoGeografico": AMBITO_NACIONAL,
    })
    departamentos = [(d["ubigeo"], d["nombre"]) for d in deptos_raw]
    print(f"  {len(departamentos)} departamentos obtenidos")
    time.sleep(SLEEP)
except Exception as e:
    print(f"  API CAIDA: {e}")
    api_ok = False

# ═══════════════════════════════════════════════════════════
# 2. PROVINCIAS
# ═══════════════════════════════════════════════════════════
if api_ok:
    print("\n=== 2. OBTENIENDO PROVINCIAS ===")
    provincias = []
    for ubigeo_dep, nombre_dep in departamentos:
        try:
            provs_raw = api("ubigeos/provincias", {
                "idEleccion": ID_ELECCION, "idAmbitoGeografico": AMBITO_NACIONAL,
                "idUbigeoDepartamento": ubigeo_dep,
            })
            for p in provs_raw:
                provincias.append({
                    "departamento": nombre_dep, "ubigeo_dep": ubigeo_dep,
                    "ubigeo_prov": p["ubigeo"], "provincia": p["nombre"],
                })
            time.sleep(SLEEP)
        except Exception as e:
            print(f"  ERROR provincias {nombre_dep}: {e}")
    df_provincias = pd.DataFrame(provincias)
    if not df_provincias.empty:
        df_provincias.to_csv(CACHE_PROVINCIAS, index=False)
else:
    df_provincias = cache_provincias

n_prov = len(df_provincias) if df_provincias is not None else 0
print(f"  {n_prov} provincias {'(cache)' if not api_ok else 'obtenidas'}")

if df_provincias is None or df_provincias.empty:
    print("  ERROR: sin provincias. Ejecuta cuando la API este disponible.")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════
# 3. DISTRITOS
# ═══════════════════════════════════════════════════════════
if api_ok:
    print("\n=== 3. OBTENIENDO DISTRITOS ===")
    distritos = []
    for i, (_, row) in enumerate(df_provincias.iterrows()):
        try:
            dists_raw = api("ubigeos/distritos", {
                "idEleccion": ID_ELECCION, "idAmbitoGeografico": AMBITO_NACIONAL,
                "idUbigeoDepartamento": row["ubigeo_dep"],
                "idUbigeoProvincia": row["ubigeo_prov"],
            })
            for d in dists_raw:
                distritos.append({
                    "departamento": row["departamento"], "ubigeo_dep": row["ubigeo_dep"],
                    "provincia": row["provincia"], "ubigeo_prov": row["ubigeo_prov"],
                    "distrito": d["nombre"], "ubigeo_dist": d["ubigeo"],
                })
            time.sleep(SLEEP)
            if (i + 1) % 20 == 0:
                print(f"  {i+1}/{n_prov} provincias procesadas", flush=True)
        except Exception as e:
            print(f"  ERROR distritos {row['provincia']}: {e}")
    df_distritos = pd.DataFrame(distritos)
    if not df_distritos.empty:
        df_distritos.to_csv(CACHE_DISTRITOS, index=False)
else:
    df_distritos = cache_distritos

n_dist = len(df_distritos) if df_distritos is not None else 0
print(f"  {n_dist} distritos {'(cache)' if not api_ok else 'obtenidos'}")

if df_distritos is None or df_distritos.empty:
    print("  ERROR: sin distritos. Ejecuta cuando la API este disponible.")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════
# 3b. ESCANEO JERARQUICO: identificar distritos con avance < 100%
# ═══════════════════════════════════════════════════════════
if api_ok:
    print("\n=== 3b. ESCANEO JERARQUICO (departamento → provincia) ===")

    deptos_pendientes = []
    for ubigeo_dep, nombre_dep in departamentos:
        try:
            data = api("resumen-general/totales", {
                "idAmbitoGeografico": AMBITO_NACIONAL, "idEleccion": ID_ELECCION,
                "tipoFiltro": "ubigeo_nivel_01",
                "idUbigeoDepartamento": ubigeo_dep,
            })
            avance = data["actasContabilizadas"]
            if avance < 100:
                deptos_pendientes.append((ubigeo_dep, nombre_dep))
            time.sleep(SLEEP / 2)
        except Exception as e:
            print(f"  ERROR escaneo depto {nombre_dep}: {e}")
            deptos_pendientes.append((ubigeo_dep, nombre_dep))

    print(f"  {len(deptos_pendientes)}/{len(departamentos)} departamentos pendientes")

    provincias_pendientes = set()
    for ubigeo_dep, nombre_dep in deptos_pendientes:
        provs = df_provincias[df_provincias["ubigeo_dep"] == ubigeo_dep]
        for _, prow in provs.iterrows():
            try:
                data = api("resumen-general/totales", {
                    "idAmbitoGeografico": AMBITO_NACIONAL, "idEleccion": ID_ELECCION,
                    "tipoFiltro": "ubigeo_nivel_02",
                    "idUbigeoDepartamento": ubigeo_dep,
                    "idUbigeoProvincia": prow["ubigeo_prov"],
                })
                avance = data["actasContabilizadas"]
                if avance < 100:
                    provincias_pendientes.add(prow["ubigeo_prov"])
                time.sleep(SLEEP / 2)
            except Exception as e:
                print(f"  ERROR escaneo prov {prow['provincia']}: {e}")
                provincias_pendientes.add(prow["ubigeo_prov"])

    if provincias_pendientes:
        df_distritos_pendientes = df_distritos[df_distritos["ubigeo_prov"].isin(provincias_pendientes)]
    else:
        df_distritos_pendientes = df_distritos.iloc[:0]  # empty
    n_dist_pendientes = len(df_distritos_pendientes)
    n_dist_completos = n_dist - n_dist_pendientes
    print(f"  {n_dist_pendientes} distritos pendientes, {n_dist_completos} al 100% (usaran cache)")
else:
    df_distritos_pendientes = df_distritos
    n_dist_pendientes = n_dist

# ═══════════════════════════════════════════════════════════
# 4. VOTOS POR DISTRITO (solo pendientes + cache)
# ═══════════════════════════════════════════════════════════
if api_ok:
    if cache_votos is not None:
        print(f"  Cache cargado: {len(cache_votos)} registros previos de votos")
    print(f"\n=== 4. VOTOS POR DISTRITO ({n_dist_pendientes} pendientes, {MAX_WORKERS} workers) ===")

    rows = [row for _, row in df_distritos_pendientes.iterrows()]

    def _fetch_votos(row):
        try:
            data = api("eleccion-presidencial/participantes-ubicacion-geografica-nombre", {
                "tipoFiltro": "ubigeo_nivel_03", "idAmbitoGeografico": AMBITO_NACIONAL,
                "ubigeoNivel1": row["ubigeo_dep"], "ubigeoNivel2": row["ubigeo_prov"],
                "ubigeoNivel3": row["ubigeo_dist"], "idEleccion": ID_ELECCION,
            })
            return [
                {
                    "departamento": row["departamento"], "provincia": row["provincia"],
                    "ubigeo_prov": row["ubigeo_prov"], "distrito": row["distrito"],
                    "ubigeo_dist": row["ubigeo_dist"],
                    "candidato": fila.get("nombreCandidato", ""),
                    "partido": fila.get("nombreAgrupacionPolitica", ""),
                    "votos": fila.get("totalVotosValidos", 0),
                }
                for fila in data
            ]
        except Exception:
            return None

    votos_new = []
    errores_votos = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_votos, row): row for row in rows}
        done = 0
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                errores_votos += 1
            else:
                votos_new.extend(result)
            done += 1
            if done % 100 == 0:
                elapsed = time.time() - t0
                print(f"  votos: {done}/{n_dist_pendientes} pendientes ({elapsed:.0f}s)", flush=True)

    df_votos_new = pd.DataFrame(votos_new)
    print(f"  Nuevos: {len(df_votos_new)} registros  |  Errores: {errores_votos} distritos")

    df_votos, n_cache_votos_used = merge_cache(df_votos_new, cache_votos)
    df_votos.to_csv(CACHE_VOTOS, index=False)
else:
    df_votos = cache_votos
    n_cache_votos_used = len(cache_votos) if cache_votos is not None else 0
    print(f"\n=== 4. VOTOS: usando cache ({n_cache_votos_used} registros) ===")

if n_cache_votos_used > 0 and api_ok:
    print(f"  Cache usado: {n_cache_votos_used} registros del cache anterior")
print(f"  Total final: {len(df_votos) if df_votos is not None else 0} registros de votos")

# ═══════════════════════════════════════════════════════════
# 5. ACTAS POR DISTRITO (solo pendientes + cache)
# ═══════════════════════════════════════════════════════════
if api_ok:
    if cache_actas is not None:
        print(f"  Cache cargado: {len(cache_actas)} registros previos de actas")
    print(f"\n=== 5. ACTAS POR DISTRITO ({n_dist_pendientes} pendientes, {MAX_WORKERS} workers) ===")

    def _fetch_actas(row):
        try:
            data = api("resumen-general/totales", {
                "idAmbitoGeografico": AMBITO_NACIONAL, "idEleccion": ID_ELECCION,
                "tipoFiltro": "ubigeo_nivel_03",
                "idUbigeoDepartamento": row["ubigeo_dep"],
                "idUbigeoProvincia": row["ubigeo_prov"],
                "idUbigeoDistrito": row["ubigeo_dist"],
            })
            return {
                "departamento": row["departamento"], "provincia": row["provincia"],
                "ubigeo_prov": row["ubigeo_prov"], "distrito": row["distrito"],
                "ubigeo_dist": row["ubigeo_dist"],
                "avance_pct": data["actasContabilizadas"],
            }
        except Exception:
            return None

    actas_new = []
    errores_actas = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_actas, row): row for row in rows}
        done = 0
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                errores_actas += 1
            else:
                actas_new.append(result)
            done += 1
            if done % 100 == 0:
                elapsed = time.time() - t0
                print(f"  actas: {done}/{n_dist_pendientes} pendientes ({elapsed:.0f}s)", flush=True)

    df_actas_new = pd.DataFrame(actas_new)
    print(f"  Nuevos: {len(df_actas_new)} registros  |  Errores: {errores_actas} distritos")

    df_actas, n_cache_actas_used = merge_cache(df_actas_new, cache_actas)
    df_actas.to_csv(CACHE_ACTAS, index=False)
else:
    df_actas = cache_actas
    n_cache_actas_used = len(cache_actas) if cache_actas is not None else 0
    print(f"\n=== 5. ACTAS: usando cache ({n_cache_actas_used} registros) ===")

if n_cache_actas_used > 0 and api_ok:
    print(f"  Cache usado: {n_cache_actas_used} registros del cache anterior")
print(f"  Total final: {len(df_actas) if df_actas is not None else 0} registros de actas")

# ═══════════════════════════════════════════════════════════
# 6. EXTRANJERO
# ═══════════════════════════════════════════════════════════
extranjero_regiones = [
    ("910000", "AFRICA"), ("920000", "AMERICA"),
    ("930000", "ASIA"), ("940000", "EUROPA"), ("950000", "OCEANIA"),
]

if api_ok:
    print("\n=== 6. OBTENIENDO EXTRANJERO ===")
    votos_ext_new = []
    actas_ext_new = []
    for ubigeo, nombre in extranjero_regiones:
        try:
            data_v = api("eleccion-presidencial/participantes-ubicacion-geografica-nombre", {
                "tipoFiltro": "ubigeo_nivel_01", "idAmbitoGeografico": AMBITO_EXTERIOR,
                "ubigeoNivel1": ubigeo, "idEleccion": ID_ELECCION,
            })
            for fila in data_v:
                votos_ext_new.append({
                    "departamento": f"EXTERIOR_{nombre}", "provincia": f"EXTERIOR_{nombre}",
                    "ubigeo_prov": ubigeo, "distrito": f"EXTERIOR_{nombre}",
                    "ubigeo_dist": ubigeo,
                    "candidato": fila.get("nombreCandidato", ""),
                    "partido": fila.get("nombreAgrupacionPolitica", ""),
                    "votos": fila.get("totalVotosValidos", 0),
                })
            data_a = api("resumen-general/totales", {
                "idAmbitoGeografico": AMBITO_EXTERIOR, "idEleccion": ID_ELECCION,
                "tipoFiltro": "ubigeo_nivel_01", "idUbigeoDepartamento": ubigeo,
            })
            actas_ext_new.append({
                "departamento": f"EXTERIOR_{nombre}", "provincia": f"EXTERIOR_{nombre}",
                "ubigeo_prov": ubigeo, "distrito": f"EXTERIOR_{nombre}",
                "ubigeo_dist": ubigeo, "avance_pct": data_a["actasContabilizadas"],
            })
            time.sleep(SLEEP)
        except Exception as e:
            print(f"  ERROR exterior {nombre}: {e}")
    if votos_ext_new:
        df_ext_new = pd.DataFrame(votos_ext_new).merge(
            pd.DataFrame(actas_ext_new),
            on=["departamento", "provincia", "ubigeo_prov", "distrito", "ubigeo_dist"],
        )
        df_ext, _ = merge_cache(df_ext_new, cache_ext)
        df_ext.to_csv(CACHE_EXTERIOR, index=False)
        avance_ext = df_ext["avance_pct"].mean()
        n_con_datos = (df_ext["avance_pct"] > 0).sum()
        print(f"  Extranjero: {len(df_ext)} registros, {len(actas_ext_new)} regiones, avance {avance_ext:.1f}% ({n_con_datos//4}/5 con datos)")
    else:
        df_ext, _ = merge_cache(pd.DataFrame(), cache_ext)
        if not df_ext.empty:
            print(f"  Extranjero: {len(df_ext)} registros del cache (API fallo)")
else:
    print("\n=== 6. EXTRANJERO: usando cache ===")
    df_ext = cache_ext if cache_ext is not None else pd.DataFrame()
    print(f"  Extranjero: {len(df_ext)} registros (cache)")

# --- MODELO EXTRANJERO: extrapolar con datos reales disponibles ---
ext_modelo_aplicado = False
if not df_ext.empty:
    avance_ext = df_ext["avance_pct"].mean()
    continentes_con_datos = df_ext[df_ext["avance_pct"] > 0]
    if len(continentes_con_datos) > 0 and avance_ext < 100:
        # Extrapolar los continentes con datos al 100%
        ext_validos = continentes_con_datos.copy()
        ext_validos["votos_estimados"] = ext_validos["votos"] / (ext_validos["avance_pct"] / 100)
        ext_validos["candidato"] = ext_validos["candidato"].fillna(ext_validos["partido"])
        total_ext_est = ext_validos["votos_estimados"].sum()
        shares_ext = ext_validos.groupby(["candidato", "partido"])["votos_estimados"].sum()
        shares_ext = shares_ext / shares_ext.sum()
        
        # Aplicar shares a TODAS las regiones del extranjero (total 1V como peso)
        PATH_1V = "base_intermedia_proyeccion.csv"
        if os.path.exists(PATH_1V):
            df_1v = pd.read_csv(PATH_1V)
            ext_1v = df_1v[df_1v["departamento"].str.contains("EXTERIOR", na=False)]
            if not ext_1v.empty:
                modelo_ext = []
                for ubigeo, nombre in extranjero_regiones:
                    total_region_1v = ext_1v[ext_1v["departamento"] == f"EXTERIOR_{nombre}"]["votos_estimados"].sum()
                    if total_region_1v == 0:
                        continue
                    for (candidato, partido), share in shares_ext.items():
                        modelo_ext.append({
                            "departamento": f"EXTERIOR_{nombre}", "provincia": f"EXTERIOR_{nombre}",
                            "ubigeo_prov": ubigeo, "distrito": f"EXTERIOR_{nombre}",
                            "ubigeo_dist": ubigeo,
                            "candidato": candidato, "partido": partido,
                            "votos": total_region_1v * share,
                            "avance_pct": 100.0,
                        })
                df_modelo = pd.DataFrame(modelo_ext)
                n_regiones_con_datos = continentes_con_datos["ubigeo_dist"].nunique()
                print(f"  Extranjero: {n_regiones_con_datos}/5 regiones con datos reales")
                print(f"  Total extranjero estimado: {df_modelo['votos'].sum():,.0f} votos")
                ext_modelo_aplicado = True
else:
    print("  Extranjero: sin datos aun")


# ═══════════════════════════════════════════════════════════
# 7. MERGE + PRORRATEO DE DISTRITOS SIN DATOS
# ═══════════════════════════════════════════════════════════
print("\n=== 7. MERGE Y PRORRATEO ===")

df_final = df_votos.merge(
    df_actas, on=["departamento", "provincia", "ubigeo_prov", "distrito", "ubigeo_dist"],
    how="left"
)

# --- 7a. Distritos en catalogo pero sin votos: estimar con distribucion provincial ---
distritos_catalogo = set(df_distritos["ubigeo_dist"].unique())
distritos_con_votos = set(df_final["ubigeo_dist"].unique())
distritos_faltantes = distritos_catalogo - distritos_con_votos

n_dist_faltantes = len(distritos_faltantes)
if n_dist_faltantes > 0:
    print(f"  {n_dist_faltantes} distritos sin votos: prorrateando con distribucion provincial")
    # Para cada provincia, calcular share de votos por candidato
    shares_prov = df_final.groupby(
        ["departamento", "provincia", "ubigeo_prov", "candidato", "partido"]
    )["votos"].sum().reset_index()
    shares_prov["total_prov"] = shares_prov.groupby(
        ["departamento", "provincia", "ubigeo_prov"]
    )["votos"].transform("sum")
    shares_prov["share"] = shares_prov["votos"] / shares_prov["total_prov"]

    # Votos promedio por distrito en cada provincia (para estimar magnitud)
    votos_por_dist = df_final.groupby(
        ["departamento", "provincia", "ubigeo_prov", "distrito", "ubigeo_dist"]
    )["votos"].sum().reset_index()
    vpd_prom = votos_por_dist.groupby(
        ["departamento", "provincia", "ubigeo_prov"]
    )["votos"].mean().reset_index()
    vpd_prom.columns = ["departamento", "provincia", "ubigeo_prov", "votos_prom_dist"]

    # Generar filas sinteticas para distritos faltantes
    sinteticos = []
    for ubigeo_dist in distritos_faltantes:
        info = df_distritos[df_distritos["ubigeo_dist"] == ubigeo_dist].iloc[0]
        depto, prov, uprov = info["departamento"], info["provincia"], info["ubigeo_prov"]
        sp = shares_prov[
            (shares_prov["departamento"] == depto) &
            (shares_prov["provincia"] == prov)
        ]
        vp = vpd_prom[
            (vpd_prom["departamento"] == depto) &
            (vpd_prom["provincia"] == prov)
        ]
        if sp.empty or vp.empty:
            continue
        total_est = vp["votos_prom_dist"].values[0]
        for _, sr in sp.iterrows():
            sinteticos.append({
                "departamento": depto, "provincia": prov,
                "ubigeo_prov": uprov, "distrito": info["distrito"],
                "ubigeo_dist": ubigeo_dist,
                "candidato": sr["candidato"], "partido": sr["partido"],
                "votos": sr["share"] * total_est,
            })

    if sinteticos:
        df_sint = pd.DataFrame(sinteticos)
        # Asignar avance promedio provincial
        avance_por_prov = df_actas.groupby(
            ["departamento", "provincia", "ubigeo_prov"]
        )["avance_pct"].mean().reset_index()
        df_sint = df_sint.merge(avance_por_prov, on=["departamento", "provincia", "ubigeo_prov"], how="left")
        df_sint["avance_pct"] = df_sint["avance_pct"].fillna(df_actas["avance_pct"].mean())
        df_final = pd.concat([df_final, df_sint], ignore_index=True)
        print(f"  {len(sinteticos)} registros sinteticos generados")

# --- 7b. Prorrateo de actas faltantes con media provincial ---
avance_prov = df_final.dropna(subset=["avance_pct"]).groupby(
    ["departamento", "provincia", "ubigeo_prov"]
)["avance_pct"].mean().reset_index()
avance_prov.columns = ["departamento", "provincia", "ubigeo_prov", "avance_prov_media"]

sin_actas = df_final["avance_pct"].isna().sum()
if sin_actas > 0:
    df_final = df_final.merge(avance_prov, on=["departamento", "provincia", "ubigeo_prov"], how="left")
    df_final["avance_pct"] = df_final["avance_pct"].fillna(df_final["avance_prov_media"])
    nacional_medio = df_final["avance_pct"].mean()
    df_final["avance_pct"] = df_final["avance_pct"].fillna(nacional_medio)
    df_final = df_final.drop(columns=["avance_prov_media"])
    print(f"  {sin_actas} registros sin actas prorrateados con media provincial")

# --- 7c. Extranjero: API si tiene datos completos, sino modelo extrapolado ---
if ext_modelo_aplicado:
    print(f"  Extranjero: usando modelo extrapolado desde datos reales parciales")
    df_total = pd.concat([df_final, df_modelo], ignore_index=True)
elif not df_ext.empty:
    df_ext["avance_pct"] = df_ext["avance_pct"].replace(0, np.nan)
    df_total = pd.concat([df_final, df_ext], ignore_index=True)
else:
    df_total = df_final.copy()

# ═══════════════════════════════════════════════════════════
# 8. EXTRAPOLACION
# ═══════════════════════════════════════════════════════════
print("\n=== 8. EXTRAPOLACION ===")

df_total["avance_pct"] = df_total["avance_pct"].replace(0, np.nan)
df_total["votos_estimados"] = df_total["votos"] / (df_total["avance_pct"] / 100)
df_total["votos_restantes"] = df_total["votos_estimados"] - df_total["votos"]

totales_region = df_total.groupby("departamento")["votos_estimados"].sum().reset_index()
totales_region.columns = ["departamento", "total_region"]
df_total = df_total.merge(totales_region, on="departamento")
df_total["pct_region_actual"] = df_total["votos"] / df_total["total_region"] * 100
df_total["pct_region_proyectado"] = df_total["votos_estimados"] / df_total["total_region"] * 100

# ═══════════════════════════════════════════════════════════
# 9. RESUMEN NACIONAL
# ═══════════════════════════════════════════════════════════
print("\n=== 9. PROYECCION NACIONAL ===")

df_total["candidato"] = df_total["candidato"].fillna("")
mask_empty = df_total["candidato"].eq("")
df_total.loc[mask_empty, "candidato"] = df_total.loc[mask_empty, "partido"]

df_nacional = df_total.groupby(["candidato", "partido"], as_index=False).agg(
    votos_actuales=("votos", "sum"),
    votos_estimados=("votos_estimados", "sum"),
    votos_restantes=("votos_restantes", "sum"),
)
df_nacional = df_nacional.sort_values("votos_estimados", ascending=False).reset_index(drop=True)
total_votos_est = df_nacional["votos_estimados"].sum()
df_nacional["pct_proyectado"] = df_nacional["votos_estimados"] / total_votos_est * 100

# ═══════════════════════════════════════════════════════════
# 10. GUARDAR OUTPUTS
# ═══════════════════════════════════════════════════════════
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

out = {
    "provincias.csv": df_provincias,
    "distritos.csv": df_distritos,
    "votos_distrital.csv": df_votos,
    "actas_distrital.csv": df_actas,
    "proyeccion_distrital.csv": df_total,
    "proyeccion_nacional.csv": df_nacional,
}

for fname, dframe in out.items():
    path = f"v2/output_{timestamp}_{fname}"
    dframe.to_csv(path, index=False)
    print(f"  Guardado: {path}")

# ═══════════════════════════════════════════════════════════
# 11. RESULTADO
# ═══════════════════════════════════════════════════════════
t_total = time.time() - t0
avance_medio = df_total["avance_pct"].mean()

print(f"\n{'=' * 60}")
print(f"  SEGUNDA VUELTA 2026 -- PROYECCION AL 100% (DISTRITAL)")
print(f"  Distritos: {n_dist}  |  Avance medio: {avance_medio:.1f}%")
if n_dist_faltantes > 0:
    print(f"  Distritos estimados (sin datos): {n_dist_faltantes}")
if ext_modelo_aplicado:
    print(f"  Extranjero: modelo extrapolado (datos reales parciales) -- {df_modelo['votos'].sum():,.0f} votos")
if n_cache_votos_used > 0:
    print(f"  Datos del cache: {n_cache_votos_used} votos, {n_cache_actas_used} actas")
if sin_actas > 0:
    print(f"  Registros prorrateados (sin actas): {sin_actas}")
print(f"  Tiempo total: {t_total:.0f}s")
print(f"{'=' * 60}")

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

top = df_nacional[df_nacional["candidato"] != ""].iloc[0]
second = df_nacional[df_nacional["candidato"] != ""].iloc[1]
diff = top["votos_estimados"] - second["votos_estimados"]

print(f"\n{'=' * 60}")
print(f"  GANADOR PROYECTADO: {top['candidato']}")
print(f"     {top['partido']}")
print(f"     {top['votos_estimados']:,.0f} votos estimados ({top['pct_proyectado']:.2f}%)")
print(f"     Ventaja sobre {second['candidato']}: {diff:,.0f} votos")
print(f"{'=' * 60}")

sys.exit(0)
