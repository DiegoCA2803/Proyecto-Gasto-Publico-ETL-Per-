# -*- coding: utf-8 -*-
# Descarga CSV del MEF vía Selenium, con verificación de tamaño y manejo de .crdownload.
# Descargar todo: python .\etl\selenium_download.py
# Descargar años nuevos : python .\etl\selenium_download.py nuevos

import re
import sys
import time
import argparse
from pathlib import Path
from typing import List, Tuple, Optional

import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By

URL_DATASET = "https://datosabiertos.mef.gob.pe/dataset/presupuesto-y-ejecucion-de-gasto"

# Carpetas fijas 
BASE_DIR = Path(__file__).resolve().parents[1]
RUTA_CHROMEDRIVER = r"Ruta del driver de Chrome"
CARPETA_RAW = BASE_DIR / "data" / "raw"
CARPETA_RAW.mkdir(parents=True, exist_ok=True)

# Patrones aceptados
ANIOS_ANTIGUOS = {str(y) for y in range(2017, 2024)}   # 2017..2023
ANIOS_NUEVOS   = {"2024", "2025"}                       # 2024..2025

# Tiempos
TIEMPO_MAX_PAGINA   = 240
TIEMPO_MAX_ARCHIVO  = 60 * 60       # 60 min
VENTANA_ESTABLE     = 12
PAUSA_ENTRE_ARCH    = 3
INTENTOS_POR_ARCH   = 3

# ---------- CLI / Filtros ----------

def parsear_cli() -> tuple[Optional[int], Optional[int], str]:
    """
    Acepta formas flexibles:
      - python etl/selenium_download.py
      - python etl/selenium_download.py 2018
      - python etl/selenium_download.py 2018 2020
      - python etl/selenium_download.py nuevos
      - python etl/selenium_download.py 2018 nuevos
      - python etl/selenium_download.py 2024 2025 nuevos
    """
    parser = argparse.ArgumentParser(
        description="Descarga CSV del MEF. Filtra por año y modo (nuevos/antiguos/todos)."
    )
    parser.add_argument("pos1", nargs="?", help="Año mínimo (YYYY) o modo (nuevos/antiguos/todos)")
    parser.add_argument("pos2", nargs="?", help="Año máximo (YYYY) o modo (nuevos/antiguos/todos)")
    parser.add_argument("--hasta", type=int, default=None, help="Año máximo (YYYY)")
    args = parser.parse_args()

    anio_desde: Optional[int] = None
    anio_hasta: Optional[int] = args.hasta
    modo = "todos"

    # Interpretación flexible de pos1 / pos2
    for tok in (args.pos1, args.pos2):
        if tok is None:
            continue
        low = tok.lower()
        if low in {"nuevos", "antiguos", "todos"}:
            modo = low
        elif tok.isdigit() and len(tok) == 4:
            val = int(tok)
            if anio_desde is None:
                anio_desde = val
            elif anio_hasta is None:
                anio_hasta = val

    # Normalizar (si solo dan desde y no hasta)
    if anio_hasta is not None and anio_desde is not None and anio_hasta < anio_desde:
        anio_desde, anio_hasta = anio_hasta, anio_desde

    return anio_desde, anio_hasta, modo


def extraer_anio(nombre: str) -> Optional[int]:
    m = re.search(r"(20\d{2})", nombre)
    return int(m.group(1)) if m else None


def tipo_dataset(nombre: str) -> str:
    n = nombre.lower()
    if n.endswith("-gasto-mensual.csv"):
        return "nuevos"
    if n.endswith("-gasto.csv"):
        return "antiguos"
    return "otro"


def filtrar_enlaces(enlaces: List[Tuple[str, str]],
                    anio_desde: Optional[int],
                    anio_hasta: Optional[int],
                    modo: str) -> List[Tuple[str, str]]:
    filtrados: List[Tuple[str, str]] = []
    for nombre, url in enlaces:
        anio = extraer_anio(nombre)
        if anio is None:
            continue
        if anio_desde is not None and anio < anio_desde:
            continue
        if anio_hasta is not None and anio > anio_hasta:
            continue
        if modo in {"nuevos", "antiguos"} and tipo_dataset(nombre) != modo:
            continue
        filtrados.append((nombre, url))
    # Orden por año y luego nombre
    filtrados.sort(key=lambda x: (extraer_anio(x[0]) or 9999, x[0].lower()))
    return filtrados

# --- utilidades ---

def nombre_seguro(nombre: str) -> str:
    return re.sub(r"[^\w\-.]", "_", nombre)

def aceptar_nombre(nombre: str) -> bool:
    n = nombre.lower().strip()
    if not n.endswith(".csv"):
        return False
    if "diario" in n:
        return False
    m_ant = re.match(r"^(20\d{2})-gasto\.csv$", n)
    if m_ant and m_ant.group(1) in ANIOS_ANTIGUOS:
        return True
    m_nue = re.match(r"^(20\d{2})-gasto-mensual\.csv$", n)
    if m_nue and m_nue.group(1) in ANIOS_NUEVOS:
        return True
    return False

def tam_esperado(url: str) -> Optional[int]:
    try:
        r = requests.head(url, allow_redirects=True, timeout=60)
        r.raise_for_status()
        cl = r.headers.get("Content-Length")
        return int(cl) if cl and cl.isdigit() else None
    except Exception:
        return None

def configurar_driver() -> webdriver.Chrome:
    prefs = {
        "download.default_directory": str(CARPETA_RAW),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
    }
    opciones = webdriver.ChromeOptions()
    opciones.add_experimental_option("prefs", prefs)
    opciones.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opciones.add_argument("--log-level=3")
    opciones.add_argument("--disable-notifications")
    opciones.add_argument("--start-maximized")

    servicio = ChromeService(RUTA_CHROMEDRIVER)
    driver = webdriver.Chrome(service=servicio, options=opciones)
    driver.set_page_load_timeout(TIEMPO_MAX_PAGINA)
    return driver

def recolectar_enlaces_csv(driver: webdriver.Chrome) -> List[Tuple[str, str]]:
    driver.get(URL_DATASET)
    time.sleep(5)
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
    out: List[Tuple[str, str]] = []
    for a in anchors:
        href = a.get_attribute("href") or ""
        txt  = (a.text or "").strip()
        if not href:
            continue
        nombre = txt if txt.lower().endswith(".csv") else href.split("/")[-1]
        if nombre.lower().endswith(".csv") and aceptar_nombre(nombre):
            out.append((nombre, href))
    # dedup
    seen, final = set(), []
    for nombre, url in out:
        key = (nombre.lower(), url)
        if key not in seen:
            seen.add(key); final.append((nombre, url))
    final.sort(key=lambda x: (int(re.search(r"(20\d{2})", x[0]).group(1)) if re.search(r"(20\d{2})", x[0]) else 9999, x[0].lower()))
    return final

def listar_archivos(dirp: Path) -> dict[str, tuple[int, float]]:
    snap = {}
    for p in dirp.iterdir():
        if p.is_file():
            try:
                st = p.stat()
                snap[p.name] = (st.st_size, st.st_mtime)
            except Exception:
                pass
    return snap

def hay_descargas_en_progreso(dirp: Path) -> bool:
    return any(p.suffix == ".crdownload" for p in dirp.iterdir() if p.is_file())

def archivo_reciente(antes: dict, despues: dict) -> Optional[Path]:
    nuevos = [n for n in despues.keys() if n not in antes]
    if nuevos:
        newest = max(nuevos, key=lambda n: despues[n][1])
        return CARPETA_RAW / newest
    if despues:
        newest = max(despues.keys(), key=lambda n: despues[n][1])
        return CARPETA_RAW / newest
    return None

def esperar_estable_o_fin(ruta_obj: Path, tiempo_max: int) -> bool:
    inicio = time.time()
    ult_tam = -1
    ult_cambio = time.time()
    while time.time() - inicio <= tiempo_max:
        if hay_descargas_en_progreso(CARPETA_RAW):
            time.sleep(1); continue
        if ruta_obj.exists():
            try:
                tam = ruta_obj.stat().st_size
            except Exception:
                tam = -1
            if tam != ult_tam:
                ult_tam = tam
                ult_cambio = time.time()
            else:
                if time.time() - ult_cambio >= VENTANA_ESTABLE:
                    return True
        time.sleep(1)
    return False

def descargar_uno(driver: webdriver.Chrome, nombre: str, url: str) -> Path:
    print(f"[descargando] {nombre}")
    esperado = tam_esperado(url)
    if esperado:
        print(f"  - tamaño esperado: {esperado/1e9:.2f} GB")
    else:
        print("  - tamaño esperado: desconocido")

    antes = listar_archivos(CARPETA_RAW)
    driver.get(url)

    inicio = time.time()
    while time.time() - inicio <= TIEMPO_MAX_ARCHIVO:
        despues = listar_archivos(CARPETA_RAW)
        cand = archivo_reciente(antes, despues)
        if cand and esperar_estable_o_fin(cand, tiempo_max=60):
            print(f"  - tamaño final: {cand.stat().st_size/1e9:.2f} GB")
            return cand
        time.sleep(1)
    raise TimeoutError("Timeout esperando descarga")

def main():
    # --- Filtros desde CLI ---
    anio_desde, anio_hasta, modo = parsear_cli()

    driver = configurar_driver()
    try:
        enlaces = recolectar_enlaces_csv(driver)
        enlaces = filtrar_enlaces(enlaces, anio_desde, anio_hasta, modo)

        if not enlaces:
            print("[error] No encontré CSV válidos con los filtros dados"); return

        print(f"[info] {len(enlaces)} archivos candidatos (modo={modo}, desde={anio_desde}, hasta={anio_hasta})")
        for nombre, url in enlaces:
            destino = CARPETA_RAW / nombre_seguro(nombre)
            if destino.exists():
                print(f"[skip] {destino.name} ya existe"); continue

            ok = False
            for intento in range(1, INTENTOS_POR_ARCH + 1):
                try:
                    tmp = descargar_uno(driver, nombre, url)
                    if tmp.name != destino.name and not destino.exists():
                        tmp.rename(destino)
                        print(f"[renombrado] {tmp.name} -> {destino.name}")
                    # validar tamaño
                    esperado = tam_esperado(url)
                    if esperado and destino.exists():
                        real = destino.stat().st_size
                        if abs(real - esperado) > max(esperado * 0.005, 10_000_000):
                            print(f"[warn] Tamaño no coincide (real {real/1e9:.2f} GB vs esp {esperado/1e9:.2f} GB). Reintento…")
                            destino.unlink(missing_ok=True)
                            time.sleep(3)
                            continue
                    ok = True
                    break
                except Exception as e:
                    print(f"[warn] intento {intento} falló: {e}")
                    time.sleep(5)

            if not ok:
                print(f"[error] no pude bajar {nombre} tras {INTENTOS_POR_ARCH} intentos")
            time.sleep(PAUSA_ENTRE_ARCH)

        print("[ok] Descargas completas.")
    finally:
        print("[info] Dejando Chrome abierto (no se cerrará automáticamente).")

if __name__ == "__main__":
    main()

