# -*- coding: utf-8 -*-
# TRANSFORMACIÓN (T de ETL): normaliza CSV del MEF (gasto mensual) y exporta Parquet.
# - Salta archivos ya procesados.
# Uso:
#   python .\etl\transformar_mensual.py                  # procesa todos
#   python .\etl\transformar_mensual.py 2020 2021        # procesa años específicos
#   python .\etl\transformar_mensual.py --overwrite      # rehace todos
#   python .\etl\transformar_mensual.py 2020 --overwrite # rehace solo 2020

import re
import sys
import argparse
from pathlib import Path
import pandas as pd
import traceback

# --- Paths ---
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR  = BASE_DIR / "data" / "raw"
OUT_DIR  = BASE_DIR / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"[info] RAW_DIR: {RAW_DIR.resolve()}")
print(f"[info] OUT_DIR: {OUT_DIR.resolve()}")

# --- Patrones de archivo ---
PATRON_OLD = re.compile(r"^(20\d{2})-Gasto\.csv$", re.IGNORECASE)         # 2017..2023
PATRON_NEW = re.compile(r"^(20\d{2})-Gasto-Mensual\.csv$", re.IGNORECASE) # 2024..2025
IGNORAR    = re.compile(r"Diario", re.IGNORECASE)

# --- Columnas de interés ---
COLS_CLAVE = [
    # tiempo
    "ANO_EJE","MES_EJE",
    # nivel de gobierno
    "NIVEL_GOBIERNO","NIVEL_GOBIERNO_NOMBRE",
    # ejecutora
    "SEC_EJEC","EJECUTORA","EJECUTORA_NOMBRE",
    "SECTOR","SECTOR_NOMBRE","PLIEGO","PLIEGO_NOMBRE",
    "DEPARTAMENTO_EJECUTORA","DEPARTAMENTO_EJECUTORA_NOMBRE",
    "PROVINCIA_EJECUTORA","PROVINCIA_EJECUTORA_NOMBRE",
    "DISTRITO_EJECUTORA","DISTRITO_EJECUTORA_NOMBRE",
    # programática
    "PROGRAMA_PPTO","PROGRAMA_PPTO_NOMBRE",
    "TIPO_ACT_PROY","TIPO_ACT_PROY_NOMBRE",
    "PRODUCTO_PROYECTO","PRODUCTO_PROYECTO_NOMBRE",
    "ACTIVIDAD_ACCION_OBRA","ACTIVIDAD_ACCION_OBRA_NOMBRE",
    "SEC_FUNC",
    # funcional
    "FUNCION","FUNCION_NOMBRE",
    "DIVISION_FUNCIONAL","DIVISION_FUNCIONAL_NOMBRE",
    "GRUPO_FUNCIONAL","GRUPO_FUNCIONAL_NOMBRE",
    # meta
    "META","FINALIDAD","META_NOMBRE","DEPARTAMENTO_META","DEPARTAMENTO_META_NOMBRE","FINALIDAD_NOMBRE",
    # financiera
    "FUENTE_FINANCIAMIENTO","FUENTE_FINANCIAMIENTO_NOMBRE",
    "RUBRO","RUBRO_NOMBRE","TIPO_RECURSO","TIPO_RECURSO_NOMBRE",
    "CATEGORIA_GASTO","CATEGORIA_GASTO_NOMBRE",
    # clasificador gasto
    "TIPO_TRANSACCION",
    "GENERICA","GENERICA_NOMBRE",
    "SUBGENERICA","SUBGENERICA_NOMBRE",
    "SUBGENERICA_DET","SUBGENERICA_DET_NOMBRE",
    "ESPECIFICA","ESPECIFICA_NOMBRE",
    "ESPECIFICA_DET","ESPECIFICA_DET_NOMBRE",
    # métricas
    "MONTO_PIA","MONTO_PIM","MONTO_CERTIFICADO","MONTO_COMPROMETIDO_ANUAL",
    "MONTO_COMPROMETIDO","MONTO_DEVENGADO","MONTO_GIRADO"
]

COLS_NUM = [
    "ANO_EJE","MES_EJE","SEC_FUNC","TIPO_TRANSACCION",
    "MONTO_PIA","MONTO_PIM","MONTO_CERTIFICADO","MONTO_COMPROMETIDO_ANUAL",
    "MONTO_COMPROMETIDO","MONTO_DEVENGADO","MONTO_GIRADO"
]

# --- Helpers ---

# Función: normalizar_columna
# Qué hace: Recibe el nombre de una columna, elimina espacios y la convierte a MAYÚSCULAS para uniformizar el esquema.
def normalizar_columna(c: str) -> str:
    return (c or "").strip().upper()

# Función: a_numero
# Qué hace: Convierte una Serie a numérica (float) de forma segura; valores no convertibles pasan a NaN.
def a_numero(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

# Función: limpiar_texto
# Qué hace: Limpia texto en una Serie: rellena NaN con "", convierte a str, recorta espacios y colapsa espacios múltiples.
def limpiar_texto(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str)
    s = s.str.strip().str.replace(r"\s+", " ", regex=True)
    return s

# Función: construir_fecha
# Qué hace: Construye una fecha (YYYY-MM-01) a partir de las columnas ANO_EJE y MES_EJE; devuelve Serie de tipo datetime.
def construir_fecha(anio_s: pd.Series, mes_s: pd.Series) -> pd.Series:
    anio = a_numero(anio_s).astype("Int64")
    mes  = a_numero(mes_s).astype("Int64")
    vals = [
        f"{int(a)}-{int(m):02d}-01" if pd.notna(a) and pd.notna(m) else None
        for a, m in zip(anio, mes)
    ]
    return pd.to_datetime(pd.Series(vals), format="%Y-%m-%d", errors="coerce")

# Función: transformar_archivo
# Qué hace: Lee un CSV mensual (por bloques), selecciona/normaliza columnas, tipa numéricas, crea FECHA y exporta Parquet por año.
#           Al finalizar correctamente, elimina el CSV original para ahorrar espacio.
def transformar_archivo(ruta_csv: Path, overwrite: bool = False, tamano_bloque: int = 300_000) -> Path | None:
    nombre = ruta_csv.name
    m_old = PATRON_OLD.match(nombre)
    m_new = PATRON_NEW.match(nombre)
    if IGNORAR.search(nombre) or not (m_old or m_new):
        print(f"[skip] {nombre} (no mensual o patrón no coincide)")
        return None

    anio = int((m_old or m_new).group(1))
    out_path = OUT_DIR / f"gasto_mensual_normalizado_{anio}.parquet"

    if out_path.exists() and not overwrite:
        print(f"[skip] {out_path.name} ya existe. Usa --overwrite para rehacerlo.")
        return out_path

    print(f"[proc] {nombre}  ->  {out_path.name}")

    acumulados = []
    filas_total = 0

    # Intento por codificaciones + fallback de engine
    for codificacion in ["utf-8","utf-8-sig","latin-1"]:
        try:
            # 1) parser rápido (C)
            for bloque in pd.read_csv(
                ruta_csv, sep=",", dtype=str, encoding=codificacion,
                on_bad_lines="skip", low_memory=False, chunksize=tamano_bloque,
                quotechar='"', doublequote=True, escapechar='\\'
            ):
                bloque.columns = [normalizar_columna(c) for c in bloque.columns]
                for c in COLS_CLAVE:
                    if c not in bloque.columns:
                        bloque[c] = None
                df = bloque[COLS_CLAVE].copy()
                for c in COLS_NUM:
                    df[c] = a_numero(df[c])
                for c in [c for c in COLS_CLAVE if c not in COLS_NUM]:
                    df[c] = limpiar_texto(df[c])
                df["FECHA"] = construir_fecha(df["ANO_EJE"], df["MES_EJE"])
                df = df[(df["ANO_EJE"] > 0) & (df["MES_EJE"].between(1,12))]
                acumulados.append(df)
                filas_total += len(df)
            break  # leído con esta codificación y engine C
        except Exception as error1:
            print(f"[warn] {nombre} falló con {codificacion} y engine C ({type(error1).__name__}). Intento engine='python'…")
            try:
                # 2) parser tolerante (python)
                for bloque in pd.read_csv(
                    ruta_csv, sep=",", dtype=str, encoding=codificacion,
                    on_bad_lines="skip", low_memory=False, chunksize=tamano_bloque,
                    engine="python", quotechar='"', doublequote=True, escapechar='\\'
                ):
                    bloque.columns = [normalizar_columna(c) for c in bloque.columns]
                    for c in COLS_CLAVE:
                        if c not in bloque.columns:
                            bloque[c] = None
                    df = bloque[COLS_CLAVE].copy()
                    for c in COLS_NUM:
                        df[c] = a_numero(df[c])
                    for c in [c for c in COLS_CLAVE if c not in COLS_NUM]:
                        df[c] = limpiar_texto(df[c])
                    df["FECHA"] = construir_fecha(df["ANO_EJE"], df["MES_EJE"])
                    df = df[(df["ANO_EJE"] > 0) & (df["MES_EJE"].between(1,12))]
                    acumulados.append(df)
                    filas_total += len(df)
                break  # leído con esta codificación y engine python
            except Exception as error2:
                print(f"[warn] {nombre} también falló con engine='python' y {codificacion}: {type(error2).__name__}")
                print(traceback.format_exc().splitlines()[-1])
                # pasa a la siguiente codificación

    if filas_total == 0:
        print(f"[warn] {nombre}: 0 filas válidas tras limpieza.")
        return None

    df_final = pd.concat(acumulados, ignore_index=True)
    df_final.to_parquet(out_path, engine="pyarrow", index=False)
    print(f"[ok] {out_path.name}  filas={len(df_final):,}")

    # --- Limpieza: borrar el CSV original tras convertir a Parquet ---
    try:
        ruta_csv.unlink(missing_ok=True)
        print(f"[limpieza] Eliminado RAW: {ruta_csv.name}")
    except Exception as e:
        # No detenemos el flujo si falla el borrado (p.ej., archivo en uso)
        print(f"[warn] No pude eliminar {ruta_csv.name}: {type(e).__name__}: {e}")

    return out_path

# Función: principal
# Qué hace: Orquesta el proceso de transformación. Lee argumentos (años/overwrite), filtra archivos objetivo y llama a transformar_archivo.
def principal():
    parser = argparse.ArgumentParser(description="Transforma CSV de gasto mensual a Parquet normalizado.")
    parser.add_argument("anios", nargs="*", type=int, help="Años a procesar (opcional). Ej: 2020 2021")
    parser.add_argument("--overwrite", action="store_true", help="Reprocesa aunque el parquet exista.")
    args = parser.parse_args()

    # Construir lista de CSV
    csvs = sorted([p for p in RAW_DIR.glob("*.csv")])
    if not csvs:
        print("[error] No hay CSV en data/raw/")
        sys.exit(1)

    # Filtrar por años si se enviaron
    if args.anios:
        objetivo = set(str(a) for a in args.anios)
        filtrados = []
        for p in csvs:
            m = PATRON_OLD.match(p.name) or PATRON_NEW.match(p.name)
            if m and m.group(1) in objetivo:
                filtrados.append(p)
        csvs = sorted(filtrados)
        if not csvs:
            print(f"[error] No encontré CSV para años: {sorted(objetivo)}")
            sys.exit(1)

    print(f"[info] Procesaré {len(csvs)} archivo(s). Overwrite={args.overwrite}")

    generados = []
    for p in csvs:
        try:
            out = transformar_archivo(p, overwrite=args.overwrite)
            if out:
                generados.append(out.name)
        except KeyboardInterrupt:
            print("\n[abort] Interrumpido por el usuario (Ctrl+C).")
            break
        except Exception:
            print(f"[error] Transformando {p.name}")
            print(traceback.format_exc())

    print("[resumen] Archivos en processed/:")
    for n in generados:
        print(f" - {n}")
    if not generados:
        print(" (ninguno)")

if __name__ == "__main__":
    principal()
