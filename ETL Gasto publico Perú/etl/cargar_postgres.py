# -*- coding: utf-8 -*-
"""
Carga (L de ETL) a PostgreSQL: robusto, vectorizado, tolerante a cortes,
con consolidación por grano, logs claros y reanudación por batch.

Uso:
  python etl/cargar_postgres.py
  python etl/cargar_postgres.py 2017 2018
  python etl/cargar_postgres.py 2017 --batch 150000 --start-batch 36
  python etl/cargar_postgres.py 2017 --batch 150000 --start-batch 36 --end-batch 50
"""

import os
import sys
import argparse
import time
from pathlib import Path
from typing import List, Dict

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DBAPIError
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Parámetros ajustables
FILAS_BATCH_POR_DEFECTO = int(os.getenv("BATCH_ROWS", "250000"))
FILAS_SUBLOTE_POR_DEFECTO = int(os.getenv("SUBBATCH_ROWS", "50000"))
ESPERA_REINTENTO_SEG = 3
MAX_REINTENTOS_BD = 3

# Rutas
DIR_BASE = Path(__file__).resolve().parents[1]
DIR_PROCESADOS = DIR_BASE / "data" / "processed"

# Conexión a BD
load_dotenv()
PG_DSN_CADENA = (
    f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASS')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
)

# Columnas esperadas en Parquet (salida de transformar_mensual.py)
COLUMNAS = [
    "ANO_EJE", "MES_EJE", "FECHA",
    "NIVEL_GOBIERNO", "NIVEL_GOBIERNO_NOMBRE",
    "SEC_EJEC", "EJECUTORA", "EJECUTORA_NOMBRE",
    "SECTOR", "SECTOR_NOMBRE", "PLIEGO", "PLIEGO_NOMBRE",
    "DEPARTAMENTO_EJECUTORA", "DEPARTAMENTO_EJECUTORA_NOMBRE",
    "PROVINCIA_EJECUTORA", "PROVINCIA_EJECUTORA_NOMBRE",
    "DISTRITO_EJECUTORA", "DISTRITO_EJECUTORA_NOMBRE",
    "PROGRAMA_PPTO", "PROGRAMA_PPTO_NOMBRE",
    "TIPO_ACT_PROY", "TIPO_ACT_PROY_NOMBRE",
    "PRODUCTO_PROYECTO", "PRODUCTO_PROYECTO_NOMBRE",
    "ACTIVIDAD_ACCION_OBRA", "ACTIVIDAD_ACCION_OBRA_NOMBRE",
    "SEC_FUNC",
    "FUNCION", "FUNCION_NOMBRE",
    "DIVISION_FUNCIONAL", "DIVISION_FUNCIONAL_NOMBRE",
    "GRUPO_FUNCIONAL", "GRUPO_FUNCIONAL_NOMBRE",
    "META", "FINALIDAD", "META_NOMBRE",
    "DEPARTAMENTO_META", "DEPARTAMENTO_META_NOMBRE", "FINALIDAD_NOMBRE",
    "FUENTE_FINANCIAMIENTO", "FUENTE_FINANCIAMIENTO_NOMBRE",
    "RUBRO", "RUBRO_NOMBRE", "TIPO_RECURSO", "TIPO_RECURSO_NOMBRE",
    "CATEGORIA_GASTO", "CATEGORIA_GASTO_NOMBRE",
    "TIPO_TRANSACCION",
    "GENERICA", "GENERICA_NOMBRE",
    "SUBGENERICA", "SUBGENERICA_NOMBRE",
    "SUBGENERICA_DET", "SUBGENERICA_DET_NOMBRE",
    "ESPECIFICA", "ESPECIFICA_NOMBRE",
    "ESPECIFICA_DET", "ESPECIFICA_DET_NOMBRE",
    "MONTO_PIA", "MONTO_PIM", "MONTO_CERTIFICADO", "MONTO_COMPROMETIDO_ANUAL",
    "MONTO_COMPROMETIDO", "MONTO_DEVENGADO", "MONTO_GIRADO",
]

# Claves y métricas en la tabla de hechos
FKS_FACT = [
    "tiempo_id","nivel_gobierno_id","ejecutora_id",
    "programatica_id","funcional_id","meta_id",
    "financiera_id","clasif_gasto_id"
]
METRICAS_FACT = [
    "monto_pia","monto_pim","monto_certificado",
    "monto_comprometido_anual","monto_comprometido",
    "monto_devengado","monto_girado"
]

# Crea un motor SQLAlchemy y fija el search_path a mef.
def nuevo_motor() -> Engine:
    motor = create_engine(
        PG_DSN_CADENA, future=True,
        pool_pre_ping=True, pool_recycle=1800,
        pool_size=5, max_overflow=0,
    )
    with motor.begin() as con:
        con.execute(text("SET search_path TO mef, public;"))
    return motor

# Asegura índices únicos en dimensiones para evitar duplicados lógicos.
def asegurar_indices_unicos(motor: Engine):
    ddls = [
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_nivel_gobierno ON mef.dim_nivel_gobierno (nivel_gobierno_codigo);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_ejecutora ON mef.dim_ejecutora (sec_ejec, ejecutora_codigo);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_programatica ON mef.dim_programatica (programa_ppto, tipo_act_proy, producto_proyecto, actividad_accion_obra, sec_func);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_funcional ON mef.dim_funcional (funcion, division_funcional, grupo_funcional);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_meta ON mef.dim_meta (meta, finalidad, dep_meta_codigo);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_financiera ON mef.dim_financiera (fuente_financiamiento, rubro, tipo_recurso, categoria_gasto);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_clasificador_gasto ON mef.dim_clasificador_gasto (tipo_transaccion, generica, subgenerica, subgenerica_det, especifica, especifica_det);",
    ]
    with motor.begin() as con:
        for ddl in ddls:
            con.execute(text(ddl))

# Lee una dimensión (id + columnas clave) para tener un mapa local.
def leer_mapa_dim(motor: Engine, tabla: str, col_id: str, cols_clave: List[str]) -> pd.DataFrame:
    cols = ", ".join([col_id] + cols_clave)
    df = pd.read_sql(f"SELECT {cols} FROM mef.{tabla};", motor)
    for c in cols_clave:
        if c == "tipo_transaccion":
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = df[c].astype("string").str.strip()
    return df

# Inserta claves nuevas en una dimensión (upsert NO CONFLICT), con reintentos.
def insertar_claves_nuevas(motor: Engine, tabla: str, todas_las_columnas: List[str], df_nuevas: pd.DataFrame):
    if df_nuevas.empty:
        return
    registros = [
        tuple(None if pd.isna(v) else v for v in fila[todas_las_columnas])
        for _, fila in df_nuevas.iterrows()
    ]
    plantilla = "(" + ",".join(["%s"] * len(todas_las_columnas)) + ")"
    sql = f"INSERT INTO mef.{tabla} ({', '.join(todas_las_columnas)}) VALUES %s ON CONFLICT DO NOTHING"
    for intento in range(1, MAX_REINTENTOS_BD + 1):
        cruda = motor.raw_connection()
        try:
            cur = cruda.cursor()
            try:
                execute_values(cur, sql, registros, template=plantilla, page_size=10000)
                cruda.commit()
                return
            finally:
                cur.close()
        except Exception:
            try: cruda.close()
            except: pass
            if intento == MAX_REINTENTOS_BD:
                raise
            print(f"    [retry] upsert {tabla} intento {intento} falló. Reintentando…")
            time.sleep(ESPERA_REINTENTO_SEG)

# Convierte a string “limpio” (strip) respetando pandas NA.
def a_cadena(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()

# Toma el batch fuente y arma un DataFrame normalizado con nombres de columnas de dimensiones/medidas.
def construir_df_normalizado(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "ano_eje": pd.to_numeric(df["ANO_EJE"], errors="coerce"),
        "mes_eje": pd.to_numeric(df["MES_EJE"], errors="coerce"),
        # nivel
        "nivel_gobierno_codigo": a_cadena(df["NIVEL_GOBIERNO"]),
        "nivel_gobierno_nombre": a_cadena(df["NIVEL_GOBIERNO_NOMBRE"]),
        # ejecutora
        "sec_ejec": a_cadena(df["SEC_EJEC"]),
        "ejecutora_codigo": a_cadena(df["EJECUTORA"]),
        "ejecutora_nombre": a_cadena(df["EJECUTORA_NOMBRE"]),
        "sector": a_cadena(df["SECTOR"]),
        "sector_nombre": a_cadena(df["SECTOR_NOMBRE"]),
        "pliego": a_cadena(df["PLIEGO"]),
        "pliego_nombre": a_cadena(df["PLIEGO_NOMBRE"]),
        "dep_ejecutora_codigo": a_cadena(df["DEPARTAMENTO_EJECUTORA"]),
        "dep_ejecutora_nombre": a_cadena(df["DEPARTAMENTO_EJECUTORA_NOMBRE"]),
        "prov_ejecutora_codigo": a_cadena(df["PROVINCIA_EJECUTORA"]),
        "prov_ejecutora_nombre": a_cadena(df["PROVINCIA_EJECUTORA_NOMBRE"]),
        "dist_ejecutora_codigo": a_cadena(df["DISTRITO_EJECUTORA"]),
        "dist_ejecutora_nombre": a_cadena(df["DISTRITO_EJECUTORA_NOMBRE"]),
        # programática
        "programa_ppto": a_cadena(df["PROGRAMA_PPTO"]),
        "programa_ppto_nombre": a_cadena(df["PROGRAMA_PPTO_NOMBRE"]),
        "tipo_act_proy": a_cadena(df["TIPO_ACT_PROY"]),
        "tipo_act_proy_nombre": a_cadena(df["TIPO_ACT_PROY_NOMBRE"]),
        "producto_proyecto": a_cadena(df["PRODUCTO_PROYECTO"]),
        "producto_proyecto_nombre": a_cadena(df["PRODUCTO_PROYECTO_NOMBRE"]),
        "actividad_accion_obra": a_cadena(df["ACTIVIDAD_ACCION_OBRA"]),
        "actividad_accion_obra_nombre": a_cadena(df["ACTIVIDAD_ACCION_OBRA_NOMBRE"]),
        "sec_func": a_cadena(df["SEC_FUNC"]),
        # funcional
        "funcion": a_cadena(df["FUNCION"]),
        "funcion_nombre": a_cadena(df["FUNCION_NOMBRE"]),
        "division_funcional": a_cadena(df["DIVISION_FUNCIONAL"]),
        "division_funcional_nombre": a_cadena(df["DIVISION_FUNCIONAL_NOMBRE"]),
        "grupo_funcional": a_cadena(df["GRUPO_FUNCIONAL"]),
        "grupo_funcional_nombre": a_cadena(df["GRUPO_FUNCIONAL_NOMBRE"]),
        # meta
        "meta": a_cadena(df["META"]),
        "finalidad": a_cadena(df["FINALIDAD"]),
        "finalidad_nombre": a_cadena(df["FINALIDAD_NOMBRE"]),
        "meta_nombre": a_cadena(df["META_NOMBRE"]),
        "dep_meta_codigo": a_cadena(df["DEPARTAMENTO_META"]),
        "dep_meta_nombre": a_cadena(df["DEPARTAMENTO_META_NOMBRE"]),
        # financiera
        "fuente_financiamiento": a_cadena(df["FUENTE_FINANCIAMIENTO"]),
        "fuente_financiamiento_nombre": a_cadena(df["FUENTE_FINANCIAMIENTO_NOMBRE"]),
        "rubro": a_cadena(df["RUBRO"]),
        "rubro_nombre": a_cadena(df["RUBRO_NOMBRE"]),
        "tipo_recurso": a_cadena(df["TIPO_RECURSO"]),
        "tipo_recurso_nombre": a_cadena(df["TIPO_RECURSO_NOMBRE"]),
        "categoria_gasto": a_cadena(df["CATEGORIA_GASTO"]),
        "categoria_gasto_nombre": a_cadena(df["CATEGORIA_GASTO_NOMBRE"]),
        # clasificador
        "tipo_transaccion": pd.to_numeric(df["TIPO_TRANSACCION"], errors="coerce"),
        "generica": a_cadena(df["GENERICA"]),
        "generica_nombre": a_cadena(df["GENERICA_NOMBRE"]),
        "subgenerica": a_cadena(df["SUBGENERICA"]),
        "subgenerica_nombre": a_cadena(df["SUBGENERICA_NOMBRE"]),
        "subgenerica_det": a_cadena(df["SUBGENERICA_DET"]),
        "subgenerica_det_nombre": a_cadena(df["SUBGENERICA_DET_NOMBRE"]),
        "especifica": a_cadena(df["ESPECIFICA"]),
        "especifica_nombre": a_cadena(df["ESPECIFICA_NOMBRE"]),
        "especifica_det": a_cadena(df["ESPECIFICA_DET"]),
        "especifica_det_nombre": a_cadena(df["ESPECIFICA_DET_NOMBRE"]),
        # métricas
        "monto_pia": pd.to_numeric(df["MONTO_PIA"], errors="coerce"),
        "monto_pim": pd.to_numeric(df["MONTO_PIM"], errors="coerce"),
        "monto_certificado": pd.to_numeric(df["MONTO_CERTIFICADO"], errors="coerce"),
        "monto_comprometido_anual": pd.to_numeric(df["MONTO_COMPROMETIDO_ANUAL"], errors="coerce"),
        "monto_comprometido": pd.to_numeric(df["MONTO_COMPROMETIDO"], errors="coerce"),
        "monto_devengado": pd.to_numeric(df["MONTO_DEVENGADO"], errors="coerce"),
        "monto_girado": pd.to_numeric(df["MONTO_GIRADO"], errors="coerce"),
    })

# Inserta la tabla de hechos en sublotes para no saturar la conexión.
def insertar_sublotes_fact(motor: Engine, df_fact: pd.DataFrame, filas_sublote: int):
    cols_sql = ", ".join(df_fact.columns)
    plantilla = "(" + ",".join(["%s"] * len(df_fact.columns)) + ")"
    total = len(df_fact)
    offset = 0
    while offset < total:
        trozo = df_fact.iloc[offset: offset + filas_sublote]
        valores = [tuple(None if pd.isna(v) else v for v in fila)
                   for fila in trozo.itertuples(index=False, name=None)]
        sql = f"""
            INSERT INTO mef.fact_gasto_mensual ({cols_sql})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        for intento in range(1, MAX_REINTENTOS_BD + 1):
            cruda = motor.raw_connection()
            try:
                cur = cruda.cursor()
                try:
                    execute_values(cur, sql, valores, template=plantilla, page_size=20000)
                    cruda.commit()
                    break
                finally:
                    cur.close()
            except Exception:
                try: cruda.close()
                except: pass
                if intento == MAX_REINTENTOS_BD:
                    raise
                print(f"    [retry] insert fact intento {intento} falló. Reintentando…")
                time.sleep(ESPERA_REINTENTO_SEG)
        offset += filas_sublote

# Carga un Parquet por batches Arrow, garantiza dimensiones, resuelve FKs y inserta hechos consolidados.
def cargar_parquet(motor: Engine, ruta_parquet: Path, filas_batch: int, filas_sublote: int,
                   batch_inicio: int = 1, batch_fin: int | None = None):
    print(f"[proc] {ruta_parquet.name}")

    try:
        pf = pq.ParquetFile(str(ruta_parquet))
    except Exception as e:
        print(f"  [error] no pude abrir {ruta_parquet.name} como Parquet: {type(e).__name__}: {e}")
        return

    batches = pf.iter_batches(batch_size=filas_batch, columns=COLUMNAS)

    # cache dim_tiempo por (anio,mes)
    dt = pd.read_sql("SELECT tiempo_id, anio, mes FROM mef.dim_tiempo;", motor)
    dt["anio"] = pd.to_numeric(dt["anio"], errors="coerce")
    dt["mes"]  = pd.to_numeric(dt["mes"], errors="coerce")

    dim_cfg = {
        "nivel": {"table":"dim_nivel_gobierno","id":"nivel_gobierno_id","keys":["nivel_gobierno_codigo"],
                  "all_cols":["nivel_gobierno_codigo","nivel_gobierno_nombre"]},
        "ejec":  {"table":"dim_ejecutora","id":"ejecutora_id","keys":["sec_ejec","ejecutora_codigo"],
                  "all_cols":["sec_ejec","ejecutora_codigo","ejecutora_nombre","sector","sector_nombre",
                              "pliego","pliego_nombre","dep_ejecutora_codigo","dep_ejecutora_nombre",
                              "prov_ejecutora_codigo","prov_ejecutora_nombre","dist_ejecutora_codigo",
                              "dist_ejecutora_nombre"]},
        "prog":  {"table":"dim_programatica","id":"programatica_id",
                  "keys":["programa_ppto","tipo_act_proy","producto_proyecto","actividad_accion_obra","sec_func"],
                  "all_cols":["programa_ppto","programa_ppto_nombre","tipo_act_proy","tipo_act_proy_nombre",
                              "producto_proyecto","producto_proyecto_nombre","actividad_accion_obra",
                              "actividad_accion_obra_nombre","sec_func"]},
        "func":  {"table":"dim_funcional","id":"funcional_id",
                  "keys":["funcion","division_funcional","grupo_funcional"],
                  "all_cols":["funcion","funcion_nombre","division_funcional","division_funcional_nombre",
                              "grupo_funcional","grupo_funcional_nombre"]},
        "meta":  {"table":"dim_meta","id":"meta_id",
                  "keys":["meta","finalidad","dep_meta_codigo"],
                  "all_cols":["meta","finalidad","finalidad_nombre","meta_nombre","dep_meta_codigo","dep_meta_nombre"]},
        "fin":   {"table":"dim_financiera","id":"financiera_id",
                  "keys":["fuente_financiamiento","rubro","tipo_recurso","categoria_gasto"],
                  "all_cols":["fuente_financiamiento","fuente_financiamiento_nombre","rubro","rubro_nombre",
                              "tipo_recurso","tipo_recurso_nombre","categoria_gasto","categoria_gasto_nombre"]},
        "clas":  {"table":"dim_clasificador_gasto","id":"clasif_gasto_id",
                  "keys":["tipo_transaccion","generica","subgenerica","subgenerica_det","especifica","especifica_det"],
                  "all_cols":["tipo_transaccion","generica","generica_nombre","subgenerica","subgenerica_nombre",
                              "subgenerica_det","subgenerica_det_nombre","especifica","especifica_nombre",
                              "especifica_det","especifica_det_nombre"]},
    }

    dim_df: Dict[str, pd.DataFrame] = {}
    for tag, cfg in dim_cfg.items():
        dim_df[tag] = leer_mapa_dim(motor, cfg["table"], cfg["id"], cfg["keys"])

    # Reanudación: saltar batches iniciales
    idx = 0
    for batch in batches:
        idx += 1
        if idx < batch_inicio:
            continue
        if batch_fin is not None and idx > batch_fin:
            print(f"  [info] end_batch={batch_fin} alcanzado. Detengo archivo.")
            break

        try:
            src = batch.to_pandas()
        except Exception as e:
            print(f"  [warn] batch {idx} no convertible a pandas: {type(e).__name__}. salto el batch.")
            continue

        for c in COLUMNAS:
            if c not in src.columns:
                src[c] = pd.NA

        df = construir_df_normalizado(src)
        filas_fuente = len(df)

        # (anio, mes) -> tiempo_id
        df_time = df[["ano_eje","mes_eje"]].rename(columns={"ano_eje":"anio","mes_eje":"mes"})
        df = pd.concat([df, df_time], axis=1)
        df = df.merge(dt, how="left", on=["anio","mes"])
        df.rename(columns={"tiempo_id":"tiempo_id"}, inplace=True)
        df.drop(columns=["anio","mes"], inplace=True)

        # upsert/merge dims
        for tag in ["nivel","ejec","prog","func","meta","fin","clas"]:
            cfg = dim_cfg[tag]; keys = cfg["keys"]; idcol = cfg["id"]; all_cols = cfg["all_cols"]
            new_keys = df[keys].drop_duplicates()
            merged = new_keys.merge(dim_df[tag][keys], on=keys, how="left", indicator=True)
            to_insert = merged[merged["_merge"] == "left_only"][keys]
            if not to_insert.empty:
                insert_df = to_insert.merge(df[all_cols].drop_duplicates(), on=keys, how="left")
                insert_df = insert_df[all_cols].drop_duplicates()
                insertar_claves_nuevas(motor, cfg["table"], all_cols, insert_df)
                dim_df[tag] = leer_mapa_dim(motor, cfg["table"], idcol, keys)
            df = df.merge(dim_df[tag][keys + [idcol]], on=keys, how="left")

        ok_mask = df[FKS_FACT].notna().all(axis=1)
        filas_fk_ok = int(ok_mask.sum())
        print(f"    [ok] batch {idx}: FKs completas {filas_fk_ok:,}/{filas_fuente:,}")

        if filas_fk_ok == 0:
            nulos = {c: int(df[c].isna().sum()) for c in FKS_FACT}
            print(f"  [warn] lote sin filas insertables. Nulos por FK: {nulos}")
            continue

        fact_df = df.loc[ok_mask, FKS_FACT + METRICAS_FACT].copy()
        fact_df = fact_df.groupby(FKS_FACT, as_index=False)[METRICAS_FACT].sum()
        consolidadas = len(fact_df)
        print(f"  [info] batch {idx}: fuente={filas_fuente:,} | fk_ok={filas_fk_ok:,} | consolidadas={consolidadas:,}")

        try:
            insertar_sublotes_fact(motor, fact_df, filas_sublote)
            print(f"  [ok] batch {idx} insertado (consolidadas={consolidadas:,})")
        except (DBAPIError, OperationalError, SQLAlchemyError):
            print(f"  [warn] fallo insert batch {idx}. intento reconexión…")
            try: motor.dispose()
            except Exception: pass
            motor = nuevo_motor()
            insertar_sublotes_fact(motor, fact_df, filas_sublote)
            print(f"  [ok] batch {idx} insertado tras reconexión")

# CLI: prepara motor, índices únicos, selecciona archivos y ejecuta carga con opciones de reanudación.
def principal():
    parser = argparse.ArgumentParser()
    parser.add_argument("anios", nargs="*", type=int, help="Años a cargar (opcional). Ej: 2017 2018")
    parser.add_argument("--batch", type=int, default=FILAS_BATCH_POR_DEFECTO, help="Filas por batch Arrow (default 250k)")
    parser.add_argument("--subbatch", type=int, default=FILAS_SUBLOTE_POR_DEFECTO, help="Filas por sublote INSERT (default 50k)")
    parser.add_argument("--start-batch", type=int, default=1, help="Batch inicial (1-based) para reanudar dentro del archivo")
    parser.add_argument("--end-batch", type=int, default=None, help="Batch final (inclusive) dentro del archivo")
    args = parser.parse_args()

    motor = nuevo_motor()
    asegurar_indices_unicos(motor)

    archivos = sorted(DIR_PROCESADOS.glob("gasto_mensual_normalizado_*.parquet"))
    if args.anios:
        mantener = {str(a) for a in args.anios}
        archivos = [f for f in archivos if any(k in f.name for k in mantener)]

    if not archivos:
        print("[error] No hay archivos Parquet para cargar.")
        sys.exit(1)

    print(f"[info] {len(archivos)} archivo(s) a cargar en PostgreSQL")
    for f in archivos:
        try:
            cargar_parquet(
                motor, f,
                filas_batch=args.batch,
                filas_sublote=args.subbatch,
                batch_inicio=args.start_batch,
                batch_fin=args.end_batch
            )
        except KeyboardInterrupt:
            print("\n[abort] Interrumpido por el usuario (Ctrl+C).")
            break
        except Exception as e:
            print(f"  [error] {f.name}: {type(e).__name__}: {e}. Continúo con el siguiente…")
        finally:
            try: motor.dispose()
            except Exception: pass
            motor = nuevo_motor()

    print("[OK] Carga completada.")

if __name__ == "__main__":
    os.environ["PYTHONUNBUFFERED"] = "1"
    principal()
