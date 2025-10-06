# Gasto Público Perú — ETL (MEF)

ETL en **Python** para descargar, normalizar y cargar a **PostgreSQL** los archivos de **“Presupuesto y Ejecución de Gasto”** del MEF (Perú).

Incluye: descarga automatizada con Selenium, transformación a Parquet y carga por *chunks* a una BD analítica. Ideal como base para dashboards y *data marts*.



* Fuente oficial: [https://datosabiertos.mef.gob.pe/dataset/presupuesto-y-ejecucion-de-gasto](https://datosabiertos.mef.gob.pe/dataset/presupuesto-y-ejecucion-de-gasto)
* Probado en Windows 10/11, Python 3.10+, PostgreSQL 17, Google Chrome + Chromedriver.

---

## Tabla de contenidos

1. [Arquitectura](#arquitectura)
2. [Estructura del proyecto](#estructura-del-proyecto)
3. [Requisitos](#requisitos)
4. [Configuración](#configuración)
5. [Uso rápido](#uso-rápido)
6. [Scripts ETL](#scripts-etl)
7. [Modelo de datos / DW](#modelo-de-datos--dw)
8. [Carpeta `sql/` (qué contiene cada archivo)](#carpeta-sql-qué-contiene-cada-archivo)
9. [Consultas de ejemplo](#consultas-de-ejemplo)
10. [Rendimiento y buenas prácticas](#rendimiento-y-buenas-prácticas)
11. [Solución de problemas](#solución-de-problemas)
12. [Cómo subir este proyecto a GitHub](#cómo-subir-este-proyecto-a-github)
13. [Licencia](#licencia)
14. [English TL;DR](#english-tldr)

---

## Arquitectura

* **Extract**: Selenium abre el dataset del MEF y descarga los CSV mensuales, validando tamaño y manejando `.crdownload`.
* **Transform**: normalización de columnas (snake_case, tipos), limpieza básica y exportación a **Parquet** en `data/processed/`. Evita reprocesar archivos ya transformados.
* **Load**: inserción a PostgreSQL por lotes (*chunks*) con `SQLAlchemy`. Flujo directo a esquema analítico (sin *staging* `origin`).

---

## Estructura del proyecto

```
gasto-publico-etl/
├─ etl/
│  ├─ cargar_postgres.py           # Carga Parquet/CSV → PostgreSQL (flujo analítico)
│  ├─ selenium_download.py         # Descarga automatizada (Selenium)
│  └─ transformar_mensual.py       # Normaliza CSV → Parquet
├─ data/
│  ├─ raw/                         # CSV descargados del MEF
│  └─ processed/                   # Parquet normalizados
├─ sql/
│  ├─ CreacionDeDataWarehouse.sql      # DDL del DW (dimensiones + fact)
│  ├─ CreacionDeUsuariosyVistas.sql    # Usuario sólo-lectura + vistas (base y agregadas)
│  ├─ ConsultasAlDataWarehouse.sql     # Consultas analíticas parametrizadas
│  └─ CreacionDBOrigen.sql             # (Opcional/legacy) scripts de DB origen – no usado en este flujo
├─ .env.example
├─ requirements.txt
└─ README.md
```

> **No** incluimos en el árbol `mef_dw.backup` ni `Schema Estrella.png`.

---

## Requisitos

* **Python 3.10+**
* **Google Chrome** y **Chromedriver** compatible (misma versión)
* **PostgreSQL 17** (o 14+)
* Paquetes Python: `pandas`, `requests`, `selenium`, `python-dotenv`, `sqlalchemy`, `psycopg2-binary`, `pyarrow`.

Instalación rápida:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Ejemplo de `requirements.txt`:

```
pandas>=2.2
requests>=2.32
selenium>=4.22
python-dotenv>=1.0
SQLAlchemy>=2.0
psycopg2-binary>=2.9
pyarrow>=17.0
```

---

## Configuración

Crea un archivo **.env** en la raíz (los scripts leen estas variables):

```env
# PostgreSQL
PG_HOST=localhost
PG_PORT=5432
PG_DB=mef_dw
PG_USER=postgres
PG_PASS=password

# ETL
CHUNK_ROWS=200000

# Rutas (ajusta a tu PC)
RUTA_CHROMEDRIVER=C:\Users\diego\OneDrive\Escritorio\Proyectos\drivers\chromedriver-win64\chromedriver.exe
CARPETA_RAW=C:\Users\diego\OneDrive\Escritorio\Proyectos\gasto-publico-etl\data\raw
CARPETA_PROCESSED=C:\Users\diego\OneDrive\Escritorio\Proyectos\gasto-publico-etl\data\processed
```

> El esquema/tabla destino por defecto es **`mef.gasto_mensual`** (configurado dentro del script `cargar_postgres.py`). Si tu versión expone flags para cambiarlo, usa `--help`.

---

## Uso rápido

1. **Descargar CSV del MEF**

```bash
python .\etl\selenium_download.py
# Tip: usa --help para ver las opciones reales (p.ej., --desde/--hasta/--tipo)
```

2. **Transformar CSV → Parquet**

```bash
# Procesa todo lo encontrado en data/raw
python .\etl\transformar_mensual.py

# Años específicos
python .\etl\transformar_mensual.py 2020 2021

# Forzar reproceso
python .\etl\transformar_mensual.py --overwrite
python .\etl\transformar_mensual.py 2020 --overwrite
```

3. **Cargar a PostgreSQL (flujo analítico)**

```bash
# Carga todos los Parquet (usa CHUNK_ROWS para controlar memoria)
python .\etl\cargar_postgres.py

# Opcional: truncar tabla destino antes de cargar
python .\etl\cargar_postgres.py --truncate

# Filtrar por patrón (ej., solo 2024)
python .\etl\cargar_postgres.py --pattern "*2024*.parquet"

# Más opciones
python .\etl\cargar_postgres.py --help
```

4. **Utilidades de revisión**

```bash
# Ver primeras 100 filas de un CSV en data/raw
python .\etl\revision_contenido.py 2024-Gasto.csv

# Detectar separador/encoding y ver muestra
python .\etl\comprobacion.py 2024-Gasto.csv
```

---

## Scripts ETL

### `etl/selenium_download.py`

* Automatiza la descarga desde la página del MEF.
* Maneja `.crdownload`, valida tamaño mínimo y reintenta si falla.
* Parámetros típicos: `--desde`, `--hasta`, `--tipo`.

### `etl/transformar_mensual.py`

* Normaliza nombres de columnas y tipos; limpia valores vacíos.
* Exporta a **Parquet** en `data/processed/`.
* **Idempotente**: salta archivos ya procesados salvo `--overwrite`.

### `etl/cargar_postgres.py`

* Ingesta por *chunks* a la tabla analítica (por defecto `mef.gasto_mensual`).
* Flags comunes: `--truncate`, `--pattern` y otros (`--help`).

### `etl/revision_contenido.py`

* Dado un nombre de archivo (en `data/raw/`), imprime las **primeras 100 filas**.

### `etl/comprobacion.py`

* Detecta automáticamente **separador** y **encoding** del CSV y muestra una **muestra** con columnas.

---

## Modelo de datos / DW

* Esquema analítico **`mef`**.
* Tabla de hechos: **`fact_gasto_mensual`** con grano **mensual** y llaves surrogate hacia las dimensiones.
* Dimensiones: `dim_tiempo`, `dim_nivel_gobierno`, `dim_ejecutora`, `dim_programatica`, `dim_funcional`, `dim_meta`, `dim_financiera`, `dim_clasificador_gasto`.
* Vistas: `vw_gasto_mensual` (base) y agregados `vw_gasto_agregado_mensual` / `vw_gasto_agregado_anual`.

---

## Carpeta `sql/` (

**CreacionDeDataWarehouse.sql**

* Crea el esquema `mef` y todas las **dimensiones** + la **fact** `fact_gasto_mensual` (PK surrogate y `UNIQUE` por combinación de llaves)
* Llena `dim_tiempo` 2010–2030.
* Índices de apoyo por `tiempo`, `ejecutora`, `programática`, `funcional` y `clasificador`.

**CreacionDeUsuariosyVistas.sql**

* Crea el rol **`bi_user`** (sólo lectura) y concede permisos.
* Vista detallada **`vw_gasto_mensual`** y agregados **`vw_gasto_agregado_mensual`** / **`vw_gasto_agregado_anual`** (con `COALESCE` para métricas).

**ConsultasAlDataWarehouse.sql**

* Conjunto de consultas parametrizadas listas para BI y validación:

  * **Devengado YTD por sector** (año/mes de corte)
  * **Top 5 ejecutoras por devengado anual**
  * **Participación (share) de ejecutoras dentro de un sector**
  * **Backlog** (comprometido − devengado) por **específica**
  * **Evolución trimestral** por **nivel de gobierno**

**CreacionDBOrigen.sql** *(opcional/legacy)*

* Guiones para una DB “origen” histórica; **no se usa** en el flujo actual.

> **Orden sugerido de ejecución**: `CreacionDeDataWarehouse.sql` → `CreacionDeUsuariosyVistas.sql` → (opcional) `ConsultasAlDataWarehouse.sql` para pruebas.

---

## Consultas de ejemplo

Total ejecutado por año y sector:

```sql
SELECT
  anio,
  sector_nombre,
  SUM(monto_devengado) AS ejecutado_total
FROM mef.vw_gasto_agregado_mensual
GROUP BY anio, sector_nombre
ORDER BY anio, ejecutado_total DESC;
```

Top 10 ejecutoras en 2024:

```sql
SELECT
  anio,
  ejecutora_nombre,
  SUM(monto_devengado) AS ejecutado_total
FROM mef.vw_gasto_agregado_mensual
WHERE anio = 2024
GROUP BY anio, ejecutora_nombre
ORDER BY ejecutado_total DESC
LIMIT 10;
```

---

## Rendimiento y buenas prácticas

* Ajusta `CHUNK_ROWS` según RAM (100k–300k filas suele ir bien).
* En PostgreSQL, para cargas grandes:

  * Crea/rehaz **índices** después de la carga.
  * Sube `maintenance_work_mem` al crear índices.
  * Considera tablas **UNLOGGED** durante ingesta si la durabilidad no es crítica.
* **Parquet** reduce I/O y acelera la ingesta frente a CSV.

---

## Solución de problemas

* **Chrome/Chromedriver**: usa versiones compatibles.
* **`.crdownload` permanente**: revisa conexión y espacio en disco.
* **Lentitud en carga**: reduce `CHUNK_ROWS` y evita índices durante la ingesta.
* **Encoding raro**: usa `comprobacion.py` y fuerza `UTF8` en la conexión.

---


## English TL;DR

**MEF Peru Public Spending — ETL** in Python. Automated Selenium downloads → CSV, transform to Parquet, and chunked loads into PostgreSQL (**no staging schema**). Folder layout mirrors `etl/`, `data/`, and `sql/` (DW DDL, BI user & views, sample analytics queries). Quickstart: `selenium_download.py` → `transformar_mensual.py` → `cargar_postgres.py`. Configure `.env` (`PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASS`, `CHUNK_ROWS`) and run. Views: `vw_gasto_mensual`, `vw_gasto_agregado_mensual`, `vw_gasto_agregado_anual`. Ready for Power BI / dashboards.
