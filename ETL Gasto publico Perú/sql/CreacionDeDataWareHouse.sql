-- Primer Paso 
-- Se crea un esquema para agrupar todo lo del proyecto
CREATE SCHEMA IF NOT EXISTS mef;

SET search_path TO mef, public;

-- Segundo paso 
-- dim_tiempo: grano mensual 
CREATE TABLE IF NOT EXISTS dim_tiempo (
  tiempo_id   SERIAL PRIMARY KEY,
  fecha       DATE UNIQUE NOT NULL,  -- YYYY-MM-01
  anio        INT  NOT NULL,
  mes         INT  NOT NULL,
  trimestre   INT  NOT NULL
);

-- lleno dim_tiempo con un rango amplio 
INSERT INTO dim_tiempo (fecha, anio, mes, trimestre)
SELECT d::date,
       EXTRACT(YEAR FROM d)::INT,
       EXTRACT(MONTH FROM d)::INT,
       EXTRACT(QUARTER FROM d)::INT
FROM generate_series('2010-01-01'::date, '2030-12-01'::date, interval '1 month') g(d)
ON CONFLICT (fecha) DO NOTHING;

-- Tercer paso 
-- nivel de gobierno 
CREATE TABLE IF NOT EXISTS dim_nivel_gobierno (
  nivel_gobierno_id      SERIAL PRIMARY KEY,
  nivel_gobierno_codigo  TEXT UNIQUE,  
  nivel_gobierno_nombre  TEXT
);

-- ejecutora 
CREATE TABLE IF NOT EXISTS dim_ejecutora (
  ejecutora_id            SERIAL PRIMARY KEY,
  sec_ejec                TEXT,
  ejecutora_codigo        TEXT,
  ejecutora_nombre        TEXT,
  sector                  TEXT,
  sector_nombre           TEXT,
  pliego                  TEXT,
  pliego_nombre           TEXT,
  dep_ejecutora_codigo    TEXT,
  dep_ejecutora_nombre    TEXT,
  prov_ejecutora_codigo   TEXT,
  prov_ejecutora_nombre   TEXT,
  dist_ejecutora_codigo   TEXT,
  dist_ejecutora_nombre   TEXT,
  UNIQUE (sec_ejec, ejecutora_codigo)
);

-- estructura programática 
CREATE TABLE IF NOT EXISTS dim_programatica (
  programatica_id             SERIAL PRIMARY KEY,
  programa_ppto               TEXT,
  programa_ppto_nombre        TEXT,
  tipo_act_proy               TEXT,
  tipo_act_proy_nombre        TEXT,
  producto_proyecto           TEXT,
  producto_proyecto_nombre    TEXT,
  actividad_accion_obra       TEXT,
  actividad_accion_obra_nombre TEXT,
  sec_func                    TEXT
);

-- clasificación funcional 
CREATE TABLE IF NOT EXISTS dim_funcional (
  funcional_id                SERIAL PRIMARY KEY,
  funcion                     TEXT,
  funcion_nombre              TEXT,
  division_funcional          TEXT,
  division_funcional_nombre   TEXT,
  grupo_funcional             TEXT,
  grupo_funcional_nombre      TEXT
);

-- meta / finalidad 
CREATE TABLE IF NOT EXISTS dim_meta (
  meta_id             SERIAL PRIMARY KEY,
  meta                TEXT,
  finalidad           TEXT,
  finalidad_nombre    TEXT,   
  dep_meta_codigo     TEXT,
  dep_meta_nombre     TEXT
);

-- financiera 
CREATE TABLE IF NOT EXISTS dim_financiera (
  financiera_id                 SERIAL PRIMARY KEY,
  fuente_financiamiento         TEXT,
  fuente_financiamiento_nombre  TEXT,
  rubro                         TEXT,
  rubro_nombre                  TEXT,
  tipo_recurso                  TEXT,
  tipo_recurso_nombre           TEXT,
  categoria_gasto               TEXT,
  categoria_gasto_nombre        TEXT
);

-- clasificador del gasto 
CREATE TABLE IF NOT EXISTS dim_clasificador_gasto (
  clasif_gasto_id         SERIAL PRIMARY KEY,
  tipo_transaccion        INT,   -- 2 = gasto
  generica                TEXT, generica_nombre            TEXT,
  subgenerica             TEXT, subgenerica_nombre         TEXT,
  subgenerica_det         TEXT, subgenerica_det_nombre     TEXT,
  especifica              TEXT, especifica_nombre          TEXT,
  especifica_det          TEXT, especifica_det_nombre      TEXT
);

--Cuarto paso
-- fact con todas las métricas; la PK es surrogate y se evita los duplicados con UNIQUE
CREATE TABLE IF NOT EXISTS fact_gasto_mensual (
  fact_id          BIGSERIAL PRIMARY KEY,

  tiempo_id        INT NOT NULL REFERENCES dim_tiempo(tiempo_id),
  nivel_gobierno_id INT NOT NULL REFERENCES dim_nivel_gobierno(nivel_gobierno_id),
  ejecutora_id     INT NOT NULL REFERENCES dim_ejecutora(ejecutora_id),
  programatica_id  INT NOT NULL REFERENCES dim_programatica(programatica_id),
  funcional_id     INT NOT NULL REFERENCES dim_funcional(funcional_id),
  meta_id          INT NOT NULL REFERENCES dim_meta(meta_id),
  financiera_id    INT NOT NULL REFERENCES dim_financiera(financiera_id),
  clasif_gasto_id  INT NOT NULL REFERENCES dim_clasificador_gasto(clasif_gasto_id),

  -- métricas de los datos
  monto_pia                  NUMERIC,
  monto_pim                  NUMERIC,
  monto_certificado          NUMERIC,
  monto_comprometido_anual   NUMERIC,
  monto_comprometido         NUMERIC,
  monto_devengado            NUMERIC,
  monto_girado               NUMERIC,

  -- llave natural del grano
  UNIQUE (tiempo_id, nivel_gobierno_id, ejecutora_id, programatica_id,
          funcional_id, meta_id, financiera_id, clasif_gasto_id)
);

-- índices para acelerar filtros frecuentes
CREATE INDEX IF NOT EXISTS idx_mensual_tiempo       ON fact_gasto_mensual(tiempo_id);
CREATE INDEX IF NOT EXISTS idx_mensual_ejecutora    ON fact_gasto_mensual(ejecutora_id);
CREATE INDEX IF NOT EXISTS idx_mensual_programatica ON fact_gasto_mensual(programatica_id);
CREATE INDEX IF NOT EXISTS idx_mensual_funcional    ON fact_gasto_mensual(funcional_id);
CREATE INDEX IF NOT EXISTS idx_mensual_clasif       ON fact_gasto_mensual(clasif_gasto_id);

SET search_path TO mef, public;

ALTER TABLE dim_meta
  ADD COLUMN IF NOT EXISTS meta_nombre TEXT;


