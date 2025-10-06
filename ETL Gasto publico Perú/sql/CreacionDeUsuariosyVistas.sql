
-- Usuario sólo-lectura para BI
CREATE ROLE bi_user LOGIN PASSWORD 'powerbi123';

-- Permisos de conexión y lectura sobre tu DB y esquema mef
GRANT CONNECT ON DATABASE mef_dw TO bi_user;

-- Asegura el search_path y permisos sobre el esquema mef
GRANT USAGE ON SCHEMA mef TO bi_user;

-- Lectura sobre todas las tablas/vistas actuales del esquema mef
GRANT SELECT ON ALL TABLES IN SCHEMA mef TO bi_user;

-- Lectura por defecto sobre tablas/vistas futuras en mef
ALTER DEFAULT PRIVILEGES IN SCHEMA mef GRANT SELECT ON TABLES TO bi_user;


--Vista base mensual con descripciones
SET search_path TO mef, public;

CREATE OR REPLACE VIEW vw_gasto_mensual AS
SELECT
  dt.tiempo_id,
  dt.fecha::date                 AS fecha,
  dt.anio                        AS anio,
  dt.mes                         AS mes,
  dt.trimestre                   AS trimestre,

  ng.nivel_gobierno_id,
  ng.nivel_gobierno_codigo,
  ng.nivel_gobierno_nombre,

  ej.ejecutora_id,
  ej.sec_ejec,
  ej.ejecutora_codigo,
  ej.ejecutora_nombre,
  ej.sector,
  ej.sector_nombre,
  ej.pliego,
  ej.pliego_nombre,
  ej.dep_ejecutora_codigo,
  ej.dep_ejecutora_nombre,
  ej.prov_ejecutora_codigo,
  ej.prov_ejecutora_nombre,
  ej.dist_ejecutora_codigo,
  ej.dist_ejecutora_nombre,

  pr.programatica_id,
  pr.programa_ppto,
  pr.programa_ppto_nombre,
  pr.tipo_act_proy,
  pr.tipo_act_proy_nombre,
  pr.producto_proyecto,
  pr.producto_proyecto_nombre,
  pr.actividad_accion_obra,
  pr.actividad_accion_obra_nombre,
  pr.sec_func,

  fu.funcional_id,
  fu.funcion,
  fu.funcion_nombre,
  fu.division_funcional,
  fu.division_funcional_nombre,
  fu.grupo_funcional,
  fu.grupo_funcional_nombre,

  me.meta_id,
  me.meta,
  me.meta_nombre,
  me.finalidad,
  me.finalidad_nombre,
  me.dep_meta_codigo,
  me.dep_meta_nombre,

  fi.financiera_id,
  fi.fuente_financiamiento,
  fi.fuente_financiamiento_nombre,
  fi.rubro,
  fi.rubro_nombre,
  fi.tipo_recurso,
  fi.tipo_recurso_nombre,
  fi.categoria_gasto,
  fi.categoria_gasto_nombre,

  cg.clasif_gasto_id,
  cg.tipo_transaccion,
  cg.generica,
  cg.generica_nombre,
  cg.subgenerica,
  cg.subgenerica_nombre,
  cg.subgenerica_det,
  cg.subgenerica_det_nombre,
  cg.especifica,
  cg.especifica_nombre,
  cg.especifica_det,
  cg.especifica_det_nombre,

  -- métricas (usa COALESCE para evitar nulls)
  COALESCE(f.monto_pia, 0)                 AS monto_pia,
  COALESCE(f.monto_pim, 0)                 AS monto_pim,
  COALESCE(f.monto_certificado, 0)         AS monto_certificado,
  COALESCE(f.monto_comprometido_anual, 0)  AS monto_comprometido_anual,
  COALESCE(f.monto_comprometido, 0)        AS monto_comprometido,
  COALESCE(f.monto_devengado, 0)           AS monto_devengado,
  COALESCE(f.monto_girado, 0)              AS monto_girado
FROM mef.fact_gasto_mensual f
JOIN mef.dim_tiempo          dt ON dt.tiempo_id         = f.tiempo_id
JOIN mef.dim_nivel_gobierno  ng ON ng.nivel_gobierno_id = f.nivel_gobierno_id
JOIN mef.dim_ejecutora       ej ON ej.ejecutora_id      = f.ejecutora_id
JOIN mef.dim_programatica    pr ON pr.programatica_id   = f.programatica_id
JOIN mef.dim_funcional       fu ON fu.funcional_id      = f.funcional_id
JOIN mef.dim_meta            me ON me.meta_id           = f.meta_id
JOIN mef.dim_financiera      fi ON fi.financiera_id     = f.financiera_id
JOIN mef.dim_clasificador_gasto cg ON cg.clasif_gasto_id = f.clasif_gasto_id;

--Agregado mensual
DROP VIEW IF EXISTS mef.vw_gasto_agregado_mensual;

CREATE OR REPLACE VIEW mef.vw_gasto_agregado_mensual AS
SELECT
  dt.anio,
  dt.mes,
  dt.trimestre,

  -- Ejecutora / Sector / Pliego
  ej.ejecutora_nombre,
  COALESCE(NULLIF(TRIM(ej.sector_nombre), ''), 'SIN SECTOR') AS sector_nombre,
  COALESCE(NULLIF(TRIM(ej.pliego_nombre), ''), 'SIN PLIEGO') AS pliego_nombre,

  -- UBICACIÓN (desde dim_ejecutora)
  COALESCE(NULLIF(TRIM(ej.dep_ejecutora_nombre), ''), 'SIN DEPARTAMENTO') AS dep_ejecutora_nombre,
  COALESCE(NULLIF(TRIM(ej.prov_ejecutora_nombre), ''), 'SIN PROVINCIA')    AS prov_ejecutora_nombre,
  COALESCE(NULLIF(TRIM(ej.dist_ejecutora_nombre), ''), 'SIN DISTRITO')     AS dist_ejecutora_nombre,

  -- Campo “amigable” para mapas (Departamento, Perú)
  CONCAT(
    'Departamento de ',
    COALESCE(NULLIF(TRIM(ej.dep_ejecutora_nombre), ''), 'SIN DEPARTAMENTO'),
    ', Perú'
  ) AS region_mapa,

  -- Financiera / Clasificador
  fi.fuente_financiamiento_nombre,
  fi.categoria_gasto_nombre,
  cg.generica_nombre,
  cg.especifica_nombre,

  -- Métricas
  SUM(COALESCE(f.monto_pia, 0))                AS pia,
  SUM(COALESCE(f.monto_pim, 0))                AS pim,
  SUM(COALESCE(f.monto_certificado, 0))        AS certificado,
  SUM(COALESCE(f.monto_comprometido_anual, 0)) AS comprometido_anual,
  SUM(COALESCE(f.monto_comprometido, 0))       AS comprometido,
  SUM(COALESCE(f.monto_devengado, 0))          AS devengado,
  SUM(COALESCE(f.monto_girado, 0))             AS girado
FROM mef.fact_gasto_mensual f
JOIN mef.dim_tiempo              dt  ON dt.tiempo_id        = f.tiempo_id
JOIN mef.dim_ejecutora           ej  ON ej.ejecutora_id     = f.ejecutora_id
JOIN mef.dim_financiera          fi  ON fi.financiera_id    = f.financiera_id
JOIN mef.dim_clasificador_gasto  cg  ON cg.clasif_gasto_id  = f.clasif_gasto_id
GROUP BY
  dt.anio,
  dt.mes,
  dt.trimestre,
  ej.ejecutora_nombre,
  COALESCE(NULLIF(TRIM(ej.sector_nombre), ''), 'SIN SECTOR'),
  COALESCE(NULLIF(TRIM(ej.pliego_nombre), ''), 'SIN PLIEGO'),
  COALESCE(NULLIF(TRIM(ej.dep_ejecutora_nombre), ''), 'SIN DEPARTAMENTO'),
  COALESCE(NULLIF(TRIM(ej.prov_ejecutora_nombre), ''), 'SIN PROVINCIA'),
  COALESCE(NULLIF(TRIM(ej.dist_ejecutora_nombre), ''), 'SIN DISTRITO'),
  CONCAT(
    'Departamento de ',
    COALESCE(NULLIF(TRIM(ej.dep_ejecutora_nombre), ''), 'SIN DEPARTAMENTO'),
    ', Perú'
  ),
  fi.fuente_financiamiento_nombre,
  fi.categoria_gasto_nombre,
  cg.generica_nombre,
  cg.especifica_nombre;




--Agregado anual
CREATE OR REPLACE VIEW vw_gasto_agregado_anual AS
SELECT
  dt.anio,
  ej.sector_nombre,
  ej.pliego_nombre,
  SUM(COALESCE(f.monto_pim, 0))       AS pim,
  SUM(COALESCE(f.monto_devengado, 0)) AS devengado,
  SUM(COALESCE(f.monto_girado, 0))    AS girado
FROM mef.fact_gasto_mensual f
JOIN mef.dim_tiempo dt ON dt.tiempo_id = f.tiempo_id
JOIN mef.dim_ejecutora ej ON ej.ejecutora_id = f.ejecutora_id
GROUP BY 1,2,3;




