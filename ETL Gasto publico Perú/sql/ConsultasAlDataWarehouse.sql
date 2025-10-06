SET search_path TO mef, public;
-- Devengado acumulado por sector
WITH params AS (
  SELECT 2025::int AS anio, 8::int AS mes_corte  -- cambia año y mes (1-12)
)
SELECT
  ej.sector_nombre,
  SUM(f.monto_devengado) AS devengado_ytd
FROM fact_gasto_mensual f
JOIN dim_tiempo dt ON dt.tiempo_id = f.tiempo_id
JOIN dim_ejecutora ej ON ej.ejecutora_id = f.ejecutora_id
CROSS JOIN params p
WHERE dt.anio = p.anio
  AND dt.mes BETWEEN 1 AND p.mes_corte
GROUP BY ej.sector_nombre
ORDER BY devengado_ytd DESC;


--Ejecutoras con mayor devengado
WITH params AS (
  SELECT 2025::int AS anio  -- cambia año
)
SELECT
  ej.ejecutora_nombre,
  SUM(f.monto_devengado) AS devengado_anual
FROM fact_gasto_mensual f
JOIN dim_tiempo dt ON dt.tiempo_id = f.tiempo_id
JOIN dim_ejecutora ej ON ej.ejecutora_id = f.ejecutora_id
CROSS JOIN params p
WHERE dt.anio = p.anio
GROUP BY ej.ejecutora_nombre
ORDER BY devengado_anual DESC
LIMIT 5;

--Participación  por ejecutora dentro del sector
WITH params AS (
  SELECT 2025::int AS anio, 8::int AS mes_corte, 'SALUD'::text AS sector  -- cambia año/mes/sector
),
ytd AS (
  SELECT
    ej.ejecutora_nombre,
    SUM(f.monto_devengado) AS dev_ytd
  FROM fact_gasto_mensual f
  JOIN dim_tiempo dt ON dt.tiempo_id = f.tiempo_id
  JOIN dim_ejecutora ej ON ej.ejecutora_id = f.ejecutora_id
  CROSS JOIN params p
  WHERE dt.anio = p.anio
    AND dt.mes BETWEEN 1 AND p.mes_corte
    AND ej.sector_nombre = p.sector
  GROUP BY ej.ejecutora_nombre
),
tot AS (
  SELECT SUM(dev_ytd) AS dev_sector FROM ytd
)
SELECT
  y.ejecutora_nombre,
  y.dev_ytd,
  CASE WHEN t.dev_sector > 0 THEN y.dev_ytd / t.dev_sector ELSE 0 END AS share
FROM ytd y CROSS JOIN tot t
ORDER BY y.dev_ytd DESC;



--Pendiente por ejecutar (comprometido menos devengado)
WITH params AS (
  SELECT 2025::int AS anio, 8::int AS mes_corte  -- cambia año y mes
)
SELECT
  cg.especifica,
  cg.especifica_nombre,
  SUM(f.monto_comprometido) AS comprometido_ytd,
  SUM(f.monto_devengado)    AS devengado_ytd,
  SUM(f.monto_comprometido) - SUM(f.monto_devengado) AS backlog
FROM fact_gasto_mensual f
JOIN dim_tiempo dt ON dt.tiempo_id = f.tiempo_id
JOIN dim_clasificador_gasto cg ON cg.clasif_gasto_id = f.clasif_gasto_id
CROSS JOIN params p
WHERE dt.anio = p.anio
  AND dt.mes BETWEEN 1 AND p.mes_corte
GROUP BY cg.especifica, cg.especifica_nombre
HAVING (SUM(f.monto_comprometido) - SUM(f.monto_devengado)) > 0
ORDER BY backlog DESC
LIMIT 20;


--Evolución trimestral por nivel de gobierno
WITH params AS (
  SELECT 2023::int AS anio_ini, 2025::int AS anio_fin  -- cambia rango
)
SELECT
  dt.anio,
  dt.trimestre,
  ng.nivel_gobierno_nombre,
  SUM(f.monto_devengado) AS dev_trimestral
FROM fact_gasto_mensual f
JOIN dim_tiempo dt ON dt.tiempo_id = f.tiempo_id
JOIN dim_nivel_gobierno ng ON ng.nivel_gobierno_id = f.nivel_gobierno_id
CROSS JOIN params p
WHERE dt.anio BETWEEN p.anio_ini AND p.anio_fin
GROUP BY dt.anio, dt.trimestre, ng.nivel_gobierno_nombre
ORDER BY dt.anio, dt.trimestre, ng.nivel_gobierno_nombre;

