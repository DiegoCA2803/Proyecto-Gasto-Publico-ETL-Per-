-- Creación del esquema
CREATE SCHEMA IF NOT EXISTS mef_origin;

SET search_path TO mef_origin, public;

-- Creación de tabla
CREATE TABLE IF NOT EXISTS mef_origin.gastos_raw (
  id BIGSERIAL PRIMARY KEY,
  ano_eje INTEGER,
  mes_eje INTEGER,
  nivel_gobierno TEXT,
  nivel_gobierno_nombre TEXT,
  sector TEXT,
  sector_nombre TEXT,
  pliego TEXT,
  pliego_nombre TEXT,
  sec_ejec TEXT,
  ejecutora TEXT,
  ejecutora_nombre TEXT,
  departamento_ejecutora TEXT,
  departamento_ejecutora_nombre TEXT,
  provincia_ejecutora TEXT,
  provincia_ejecutora_nombre TEXT,
  distrito_ejecutora TEXT,
  distrito_ejecutora_nombre TEXT,
  programa_ppto INTEGER,
  programa_ppto_nombre TEXT,
  tipo_act_proy INTEGER,
  tipo_act_proy_nombre INTEGER,
  producto_proyecto INTEGER,
  producto_proyecto_nombre TEXT,
  actividad_accion_obra INTEGER,
  actividad_accion_obra_nombre TEXT,
  funcion TEXT,
  funcion_nombre TEXT,
  division_funcional TEXT,
  division_funcional_nombre TEXT,
  grupo_funcional TEXT,
  grupo_funcional_nombre TEXT,
  meta TEXT,
  finalidad TEXT,
  meta_nombre TEXT,
  departamento_meta TEXT,
  departamento_meta_nombre TEXT,
  sec_func INTEGER,
  fuente_financiamiento TEXT,
  fuente_financiamiento_nombre TEXT,
  rubro TEXT,
  rubro_nombre TEXT,
  tipo_recurso TEXT,
  tipo_recurso_nombre TEXT,
  categoria_gasto INTEGER,
  categoria_gasto_nombre TEXT,
  tipo_transaccion INTEGER,
  generica INTEGER,
  generica_nombre TEXT,
  subgenerica INTEGER,
  subgenerica_nombre TEXT,
  subgenerica_det INTEGER,
  subgenerica_det_nombre TEXT,
  especifica INTEGER,
  especifica_nombre TEXT,
  especifica_det INTEGER,
  especifica_det_nombre TEXT,
  monto_pia NUMERIC(20,2),
  monto_pim NUMERIC(20,2),
  monto_certificado NUMERIC(20,2),
  monto_comprometido_anual NUMERIC(20,2),
  monto_comprometido NUMERIC(20,2),
  monto_devengado NUMERIC(20,2),
  monto_girado NUMERIC(20,2)
);

-- Comentarios
COMMENT ON COLUMN mef_origin.gastos_raw.ano_eje IS 'Año de ejecución del presupuesto.';
COMMENT ON COLUMN mef_origin.gastos_raw.mes_eje IS 'Mes de ejecución del presupuesto.';
COMMENT ON COLUMN mef_origin.gastos_raw.nivel_gobierno IS 'Código (letra) que identifica el Nivel de Gobierno: E, R, M; para Nacional, Regionales y Locales, respectivamente.';
COMMENT ON COLUMN mef_origin.gastos_raw.nivel_gobierno_nombre IS 'Descripción de Nivel de Gobierno: Nacional, Regionales y Locales.';
COMMENT ON COLUMN mef_origin.gastos_raw.sector IS 'Código de Sector al que pertenece la Entidad.';
COMMENT ON COLUMN mef_origin.gastos_raw.sector_nombre IS 'Descripción del Sector al que pertenece la Entidad.';
COMMENT ON COLUMN mef_origin.gastos_raw.pliego IS 'Código de Pliego al que pertenece la Entidad.';
COMMENT ON COLUMN mef_origin.gastos_raw.pliego_nombre IS 'Descripción de Pliego al que pertenece la Entidad.';
COMMENT ON COLUMN mef_origin.gastos_raw.sec_ejec IS 'Código de Unidad Ejecutora (UE).';
COMMENT ON COLUMN mef_origin.gastos_raw.ejecutora IS 'Código de Unidad Ejecutora.';
COMMENT ON COLUMN mef_origin.gastos_raw.ejecutora_nombre IS 'Nombre de la Unidad Ejecutora.';
COMMENT ON COLUMN mef_origin.gastos_raw.departamento_ejecutora IS 'Código de Departamento de la UE.';
COMMENT ON COLUMN mef_origin.gastos_raw.departamento_ejecutora_nombre IS 'Nombre de Departamento de la UE.';
COMMENT ON COLUMN mef_origin.gastos_raw.provincia_ejecutora IS 'Código de Provincia de la UE.';
COMMENT ON COLUMN mef_origin.gastos_raw.provincia_ejecutora_nombre IS 'Nombre de Provincia de la UE.';
COMMENT ON COLUMN mef_origin.gastos_raw.distrito_ejecutora IS 'Código de Distrito de la UE.';
COMMENT ON COLUMN mef_origin.gastos_raw.distrito_ejecutora_nombre IS 'Nombre de Distrito de la UE.';
COMMENT ON COLUMN mef_origin.gastos_raw.programa_ppto IS 'Código del Programa Presupuestal.';
COMMENT ON COLUMN mef_origin.gastos_raw.programa_ppto_nombre IS 'Nombre del Programa Presupuestal.';
COMMENT ON COLUMN mef_origin.gastos_raw.tipo_act_proy IS 'Código de Tipo (Actividad/Acción/Proyecto).';
COMMENT ON COLUMN mef_origin.gastos_raw.tipo_act_proy_nombre IS 'Descripción de Tipo (Actividad/Acción/Proyecto).';
COMMENT ON COLUMN mef_origin.gastos_raw.producto_proyecto IS 'Código del Producto/Proyecto.';
COMMENT ON COLUMN mef_origin.gastos_raw.producto_proyecto_nombre IS 'Nombre del Producto/Proyecto.';
COMMENT ON COLUMN mef_origin.gastos_raw.actividad_accion_obra IS 'Código de Actividad/Acción/Obra.';
COMMENT ON COLUMN mef_origin.gastos_raw.actividad_accion_obra_nombre IS 'Nombre de Actividad/Acción/Obra.';
COMMENT ON COLUMN mef_origin.gastos_raw.funcion IS 'Código de Función de gasto.';
COMMENT ON COLUMN mef_origin.gastos_raw.funcion_nombre IS 'Nombre de la Función.';
COMMENT ON COLUMN mef_origin.gastos_raw.division_funcional IS 'Código de División Funcional.';
COMMENT ON COLUMN mef_origin.gastos_raw.division_funcional_nombre IS 'Nombre de la División Funcional.';
COMMENT ON COLUMN mef_origin.gastos_raw.grupo_funcional IS 'Código de Grupo Funcional.';
COMMENT ON COLUMN mef_origin.gastos_raw.grupo_funcional_nombre IS 'Nombre del Grupo Funcional.';
COMMENT ON COLUMN mef_origin.gastos_raw.meta IS 'Código de la Meta presupuestal.';
COMMENT ON COLUMN mef_origin.gastos_raw.finalidad IS 'Código de Finalidad.';
COMMENT ON COLUMN mef_origin.gastos_raw.meta_nombre IS 'Nombre de la Meta presupuestal.';
COMMENT ON COLUMN mef_origin.gastos_raw.departamento_meta IS 'Código del Departamento de la Meta.';
COMMENT ON COLUMN mef_origin.gastos_raw.departamento_meta_nombre IS 'Nombre del Departamento de la Meta.';
COMMENT ON COLUMN mef_origin.gastos_raw.sec_func IS 'Código de la Sección Funcional (Sec Func).';
COMMENT ON COLUMN mef_origin.gastos_raw.fuente_financiamiento IS 'Código de la Fuente de Financiamiento.';
COMMENT ON COLUMN mef_origin.gastos_raw.fuente_financiamiento_nombre IS 'Descripción de la Fuente de Financiamiento.';
COMMENT ON COLUMN mef_origin.gastos_raw.rubro IS 'Código de Rubro.';
COMMENT ON COLUMN mef_origin.gastos_raw.rubro_nombre IS 'Descripción de Rubro.';
COMMENT ON COLUMN mef_origin.gastos_raw.tipo_recurso IS 'Código de Tipo de Recurso.';
COMMENT ON COLUMN mef_origin.gastos_raw.tipo_recurso_nombre IS 'Descripción de Tipo de Recurso.';
COMMENT ON COLUMN mef_origin.gastos_raw.categoria_gasto IS 'Código de Categoría de Gasto.';
COMMENT ON COLUMN mef_origin.gastos_raw.categoria_gasto_nombre IS 'Descripción de Categoría de Gasto.';
COMMENT ON COLUMN mef_origin.gastos_raw.tipo_transaccion IS 'Código de Tipo de Transacción.';
COMMENT ON COLUMN mef_origin.gastos_raw.generica IS 'Código de Genérica.';
COMMENT ON COLUMN mef_origin.gastos_raw.generica_nombre IS 'Descripción de Genérica.';
COMMENT ON COLUMN mef_origin.gastos_raw.subgenerica IS 'Código de Subgenérica.';
COMMENT ON COLUMN mef_origin.gastos_raw.subgenerica_nombre IS 'Descripción de Subgenérica.';
COMMENT ON COLUMN mef_origin.gastos_raw.subgenerica_det IS 'Código de Subgenérica Detallada.';
COMMENT ON COLUMN mef_origin.gastos_raw.subgenerica_det_nombre IS 'Descripción de Subgenérica Detallada.';
COMMENT ON COLUMN mef_origin.gastos_raw.especifica IS 'Código de Específica.';
COMMENT ON COLUMN mef_origin.gastos_raw.especifica_nombre IS 'Descripción de Específica.';
COMMENT ON COLUMN mef_origin.gastos_raw.especifica_det IS 'Código de Específica Detallada.';
COMMENT ON COLUMN mef_origin.gastos_raw.especifica_det_nombre IS 'Descripción de Específica Detallada.';
COMMENT ON COLUMN mef_origin.gastos_raw.monto_pia IS 'Presupuesto Institucional de Apertura (PIA).';
COMMENT ON COLUMN mef_origin.gastos_raw.monto_pim IS 'Presupuesto Institucional Modificado (PIM).';
COMMENT ON COLUMN mef_origin.gastos_raw.monto_certificado IS 'Monto Certificado.';
COMMENT ON COLUMN mef_origin.gastos_raw.monto_comprometido_anual IS 'Monto Comprometido Anual.';
COMMENT ON COLUMN mef_origin.gastos_raw.monto_comprometido IS 'Monto Comprometido Mensual.';
COMMENT ON COLUMN mef_origin.gastos_raw.monto_devengado IS 'Monto Devengado.';
COMMENT ON COLUMN mef_origin.gastos_raw.monto_girado IS 'Monto Girado.';

-- Indices
CREATE INDEX IF NOT EXISTS idx_gastos_raw_anio_mes 
  ON mef_origin.gastos_raw (ano_eje, mes_eje);

CREATE INDEX IF NOT EXISTS idx_gastos_raw_ejecutora 
  ON mef_origin.gastos_raw (ejecutora);

CREATE INDEX IF NOT EXISTS idx_gastos_raw_sector 
  ON mef_origin.gastos_raw (sector);

CREATE INDEX IF NOT EXISTS idx_gastos_raw_funcion 
  ON mef_origin.gastos_raw (funcion);

CREATE INDEX IF NOT EXISTS idx_gastos_raw_clasif 
  ON mef_origin.gastos_raw (categoria_gasto, generica, subgenerica, especifica);

ALTER TABLE mef_origin.gastos_raw
  ALTER COLUMN tipo_act_proy_nombre TYPE TEXT;


