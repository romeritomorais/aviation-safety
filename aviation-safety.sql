-- Databricks notebook source
-- MAGIC %md
-- MAGIC # aviation-safety database (1919_2019)  by Romerito Morais
-- MAGIC https://www.kaggle.com/datasets/donat1/aviationsafety-database-1919-2019?resource=download

-- COMMAND ----------

CREATE
OR REPLACE VIEW view_aviation AS
SELECT
  *
FROM
  JSON.`/mnt/aviation`

-- COMMAND ----------

CREATE
OR REPLACE VIEW view_aviation_details AS
SELECT
  json_table.*
FROM
  view_aviation

-- COMMAND ----------

CREATE
OR REPLACE VIEW view_aviation_cleaned AS WITH trata_coluna_crew AS (
  SELECT
    split(`Crew:`, '[/]') AS equipe,
    split(`Passengers:`, '[/]') AS passageiros,
    split(`Total:`, '[/]') AS total,
    *
  FROM
    view_aviation_details
),
separa_campos AS (
  SELECT
    translate(equipe [1], 'Occupants:', '') AS equipe_ocupantes,
    translate(equipe [0], 'Fatalities:', '') AS equipe_fatalidades,
    translate(passageiros [1], 'Occupants:', '') AS passageiros_ocupantes,
    translate(passageiros [0], 'Fatalities:', '') AS passageiros_fatalidades,
    translate(total [1], 'Occupants:', '') AS total_ocupantes,
    translate(total [0], 'Fatalities:', '') AS total_fatalidades,
    *
  FROM
    trata_coluna_crew
),
renomeia_colunas AS (
  select
    equipe_ocupantes,
    equipe_fatalidades,
    passageiros_ocupantes,
    passageiros_fatalidades,
    total_ocupantes,
    total_fatalidades,
    `Aircraft damage:` AS danos_na_aeronave,
    `Aircraft fate:` AS destino_aeronave,
    `C/n / msn:` AS cn_msn,
    `Collision casualties:` AS vitimas_da_colisao,
    `Crash site elevation:` AS elevacao_do_local_do_acidente,
    `Cycles:` AS ciclos,
    `Date:` AS data_acidente,
    `Departure airport:` AS aeroporto_de_partida,
    `Destination airport:` AS aeroporto_de_destino,
    `Engines:` AS fabricante_do_motor,
    `First flight:` AS primeiro_voo,
    `Flightnumber:` AS numero_do_voo,
    `Leased from:` AS alugado_de,
    `Location:` AS localizacao,
    `Nature:` AS tipo_aeronave,
    `On behalf of:` AS en_nome_de,
    `Operated by:` AS operado_por,
    `Operating for:` AS operando_para,
    `Operator:` AS operador,
    `Phase:` AS estagio,
    `Registration:` AS registro,
    `Status:` AS situacao,
    `Total airframe hrs:` AS horas_totais_da_fuselagem,
    `Type:` AS tipo
  from
    separa_campos
)
select
  *
from
  renomeia_colunas

-- COMMAND ----------

CREATE
OR REPLACE VIEW view_aviation_separa_data AS WITH trata_coluna_data_acidente AS (
  SELECT
    split(data_acidente, '[ ]') AS splitter,
    *
  FROM
    view_aviation_cleaned
),
separa_campos AS (
  select
    splitter [0] AS dia_da_semana,
    splitter [1] AS dia,
    splitter [2] AS mes,
    splitter [3] AS ano,
    *
  from
    trata_coluna_data_acidente
)
SELECT
  *
EXCEPT(
    splitter,
    elevacao_do_local_do_acidente,
    data_acidente,
    en_nome_de,
    operando_para,
    operado_por,
    horas_totais_da_fuselagem,
    alugado_de,
    vitimas_da_colisao,
    cn_msn,
    situacao
  )
FROM
  separa_campos
WHERE
  aeroporto_de_partida not in ('?', '-')

-- COMMAND ----------

CREATE
OR REPLACE VIEW view_aviation_campos_ordenados AS
select
  trim(substring(primeiro_voo, 1, 5)) AS primeiro_voo,
  tipo_aeronave,
  fabricante_do_motor,
  operador,
  tipo,
  aeroporto_de_partida,
  aeroporto_de_destino,
  dia AS dia_acidente,
  mes AS mes_acidente,
  ano AS ano_acidente,
  dia_da_semana AS dia_semana_acidente,
  equipe_ocupantes AS tripulantes_total,
  equipe_fatalidades AS tripulantes_mortos,
  passageiros_ocupantes AS passageiros_total,
  passageiros_fatalidades AS passageiros_mortos,
  total_fatalidades
from
  view_aviation_separa_data;

-- COMMAND ----------

select
  ano_acidente,
  count(*) AS quantidade_de_acidentes
from
  view_aviation_campos_ordenados
Group by
  ano_acidente
