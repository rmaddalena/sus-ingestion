CREATE OR REFRESH MATERIALIZED VIEW leads_from_sus
AS
SELECT 
  curated_sus_establishments.cnes,
  SUBSTR(curated_sus_establishments.cod_cep, 1,5)||'-'||RIGHT(curated_sus_establishments.cod_cep,3) AS cep,
  curated_sus_establishments.dt_atual AS data_atualizacao,
  curated_sus_establishments.treated_cnpj AS cpf_ou_cnpj,
  stg_establishment_details.fantasia AS nome_fantasia,
  stg_establishment_details.raz_soci AS razao_social,
  curated_sus_establishments.total_instalacoes,
  curated_sus_establishments.total_leitos,
  stg_establishment_type.descricao AS tipo_estabelecimento,
  stg_establishment_juridical_nature.descricao AS natureza_juridica,
  stg_establishment_city.descricao AS cidade

FROM curated_sus_establishments
  LEFT JOIN stg_establishment_type
    ON curated_sus_establishments.tp_unid = stg_establishment_type.codigo
  LEFT JOIN stg_establishment_juridical_nature
    ON curated_sus_establishments.nat_jur = stg_establishment_juridical_nature.codigo
  LEFT JOIN stg_establishment_city
    ON curated_sus_establishments.codufmun = stg_establishment_city.codigo
  LEFT JOIN stg_establishment_details
    ON curated_sus_establishments.cnes = stg_establishment_details.cnes