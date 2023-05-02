-- bi_event_finance.sql

{{ config(materialized='table') }}

with bi_event_finance as (

    SELECT
    *
    , ROUND((partage_gezelschap * 0.09)::numeric, 2)                      as partage_gezelschap_btw
    , ROUND((auteursrechten_gezelschap * 0.21)::numeric, 2)               as auteursrechten_gezelschap_btw
    , ROUND(((partage_gezelschap * 1.09)
        + auteursrechten_gezelschap
        + (auteursrechten_gezelschap * 0.21))::numeric, 2)                as betaling_aan_gezelschap
    
    , CASE 
          WHEN partage_gezelschap = c_uitkoopsom
              THEN 'uitkoop'
          WHEN partage_gezelschap = c_garantiebedrag
              THEN 'garantie'
          WHEN partage_gezelschap = 0
              THEN 'geen (0)'
          WHEN partage_gezelschap = partage_recette
              THEN 'recette'
          ELSE 'leeg/onbekend'
      END                                                                 as partage_bron

    , CASE 
          WHEN auteursrechten_gezelschap = auteursrechten_uitkoop
              THEN 'uitkoop'
          WHEN auteursrechten_gezelschap = auteursrechten_garantie
              THEN 'garantie'
          WHEN auteursrechten_gezelschap = 0
              THEN 'geen (0)'
          WHEN auteursrechten_gezelschap = auteursrechten_recette
              THEN 'recette'
          ELSE 'leeg/onbekend'
      END          

    FROM {{ ref('_bi_event_finance_3') }}

)

select * from bi_event_finance


