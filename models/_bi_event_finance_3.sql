-- _bi_event_finance_3.sql

{{ config(materialized='table') }}

with event_finance_3 as (

    SELECT
        *    
        -- Auteursrechten
        , ROUND((recette_excl_btw / (1 + c_auteursrechten))::numeric, 2)                            as recette_netto
        , ROUND((recette_excl_btw / (1 + c_auteursrechten) * c_auteursrechten)::numeric, 2)         as auteursrechten_recette
        , ROUND((c_uitkoopsom * c_auteursrechten)::numeric, 2)                                      as auteursrechten_uitkoop
        , ROUND((c_garantiebedrag * c_auteursrechten)::numeric, 2)                                  as auteursrechten_garantie
        
        -- Auteursrechten voor gezelschap
        -- TODO: wat is verschil tussen event_typeborderel = 'Partage boven garantie' en 'Partage normaal'
        , CASE 
            -- Scenario 1: met uitkoopsom en AR over recette is lager dan AR over uitkoopsom: dan AR over uitkoopsom
            WHEN c_uitkoopsom IS NOT NULL AND ROUND((recette_excl_btw / (1 + c_auteursrechten) * c_auteursrechten)::numeric, 2) < ROUND((c_uitkoopsom * c_auteursrechten)::numeric, 2)
                THEN ROUND((c_uitkoopsom * c_auteursrechten)::numeric, 2)
            -- Scenario 2: AR over recette is lager dan AR over garantie: dan AR over garantie
            WHEN ROUND((recette_excl_btw / (1 + c_auteursrechten) * c_auteursrechten)::numeric, 2) < ROUND((c_garantiebedrag * c_auteursrechten)::numeric, 2)
                THEN ROUND((c_garantiebedrag * c_auteursrechten)::numeric, 2)
            ELSE ROUND((recette_excl_btw / (1 + c_auteursrechten) * c_auteursrechten)::numeric, 2)
        END                                                                                                       as auteursrechten_gezelschap
        
        -- Partage
        , ROUND((recette_excl_btw / (1 + c_auteursrechten) * c_partage)::numeric, 2)                              as partage_recette
        , CASE 
            WHEN c_uitkoopsom IS NOT NULL
                THEN c_uitkoopsom
            WHEN ROUND((recette_excl_btw / (1 + c_auteursrechten) * c_partage)::numeric, 2) < c_garantiebedrag
                THEN c_garantiebedrag
            ELSE ROUND((recette_excl_btw / (1 + c_auteursrechten) * c_partage)::numeric, 2)
        END                                                                                                       as partage_gezelschap

    FROM {{ ref('_bi_event_finance_2') }}

)

select * from event_finance_3
