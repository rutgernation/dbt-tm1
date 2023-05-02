-- _bi_event_finance_1.sql

{{ config(materialized='table') }}

with event_finance_1 as (

    -- EVENT OPBRENGST EN KOSTEN
    SELECT 
        event_id
        , event_naam
        , event_datum
        , event_typeborderel
    
        -- Totalen
        , SUM(level0.ttot_aantal_max)                                  as ttot_aantal_max
        , SUM(level0.ttot_aantal)                                      as ttot_aantal
        , SUM(level0.ttot_aantal_pt_gratis)                            as ttot_aantal_pt_gratis
        , SUM(level0.ttot_aantal_pt_reductie)                          as ttot_aantal_pt_reductie
        , SUM(level0.ttot_aantal_toeslag)                              as ttot_aantal_toeslag
        , SUM(level0.ttot_aantal_verkocht)                             as ttot_aantal_verkocht
        , SUM(level0.ttot_ticketprijs_verkocht)                        as ttot_ticketprijs_verkocht
        , SUM(level0.ttot_aantal_nietbetaald)                          as ttot_aantal_nietbetaald
        , SUM(level0.ttot_bedrag_nietbetaald)                          as ttot_bedrag_nietbetaald
    
        -- Toeslagen
        , c_consumptietoeslag
        , SUM(level0.ttot_aantal_toeslag * c_consumptietoeslag)        as toeslag_consumptie
        , c_administratiekosten
        , SUM(level0.ttot_aantal_toeslag * c_administratiekosten)      as toeslag_administratiekosten
        , c_theatertoeslag
        , SUM(level0.ttot_aantal_toeslag * c_theatertoeslag)           as toeslag_theater
        , SUM(level0.ttot_aantal_toeslag * c_theatertoeslag) 
          + SUM(level0.ttot_aantal_toeslag * c_consumptietoeslag) 
          + SUM(level0.ttot_aantal_toeslag * c_administratiekosten)    as toeslag_totaal
    
        -- Recette
        , event_btw
        
        -- Auteursrechten
        , c_auteursrechten
        
        -- Partage
        , c_partage
        , c_garantiebedrag
        , c_uitkoopsom
    
    FROM {{ ref('_bi_event_finance_0') }} level0

    WHERE
        1 = 1
    --  and level0.ttot_aantal_verkocht > 100
    GROUP BY
        level0.event_id
        , level0.event_naam
        , level0.event_datum
        
        , level0.event_typeborderel
        , level0.c_consumptietoeslag
        , level0.c_administratiekosten
        , level0.event_btw
    --  , level0.c_garantie
        , level0.c_garantiebedrag
    --  , level0.c_uitkoop
        , level0.c_uitkoopsom
        , level0.c_auteursrechten
        , level0.c_partage
        , level0.c_theatertoeslag
    --  , level0.c_impresariaat
    --  , level0.c_prognoseaantal
    ORDER BY
        level0.event_datum

)

select * from event_finance_1
