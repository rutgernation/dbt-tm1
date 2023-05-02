-- _bi_event_finance_2.sql

{{ config(materialized='table') }}

with event_finance_2 as (

    SELECT
        event_id
        , event_naam
        , event_datum
        , event_typeborderel
        
        -- Ticket Totalen
        , ttot_aantal_max
        , ttot_aantal
        , ttot_aantal_pt_gratis
        , ttot_aantal_pt_reductie
        , ttot_aantal_toeslag
        , ttot_aantal_verkocht
        , ttot_ticketprijs_verkocht
        , ttot_aantal_nietbetaald
        , ttot_bedrag_nietbetaald
        
        -- Toeslagen
        , c_consumptietoeslag
        , toeslag_consumptie
        , c_administratiekosten
        , toeslag_administratiekosten
        , c_theatertoeslag
        , toeslag_theater
        , toeslag_totaal 
        
        -- Recette
        , ttot_ticketprijs_verkocht - toeslag_totaal                                                 				as recette
        , ROUND(cast((ttot_ticketprijs_verkocht - toeslag_totaal) / (1 + event_btw) * event_btw as numeric), 2)     as recette_btw
        , ROUND(cast((ttot_ticketprijs_verkocht - toeslag_totaal) / (1 + event_btw) as numeric), 2)                 as recette_excl_btw
        
        -- Auteursrechten
        , c_auteursrechten
        
        -- Partage
        , c_partage
        , c_garantiebedrag
        , c_uitkoopsom

    FROM {{ ref('_bi_event_finance_1') }}

)

select * from event_finance_2


