-- _bi_event_finance_0.sql

{{ config(materialized='table') }}

with event_finance_0 as (

SELECT 
        e.id                                         as event_id
        , e.name                                     as event_naam
        , DATE(e.startts)                            as event_datum
    
        -- Finance velden
        , cbor.caption                               as event_typeborderel
--      , e.c_consumptietoeslag                      as c_consumptietoeslag
        , CASE WHEN e.c_consumptietoeslag IS NULL THEN 0
          ELSE e.c_consumptietoeslag END             as c_consumptietoeslag
--      , e.c_administratiekosten                    as c_administratiekosten
        , CASE WHEN e.c_administratiekosten IS NULL THEN 0
          ELSE e.c_administratiekosten END           as c_administratiekosten
--      , e.c_theatertoeslag                         as c_theatertoeslag
        , CASE WHEN e.c_theatertoeslag IS NULL THEN 0 
          ELSE e.c_theatertoeslag END                as c_theatertoeslag
--      , e.c_auteursrechten                         as c_auteursrechten
        , CASE WHEN e.c_auteursrechten IS NULL THEN 0 
          ELSE (e.c_auteursrechten/100) END          as c_auteursrechten
--      , e.c_partage                                as c_partage
        , CASE WHEN e.c_partage IS NULL THEN 0
          ELSE (e.c_partage/100) END                 as c_partage
        
        , e.c_garantiebedrag                         as c_garantiebedrag
        , e.c_uitkoopsom                             as c_uitkoopsom
--      , cbtw.caption                               as event_btw
        , CASE WHEN cbtw.caption IS NULL  THEN 0.00
               WHEN cbtw.caption = '6%'   THEN 0.06
               WHEN cbtw.caption = '9%'   THEN 0.09
               WHEN cbtw.caption = '21%'  THEN 0.21
          ELSE 0.00 END                              as event_btw
    
        -- Ticket aantallen
        , COUNT(t.id)                                                                                            as ttot_aantal_max
        , COUNT(CASE WHEN t.currentstatus = 101 THEN t.id END)                                                   as ttot_aantal
        , COUNT(CASE WHEN pt.typeid = 2301 THEN t.id END)                                                        as ttot_aantal_pt_normaal
        , COUNT(CASE WHEN pt.typeid = 2302 THEN t.id END)                                                        as ttot_aantal_pt_reductie
        , COUNT(CASE WHEN pt.typeid = 2304 THEN t.id END)                                                        as ttot_aantal_pt_gratis
        , COUNT(CASE WHEN pt.typeid IN (2301,2302) THEN t.id END)                                                as ttot_aantal_toeslag
        , COUNT(CASE WHEN t.currentstatus = 101 AND t.price > 0 /*AND o.paymentstatus IN (1,2)*/ THEN 1 END)     as ttot_aantal_verkocht
        , SUM(CASE WHEN t.currentstatus = 101 AND t.price > 0 /*AND o.paymentstatus IN (1,2)*/ THEN t.price END) as ttot_ticketprijs_verkocht
        , COUNT(CASE WHEN t.currentstatus = 101 AND t.price > 0 AND o.paymentstatus NOT IN (1,2) THEN 1 END)     as ttot_aantal_nietbetaald
        , SUM(CASE WHEN t.currentstatus = 101 AND t.price > 0 AND o.paymentstatus NOT IN (1,2) THEN t.price END) as ttot_bedrag_nietbetaald
    FROM 
    	{{ ref('_bi_events_filtered') }} e
    	LEFT JOIN tickettypes tt           on tt.eventid = e.id
        LEFT JOIN {{ ref('tickets') }} t   on t.tickettypeid = tt.id
        LEFT JOIN orders o                 on t.orderid = o.orderid
        LEFT JOIN tickettypeprices ttp     on ttp.id = t.tickettypepriceid
        LEFT JOIN pricetypes pt            on ttp.pricetypeid = pt.id
        LEFT JOIN customfieldvalues cbor   on cbor.id = e.c_typeborderel
        LEFT JOIN customfieldvalues cbtw   on cbtw.id = e.c_btw
    WHERE
        t.id > 0
    GROUP BY 
    	e.id
        , e.name
        , e.startts
        , t.currentstatus
        , o.paymentstatus
        , t.price
        , e.c_consumptietoeslag
        , e.c_garantiebedrag
        , e.c_uitkoopsom
        , e.c_administratiekosten
        , e.c_theatertoeslag
        , e.c_auteursrechten
        , e.c_partage
        , cbor.caption
        , cbtw.caption

)

select * from event_finance_0