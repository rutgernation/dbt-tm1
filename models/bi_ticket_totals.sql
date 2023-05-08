-- bi_ticket_totals.sql

{{ config(materialized='table') }}

with ticket_totals as (

    -- TICKET TOTALEN (groupby: event datum)
    SELECT 
    --  ttemp.event_id                                   as event_id
    --  , ttemp.event_naam                               as event_naam
        ttemp.event_datum	                             as event_datum
        -- Totalen
        , SUM(ttemp.ttot_aantal)                         as ttot_aantal
        , SUM(ttemp.ttot_aantal_gratis)                  as ttot_aantal_gratis
        , SUM(ttemp.ttot_aantal_gereserveerd)            as ttot_aantal_gereserveerd
        , SUM(ttemp.ttot_aantal_verkocht)                as ttot_aantal_verkocht
        , SUM(ttemp.ttot_aantal_locked)                  as ttot_aantal_locked
        , SUM(ttemp.ttot_aantal_beschikbaar)             as ttot_aantal_beschikbaar
        , SUM(ttemp.ttot_bedrag_omzet)                   as ttot_bedrag_omzet
        , SUM(ttemp.ttot_bedrag_overig)                  as ttot_bedrag_overig
        -- Percentages
        , ROUND(SUM(ttemp.ttot_aantal_verkocht)/SUM(ttemp.ttot_aantal)*100, 1)      as ttot_perc_verkocht 
        , ROUND(SUM(ttemp.ttot_aantal_beschikbaar)/SUM(ttemp.ttot_aantal)*100, 1)   as ttot_perc_beschikbaar
        , ROUND(SUM(ttemp.ttot_aantal_locked)/SUM(ttemp.ttot_aantal)*100, 1)        as ttot_perc_locked
        , ROUND(SUM(ttemp.ttot_aantal_gratis)/SUM(ttemp.ttot_aantal)*100, 1)        as ttot_perc_gratis
        
    FROM
    (
        SELECT 
            e.id                                         as event_id
    --      , e.namenl                                   as event_naam
            , DATE(e.startts)                            as event_datum
            -- Ticket aantallen
            , COUNT(t.id)                                                                           as ttot_aantal
            , COUNT(CASE WHEN t.currentstatus = 101 AND t.price = 0 THEN 1 END)                     as ttot_aantal_gratis
            , COUNT(CASE WHEN t.currentstatus = 101 AND o.paymentstatus = 0 THEN 1 END)             as ttot_aantal_gereserveerd
            , COUNT(CASE WHEN t.currentstatus = 101 AND o.paymentstatus IN (1,2) THEN 1 END)        as ttot_aantal_verkocht
            , COUNT(CASE WHEN t.currentstatus = 104 THEN 1 END)                                     as ttot_aantal_locked
            , COUNT(CASE WHEN t.currentstatus IS NULL THEN 1 END)                                   as ttot_aantal_beschikbaar
            -- Omzet gerelateerd
            , SUM(CASE WHEN o.status = 21002 AND o.paymentstatus <> 0 THEN t.price END)             as ttot_bedrag_omzet
            , SUM(CASE WHEN o.status <> 21002 OR o.paymentstatus = 0 THEN t.price END)              as ttot_bedrag_overig
        FROM 
            {{ ref('_bi_events_filtered') }} e
            LEFT JOIN public.tickettypes tt          on tt.eventid = e.id
            LEFT JOIN public.tickets t               on t.tickettypeid = tt.id
            LEFT JOIN public.orders o                on t.orderid = o.orderid
        WHERE
            t.id > 0
        GROUP BY 
            e.id
    --      , e.namenl
            , DATE(e.startts)
            , t.currentstatus
            , o.paymentstatus
            , t.price
    ) ttemp

    WHERE
        1 = 1
    --  and ttemp.ttot_aantal_verkocht > 100
    GROUP BY
        ttemp.event_datum
    ORDER BY
        ttemp.event_datum

)

select * from ticket_totals