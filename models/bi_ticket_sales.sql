-- bi_ticket_sales.sql

{{ config(materialized='table') }}

with ticket_sales as (

    -- Query om één 'verkoopdatum' te bepalen (rekening houdend met meerdere betalingen per order)
    SELECT 
        t.id                                                     as ticket_id
        , e.id                                                   as event_id
        , t.orderid                                              as order_id
        , MIN(e.startts)                                         as event_date
        
        , SUM(p.amount)                                          as payment_amount
        , COUNT(p.id)                                            as payment_count
        , CASE 
            WHEN COUNT(p.id) = 0 THEN 'Geen Betalingen'
            WHEN COUNT(p.id) = 1 THEN 'Één Betaling'
            WHEN COUNT(p.id) > 1 AND (MAX(DATE(p.paidts)) = MIN(DATE(p.paidts)))        THEN 'Meer Betalingen (binnen 1 dag)'
            WHEN COUNT(p.id) > 1 AND (MAX(DATE(p.paidts)) - MIN(DATE(p.paidts)) <= 7)   THEN 'Meer Betalingen (1 dag - 1 week)'
            WHEN COUNT(p.id) > 1 AND (MAX(DATE(p.paidts)) - MIN(DATE(p.paidts)) <= 30)  THEN 'Meer Betalingen (1 week - 1 maand)'
            WHEN COUNT(p.id) > 1 AND (MAX(DATE(p.paidts)) - MIN(DATE(p.paidts)) <= 365) THEN 'Meer Betalingen (1 maand - 1 jaar)'
            WHEN COUNT(p.id) > 1                                                        THEN 'Meer Betalingen (>1 jaar verschil)'
            
            ELSE 'ONBEKEND' END                                  as payment_count_detail
            
        , CASE 
            WHEN COUNT(p.id) = 0                                                   THEN '0 Betalingen'
            WHEN COUNT(p.id) = 1                                                   THEN '1 Verkoopdatum'
            WHEN COUNT(p.id) > 1 AND (MAX(DATE(p.paidts)) = MIN(DATE(p.paidts)))   THEN '1 Verkoopdatum'     -- Meer betalingen op dezelfde dag: telt als 1 verkoop
            WHEN COUNT(p.id) > 1                                                   THEN 'Meer Betalingen'
            ELSE 'ONBEKEND' END                                  as payment_count_label
            
        , DATE(MAX(p.paidts))                                    as payment_date
        , MAX(p.paidts)                                          as payment_lastdate
        , CASE WHEN COUNT(p.id) = 1 THEN NULL
            ELSE MIN(p.paidts) END                               as payment_firstdate
        , CASE WHEN COUNT(p.id) = 1 THEN NULL
            ELSE MAX(DATE(p.paidts)) - MIN(DATE(p.paidts)) END   as payment_delta_firsttolast
        , MIN(DATE(e.startts)) - MAX(DATE(p.paidts))             as payment_delta_lasttoevent
        
    FROM 
        {{ ref('_bi_events_filtered') }} e
        LEFT JOIN public.tickettypes tt    on tt.eventid = e.id
        LEFT JOIN public.tickets t         on t.tickettypeid = tt.id
        LEFT JOIN public.orders o          on t.orderid = o.orderid 
        LEFT JOIN public.orders_payments p on p.orderid = o.orderid
    WHERE
        t.orderid        is not NULL
        and p.paidts     >= '2022-05-01'
    GROUP BY
        t.id
        , e.id
        , t.orderid

)

select * from ticket_sales