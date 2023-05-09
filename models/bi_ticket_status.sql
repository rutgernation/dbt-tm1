-- bi_ticket_status.sql

{{ config(materialized='table') }}

with ticket_status as (

    -- TICKET_STATUS: Query om ticket, order en verkoopstatus te bepalen 
    SELECT DISTINCT
        e.id                                 event_id

        ,t.id                                ticket_id
    --  ,t.currentstatus                     ticket_status_code
        ,CASE 
            WHEN t.currentstatus = 101 AND t.price = 0                    THEN 'Gratis'
            WHEN t.currentstatus = 101 AND o.paymentstatus = 0            THEN 'Gereserveerd'
            WHEN t.currentstatus = 101 AND o.paymentstatus IN (1,2)       THEN 'Verkocht'
            WHEN t.currentstatus = 104                                    THEN 'Locked'
            WHEN t.currentstatus IS NULL                                  THEN 'Beschikbaar'
                                                                        ELSE 'Onbekend' --NULL
            END AS ticket_status
        ,CASE 
            WHEN t.currentstatus = 101 AND t.price = 0                    THEN 1
            WHEN t.currentstatus = 101 AND o.paymentstatus = 0            THEN 3
            WHEN t.currentstatus = 101 AND o.paymentstatus IN (1,2)       THEN 2
            WHEN t.currentstatus = 104                                    THEN 9
            WHEN t.currentstatus IS NULL                                  THEN 4
                                                                        ELSE 8 --NULL
            END AS ticket_status_sort
        ,t.seatdescriptionnl                 ticket_seatdescription
    --  ,tt.namenl                           ticket_type                  -- meestal leeg, soms 'Ticket' of 'DOMUSDELA'
        ,pt.namenl                           ticket_pricetype
        ,lt.name                             ticket_locktype
        ,t.price                             ticket_price
        ,DATE(t.deliveredts)                 ticket_delivered
        ,t.deliveredts                       ticket_delivered_ts
    --  ,t.orderid                           ticket_orderid               -- irrelevant naast o.orderid
    --  ,t.orderprice                        ticket_orderrpice            -- irrelevant naast t.price (want overal gelijke waardes)
        
        -- Order 
        ,o.orderid                           order_id
        ,o.code                              order_code
    --  ,o.status                            order_status_code            -- Zijn slechts 3 mogelijke waardes
        ,CASE 
            WHEN o.status = 21001            THEN 'Onbevestigd'
            WHEN o.status = 21002            THEN 'Bevestigd'
            WHEN o.status = 21003            THEN 'Archief'
                                            ELSE NULL
            END AS order_status
        
    --  ,o.paymentstatus                     order_paymentstatus          -- Zijn slechts 3 mogelijke waardes
        ,CASE 
            WHEN o.paymentstatus = 1         THEN 'Volledig'
            WHEN o.paymentstatus = 0         THEN 'Niet/Deels'
            WHEN o.paymentstatus = 2         THEN 'Teveel'
                                            ELSE NULL
            END AS order_status_payment
        
    --  ,o.deliverystatus                    order_deliverystatus
        ,st.name	                         order_status_delivery
        ,DATE(o.createdts)                   order_created
        ,o.createdts                         order_created_ts
        ,DATE(o.lastupdatets)                order_latestupdate
        ,o.lastupdatets                      order_latestupdate_ts

    FROM 
        {{ ref('_bi_events_filtered') }} e
    LEFT JOIN public.tickettypes tt          on tt.eventid = e.id
    LEFT JOIN public.tickets t               on t.tickettypeid = tt.id
    LEFT JOIN public.orders o                on t.orderid = o.orderid 
    LEFT JOIN public.systemtype st           on o.deliverystatus = st.id
    LEFT JOIN public.orders_payments p       on p.orderid = o.orderid
    LEFT JOIN public.tickettypeprices ttp    on ttp.id = t.tickettypepriceid
    LEFT JOIN public.pricetypes pt           on ttp.pricetypeid = pt.id
    LEFT JOIN public.locktype lt             on lt.id = t.locktypeid
    WHERE  	
        t.id > 0    

)

select * from ticket_status