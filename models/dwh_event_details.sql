-- dwh_event_details.sql

{{ config(materialized='table') }}

with event_details as (
    SELECT 	
        e.id                    as event_id
        -- nuttige detail velden over de events
        , e.name	                as event_name
        , e.subtitle             as event_subtitel
        , DATE(e.startts)	    as event_start
        , e.startts              as event_start_ts
        , DATE(e.salestartts)   as event_salesstart
        , e.salestartts         as event_salesstart_ts
        , DATE(e.lastupdatets)  as event_lastupdate
        , e.lastupdatets        as event_lastupdate_ts
    
        -- links naar andere tabellen die nuttig zijn om mee te nemen
    --  , e.currentstatus       as event_status_id
        , sta.name              as event_status
    --  , e.typeid              as event_type_id
        , 'Evenement'           as event_type
    --  , e.locationid          as event_location_id
        , el.name	            as event_location
    --  , e.ticketfeeid         as ticketfee_id
        , tf.name               as ticketfee             -- niet actief voor huidige voorstelling, wel verleden
    --  , e.seatingplanid       as seatingplan_id
        , sp.name               as seatingplan
    --  , e.productionid	    as production_id
        , pr.caption            as production            -- RW: tm.production bestaat niet, vervangen door join op customfieldvalues

    FROM      public.events e
    LEFT JOIN public.systemtype sta         on sta.id = e.currentstatus
    LEFT JOIN public.customfieldvalues pr   on pr.id = e.productionid
    LEFT JOIN public.eventlocations el      on el.id = e.locationid
    LEFT JOIN public.ticketfee tf           on tf.id = e.ticketfeeid
    LEFT JOIN public.ticketlayout tl        on tl.id = e.ticketlayoutid
    LEFT JOIN public.seatingplan sp         on sp.id = e.seatingplanid
    LEFT JOIN public.pricelist pl           on pl.id = e.seatingplanpricelistid
    WHERE 
        e.startts		>= '2022-08-01' 
        and e.startts	<= '2026-12-31' -- geen filter binnen dit seizoen/jaar anders mis je tickets in de verkooptotalen
        and e.name		not like 'Borrelarrangement%' 
        and e.name 		not like 'Theaterdiner%' 
        and e.name		not like 'Ik wil er wel wat bij...-arrangement%' 
        and e.name		not like 'Consumptiebon%'
        
    ORDER BY 
        e.startts
      , e.name
)

select * from event_details