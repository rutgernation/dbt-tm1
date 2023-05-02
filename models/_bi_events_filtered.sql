-- _bi_events_filtered.sql

{{ config(materialized='table') }}

with events_filtered as (
    
    SELECT *
    FROM public.events e
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

select * from events_filtered