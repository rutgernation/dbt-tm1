-- bi_event_genres.sql

{{ config(materialized='table') }}

-- QUERY voor DETAILTABEL met 1 rij per event en 3 genres (1:1 relatie)
SELECT DISTINCT
event_id

-- Expliciet de genre array index als losse velden 
, g1.caption                as event_genre_1
, g2.caption                as event_genre_2
, g3.caption                as event_genre_3

-- Analyse 
, event_genre_array
, event_genre_isleeg
, event_genre_meerdere
, event_vervallen
, event_verhuring

-- Genre codes vanuit de tm.customfieldvalues ()
-- , event_genre_1_id            as event_genre_1_id
-- , event_genre_2_id            as event_genre_2_id
-- , event_genre_3_id            as event_genre_3_id

FROM {{ ref('_bi_event_genres_0') }} AS genres

LEFT JOIN public.customfieldvalues g1  on g1.id  = genres.event_genre_1_id
LEFT JOIN public.customfieldvalues g2  on g2.id  = genres.event_genre_2_id
LEFT JOIN public.customfieldvalues g3  on g3.id  = genres.event_genre_3_id

ORDER BY
    event_id

