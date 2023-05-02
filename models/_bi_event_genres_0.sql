-- _bi_event_genres_0.sql

{{ config(materialized='table') }}

with _bi_event_genres_0 as (
    SELECT DISTINCT
        e.id                        as event_id
        , e.name                    as event_name
        , e.subtitle                as event_subtitel
        , e.c_genre                 as event_genre_array
    --  , unnest((e.c_genre)        as event_genre_id
	    , CASE jsonb_typeof(e.c_genre)
	           WHEN 'array' 
	           THEN (e.c_genre -> 0)::integer
	           ELSE NULL
	       END                      as event_genre_1_id
        , CASE 
          WHEN (e.c_genre->2)::integer = (e.c_genre->1)::integer THEN NULL
          ELSE (e.c_genre->2)::integer  END as event_genre_2_id
        , CASE 
          WHEN (e.c_genre->3)::integer = (e.c_genre->1)::integer OR (e.c_genre->3)::integer = (e.c_genre->2)::integer THEN NULL
          ELSE (e.c_genre->3)::integer  END as event_genre_3_id
        , CASE 
          WHEN e.c_genre is NULL 
          THEN 1
          ELSE 0  END            as event_genre_isleeg
        , CASE 
          WHEN (e.c_genre->2)::integer is not NULL AND (e.c_genre->2)::integer <> (e.c_genre->1)::integer THEN 1
          ELSE 0  END            as event_genre_meerdere
        
        -- Check hard-coded of een van de eerste 3 genre velden gevuld is met codes voor vervallen en verhuring
        , CASE WHEN (e.c_genre->1)::integer = 14370 OR (e.c_genre->2)::integer = 14370 OR (e.c_genre->3)::integer = 14370 THEN 1 ELSE 0 END as event_vervallen
        , CASE WHEN (e.c_genre->1)::integer = 14363 OR (e.c_genre->2)::integer = 14363 OR (e.c_genre->3)::integer = 14363 THEN 1 ELSE 0 END as event_verhuring
    
    FROM 
		"_bi_events_filtered" e
    WHERE 
    --  and (e.c_genre->2)::integer is not null
    	true
)

select * from _bi_event_genres_0


