-- tickets.SQL

{{ config(
    materialized = 'table',
    indexes = [
      {'columns': ['tickettypeid']},
      {'columns': ['orderid']},
    ]
) }}

with tickets as (
    select distinct on (id) *
    from (

        select id, orderid, currentstatus, tickettypeid, tickettypepriceid, price, servicecharge, locktypeid, bundleid, aboparentid, vouchercodeid, seatdescriptionnl, seatdescriptionnl, seatdescriptionfr, seatdescriptionde, seatrownumber, seatseatnumber, seatpriority, ticketholderid, ticketholdername, properties, lastupdatets
        from tickets_updated

        union

        select id, orderid, currentstatus, tickettypeid, tickettypepriceid, price, servicecharge, locktypeid, bundleid, aboparentid, vouchercodeid, seatdescriptionnl, seatdescriptionnl, seatdescriptionfr, seatdescriptionde, seatrownumber, seatseatnumber, seatpriority, ticketholderid, ticketholdername, properties, lastupdatets
        from tickets_not_updated

    ) as combined_tickets
    order by id, lastupdatets desc nulls last
)

select * from tickets