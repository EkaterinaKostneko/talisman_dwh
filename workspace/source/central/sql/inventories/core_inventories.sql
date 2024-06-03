truncate core.sa_purchase_series ;

insert into core.sa_purchase_series
(
select
c.docnumber,
c.docdate,
c.series,
c.code1c
sum(ct.purchaseprice)
from
ods.checktables ct
inner join
stg_dwh.consignments c
on
	c.docnumber = ct.docnumber
	and c.docdate = ct.docdate
	and c.series = ct.series
	and c.code1c = ct.tovarcode1c
group by
	c.docnumber,
	c.docdate,
	c.series,
	c.code1c
)

;

drop materialized view marts.mv_mart_inventories cascade;

create materialized view marts.mv_mart_inventories
as
SELECT
	PharmCode,
	c.DocDate,
	c.Docnumber,
	Code1C,
	MfrName,
	c.Series,
	ShelfLife,
	Price,
	Quantity,
	VSDQuantity,
	MarkingFlag,
	UpdateDate::date UpdateDate,
    c.shelflife-current_date Days,
    sps.purchase
from stg_dwh.consignments c
left join core.sa_purchase_series sps
on
	c.docnumber = sps.docnumber
	and c.docdate = sps.docdate
	and c.series = sps.series
	and c.code1c = sps.itemcode1c
;

create or replace view marts.mart_inventories
as
select *
from marts.mv_mart_inventories mmi



