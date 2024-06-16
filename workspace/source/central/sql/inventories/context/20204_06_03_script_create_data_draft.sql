drop index if exists checktables_item_idx;
drop index if exists  consignments_item_idx;

CREATE INDEX checktables_item_idx ON ods.checktables
USING btree (docnumber, docdate, series, tovarcode1c);

CREATE INDEX consignments_item_idx ON stg_dwh.consignments
USING btree (docnumber, docdate, series, code1c);

drop table if exists core.sa_purchase_series;

create table core.sa_purchase_series
(
	DocNumber varchar(10) ,
	DocDate timestamp,
	Series varchar(30) ,
	ItemCode1c varchar(30),
	Purchase numeric(12, 2)
);

truncate core.sa_purchase_series ;

insert into core.sa_purchase_series
(
select
c.docnumber,
c.docdate,
c.series,
c.code1c ,
avg(ct.purchaseprice)
from
ods.checktables ct
inner join
stg_dwh.consignments c
on
	c.docnumber = ct.docnumber
	and c.docdate = ct.docdate
	and c.series = ct.series
	and c.code1c = ct.tovarcode1c
--where
--	c.docdate = '01.05.2024'
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



