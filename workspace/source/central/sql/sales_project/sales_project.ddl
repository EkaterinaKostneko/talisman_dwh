DROP TABLE IF EXISTS stg_dwh.Consignments ;

CREATE TABLE stg_dwh.Consignments  (
	ID bigint ,
	DateTime timestamp,
	ProductID integer,
	PharmCode varchar(10) ,
	Code varchar(20) ,
	Code1C varchar(30) ,
	DocID varchar(15) ,
	DocType varchar(20) ,
	DocNumber varchar(10) ,
	DocDate timestamp,
	MfrCode integer,
	MfrName varchar(80) ,
	Series varchar(30) ,
	ShelfLife date,
	Price decimal(12,2),
	Quantity decimal(12,4),
	Fasovka integer,
	StorageQty integer,
	StoragePlace varchar(50) ,
	MarkingFlag integer,
	UpdateDate timestamp,
	VSDQuantity decimal(12,4),
	Name varchar(80) ,
	Reserve decimal(12,4)
);


create table core.sa_purchase_series
(
	DocNumber varchar(10) ,
	DocDate timestamp,
	Series varchar(30) ,
	Purchase numeric(12, 2)
);


drop materialized view marts.mv_mart_inventories cascade;

create materialized view marts.mv_mart_inventories
as
SELECT
	PharmCode,
	c.DocDate,
	Code1C,
	MfrName,
	c.Series,
	ShelfLife,
	Price,
	Quantity,
	VSDQuantity,
	MarkingFlag,
	UpdateDate::date UpdateDate,
    EXTRACT(days FROM AGE(shelflife, CURRENT_DATE)) Days,
    sps.purchase
from stg_dwh.consignments c
left join core.sa_purchase_series sps
on
	c.docnumber = sps.docnumber
	and c.docdate = sps.docdate
	and c.series = sps.series


create or replace view marts.mart_inventories
as
select *
from marts.mv_mart_inventories mmi