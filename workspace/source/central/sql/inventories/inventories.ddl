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


create materialized view marts.mv_mart_inventories
as
SELECT
	PharmCode,
	DocDate,
	Code1C,
	MfrName,
	Series,
	ShelfLife,
	Price,
	Quantity,
	VSDQuantity,
	MarkingFlag,
	UpdateDate::date UpdateDate,
    date_part('day', shelflife) - date_part('day', CURRENT_DATE) Days
from stg_dwh.consignments c

create or replace view marts.mart_inventories
as
select *
from marts.mv_mart_inventories mmi