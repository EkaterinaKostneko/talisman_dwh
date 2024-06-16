with a as
(
select
sd.docdate,
sum(sd.quantity) db_quantity,
sum(sd.revenue) db_revenue
from
dds.sales_db sd
group by
	sd.docdate
	)
,
b as (
select
docdate,
sum(mtrbs.intquantity) tr_quantity,
sum(turnover) tr_turnover
from
marts.mart_total_results_by_store mtrbs
group by
	docdate
	)
select
a.docdate,
db_revenue,
db_quantity,
tr_turnover,
tr_quantity,
db_revenue-tr_turnover dif
from
a join b
on a.docdate = b.docdate
where
a.docdate between '01-05-2024' and '31-05-2024'
and (db_revenue-tr_turnover)/tr_turnover >0.15
order by a.docdate asc


select
docdate,
sum(ci.checksum_selling)
from
stg_dwh.checkheaders_inc ci
where
docdate between '01-05-2024' and '31-05-2024'
group by
docdate

with a as
(
select
sd.docdate,
--iddoc,
ms."МестоХран" ,
--sum(sd.quantity) db_quantity ,
sum(sd.revenue)
	db_revenue
--sum(sd.purchase) db_purchase
from
dds.sales_db sd
join
marts.mart_sprav ms
on sd.idstore = ms."ID_Контрагенты"
--where
--	sd.docdate between '01-05-2024' and '07-05-2024'
group by
	sd.docdate
--	,iddoc
	,ms."МестоХран"
	)
,
b as (
select
docdate,
mtrbs.pharmacycode  ,
--sum(mtrbs.intquantity) quanity ,
--sum(checksum_purchase) purchase,
--sum(turnover) tr_turnover
sum(totalsum) tr_totalsum
from
marts.mart_total_results_by_store mtrbs
--where
--	docdate between '01-05-2024' and '07-05-2024'
group by
	docdate
--	,iddoc,
	,mtrbs.pharmacycode
	)
select
*
from
a join b
on a.docdate = b.docdate
and "МестоХран" = b.pharmacycode
where
a.docdate between '07-05-2024' and '07-05-2024'


with a as
(
select
docdate,
pharmacycode,
sum(checksum_selling) as ch_totalsum
from
ods.checkheaders c
group by
	c.docdate
--	,iddoc
	,c.pharmacycode
	)
,
b as (
select
docdate,
mtrbs.pharmacycode  ,
sum(totalsum) tr_totalsum
from
marts.mart_total_results_by_store mtrbs
group by
	docdate
	,mtrbs.pharmacycode
	)
select
*
from
a join b
on a.docdate = b.docdate
and a.pharmacycode = b.pharmacycode
where
a.docdate between '07-05-2024' and '07-05-2024'


select *
from
marts.mart_total_results_by_store mtrbs
where docdate = '04-05-2024'