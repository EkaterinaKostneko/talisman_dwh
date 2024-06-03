with a as
(
select
sd.docdate,
--iddoc,
--idstore,
sum(sd.quantity) db_quantity ,
sum(sd.revenue) db_revenue ,
sum(sd.purchase) db_purchase
from
dds.sales_db sd
group by
	sd.docdate
--	,iddoc
--	,idstore;
	)
,
b as (
select
docdate,
--pharmacycode,
sum(mtrbs.intquantity) quanity ,
sum(checksum_purchase) purchase,
sum(turnover) turnover
--sum(totalsum) totalsum
from
marts.mart_total_results_by_store mtrbs
group by
	docdate
--	,iddoc,
--	,pharmacycode
	)
select
*
from
a join b
on a.docdate = b.docdate
where
a.docdate between '01-05-2024' and '08-05-2024'