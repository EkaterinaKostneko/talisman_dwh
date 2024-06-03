with a as
(
select
sd.docdate,
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
sum(turnover) tr_turnover
from
marts.mart_total_results_by_store mtrbs
group by
	docdate
	)
select
a.docdate,
db_revenue,
tr_turnover,
db_revenue-tr_turnover dif
from
a join b
on a.docdate = b.docdate
where
a.docdate between '01-05-2024' and '31-05-2024'
and (db_revenue-tr_turnover)/tr_turnover >0.15
order by a.docdate asc