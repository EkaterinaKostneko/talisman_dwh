select
docdate,
sum(totalsum)
from
marts.mart_total_results_by_store mtrbs
--ods.checkheaders
where
docdate between '01-05-2024' and '03-05-2024'
group by docdate

select
docdate,
sum(sd.revenue)
from
dds.sales_db sd
where
docdate between '01-05-2024' and '03-05-2024'
group by docdate
