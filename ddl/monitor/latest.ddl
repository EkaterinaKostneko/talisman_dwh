drop materialized view if exists marts.latest ;

create materialized  view marts.latest
as
(select 'checktables' 	"table", docdate "date"
from ods.checktables c
order by c.docdate desc limit 1)
union all
(select 'checkheaders' 	"table", docdate "date"
from ods.checkheaders c
order by c.docdate desc limit 1)
union all
(select 'total results'  "table", c.docdate  "date"
from marts.mart_total_results_by_store  c
order by c.docdate desc limit 1)
union all
(select 'mart_assortiment_ce'  "table", c.docdate  "date"
from marts.mart_assortiment_ce c
order by c.docdate desc limit 1)
union all
(select 'margin_project'  "table", c."Дата" "date"
from marts.mart_margin_projects c
order by c."Дата"  desc limit 1)
union all
(select 'delivery_goods'  "table", c."Дата оформления"::date "date"
from marts.mart_delivery_goods c
order by c."Дата оформления"::date  desc limit 1)
union all
(select 'sales_db'  "table", c.docdate  "date"
from dds.sales_db c
order by c.docdate  desc limit 1)
union all
(select 'mart_orders'  "table", c."Дата"::date  "date"
from marts.mart_orders c
order by c."Дата"::date  desc limit 1)
;