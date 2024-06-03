drop view marts.latest ;

create or replace view marts.latest
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
(select 'mart_assortiment'  "table", c.docdate  "date"
from marts.mart_assortiment  c
order by c.docdate desc limit 1)
union all
(select 'mart_assortiment_ce'  "table", c.docdate  "date"
from marts.mart_assortiment_ce c
order by c.docdate desc limit 1);