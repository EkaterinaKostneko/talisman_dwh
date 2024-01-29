TRUNCATE TABLE marts.mart_backmargin;

INSERT INTO marts.mart_backmargin
(	
	docmonth,
	docyear,
	product,
	backmargin
)
with r as
(
select	
		docdate,
		EXTRACT(MONTH FROM docdate) as DocMonth,
		EXTRACT(YEAR FROM docdate) as DocYear,
		product,
		backmargin
from ods.backmargin b 
where backmargin <> 0
order by product, docyear, docmonth
)
select 
	docmonth,
	docyear,
	product,
	backmargin
 from (
select 
*,
row_number() OVER (PARTITION BY product, docyear, docmonth ORDER BY docdate DESC) AS row_num
from r
) sub
where row_num = 1 
;