select
	DocTabl.iddoc,
	sp21207,
	sp21226,
	sp21209,
	sp25086,
	sp25076,
	sp25077,
	sp25078,
	sp25079,
	sp25080
from
	dt21203 DocTabl
left join _1SJOURN  
on DocTabl.iddoc = _1SJOURN.iddoc 
--where CAST (SUBSTRING(_1SJOURN.DATE_TIME_IDDOC, 1, 8) as date) between cast('{{ AF_INC_BEGIN }}' as DATETIME2) and cast('{{AF_INC_END }}' as DATETIME2);
where CAST (SUBSTRING(_1SJOURN.DATE_TIME_IDDOC, 1, 8) as date) between '2023-10-01' and '2023-10-31';