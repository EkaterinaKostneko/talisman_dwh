TRUNCATE TABLE ods.backmargin;

INSERT INTO ods.backmargin
(	
	docdate,
	iddoc,
	product,
	backmargin
)
select distinct
DocJourn.docdate,
Doc.iddoc,
DocTabl.SP22329 as product,
DocTabl.SP23942 as backmargin
from stg_dwh.dh22325_range_lists Doc
left join stg_dwh.dt22325_range_lists DocTabl
on Doc.iddoc = DocTabl.iddoc
left join ods."_1sjourn" DocJourn
on Doc.iddoc = DocJourn.iddoc 
;