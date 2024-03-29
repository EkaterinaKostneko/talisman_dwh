--ядро для плановых показателей
truncate core.targets ;

insert into core.targets
(
select
	trim(s.objid) idstore,
	s.date,
 	(CASE
		WHEN id = 22956 THEN value::numeric(12,2)
	END)	averagebill ,
 	(CASE
		WHEN id = 22292 THEN value::numeric(12,2)
	END)	revenue ,
 	(CASE
		WHEN id = 24858 THEN value::numeric(12,2)
	END)	points
from stg_dwh."_1sconst" s
where
	s.id = 22956  --МестаХранения - СреднийЧекПлан
or  s.id = 22292  --МестаХранения - ВыручкаПлан
or  s.id = 24858  --МестаХранения - БонусыПлан
)
