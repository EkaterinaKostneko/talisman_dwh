--ядро для плановых показателей
truncate core.targets ;

insert into core.targets
(
select
	trim(s.objid) idstore,
	s.date tdate,
	s.id nam,
    value::numeric(12,2) val
from stg_dwh."_1sconst" s
where
	s.id = 22956  --МестаХранения - СреднийЧекПлан
or  s.id = 22292  --МестаХранения - ВыручкаПлан
or  s.id = 24858  --МестаХранения - БонусыПлан
or  s.id = 22845  --МестаХранения - ПроцентНаценки
union all
--для плана по валовой прибыли процент наценки переходит на следующий месяц
select
	trim(s.objid) idstore,
	s.date + interval '1 month' tdate,
	1 nam,
    value::numeric(12,2) val
from stg_dwh."_1sconst" s
where
	s.id = 22845  --МестаХранения - ПроцентНаценки
)
;