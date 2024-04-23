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
	END)	points ,
    (CASE
        WHEN id = 22845 THEN value::numeric(12,2)
    END)	markup
from stg_dwh."_1sconst" s
where
	s.id = 22956  --МестаХранения - СреднийЧекПлан
or  s.id = 22292  --МестаХранения - ВыручкаПлан
or  s.id = 24858  --МестаХранения - БонусыПлан
or  s.id = 22845  --МестаХранения - ПроцентНаценки
);

--добавим план прибыли на следующий месяц
insert into core.targets
(
select
	trim(s.objid) idstore,
	s.date + interval '1 month' date, --процент наценки на следующий месяц для планирования Валовой прибыли
    (CASE
        WHEN id = 22845 THEN value::numeric(12,2)
    END)	markuplast
from stg_dwh."_1sconst" s
where
s.id = 22845  --МестаХранения - ПроцентНаценки
)
