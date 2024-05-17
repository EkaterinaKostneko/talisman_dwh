drop table marts.mart_targets;

CREATE TABLE marts.mart_targets (
	idstore varchar NULL,
	tdate date NULL,
	points numeric(12, 2)       NULL,
	revenue numeric(12, 2)      NULL,
	averagebill numeric(12, 2)  NULL,
	markup      numeric(12, 2) NULL,
	profit      numeric(12, 2) NULL,
	traffic     numeric(12, 2) NULL
);

CREATE TABLE core.targets (
	idstore     varchar NULL,
	tdate       date NULL,
	nam			int2,
	val    		numeric(12, 2) NULL
);

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



create or replace view core.targets_step1
as
(
SELECT distinct
	idstore ,
	date_trunc('month', tdate) AS mon,
	nam,
    last_value(t.val) OVER (PARTITION BY idstore, nam, date_trunc('month', tdate)
    								ORDER BY idstore, nam, tdate
    								ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    								) AS val_per_month
FROM core.targets t
)

create or replace view core.targets_step2
as
(
WITH r AS (
select
idstore ,
mon,
 	CASE
		WHEN nam = 22956 THEN val_per_month::numeric(12,2)
	END	averagebill ,
 	CASE
		WHEN nam = 22292 THEN val_per_month::numeric(12,2)
	END	revenue ,
 	CASE
		WHEN nam = 24858 THEN val_per_month::numeric(12,2)
	END	points ,
    CASE
        WHEN nam = 22845 THEN val_per_month::numeric(12,2)
    END	markup ,
    CASE
        WHEN nam = 1 THEN val_per_month::numeric(12,2)
    END	markuplast
 from core.targets_step1 t
)
select
idstore,
mon,
sum(averagebill) averagebill,
sum(revenue) revenue,
sum(points) points,
sum(markup) markup,
sum(markuplast) markuplast
from r
group by idstore,mon
order by idstore,mon desc
)

create or replace view core.targets_final
as
select *
from core.targets_step2


select

from
core.targets t
group by
	idstore ,
	tdate


join marts.mart_sprav ms
on trim(s.objid) = ms."ID_Хранение"

CREATE OR REPLACE VIEW marts.mart_targets_pretty
AS
SELECT
    tdate     		"Дата",
    idstore     	"ID аптеки",
    averagebill		"СреднийЧекПлан",
    revenue	    	"ВыручкаПлан",
    points			"БаллыПлан"
FROM marts.mart_targets


CREATE OR REPLACE VIEW marts.mart_targets
AS
select
tdate,
TRIM(ms."ID_Хранение") idstore,
TRIM(ms."МестоХран") pharmacy ,
ms."Аптека" address ,
t.averagebill ,
t.revenue ,
t.points
from core.targets t
join marts.mart_sprav ms
on t.idstore = ms."ID_Хранение"


CREATE OR REPLACE VIEW core.targets_g
as
select
tdate,
idstore,
max(t.averagebill) averagebill,
max(t.revenue) revenue,
max(t.points) points
from core.targets t
group by
tdate,
idstore

drop view marts.mart_targets_g

CREATE OR REPLACE VIEW marts.mart_targets_g
AS
select
tdate,
TRIM(ms."ID_Хранение") idstore,
TRIM(ms."МестоХран") pharmacy ,
ms."Аптека" address ,
t.averagebill ,
t.revenue ,
t.points
from core.targets_g t
join marts.mart_sprav ms
on t.idstore = ms."ID_Хранение"


CREATE OR REPLACE VIEW marts.mart_targets_g_pretty
AS
SELECT
    tdate     		"Дата",
    idstore     	"ID аптеки",
    averagebill		"СреднийЧекПлан",
    revenue	    	"ВыручкаПлан",
    points			"БаллыПлан"
FROM marts.mart_targets_g

CREATE TABLE core.targets (
	idstore     varchar NULL,
	tdate       date NULL,
	nam			int2,
	val    		numeric(12, 2) NULL
);

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

truncate core.targets_step1 ;

insert into core.targets_step1
(
create or replace view core.targets_step1
as
(
SELECT distinct
	idstore ,
	date_trunc('month', tdate) AS month,
	nam,
    last_value(t.val) OVER (PARTITION BY idstore, nam, date_trunc('month', tdate)
    								ORDER BY idstore, nam, tdate desc
    								ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    								) AS nam_per_month
FROM core.targets t
)
;


