drop table marts.mart_targets;

CREATE TABLE marts.mart_targets (
	idstore varchar NULL,
	tdate date NULL,
	points numeric(12, 2) NULL,
	revenue numeric(12, 2) NULL,
	averagebill numeric(12, 2) NULL
);

CREATE TABLE core.targets (
	idstore varchar NULL,
	tdate date NULL,
	points numeric(12, 2) NULL,
	revenue numeric(12, 2) NULL,
	averagebill numeric(12, 2) NULL
);

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



