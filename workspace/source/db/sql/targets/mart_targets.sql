--DROP IF EXITS VIEW marts.mart_targets;

CREATE OR REPLACE VIEW marts.mart_targets
AS
select
tdate               ,
TRIM(ms."ID_Хранение")  idstore,
TRIM(ms."МестоХран")    pharmacy ,
ms."Аптека"             address ,
t.averagebill           ,
t.revenue               ,
(t.markuplast*t.revenue)/(100+t.markuplast) profit    ,
t.points            ,
t.markup            ,
t.revenue/t.averagebill traffic
from core.targets t
join marts.mart_sprav ms
on t.idstore = ms."ID_Хранение"