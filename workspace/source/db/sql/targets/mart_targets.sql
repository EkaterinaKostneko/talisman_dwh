DROP VIEW marts.mart_targets;

CREATE OR REPLACE VIEW marts.mart_targets
AS
SELECT
mon as tdate,
TRIM(ms."ID_Хранение")  idstore,
TRIM(ms."МестоХран")    pharmacy ,
ms."Аптека"             address ,
t.averagebill           ,
t.revenue               ,
((t.markuplast*t.revenue)/(100+t.markuplast))::numeric(12,2) profit    ,
t.points            ,
t.markup            ,
t.markuplast,
(t.revenue/t.averagebill)::numeric(12,2) traffic
from core.targets_final t
join marts.mart_sprav ms
on t.idstore = ms."ID_Хранение"