SELECT
	row_id,
	objid,
	id,
	date,
	value
FROM _1SCONST 
WHERE
    ID = 26218  --Группа аптек по ТО
or  ID = 22956  --МестаХранения - СреднийЧекПлан
or  ID = 22292  --МестаХранения - ВыручкаПлан
or  ID = 24858  --МестаХранения - БонусыПлан
or  ID = 22845  --МестаХранения - ПроцентНаценки
;
