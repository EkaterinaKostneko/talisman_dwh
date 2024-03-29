--DROP IF EXITS VIEW marts.mart_targets_pretty;

CREATE OR REPLACE VIEW marts.mart_targets_pretty
AS
SELECT
    tdate     		"Дата",
    idstore     	"ID аптеки",
    averagebill		"СреднийЧекПлан",
    revenue	    	"ВыручкаПлан",
    points			"БаллыПлан"
FROM marts.mart_targets